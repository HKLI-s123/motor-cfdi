// etl_cfdi_worker.js

const { readFileSync } = require("fs");
const { Pool } = require("pg");
const { XMLParser } = require("fast-xml-parser");

// ==========================
// 🔌 CONEXIÓN A POSTGRES
// ==========================
const pool = new Pool({
  host: "localhost",
  database: "CuentIA",
  user: "postgres",
  password: "admin",
});

// ==========================
// 🧠 CONFIG XML PARSER
// ==========================
const parser = new XMLParser({
  ignoreAttributes: false,
  removeNSPrefix: true, // quita cfdi:, tfd:, pago20:, etc.
  attributeNamePrefix: "", // atributos como "UUID", "Fecha"
});
  
  function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
  
  async function main() {
    console.log(`🚀 Worker iniciado PID=${process.pid}`);
  
    while (true) {
      const client = await pool.connect();
      try {
        await client.query("BEGIN");
  
        // TOMAR UN LOTE SEGURO SIN PISARSE ENTRE WORKERS
        const { rows: files } = await client.query(
          `
          SELECT id, rfc, uuid, fecha_emision, origen, file_path
          FROM cfdi_files
          WHERE procesado = FALSE
          ORDER BY id
          FOR UPDATE SKIP LOCKED
          LIMIT 200;
          `
        );
  
        if (files.length === 0) {
          await client.query("COMMIT");
          console.log(`😴 Worker ${process.pid} sin trabajo, durmiendo...`);
          await sleep(3000);
          continue;
        }
  
        await client.query("COMMIT");
  
        console.log(`📄 Worker ${process.pid} procesando ${files.length} CFDIs`);
  
        // PROCESAMIENTO NORMAL (TU LÓGICA COMPLETA)
        for (const file of files) {
          await procesarArchivoCfdi(client, file); 
        }
  
      } catch (err) {
        console.error(`❌ Error en worker ${process.pid}:`, err.message);
        await client.query("ROLLBACK");
      } finally {
        client.release();
      }
    }
  }

// ==========================
// 🔧 FUNCIONES AUXILIARES
// ==========================

async function procesarArchivoCfdi(client, file) {
   const { id: cfdiFileId, rfc: rfcRelacionado, file_path } = file;
      
   console.log(`\n📂 [${process.pid}] Procesando: ${file_path}`);

      try {
        const xmlContent = readFileSync(file_path, "utf8");
        const json = parser.parse(xmlContent);

        // CFDI root (tanto 3.3 como 4.0 usan cfdi:Comprobante)
        const comprobante = json.Comprobante;
        if (!comprobante) {
          await marcarProcesadoError(client, cfdiFileId, "Sin nodo Comprobante");
          return;
        }

        const emisor = comprobante.Emisor || {};
        const receptor = comprobante.Receptor || {};
        const complementos = comprobante.Complemento || {};

        // ===========================================
        // 🔥 DETECTAR CANCELADOS ANTES DE PROCESAR
        // ===========================================
        const esCancelado =
          file_path.includes("/cancelados/") ||
          file_path.includes("\\cancelados\\");
        
        if (esCancelado) {        
          const uuid = extraerUUID(complementos, xmlContent);
          if (!uuid) {
            await marcarProcesadoError(client, cfdiFileId, "Cancelado sin UUID");
            return;
          }
        
          await client.query("BEGIN");
        
          await procesarCfdiCancelado(client, comprobante, rfcRelacionado, uuid);
        
          await client.query("COMMIT");
        
          // Marcamos como procesado
          await client.query(
            `UPDATE cfdi_files SET procesado = TRUE WHERE id = $1`,
            [cfdiFileId]
          );
        
          console.log(`✔ CFDI cancelado procesado correctamente (UUID: ${uuid})`);
        
          return; // 🔥 IMPORTANTE: NO PROCESAR ENCABEZADO NI CONCEPTOS
        }

        const tipoComprobante = (comprobante.TipoDeComprobante || "").toUpperCase();

        // Determinar movimiento (Ingreso/Egreso/Complemento) relativo al RFC relacionado
        const movimiento = determinarMovimiento(
          tipoComprobante,
          emisor.Rfc,
          receptor.Rfc,
          rfcRelacionado
        );

        // ===========================================
        // 🧮 MANEJO DE NOTAS DE CRÉDITO COMO NEGATIVOS
        // ===========================================
        if (tipoComprobante === "E") {        
          // Negativizamos en el comprobante original ANTES de insertarlo
          comprobante.SubTotal = "-" + String(parseNumber(comprobante.SubTotal) || 0);
          comprobante.Total    = "-" + String(parseNumber(comprobante.Total) || 0);
          if (comprobante.Descuento) {
            comprobante.Descuento = "-" + String(parseNumber(comprobante.Descuento) || 0);
          }
        
          // También negativizamos impuestos
          if (comprobante.Impuestos) {
            const imp = comprobante.Impuestos;
            if (imp.TotalImpuestosTrasladados)
              imp.TotalImpuestosTrasladados = "-" + String(parseNumber(imp.TotalImpuestosTrasladados));
            if (imp.TotalImpuestosRetenidos)
              imp.TotalImpuestosRetenidos = "-" + String(parseNumber(imp.TotalImpuestosRetenidos));
          }
        
          // Los conceptos también deben reflejar negativo
          if (comprobante.Conceptos?.Concepto) {
            let conceptos = comprobante.Conceptos.Concepto;
            if (!Array.isArray(conceptos)) conceptos = [conceptos];
        
            for (const c of conceptos) {
              c.Importe = "-" + String(parseNumber(c.Importe) || 0);
              c.ValorUnitario = "-" + String(parseNumber(c.ValorUnitario) || 0);
              if (c.Descuento) c.Descuento = "-" + String(parseNumber(c.Descuento) || 0);
        
              // Impuestos dentro del concepto
              if (c.Impuestos?.Traslados?.Traslado) {
                let tr = c.Impuestos.Traslados.Traslado;
                if (!Array.isArray(tr)) tr = [tr];
                for (const t of tr) {
                  t.Importe = "-" + String(parseNumber(t.Importe) || 0);
                  t.Base = "-" + String(parseNumber(t.Base) || 0);

                }
              }
            }
        
            comprobante.Conceptos.Concepto = conceptos;
          }
        }

        // Transacción por CFDI: encabezado + conceptos + pagos/notas
        await client.query("BEGIN");

        // 1) Insertar en cfdis (encabezado)
        await insertarCfdiEncabezado({
          client,
          comprobante,
          emisor,
          receptor,
          rfcRelacionado,
          movimiento,
          fuente: file.origen,
        });

        const uuid = extraerUUID(complementos, xmlContent);
        if (!uuid) {
          await client.query("ROLLBACK");
          await marcarProcesadoError(client, cfdiFileId, "Sin UUID en el XML");
          return;
        }

        // 2) Insertar conceptos
        await insertarConceptos({
          client,
          comprobante,
          uuid,
          rfcRelacionado,
          movimiento,
        });

        // 3) Si es nota de crédito (E), intentar registrar en notas_credito_cfdi
        if (tipoComprobante === "E") {
          await insertarNotaCredito({
            client,
            comprobante,
            emisor,
            receptor,
            uuid,
            rfcRelacionado,
          });
        }

        // 4) Si es complemento de pago (P), registrar en pagos_cfdi
        if (tipoComprobante === "P") {
          await insertarPagosDesdeComplemento({
            client,
            comprobante,
            complementos,
            emisor,
            receptor,
            uuidComplemento: uuid,
            rfcRelacionado,
          });
        }

        // Marcar el cfdi_file como procesado
        await client.query(
          `UPDATE cfdi_files SET procesado = TRUE WHERE id = $1`,
          [cfdiFileId]
        );

        await client.query("COMMIT");
        console.log(`✅ Worker ${process.pid} procesó UUID: ${uuid}`);

      } catch (err) {
    console.error(`❌ ETL Error en PID ${process.pid}:`, err.message);
    await client.query("ROLLBACK");
    await marcarProcesadoError(client, cfdiFileId, err.message);
  }
}


async function procesarCfdiCancelado(client, comprobante, rfcRelacionado, uuid) {
  
  const tipo = (comprobante.TipoDeComprobante || "").toUpperCase();
  const emisor = comprobante.Emisor?.Rfc || "";
  const receptor = comprobante.Receptor?.Rfc || "";

  console.log(`⚠ Procesando CANCELADO tipo ${tipo} (UUID ${uuid})`);

  // 1️⃣ CANCELADO ES COMPLEMENTO DE PAGO
  if (tipo === "P") {
    console.log("🔄 Cancelando complemento de pago…");

    await client.query(
      `
      UPDATE pagos_cfdi
      SET estatus = 'Cancelado'
      WHERE uuid_complemento = $1 AND rfc_relacionado = $2
      `,
      [uuid, rfcRelacionado]
    );

    console.log("✔ Pago cancelado correctamente.");
    return;
  }

  // 2️⃣ CANCELADO ES NOTA DE CRÉDITO
  if (tipo === "E") {
    console.log("🔄 Cancelando nota de crédito…");

    await client.query(
      `
      UPDATE notas_credito_cfdi
      SET estatus = 'Cancelado'
      WHERE uuid_nota = $1 AND rfc_relacionado = $2
      `,
      [uuid, rfcRelacionado]
    );

    console.log("✔ Nota de crédito cancelada.");
    return;
  }

  // 3️⃣ CANCELADO ES INGRESO / EGRESO
  console.log("🔄 Cancelando CFDI normal…");
  await client.query(
    `
    UPDATE cfdis
    SET status = 'Cancelado', categoria = 'cancelado'
    WHERE uuid = $1 AND rfc_relacionado = $2
    `,
    [uuid, rfcRelacionado]
  );

  console.log("✔ CFDI de ingreso/egreso cancelado.");
}


function determinarMovimiento(tipoComprobante, rfcEmisor = "", rfcReceptor = "", rfcRelacionado = "") {
  const emisor = (rfcEmisor || "").toUpperCase();
  const receptor = (rfcReceptor || "").toUpperCase();
  const owner = (rfcRelacionado || "").toUpperCase();

  if (tipoComprobante === "P") {
    return "Complemento Pago";
  }

  // 🟠 Nómina (CFDI Tipo N)
  if (tipoComprobante === "N") {
    // Si YO soy el empleador → EGRESO
    if (emisor === owner) {
      return "Egreso";
    }

    // Si YO soy el empleado → Ingreso de sueldo (NO comercial)
    if (receptor === owner) {
      return "Ingreso";
    }

    // Caso extraño: nómina ajena
    return "Nómina Desconocida";
  }

  if (emisor === owner && receptor !== owner) {
    return "Ingreso";
  }

  if (receptor === owner && emisor !== owner) {
    return "Egreso";
  }

  // Caso raro: ni emisor ni receptor son el owner (CFDI ajeno)
  return "Desconocido";
}

function extraerUUID(complementos, xmlText) {
  // Intentar desde TimbreFiscalDigital en Complemento
  try {
    const timbre =
      complementos.TimbreFiscalDigital ||
      (complementos.Complemento && complementos.Complemento.TimbreFiscalDigital);
    if (timbre && timbre.UUID) return timbre.UUID;
  } catch (_) {}

  // Respaldo: regex directa
  const match = xmlText.match(/UUID="([^"]+)"/);
  return match ? match[1] : null;
}

async function marcarProcesadoError(client, cfdiFileId, errorMessage) {
  // Puedes crear un campo error_message en cfdi_files si quieres guardar el detalle
  console.log(`⚠ Marcando cfdi_files.id=${cfdiFileId} como procesado con error: ${errorMessage}`);
  await client.query(`UPDATE cfdi_files SET procesado = TRUE WHERE id = $1`, [cfdiFileId]);
}

// ==========================
// 🧱 INSERT: CFDI ENCABEZADO
// ==========================
async function insertarCfdiEncabezado({
  client,
  comprobante,
  emisor,
  receptor,
  rfcRelacionado,
  movimiento,
  fuente,
}) {
  const fechaStr = comprobante.Fecha || null;
  const fecha = fechaStr ? new Date(fechaStr) : null;

  const subtotal = parseNumber(comprobante.SubTotal);
  const descuento = parseNumber(comprobante.Descuento);
  const total = parseNumber(comprobante.Total);
  const moneda = comprobante.Moneda || "";
  const tipoCambio = parseNumber(comprobante.TipoCambio);

  const tipoComprobante = (comprobante.TipoDeComprobante || "").toUpperCase();
  const serie = comprobante.Serie || "";
  const folio = comprobante.Folio || "";
  const lugarExpedicion = comprobante.LugarExpedicion || "";
  const metodoPago = comprobante.MetodoPago || "";
  const formaPago = comprobante.FormaPago || ""; // lo usaremos para TipoPago

  const usoCFDI = receptor.UsoCFDI || "";
  const regimenEmisor = emisor.RegimenFiscal || "";
  const regimenReceptor = receptor.RegimenFiscalReceptor || "";
  const rfcEmisor = emisor.Rfc || "";
  const rfcReceptor = receptor.Rfc || "";
  const razonEmisor = emisor.Nombre || "";
  const razonReceptor = receptor.Nombre || "";

  // Status por defecto (después puedes sincronizar con cancelados)
  const status = "Vigente";
  const categoria = ""; // lo podrás alimentar después
  const now = new Date();

  // Totales de impuestos: por ahora básicos, puedes refinar con concepto-impuesto
  const { totalesImpuestos } = calcularImpuestosPorConcepto(comprobante);

  // rfc_cliente: contraparte
  const rfcCliente =
    movimiento === "Ingreso"
      ? rfcReceptor
      : movimiento === "Egreso"
      ? rfcEmisor
      : "";

  // UUID aún no lo tenemos aquí (se inserta luego con UPDATE si quieres),
  // pero es mejor insertarlo directo aquí, así que devolvemos el UUID arriba y aquí solo asumimos que se llamará luego.
  // Para simplificar, vamos a extraer también el UUID rápido desde Timbre si está:
  let uuid = null;
  try {
    const complemento = comprobante.Complemento || {};
    const timbre = complemento.TimbreFiscalDigital;
    if (timbre && timbre.UUID) uuid = timbre.UUID;
  } catch (_) {}

  // Si no se pudo, dejamos uuid vacío, pero idealmente debe llenarse
  uuid = uuid || "";

  const query = `
    INSERT INTO cfdis (
      uuid,
      version,
      rfc_emisor,
      razonsocialemisor,
      rfc_receptor,
      razonsocialreceptor,
      fecha,
      tipocomprobante,
      serie,
      folio,
      status,
      metodopago,
      tipopago,
      regimenfiscal,
      lugarexpedicion,
      subtotal,
      descuento,
      total,
      totalretenidoiva,
      totalretenidoieps,
      totalretenidoisr,
      totalretenidos,
      totaltrasladosiva,
      totaltrasladoieps,
      totaltraslado,
      totaltrasladoivadieciseis,
      totaltrasladoivaexento,
      totaltrasladoivacero,
      totaltrasladoivaocho,
      baseiva0,
      baseiva8,
      baseiva16,
      baseivaexento,
      usocfdi,
      moneda,
      movimiento,
      fechaprocesada,
      regimenfiscalreceptor,
      rfc_cliente,
      fuente,
      rfc_relacionado,
      categoria,
      tipocambio
    )
    VALUES (
      $1, $2, $3, $4, $5, $6, $7,
      $8, $9, $10, $11, $12, $13,
      $14, $15, $16, $17, $18, 
      $19, $20, $21, $22, $23, $24, $25,
      $26, $27, $28, $29, $30, $31, $32, $33,
      $34, $35, $36, $37, $38, $39, $40, $41, $42, $43
    )
    ON CONFLICT (uuid) DO NOTHING
  `;

  const values = [
    uuid,
    comprobante.Version || "",
    rfcEmisor,
    razonEmisor,
    rfcReceptor,
    razonReceptor,
    fecha,
    tipoComprobante,
    serie,
    folio,
    status,
    metodoPago,
    formaPago,
    regimenEmisor,
    lugarExpedicion,
    subtotal,
    descuento,
    total,
    totalesImpuestos.TotalRetenidoIVA,
    totalesImpuestos.TotalRetenidoIEPS,
    totalesImpuestos.TotalRetenidoISR,
    totalesImpuestos.TotalRetenidos,
    totalesImpuestos.TotalTrasladadoIVA,
    totalesImpuestos.TotalTrasladadoIEPS,
    totalesImpuestos.TotalTraslado,
    totalesImpuestos.TotalTrasladadoIVADieciseis,
    totalesImpuestos.TotalTrasladadoIVAExento,
    totalesImpuestos.TotalTrasladadoIVACero,
    totalesImpuestos.TotalTrasladadoIVAOcho,
    totalesImpuestos.BaseIVA0,
    totalesImpuestos.BaseIVA8,
    totalesImpuestos.BaseIVA16,
    totalesImpuestos.BaseIVAExento,
    usoCFDI,
    moneda,
    movimiento,
    now,
    regimenReceptor,
    rfcCliente,
    fuente || "",
    rfcRelacionado,
    categoria,
    tipoCambio,
  ];

  await client.query(query, values);
}

// ==========================
// 🧮 IMPUESTOS POR CONCEPTO
// ==========================
function calcularImpuestosPorConcepto(comprobante) {
  const conceptosNode = comprobante.Conceptos || {};
  let conceptos = conceptosNode.Concepto || [];
  if (!Array.isArray(conceptos)) conceptos = [conceptos];

  const totals = {
    TotalRetenidoIVA: 0,
    TotalRetenidoIEPS: 0,
    TotalRetenidoISR: 0,
    TotalRetenidos: 0,
    TotalTrasladadoIVA: 0,
    TotalTrasladadoIEPS: 0,
    TotalTraslado: 0,
    TotalTrasladadoIVADieciseis: 0,
    TotalTrasladadoIVAExento: 0,
    TotalTrasladadoIVACero: 0,
    TotalTrasladadoIVAOcho: 0,
    BaseIVA0: 0,
    BaseIVA8: 0,
    BaseIVA16: 0,
    BaseIVAExento: 0,
  };

  for (const c of conceptos) {
    const impuestos = c.Impuestos || {};
    const trasladosNode = impuestos.Traslados || {};
    const retencionesNode = impuestos.Retenciones || {};

    let traslados = trasladosNode.Traslado || [];
    if (!Array.isArray(traslados)) traslados = traslados ? [traslados] : [];

    let retenciones = retencionesNode.Retencion || [];
    if (!Array.isArray(retenciones)) retenciones = retenciones ? [retenciones] : [];

    for (const t of traslados) {
      const impuesto = t.Impuesto;
      const tasa = parseNumber(t.TasaOCuota);
      const base = parseNumber(t.Base);
      const importe = parseNumber(t.Importe);
      const tipoFactor = (t.TipoFactor || "").toUpperCase();

      if (impuesto === "002") {
        // IVA
        totals.TotalTrasladadoIVA += importe;
        totals.TotalTraslado += importe;

        if (tipoFactor === "EXENTO") {
          totals.BaseIVAExento += base;
        } else if (tasa >= 0.1599 && tasa <= 0.1601) {
          totals.BaseIVA16 += base;
          totals.TotalTrasladadoIVADieciseis += importe;
        } else if (tasa >= 0.0799 && tasa <= 0.0801) {
          totals.BaseIVA8 += base;
          totals.TotalTrasladadoIVAOcho += importe;
        } else if (tasa === 0) {
          totals.BaseIVA0 += base;
          totals.TotalTrasladadoIVACero += importe;
        }
      } else if (impuesto === "003") {
        // IEPS
        totals.TotalTrasladadoIEPS += importe;
        totals.TotalTraslado += importe;
      }
    }

    for (const r of retenciones) {
      const impuesto = r.Impuesto;
      const importe = parseNumber(r.Importe);

      if (impuesto === "002") {
        totals.TotalRetenidoIVA += importe;
      } else if (impuesto === "001") {
        totals.TotalRetenidoISR += importe;
      } else if (impuesto === "003") {
        totals.TotalRetenidoIEPS += importe;
      }
      totals.TotalRetenidos += importe;
    }
  }

  return { totalesImpuestos: totals };
}

// ==========================
// 🧱 INSERT: CONCEPTOS
// ==========================
async function insertarConceptos({ client, comprobante, uuid, rfcRelacionado, movimiento }) {
  const conceptosNode = comprobante.Conceptos || {};
  let conceptos = conceptosNode.Concepto || [];

  if (!Array.isArray(conceptos)) conceptos = [conceptos];

  const fechaStr = comprobante.Fecha || null;
  const fecha = fechaStr ? fechaStr.substring(0, 10) : null;

  const query = `
    INSERT INTO conceptos_cfdis (
      claveproductoservicio,
      cantidad,
      claveunidad,
      unidad,
      descripcion,
      valorunitario,
      importe,
      descuento,
      uuid_relacionado,
      rfc_relacionado,
      fecha,
      movimiento
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
    )
  `;

  for (const c of conceptos) {
    const values = [
      c.ClaveProdServ || "",
      parseNumber(c.Cantidad),
      c.ClaveUnidad || "",
      c.Unidad || "",
      c.Descripcion || "",
      parseNumber(c.ValorUnitario),
      parseNumber(c.Importe),
      c.Descuento != null ? parseNumber(c.Descuento) : null,
      uuid,
      rfcRelacionado,
      fecha,
      movimiento.substring(0, 10),
    ];

    await client.query(query, values);
  }
}

// ==========================
// 🧱 INSERT: NOTAS DE CRÉDITO
// ==========================
async function insertarNotaCredito({ client, comprobante, emisor, receptor, uuid, rfcRelacionado }) {
  const fechaStr = comprobante.Fecha || null;
  const fechaEmision = fechaStr ? new Date(fechaStr) : null;

  const subtotal = parseNumber(comprobante.SubTotal);
  const total = parseNumber(comprobante.Total);
  const moneda = comprobante.Moneda || "";
  const tipoCambio = parseNumber(comprobante.TipoCambio);
  const formaPago = comprobante.FormaPago || "";
  const metodoPago = comprobante.MetodoPago || "";

  const { totalesImpuestos } = calcularImpuestosPorConcepto(comprobante);

  // Factura relacionada (si existe CfdiRelacionados)
  let uuidFacturaRel = null;
  const relacionados = comprobante.CfdiRelacionados || comprobante["CfdiRelacionados"];
  if (relacionados) {
    const rel = relacionados.CfdiRelacionado;
    if (rel) {
      if (Array.isArray(rel)) {
        uuidFacturaRel = rel[0].UUID || null;
      } else {
        uuidFacturaRel = rel.UUID || null;
      }
    }
  }

  const query = `
    INSERT INTO notas_credito_cfdi (
      uuid_nota,
      uuid_factura_relacionada,
      fecha_emision,
      rfc_emisor,
      nombre_emisor,
      regimen_emisor,
      rfc_receptor,
      nombre_receptor,
      regimen_receptor,
      subtotal,
      iva_8,
      iva_16,
      total_trasladados,
      retencion_isr,
      retencion_iva,
      total_retenidos,
      descuento,
      total,
      forma_pago,
      moneda,
      tipo_cambio,
      tipo_comprobante,
      metodo_pago,
      rfc_relacionado,
      estatus
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,
      $10,$11,$12,$13,$14,$15,$16,$17,$18,
      $19,$20,$21,$22,$23,$24,$25
    )
    ON CONFLICT (uuid_nota) DO NOTHING
  `;

  const values = [
    uuid,
    uuidFacturaRel,
    fechaEmision,
    emisor.Rfc || "",
    emisor.Nombre || "",
    emisor.RegimenFiscal || "",
    receptor.Rfc || "",
    receptor.Nombre || "",
    receptor.RegimenFiscalReceptor || "",
    subtotal,
    totalesImpuestos.TotalTrasladadoIVAOcho,
    totalesImpuestos.TotalTrasladadoIVADieciseis,
    totalesImpuestos.TotalTraslado,
    totalesImpuestos.TotalRetenidoISR,
    totalesImpuestos.TotalRetenidoIVA,
    totalesImpuestos.TotalRetenidos,
    parseNumber(comprobante.Descuento),
    total,
    formaPago,
    moneda,
    tipoCambio,
    (comprobante.TipoDeComprobante || "").toUpperCase(),
    metodoPago,
    rfcRelacionado,
    "Vigente",
  ];

  await client.query(query, values);
}

// ==========================
// 🧱 INSERT: PAGOS CFDI
// ==========================
async function insertarPagosDesdeComplemento({
  client,
  comprobante,
  complementos,
  emisor,
  receptor,
  uuidComplemento,
  rfcRelacionado,
}) {
  // pago10 / pago20 / Pagos genéricos después de removeNSPrefix
  const pagosRoot =
    complementos.Pagos ||
    complementos.Pago10 ||
    complementos.Pago20 ||
    complementos.pago10 ||
    complementos.pago20 ||
    null;

  if (!pagosRoot) {
    console.warn("⚠ No se encontró nodo Pagos en complemento, saltando pagos_cfdi.");
    return;
  }

  let pagos = pagosRoot.Pago || [];
  if (!Array.isArray(pagos)) pagos = [pagos];

  const fechaEmisionStr = comprobante.Fecha || null;
  const fechaEmision = fechaEmisionStr ? new Date(fechaEmisionStr) : null;

  const query = `
    INSERT INTO pagos_cfdi (
      fecha_emision,
      uuid_complemento,
      rfc_emisor,
      nombre_emisor,
      regimen_emisor,
      rfc_receptor,
      nombre_receptor,
      regimen_receptor,
      fecha_pago,
      forma_pago,
      moneda_pago,
      tipo_cambio_pago,
      monto,
      no_operacion,
      rfc_cta_ordenante,
      banco_ordenante,
      cta_ordenante,
      rfc_cta_beneficiario,
      cta_beneficiario,
      uuid_factura,
      serie,
      folio,
      moneda_dr,
      equivalencia_dr,
      num_parcialidad,
      imp_saldo_ant,
      imp_pagado,
      imp_saldo_insoluto,
      objeto_imp_dr,
      metodo_pago_dr,
      fecha_factura,
      forma_pago_factura,
      condiciones_pago,
      subtotal,
      descuento,
      moneda,
      tipo_cambio,
      total,
      tipo_comprobante,
      metodo_pago,
      exportacion,
      total_imp_trasladados,
      total_imp_retenidos,
      base_16,
      importe_trasladado_16,
      tipo_factor_16,
      tasa_cuota_16,
      impuesto_retenido,
      importe_retenido,
      base_8,
      importe_trasladado_8,
      tipo_factor_8,
      tasa_cuota_8,
      base_exento,
      impuesto_exento,
      tipo_exento,
      rfc_relacionado
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
      $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
      $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
      $31,$32,$33,$34,$35,$36,$37,$38,$39,$40,
      $41,$42,$43,$44,$45,$46,$47,$48,$49,$50,
      $51,$52,$53,$54,$55,$56,$57
    )
  `;

  for (const pago of pagos) {
    const fechaPago = pago.FechaPago ? new Date(pago.FechaPago) : null;
    const formaPagoP = pago.FormaDePagoP || "";
    const monedaP = pago.MonedaP || "";
    const tipoCambioP = parseNumber(pago.TipoCambioP);
    const monto = parseNumber(pago.Monto);
    const numOperacion = pago.NumOperacion || "";
    const rfcCtaOrd = pago.RfcEmisorCtaOrd || "";
    const nomBancoOrd = pago.NomBancoOrdExt || "";
    const ctaOrd = pago.CtaOrdenante || "";
    const rfcCtaBen = pago.RfcEmisorCtaBen || "";
    const ctaBen = pago.CtaBeneficiario || "";

    let doctos = pago.DoctoRelacionado || [];
    if (!Array.isArray(doctos)) doctos = [doctos];

    if (doctos.length === 0) {
      // Pago sin documento relacionado (raro pero posible)
      await client.query(query, [
        fechaEmision,
        uuidComplemento,
        emisor.Rfc || "",
        emisor.Nombre || "",
        emisor.RegimenFiscal || "",
        receptor.Rfc || "",
        receptor.Nombre || "",
        receptor.RegimenFiscalReceptor || "",
        fechaPago,
        formaPagoP,
        monedaP,
        tipoCambioP,
        monto,
        numOperacion,
        rfcCtaOrd,
        nomBancoOrd,
        ctaOrd,
        rfcCtaBen,
        ctaBen,
        null, // uuid_factura
        null, // serie
        null, // folio
        null, // moneda_dr
        null, // equivalencia_dr
        null, // num_parcialidad
        null, // imp_saldo_ant
        null, // imp_pagado
        null, // imp_saldo_insoluto
        null, // objeto_imp_dr
        null, // metodo_pago_dr
        null, // fecha_factura
        null, // forma_pago_factura
        null, // condiciones_pago
        null, // subtotal
        null, // descuento
        null, // moneda
        null, // tipo_cambio
        null, // total
        "P",
        "", // metodo_pago (factura)
        "", // exportacion
        null, // total_imp_trasladados
        null, // total_imp_retenidos
        null, // base_16
        null, // importe_trasladado_16
        null, // tipo_factor_16
        null, // tasa_cuota_16
        null, // impuesto_retenido
        null, // importe_retenido
        null, // base_8
        null, // importe_trasladado_8
        null, // tipo_factor_8
        null, // tasa_cuota_8
        null, // base_exento
        null, // impuesto_exento
        null, // tipo_exento
        rfcRelacionado,
      ]);
      continue;
    }

    // Un registro por cada documento relacionado
    for (const dr of doctos) {
      const uuidFactura = dr.IdDocumento || null;
      const serie = dr.Serie || "";
      const folio = dr.Folio || "";
      const monedaDR = dr.MonedaDR || "";
      const equivalenciaDR = parseNumber(dr.EquivalenciaDR);
      const numParcialidad = dr.NumParcialidad ? parseInt(dr.NumParcialidad) : null;
      const impSaldoAnt = parseNumber(dr.ImpSaldoAnt);
      const impPagado = parseNumber(dr.ImpPagado);
      const impSaldoInsoluto = parseNumber(dr.ImpSaldoInsoluto);
      const objetoImpDR = dr.ObjetoImpDR || "";

      await client.query(query, [
        fechaEmision,
        uuidComplemento,
        emisor.Rfc || "",
        emisor.Nombre || "",
        emisor.RegimenFiscal || "",
        receptor.Rfc || "",
        receptor.Nombre || "",
        receptor.RegimenFiscalReceptor || "",
        fechaPago,
        formaPagoP,
        monedaP,
        tipoCambioP,
        monto,
        numOperacion,
        rfcCtaOrd,
        nomBancoOrd,
        ctaOrd,
        rfcCtaBen,
        ctaBen,
        uuidFactura,
        serie,
        folio,
        monedaDR,
        equivalenciaDR,
        numParcialidad,
        impSaldoAnt,
        impPagado,
        impSaldoInsoluto,
        objetoImpDR,
        null, // metodo_pago_dr (puedes derivarlo después)
        null, // fecha_factura
        null, // forma_pago_factura
        null, // condiciones_pago
        null, // subtotal
        null, // descuento
        null, // moneda
        null, // tipo_cambio
        null, // total
        "P",
        "", // metodo_pago
        "", // exportacion
        null, // total_imp_trasladados
        null, // total_imp_retenidos
        null, // base_16
        null, // importe_trasladado_16
        null, // tipo_factor_16
        null, // tasa_cuota_16
        null, // impuesto_retenido
        null, // importe_retenido
        null, // base_8
        null, // importe_trasladado_8
        null, // tipo_factor_8
        null, // tasa_cuota_8
        null, // base_exento
        null, // impuesto_exento
        null, // tipo_exento
        rfcRelacionado,
      ]);
    }
  }
}

function parseNumber(v) {
  if (v === undefined || v === null || v === "") return 0;
  const n = Number(v);
  return isNaN(n) ? 0 : n;
}

// ==========================
// 🏁 EJECUTAR
// ==========================
main();

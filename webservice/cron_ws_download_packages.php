<?php

require_once "SatClient.php";
require __DIR__ . "/vendor/autoload.php";

use ZipArchive;


//
// ======================================================
// 📌 CONEXIÓN A POSTGRES
// ======================================================
$pdo = new PDO("pgsql:host=localhost;dbname=CuentIA", "postgres", "admin");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);


//
// ======================================================
// 📌 OBTENER SOLICITUDES LISTAS PARA DESCARGA
// ======================================================

function runDownloader(PDO $pdo)
{
    echo "\n▶ Ejecutando DOWNLOADER a las " . date("Y-m-d H:i:s") . "\n";

$sql = "
    SELECT id, rfc, request_id, date_from, date_to, zip_count, tipo
    FROM cfdi_webservice_requests
    WHERE status = 'ready'
";
$requests = $pdo->query($sql)->fetchAll(PDO::FETCH_ASSOC);

if (empty($requests)) {
    echo "No hay solicitudes listas para descargar.\n";
    exit;
}

echo "📦 Descargando paquetes de " . count($requests) . " solicitudes...\n";


//
// ======================================================
// 📌 PROCESAR CADA SOLICITUD LISTA
// ======================================================
foreach ($requests as $req) {

    $id = $req["id"];
    $rfc = $req["rfc"];
    $requestId = $req["request_id"];
    $dateFrom = $req["date_from"];
    $dateTo = $req["date_to"];
    $zipCount = (int) $req["zip_count"];
    $tipo      = $req["tipo"]; // emitidos | recibidos


    echo "\n=============================\n";
    echo "RFC: $rfc ($tipo) | $dateFrom → $dateTo\n";
    echo "=============================\n";

    try {

        $client = new SatClient($rfc);
        $service = $client->getService();

        // Carpeta destino
        $rangeFolder = "{$dateFrom}_{$dateTo}";
        $baseDir = __DIR__ . "/../storage/cfdis/ws/$rfc/$tipo/$rangeFolder";

        $zipDir = "$baseDir/zips";
        $xmlDir = "$baseDir/xml";

        if (!is_dir($zipDir)) mkdir($zipDir, 0777, true);
        if (!is_dir($xmlDir)) mkdir($xmlDir, 0777, true);

        // Obtener paquetes nuevamente
        $verify = $service->verify($requestId);
        $packages = $verify->getPackagesIds();

        if (empty($packages)) {
            throw new Exception("No hay paquetes disponibles.");
        }

        echo "   Paquetes detectados: " . count($packages) . "\n";

        foreach ($packages as $pkgId) {

            echo "⬇️ Descargando paquete $pkgId...\n";

            $download = $service->download($pkgId);

            if (!$download->getStatus()->isAccepted()) {
                echo "❌ Error SAT descargando paquete $pkgId\n";
                echo "   " . $download->getStatus()->getMessage() . "\n";
                continue;
            }

            $zipPath = "$zipDir/$pkgId.zip";

            // Guardar ZIP
            file_put_contents($zipPath, $download->getPackageContent());
            echo "   ✔ ZIP guardado: $zipPath\n";

            // EXTRAER ZIP
            $zip = new ZipArchive();
            if ($zip->open($zipPath) === TRUE) {
                $zip->extractTo($xmlDir);
                $zip->close();
                echo "   ✔ ZIP extraído en $xmlDir\n";
            } else {
                throw new Exception("No se pudo abrir el ZIP $pkgId.zip");
            }
        }

        // Registrar los XML en cfdi_files
        foreach (glob("$xmlDir/*.xml") as $xmlFile) {

            $content = file_get_contents($xmlFile);

            // Expresiones regulares rápidas para extraer UUID y Fecha
            preg_match('/UUID="([^"]+)"/', $content, $uuidMatch);
            preg_match('/Fecha="([^"]+)"/', $content, $fechaMatch);

            $uuid = $uuidMatch[1] ?? null;
            $fecha = isset($fechaMatch[1]) ? substr($fechaMatch[1], 0, 10) : null;

            if (!$uuid) {
                echo "⚠ No se pudo extraer UUID en archivo $xmlFile\n";
                continue;
            }

         // Verificar si existe (uuid + rfc + tipo)
            $stmt = $pdo->prepare("
                SELECT id FROM cfdi_files 
                WHERE uuid = ? AND rfc = ? AND tipo = ?
            ");
            
            $stmt->execute([$uuid, $rfc, $tipo]);

            if ($stmt->rowCount() == 0) {
                $insert = $pdo->prepare("
                    INSERT INTO cfdi_files (rfc, uuid, fecha_emision, origen, file_path, procesado, tipo)
                    VALUES (?, ?, ?, 'webservice', ?, FALSE, ?)
                ");
                $insert->execute([$rfc, $uuid, $fecha, $xmlFile, $tipo]);
                echo "   ➕ Registrado UUID: $uuid ($tipo)\n";
            }
        }

        // Marcar solicitud como descargada
        $stmt = $pdo->prepare("
            UPDATE cfdi_webservice_requests
            SET status = 'downloaded', updated_at = NOW()
            WHERE id = ?
        ");
        $stmt->execute([$id]);

        // Actualizar progreso
        $stmt = $pdo->prepare("
            UPDATE cfdi_webservice_progress
            SET last_completed_to = ?, updated_at = NOW()
            WHERE rfc = ?
        ");
        $stmt->execute([$dateTo, $rfc]);

        echo "✔ Solicitud $id descargada y procesada correctamente.\n";

    } catch (Exception $e) {
        echo "❌ ERROR: " . $e->getMessage() . "\n";

        // Marcamos como error
        $stmt = $pdo->prepare("
            UPDATE cfdi_webservice_requests
            SET status = 'error', error_message = ?, updated_at = NOW()
            WHERE id = ?
        ");
        $stmt->execute([$e->getMessage(), $id]);
    }

    usleep(200000); // descanso pequeño para no saturar SAT
}

echo "\n✔ Ciclo DOWNLOADER finalizado.\n";
}

// ======================================================
// 🔁 LOOP INFINITO PARA PM2
// ======================================================
while (true) {

    echo "\n==============================\n";
    echo "🚀 INICIANDO CICLO DOWNLOADER WS\n";
    echo "==============================\n";

    runDownloader($pdo);

    echo "⏳ Esperando 3 minutos...\n";
    sleep(180); // 3 minutos
}

?>
<?php
declare(strict_types=1);

require __DIR__ . '/vendor/autoload.php';

use PhpCfdi\CfdiSatScraper\SatScraper;
use PhpCfdi\CfdiSatScraper\Sessions\Fiel\FielSessionManager;
use PhpCfdi\Credentials\Credential;
use PhpCfdi\CfdiSatScraper\QueryByFilters;
use PhpCfdi\CfdiSatScraper\ResourceType;
use PhpCfdi\CfdiSatScraper\Filters\DownloadType;
use PhpCfdi\CfdiSatScraper\Filters\Options\StatesVoucherOption;
use PhpCfdi\CfdiSatScraper\SatHttpGateway;
use GuzzleHttp\Client;

// =======================================================
// 🔧 FUNCIÓN PARA LOCALIZAR LA CARPETA DE FIEL POR RFC
// =======================================================
function findFielDirForRfc(string $rfc): ?string
{
    $base = __DIR__ . '/../../api/uploads';

    $candidates = [
        $base . '/clientes/' . $rfc,
        $base . '/own-firma/' . $rfc,
        __DIR__ . '/rfcs/' . $rfc, // fallback por compatibilidad
    ];

    foreach ($candidates as $dir) {
        if (is_dir($dir)) {
            return $dir;
        }
    }

    return null;
}


// =======================================================
// 🔧 FUNCIÓN PARA REGISTRAR XML EN cfdi_files
// =======================================================
function registrarCfdiFile(PDO $pdo, string $rfc, string $uuid, string $xmlPath, string $origen = 'scraper', string $tipo): void
{
    if (!file_exists($xmlPath)) {
        echo "   ⚠️ XML no encontrado en disco: $xmlPath\n";
        return;
    }

    $content = @file_get_contents($xmlPath);
    if ($content === false) {
        echo "   ⚠️ No se pudo leer XML: $xmlPath\n";
        return;
    }

    // Extraer Fecha (YYYY-MM-DD) del atributo Fecha=""
    $fecha = null;
    if (preg_match('/Fecha="([^"]+)"/', $content, $m)) {
        $fechaStr = $m[1]; // ej: 2020-12-18T15:52:59
        $fecha = substr($fechaStr, 0, 10); // 2020-12-18
    }

    // Verificar si ya existe
    $check = $pdo->prepare("SELECT id FROM cfdi_files WHERE uuid = ? AND rfc = ?");
    $check->execute([$uuid, $rfc]);

    if ($check->rowCount() > 0) {
        echo "   🔁 UUID ya registrado (uuid=$uuid, rfc=$rfc), se omite.\n";
        return;
    }

    // Insertar
    $insert = $pdo->prepare("
        INSERT INTO cfdi_files (rfc, uuid, fecha_emision, origen, file_path, procesado, tipo)
        VALUES (?, ?, ?, ?, ?, FALSE,?)
    ");
    $insert->execute([$rfc, $uuid, $fecha, $origen, $xmlPath, $tipo]);

    echo "   ➕ Registrado en cfdi_files (uuid=$uuid, tipo=$tipo)\n";

}


while (true) {

    // Obtener hora México
    $now = new DateTime("now", new DateTimeZone("America/Mexico_City"));
    $hour = intval($now->format("H")); // 00-23

    // Si NO está entre 1am y 6am → dormir hasta la siguiente hora permitida
    if ($hour < 9 || $hour >= 18) {

        echo "⏳ Fuera de horario (actual: {$hour}h). Scraper duerme 5 minutos...\n";
        sleep(300); // 5 minutos
        continue;   // reinicia loop y vuelve a checar hora
    }

    echo "\n=============================\n";
    echo "⏰ Horario válido ({$hour}h). Ejecutando scraper...\n";
    echo "=============================\n\n";

// ================================
// 🔌 CONEXIÓN A POSTGRES
// ================================
$pdo = new PDO("pgsql:host=localhost;dbname=CuentIA", "postgres", "admin");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

echo "🔍 Seleccionando RFC disponible...\n";

$day = intval((new DateTime())->format("d"));
$order = ($day % 2 === 0) ? "ASC" : "DESC";

$pdo->beginTransaction();

$stmt = $pdo->prepare("
    SELECT rfc 
    FROM clientes
    WHERE \"syncPaused\" = FALSE 
      AND \"syncStatus\" = 'activo'
      AND scraper_lock = FALSE
      AND scraper_available = TRUE
      AND (
       scraper_last_run IS NULL
       OR scraper_last_run < CURRENT_DATE
      )
    ORDER BY rfc $order
    FOR UPDATE SKIP LOCKED
    LIMIT 1
");
$stmt->execute();

$rfc = $stmt->fetchColumn();

if (!$rfc) {
    $pdo->commit();
    echo "✔ No quedan RFCs libres. Durmiendo 60s...\n";
    sleep(60);
    continue;    
}

// bloquear
$pdo->prepare("UPDATE clientes SET scraper_lock = TRUE WHERE rfc = ?")
    ->execute([$rfc]);

$pdo->commit();

echo "🔐 RFC tomado por este worker: $rfc\n";

// ================================
// 📅 RANGO DE FECHAS
// ================================

// 🟢 MODO PRINCIPAL: Día anterior (para cron producción)
$tz = new DateTimeZone('America/Mexico_City');
$today = new DateTimeImmutable('today', $tz);

$from = $today->sub(new DateInterval('P5D')); // hace 5 días
$to   = $today->sub(new DateInterval('P1D')); // ayer

// ✅ Marcar límite del scraper para WS
$pdo->prepare("
    UPDATE cfdi_webservice_progress
    SET scraper_first_date = 
        CASE
            WHEN scraper_first_date IS NULL THEN ?
            WHEN ? < scraper_first_date THEN ?
            ELSE scraper_first_date
        END
    WHERE rfc = ?
")->execute([
    $from->format('Y-m-d'),
    $from->format('Y-m-d'),
    $from->format('Y-m-d'),
    $rfc
]);

echo "📌 scraper_first_date fijada/validada → {$from->format('Y-m-d')}\n";

// ================================
// 📅 RANGO PARA CANCELADOS
// ================================
$yearStart = new DateTimeImmutable($today->format('Y') . '-01-01', $tz);
$cancelFrom = $yearStart;
$cancelTo = $today; // HOY

// 🟡 MODO MANUAL (OPCIÓN 2) – deja comentado, lo activas cuando se requiera:
/*
$from = new DateTimeImmutable('2025-11-01', $tz);
$to   = new DateTimeImmutable('2025-11-30', $tz);
*/

// Carpeta base donde se guardarán los XML del SCRAPER
$storageBase = __DIR__ . '/../storage/cfdis/scraper';

$rfcOriginal = $rfc; // ← SIEMPRE EXISTE
$rfc = strtoupper(trim($rfcOriginal));

try {
// ================================
// 🧠 PROCESO PRINCIPAL
// ================================

    $rfcDir = findFielDirForRfc($rfc);

    if (null === $rfcDir) {
        echo "⚠️ No se encontró carpeta con FIEL para $rfc. Se desactiva scraping.\n";
    
        $pdo->prepare("
            UPDATE clientes
            SET scraper_available = FALSE
            WHERE rfc = ?
        ")->execute([$rfc]);

        $pdo->prepare("
              UPDATE clientes 
              SET scraper_lock = FALSE,
                  scraper_last_run = CURRENT_DATE
              WHERE rfc = ?
        ")->execute([$rfc]);
    
        goto UNLOCK;
    }
    
    // Buscar archivos .cer, .key y .txt automáticamente
    $cerFiles = glob($rfcDir . '/*.cer');
    $keyFiles = glob($rfcDir . '/*.key');
    $txtFiles = glob($rfcDir . '/*.txt');
    
    if (count($cerFiles) === 0 || count($keyFiles) === 0 || count($txtFiles) === 0) {
        echo "⚠️ No se encontraron credenciales completas (.cer/.key/.txt) para $rfc, se salta.\n";
        goto UNLOCK;
    }

    echo "\n=====================================\n";
    echo "🔹 Procesando RFC: $rfc\n";
    echo "=====================================\n";

    $cerFile = $cerFiles[0];
    $keyFile = $keyFiles[0];
    $password = trim(file_get_contents($txtFiles[0]));

    echo "🔐 Intentando cargar credenciales para $rfc...\n";

    try {
        // Primer intento normal
        try {
            $credential = Credential::openFiles($cerFile, $keyFile, $password);
            echo "✅ Credenciales cargadas exitosamente.\n";
        } catch (\Throwable $e) {
            // Fallback en caso de 'bad decrypt'
            if (strpos($e->getMessage(), 'bad decrypt') !== false) {
                echo "⚠️ 'bad decrypt' detectado, intentando convertir .key a PKCS#8...\n";

                $rfcDirNorm = str_replace('\\', '/', $rfcDir);
                $keyFileNorm = str_replace('\\', '/', $keyFile);
                $pkcs8Key = $rfcDirNorm . '/' . pathinfo($keyFileNorm, PATHINFO_FILENAME) . '_pkcs8.key';

                $opensslPath = '/usr/bin/openssl'; // Linux

                // Elimina posible BOM en el password
                $password2 = preg_replace('/\x{FEFF}/u', '', $password);

                $cmd = $opensslPath
                    . ' pkcs8 -inform DER -in ' . escapeshellarg($keyFileNorm)
                    . ' -out ' . escapeshellarg($pkcs8Key)
                    . ' -passin pass:' . escapeshellarg($password2)
                    . ' -topk8 -nocrypt';

                exec($cmd, $output, $returnVar);

                if ($returnVar === 0 && file_exists($pkcs8Key)) {
                    echo "✅ Conversión PKCS#8 exitosa, usando $pkcs8Key\n";
                    $keyFile = $pkcs8Key;
                    $credential = Credential::openFiles($cerFile, $keyFile, $password);
                } else {
                    throw new \Exception("❌ No se pudo convertir la .key a PKCS#8. Salida OpenSSL: " . implode("\n", $output));
                }
            } else {
                throw $e;
            }
        }

        echo "🌐 Iniciando sesión con FIEL en SAT para $rfc...\n";

        // Cliente HTTP compatible con SSL del SAT
        $client = new Client([
            'curl' => [CURLOPT_SSL_CIPHER_LIST => 'DEFAULT@SECLEVEL=1'],
        ]);

        $sessionManager = FielSessionManager::create($credential);
        $scraper = new SatScraper($sessionManager, new SatHttpGateway($client));

        echo "✅ Sesión iniciada correctamente.\n";

        // Descripción del rango
        echo "📅 Rango de búsqueda: " . $from->format('Y-m-d') . " → " . $to->format('Y-m-d') . "\n";

        // Directorio base del día para este RFC
        $dayFolder = $from->format('Y-m-d');
        $pendingDir = $storageBase . '/' . $rfc . '/' . $dayFolder;

        if (!is_dir($pendingDir) && !mkdir($pendingDir, 0777, true) && !is_dir($pendingDir)) {
            throw new \RuntimeException(sprintf('No se pudo crear directorio: %s', $pendingDir));
        }

        $downloadTypes = [
            'recibidos' => DownloadType::recibidos(),
            'emitidos'  => DownloadType::emitidos(),
        ];

        // ================================
        // 📥 CFDIs VIGENTES (emitidos / recibidos)
        // ================================
        foreach ($downloadTypes as $typeName => $type) {
            echo "\n📥 Descargando CFDIs $typeName (vigentes) para $rfc...\n";

            $query = new QueryByFilters($from, $to);
            $query->setDownloadType($type);
            $query->setStateVoucher(StatesVoucherOption::vigentes());

            $list = $scraper->listByPeriod($query);
            echo "   📊 Encontrados " . count($list) . " CFDIs $typeName vigentes.\n";

            if (count($list) > 0) {
                $typeDir = $pendingDir . '/' . $typeName;
                if (!is_dir($typeDir) && !mkdir($typeDir, 0777, true) && !is_dir($typeDir)) {
                    throw new \RuntimeException(sprintf('No se pudo crear directorio: %s', $typeDir));
                }

                $downloadedUuids = $scraper
                    ->resourceDownloader(ResourceType::xml(), $list, 20)
                    ->saveTo($typeDir);

                echo "   📥 Descargados " . count($downloadedUuids) . " CFDIs $typeName.\n";

                foreach ($downloadedUuids as $uuid) {
                    $xmlPath = $typeDir . '/' . $uuid . '.xml';
                    echo "      → $xmlPath\n";
                    registrarCfdiFile($pdo, $rfc, $uuid, $xmlPath, 'scraper',$typeName);
                }
            } else {
                echo "   ℹ️ No hay CFDIs $typeName vigentes para descargar.\n";
            }
        }

        // ================================
        // 📥 CFDIs CANCELADOS
        // ================================
        echo "\n📥 Descargando CFDIs cancelados (emitidos + recibidos) para $rfc...\n";

        $canceladosDir = $pendingDir . '/cancelados';
        if (!is_dir($canceladosDir) && !mkdir($canceladosDir, 0777, true) && !is_dir($canceladosDir)) {
            throw new \RuntimeException(sprintf('No se pudo crear directorio: %s', $canceladosDir));
        }

        $totalCancelados = 0;
        $allCanceladosUuids = [];

        foreach ($downloadTypes as $typeName => $type) {
            $query = new QueryByFilters($cancelFrom, $cancelTo);
            $query->setDownloadType($type);
            $query->setStateVoucher(StatesVoucherOption::cancelados());

            $list = $scraper->listByPeriod($query);
            echo "   📊 Encontrados " . count($list) . " CFDIs $typeName cancelados (año actual).\n";
            $totalCancelados += count($list);

            if (count($list) > 0) {
                $downloadedUuids = $scraper
                    ->resourceDownloader(ResourceType::xml(), $list, 20)
                    ->saveTo($canceladosDir);

                $allCanceladosUuids = array_merge($allCanceladosUuids, $downloadedUuids);
            }
        }

        if ($totalCancelados > 0) {
            echo "   📥 Descargados " . count($allCanceladosUuids) . " CFDIs cancelados.\n";
            foreach ($allCanceladosUuids as $uuid) {
                $xmlPath = $canceladosDir . '/' . $uuid . '.xml';
                echo "      → $xmlPath\n";
                registrarCfdiFile($pdo, $rfc, $uuid, $xmlPath, 'scraper', 'cancelado');
            }
        } else {
            echo "   ℹ️ No hay CFDIs cancelados para este rango.\n";
        }

    } catch (\Throwable $e) {
        echo "⚠️ Ocurrió un error para $rfc: " . $e->getMessage() . "\n";
        echo "   🔍 Tipo: " . get_class($e) . "\n";
        echo "   🔍 Archivo: " . $e->getFile() . ":" . $e->getLine() . "\n";

        $previous = $e->getPrevious();
        if ($previous) {
            echo "   🔍 Excepción anterior: " . get_class($previous) . " → " . $previous->getMessage() . "\n";
        }

        echo "   🔍 Stack trace:\n" . $e->getTraceAsString() . "\n";
    }

   } catch (\Throwable $e) {
       echo "⚠️ Error: " . $e->getMessage() . "\n";
   }

UNLOCK:
    
echo "🔓 Liberando lock para RFC $rfc...\n";
$pdo->prepare("UPDATE clientes SET scraper_lock = FALSE WHERE rfc = ?")
->execute([$rfc]);

$pdo->prepare("
    UPDATE clientes 
    SET scraper_lock = FALSE,
        scraper_last_run = CURRENT_DATE
    WHERE rfc = ?
")->execute([$rfc]);

echo "\n🎉 Proceso SCRAPER completado.\n";

sleep(5);
}
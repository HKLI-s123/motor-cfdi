<?php

require_once "SatClient.php";
require __DIR__ . "/vendor/autoload.php";

use PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters;
use PhpCfdi\SatWsDescargaMasiva\Shared\DateTimePeriod;
use PhpCfdi\SatWsDescargaMasiva\Shared\RequestType;
use PhpCfdi\SatWsDescargaMasiva\Shared\DownloadType;
use PhpCfdi\SatWsDescargaMasiva\Shared\DocumentStatus;

//
// ==========================
// 📌 CONEXIÓN A POSTGRES
// ==========================
$pdo = new PDO("pgsql:host=localhost;dbname=CuentIA", "postgres", "admin");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

//
// ==========================
// 📌 FUNCIÓN: obtener o crear progreso
// ==========================
function getOrCreateProgress(PDO $pdo, string $rfc)
{
    $stmt = $pdo->prepare("SELECT * FROM cfdi_webservice_progress WHERE rfc = ?");
    $stmt->execute([$rfc]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);

    if ($row) return $row;

    // Crear nuevo progreso (hace 5 años)
    $startFrom = (new DateTime())->modify("-5 years")->format("Y-m-d");

    $insert = $pdo->prepare("
        INSERT INTO cfdi_webservice_progress (rfc, current_from, current_to, status)
        VALUES (?, ?, ?, 'idle')
        RETURNING *
    ");

    $start = new DateTime($startFrom);
    $end = (clone $start)->modify("+6 days");

    $insert->execute([$rfc, $start->format("Y-m-d"), $end->format("Y-m-d")]);

    return $insert->fetch(PDO::FETCH_ASSOC);
}

function runRequester(PDO $pdo)
{
    echo "\n▶ Ejecutando requester a las " . date("Y-m-d H:i:s") . "\n";
//
// ==========================
// 📌 OBTENER RFCs ACTIVOS
// ==========================
// ==========================
$sql = '
    SELECT c.rfc
    FROM clientes c
    JOIN cfdi_webservice_progress p ON p.rfc = c.rfc
    WHERE c."syncPaused" = FALSE
      AND c."syncStatus" = \'activo\'
      AND p."statusRequests" != \'completed\'
';
$rfcs = $pdo->query($sql)->fetchAll(PDO::FETCH_COLUMN);

echo "🔍 RFCs a procesar: " . count($rfcs) . PHP_EOL;

//
// ==========================
// 📌 PROCESO PRINCIPAL
// ==========================
foreach ($rfcs as $rfc) {
    echo "\n📌 Procesando RFC: $rfc\n";

    try {
        $progress = getOrCreateProgress($pdo, $rfc);

        $currentFrom = new DateTime($progress["current_from"]);
        $currentTo   = new DateTime($progress["current_to"]);

        // ==========================
        // 🔍 Obtener fecha mínima descargada por SCRAPER
        // ==========================
        $stmt = $pdo->prepare("SELECT scraper_first_date FROM cfdi_webservice_progress WHERE rfc = ?");
        $stmt->execute([$rfc]);
        $scraperFirst = $stmt->fetchColumn();
    
        if ($scraperFirst) {
            $scraperFirstDate = new DateTime($scraperFirst);
            echo "📌 scraper_first_date = {$scraperFirstDate->format('Y-m-d')}\n";
        } else {
            $scraperFirstDate = null;
            echo "📌 scraper_first_date vacío → WS descargará desde hace 5 años\n";
        }

        // =========================
        // 🛑 DETENER WS si ya alcanzó la fecha mínima del scraper
        // =========================
        if ($scraperFirstDate && $currentFrom >= $scraperFirstDate) {
            echo "🛑 Deteniendo WS: Ya se alcanzó scraper_first_date ({$scraperFirstDate->format('Y-m-d')})\n";

            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_progress
                SET \"statusRequests\" = 'completed',
                    updated_at = NOW()
                WHERE rfc = ?
            ");
            
            $stmt->execute([$rfc]);

            continue;
        }

        // Antier: límite para evitar duplicar scraper
        $antier = (new DateTime())->modify("-2 days");

        if ($currentFrom > $antier) {
            echo "✔ RFC $rfc ya está completo hasta antier\n";
            continue;
        }

        // Ajustar límite superior: antier y scraper_first_date
        $limitTo = clone $antier;
        
        if ($scraperFirstDate && $scraperFirstDate < $limitTo) {
            $limitTo = $scraperFirstDate;
        }
        
        // Evitar que currentTo pase el límite
        if ($currentTo > $limitTo) {
            $currentTo = clone $limitTo;
        }

        echo "⏳ Rango: {$currentFrom->format('Y-m-d')} → {$currentTo->format('Y-m-d')}\n";

        // Crear cliente WS
        $client = new SatClient($rfc);
        $service = $client->getService();
        
       // ============================
       // 1️⃣ SOLICITUD EMITIDOS
       // ============================
       $paramsEmitidos = QueryParameters::create(
           DateTimePeriod::createFromValues(
               $currentFrom->format("Y-m-d 00:00:00"),
               $currentTo->format("Y-m-d 23:59:59")
           )
       )
       ->withDownloadType(DownloadType::issued())
       ->withRequestType(RequestType::xml())
       // Emitidos permiten vigentes + cancelados, NO filtramos:
       ->withDocumentStatus(DocumentStatus::undefined());
       
       $queryE = $service->query($paramsEmitidos);
       
       if ($queryE->getStatus()->isAccepted()) {
           $requestId = $queryE->getRequestId();
           echo "✅ Solicitud emitidos generada: $requestId\n";
       
           $stmt = $pdo->prepare("
               INSERT INTO cfdi_webservice_requests
               (rfc, date_from, date_to, request_id, tipo, status)
               VALUES (?, ?, ?, ?, 'emitidos', 'pending')
           ");
           $stmt->execute([$rfc, $currentFrom->format("Y-m-d"), $currentTo->format("Y-m-d"), $requestId]);
       } else {
           echo "❌ Error en emitidos: " . $queryE->getStatus()->getMessage() . "\n";
       }
       
       
       
       // ============================
       // 2️⃣ SOLICITUD RECIBIDOS (SOLO VIGENTES)
       // ============================
       $paramsRecibidos = QueryParameters::create(
           DateTimePeriod::createFromValues(
               $currentFrom->format("Y-m-d 00:00:00"),
               $currentTo->format("Y-m-d 23:59:59")
           )
       )
       ->withDownloadType(DownloadType::received())
       ->withRequestType(RequestType::xml())
       // Recibidos NO pueden incluir cancelados
       ->withDocumentStatus(DocumentStatus::active());
       
       $queryR = $service->query($paramsRecibidos);
       
       if ($queryR->getStatus()->isAccepted()) {
       
           $requestId = $queryR->getRequestId();
           echo "✅ Solicitud recibidos generada: $requestId\n";
       
           $stmt = $pdo->prepare("
               INSERT INTO cfdi_webservice_requests
               (rfc, date_from, date_to, request_id, tipo, status)
               VALUES (?, ?, ?, ?, 'recibidos', 'pending')
           ");
       
           $stmt->execute([$rfc, $currentFrom->format("Y-m-d"), $currentTo->format("Y-m-d"), $requestId]);
       
       } else {
       
           $msg = $queryR->getStatus()->getMessage();
       
           if (str_contains($msg, "cancelados")) {
               echo "⚠ SAT bloqueó recibidos porque hay cancelados → se ignoran.\n";
           } else {
               echo "❌ Error en recibidos: $msg\n";
           }
       }

        // Avanzar RANGO: +1 semana
        $newFrom = (clone $currentTo)->modify("+1 day");
        $newTo   = (clone $newFrom)->modify("+6 days");

        $upperLimit = clone $antier;

        if ($scraperFirstDate && $scraperFirstDate < $upperLimit) {
            $upperLimit = $scraperFirstDate;
        }
        
        if ($newTo > $upperLimit) {
            $newTo = clone $upperLimit;
        }
        
        // Si el nuevo rango ya no tiene sentido → detener
        if ($newFrom >= $upperLimit) {
            echo "🛑 WS completado para RFC $rfc (ya alcanzó scraper_first_date)\n";

            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_progress
                SET \"statusRequests\" = 'completed',
                    updated_at = NOW()
                WHERE rfc = ?
            ");
            $stmt->execute([$rfc]);
    
            continue;
        }

        $update = $pdo->prepare("
            UPDATE cfdi_webservice_progress
            SET current_from = ?, current_to = ?, status = 'running', updated_at = NOW()
            WHERE rfc = ?
        ");

        $update->execute([
            $newFrom->format("Y-m-d"),
            $newTo->format("Y-m-d"),
            $rfc
        ]);

        echo "➡ Avanzando progreso: {$newFrom->format('Y-m-d')} → {$newTo->format('Y-m-d')}\n";

    } catch (Exception $e) {
        echo "⚠ Error con RFC $rfc → " . $e->getMessage() . "\n";
    }

    // Evitar saturar
    usleep(300000); // 0.3 segundos
}

echo "✔ Requester finalizado para este ciclo.\n";
}

// ==========================================
// 🔁 LOOP INFINITO PARA PM2
// ==========================================
while (true) {

    echo "\n===============================\n";
    echo "🚀 INICIANDO CICLO REQUESTER WS\n";
    echo "===============================\n";

    runRequester($pdo);

    echo "⏳ Esperando 30 segundos...\n";
    sleep(30); // 10 minutos
}


?>

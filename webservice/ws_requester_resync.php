<?php
declare(strict_types=1);

require_once "SatClient.php";
require __DIR__ . "/vendor/autoload.php";

use PhpCfdi\SatWsDescargaMasiva\Services\Query\QueryParameters;
use PhpCfdi\SatWsDescargaMasiva\Shared\DateTimePeriod;
use PhpCfdi\SatWsDescargaMasiva\Shared\RequestType;
use PhpCfdi\SatWsDescargaMasiva\Shared\DownloadType;
use PhpCfdi\SatWsDescargaMasiva\Shared\DocumentStatus;

// ======================================================
// 🔌 CONEXIÓN A POSTGRES
// ======================================================
$pdo = new PDO(
    "pgsql:host=localhost;dbname=none",
    "none",
    "none",
    [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]
);

echo "\n===============================\n";
echo "🔁 INICIANDO RESYNC WS CFDI\n";
echo "===============================\n\n";

// ======================================================
// 📌 OBTENER RFCs PARA RESYNC
// ======================================================
$pdo->beginTransaction();

$sql = "
SELECT rfc, last_resync_at, resync_days
FROM cfdi_webservice_progress
WHERE
    last_resync_at IS NULL
    OR last_resync_at < NOW() - INTERVAL '15 days'
ORDER BY last_resync_at NULLS FIRST
LIMIT 20
FOR UPDATE SKIP LOCKED
";

$rows = $pdo->query($sql)->fetchAll();

$pdo->commit();


if (empty($rows)) {
    echo "✔ No hay RFCs pendientes de resync.\n";
    exit;
}

echo "🔍 RFCs a resincronizar: " . count($rows) . "\n";

// ======================================================
// 🔁 PROCESO PRINCIPAL
// ======================================================
foreach ($rows as $row) {

    $rfc = $row['rfc'];
    $resyncDays = (int)($row['resync_days'] ?? 90);

    echo "\n---------------------------------\n";
    echo "📌 RESYNC RFC: $rfc\n";
    echo "---------------------------------\n";

    try {

        $generoSolicitudes = false;

        // --------------------------------------------------
        // 📅 DEFINIR RANGO DE RESYNC
        // --------------------------------------------------
        $to = (new DateTime())->modify('-3 days'); // evitar latencia + solape WS diario/scraper
        $from = (clone $to)->modify("-{$resyncDays} days");

        echo "📅 Rango RESYNC: " .
            $from->format('Y-m-d') . " → " .
            $to->format('Y-m-d') . "\n";

        // --------------------------------------------------
        // 🚫 EVITAR DUPLICADOS ACTIVO
        // --------------------------------------------------
        $check = $pdo->prepare("
            SELECT 1
            FROM cfdi_webservice_requests
            WHERE rfc = ?
              AND date_from = ?
              AND date_to = ?
              AND status IN ('pending','in_progress','ready')
            LIMIT 1
        ");
        $check->execute([
            $rfc,
            $from->format('Y-m-d'),
            $to->format('Y-m-d'),
        ]);

        if ($check->fetchColumn()) {
            echo "⚠ Ya existe solicitud activa para este rango. Se omite.\n";
            continue;
        }

        // --------------------------------------------------
        // 🌐 CREAR CLIENTE SAT
        // --------------------------------------------------
        $client = new SatClient($rfc);
        $service = $client->getService();

        // =================================================
        // 1️⃣ SOLICITUD EMITIDOS (vigentes + cancelados)
        // =================================================
        $paramsEmitidos = QueryParameters::create(
            DateTimePeriod::createFromValues(
                $from->format("Y-m-d 00:00:00"),
                $to->format("Y-m-d 23:59:59")
            )
        )
        ->withDownloadType(DownloadType::issued())
        ->withRequestType(RequestType::xml())
        ->withDocumentStatus(DocumentStatus::undefined());

        $queryE = $service->query($paramsEmitidos);

        if ($queryE->getStatus()->isAccepted()) {

            $requestId = $queryE->getRequestId();

            $stmt = $pdo->prepare("
                INSERT INTO cfdi_webservice_requests
                (rfc, date_from, date_to, request_id, tipo, status)
                VALUES (?, ?, ?, ?, 'emitidos', 'pending')
            ");
            $stmt->execute([
                $rfc,
                $from->format('Y-m-d'),
                $to->format('Y-m-d'),
                $requestId
            ]);

            echo "✅ Emitidos RESYNC generado: $requestId\n";
            $generoSolicitudes = true;

        } else {
            echo "❌ Error emitidos: " .
                $queryE->getStatus()->getMessage() . "\n";
        }

        // =================================================
        // 2️⃣ SOLICITUD RECIBIDOS (solo vigentes)
        // =================================================
        $paramsRecibidos = QueryParameters::create(
            DateTimePeriod::createFromValues(
                $from->format("Y-m-d 00:00:00"),
                $to->format("Y-m-d 23:59:59")
            )
        )
        ->withDownloadType(DownloadType::received())
        ->withRequestType(RequestType::xml())
        ->withDocumentStatus(DocumentStatus::active());

        $queryR = $service->query($paramsRecibidos);

        if ($queryR->getStatus()->isAccepted()) {

            $requestId = $queryR->getRequestId();

            $stmt = $pdo->prepare("
                INSERT INTO cfdi_webservice_requests
                (rfc, date_from, date_to, request_id, tipo, status)
                VALUES (?, ?, ?, ?, 'recibidos', 'pending')
            ");
            $stmt->execute([
                $rfc,
                $from->format('Y-m-d'),
                $to->format('Y-m-d'),
                $requestId
            ]);

            echo "✅ Recibidos RESYNC generado: $requestId\n";
            $generoSolicitudes = true;

        } else {

            $msg = $queryR->getStatus()->getMessage();

            if (str_contains(strtolower($msg), 'cancel')) {
                echo "⚠ SAT bloqueó recibidos por cancelados.\n";
            } else {
                echo "❌ Error recibidos: $msg\n";
            }
        }

        // --------------------------------------------------
        // ✅ MARCAR RESYNC
        // --------------------------------------------------
        if ($generoSolicitudes) {
        
            $update = $pdo->prepare("
                UPDATE cfdi_webservice_progress
                SET last_resync_at = NOW()
                WHERE rfc = ?
            ");
            $update->execute([$rfc]);
        
            echo "📌 last_resync_at actualizado.\n";
        
        } else {
        
            echo "⚠ No se generaron solicitudes de resync, no se actualiza last_resync_at.\n";
        }

    } catch (Throwable $e) {

        echo "❌ ERROR RFC $rfc\n";
        echo "   " . $e->getMessage() . "\n";
        echo "   " . $e->getFile() . ":" . $e->getLine() . "\n";

    }

    // respirar SAT 😌
    usleep(300000); // 0.3s
}

echo "\n🎉 RESYNC COMPLETADO.\n";
<?php

require_once "SatClient.php";
require __DIR__ . "/vendor/autoload.php";

//
// ========================================
// 📌 CONEXIÓN A POSTGRES
// ========================================
$pdo = new PDO("pgsql:host=localhost;dbname=CuentIA", "postgres", "admin");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);


//
// ========================================
// 📌 OBTENER SOLICITUDES A VERIFICAR
// ========================================

function runChecker(PDO $pdo)
{
    echo "\n▶ Ejecutando CHECKER a las " . date("Y-m-d H:i:s") . "\n";

$sql = "
    SELECT id, rfc, request_id, date_from, date_to, tipo
    FROM cfdi_webservice_requests
    WHERE status IN ('pending', 'in_progress')
";
$requests = $pdo->query($sql)->fetchAll(PDO::FETCH_ASSOC);

if (empty($requests)) {
    echo "No hay solicitudes pendientes o en proceso.\n";
    exit;
}

echo "Verificando " . count($requests) . " solicitudes...\n";


//
// ========================================
// 📌 PROCESAR SOLICITUD POR SOLICITUD
// ========================================
foreach ($requests as $req) {
    $id = $req["id"];
    $rfc = $req["rfc"];
    $requestId = $req["request_id"];
    $tipo      = $req["tipo"]; // emitidos / recibidos

    echo "\n🔍 Verificando RFC: $rfc — Solicitud $requestId ($tipo)\n";

    try {
        $client = new SatClient($rfc);
        $service = $client->getService();

        $verify = $service->verify($requestId);

        // Error general del SAT
        if (!$verify->getStatus()->isAccepted()) {
            $msg = $verify->getStatus()->getMessage();
            echo "❌ Error SAT: $msg\n";

            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'error', error_message = ?, updated_at = NOW()
                WHERE id = ?
            ");
            $stmt->execute([$msg, $id]);

            continue;
        }

        // Error específico de la solicitud
        if (!$verify->getCodeRequest()->isAccepted()) {
            $msg = $verify->getCodeRequest()->getMessage();
            echo "❌ Solicitud rechazada: $msg\n";

            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'error', error_message = ?, updated_at = NOW()
                WHERE id = ?
            ");
            $stmt->execute([$msg, $id]);

            continue;
        }

        $statusRequest = $verify->getStatusRequest();

        if (!$statusRequest->isFinished()) {
            echo "⏳ Solicitud aún NO está lista — Estado SAT: " . $statusRequest->getMessage() . "\n";

            // Cambiar a in_progress si estaba en pending
            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'in_progress', updated_at = NOW()
                WHERE id = ? AND status = 'pending'
            ");
            $stmt->execute([$id]);

            continue;
        }

        // Si está lista, obtener paquetes
        $packages = $verify->getPackagesIds();
        $zipCount = count($packages);

        echo "✅ Solicitud lista — Paquetes: $zipCount\n";

        // Actualizar registro como listo
        $stmt = $pdo->prepare("
            UPDATE cfdi_webservice_requests
            SET status = 'ready', zip_count = ?, updated_at = NOW()
            WHERE id = ?
        ");
        $stmt->execute([$zipCount, $id]);

        // Guardar paquetes en tabla aparte (si deseas)
        foreach ($packages as $pkgId) {
            echo "   📦 Paquete: $pkgId\n";
        }

    } catch (Exception $e) {
        echo "⚠ Error en procesamiento RFC $rfc → " . $e->getMessage() . "\n";

        $stmt = $pdo->prepare("
            UPDATE cfdi_webservice_requests
            SET status = 'error', error_message = ?, updated_at = NOW()
            WHERE id = ?
        ");
        $stmt->execute([$e->getMessage(), $id]);
    }

    usleep(150000); // descanso de 150ms para no saturar
}

echo "\n✔ Ciclo CHECKER terminado.\n";
}


// ========================================
// 🔁 LOOP INFINITO PARA PM2
// ========================================
while (true) {

    echo "\n==============================\n";
    echo "🚀 INICIANDO CICLO CHECKER WS\n";
    echo "==============================\n";

    runChecker($pdo);

    echo "⏳ Esperando 3 minutos...\n";
    sleep(180); // 3 minutos
}

?>
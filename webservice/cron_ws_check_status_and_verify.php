<?php

require_once "SatClient.php";
require __DIR__ . "/vendor/autoload.php";

//
// ========================================
// 📌 CONEXIÓN A POSTGRES
// ========================================
$pdo = new PDO("pgsql:host=localhost;dbname=none", "none", "none");
$pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);


//
// ========================================
// 📌 OBTENER SOLICITUDES A VERIFICAR
// ========================================

function runChecker(PDO $pdo)
{
    echo "\n▶ Ejecutando CHECKER a las " . date("Y-m-d H:i:s") . "\n";

$sql = "
    SELECT id, rfc, request_id, date_from, date_to, tipo, created_at
    FROM cfdi_webservice_requests
    WHERE status IN ('pending', 'in_progress')
";
$requests = $pdo->query($sql)->fetchAll(PDO::FETCH_ASSOC);

if (empty($requests)) {
    echo "😴 No hay solicitudes pendientes o en proceso.\n";
    return;
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
    echo "📅 Rango WS: {$req['date_from']} → {$req['date_to']}\n";

    try {

        // ⏰ TTL para solicitudes atascadas
        $createdAt = new DateTime($req['created_at'] ?? 'now');
        $now = new DateTime();
        
        $daysDiff = $createdAt->diff($now)->days;
        
        // TTL: 7 días para in_progress / pending
        if ($daysDiff >= 7) {
        
            echo "⛔ Solicitud excedió TTL ({$daysDiff} días). Se marca como ERROR.\n";
        
            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'error',
                    error_message = 'TTL excedido (>7 días sin respuesta SAT)',
                    updated_at = NOW()
                WHERE id = ?
            ");
            $stmt->execute([$id]);
        
            continue;
        }

        $client = new SatClient($rfc);
        $service = $client->getService();

        $verify = $service->verify($requestId);

        // Error general del SAT
        if (!$verify->getStatus()->isAccepted()) {
            $msg = $verify->getStatus()->getMessage();
            echo "⚠ SAT no aceptó verificación (transitorio): $msg\n";
        
            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'pending',
                    error_message = ?,
                    updated_at = NOW()
                WHERE id = ?
            ");
            $stmt->execute([$msg, $id]);
        
            continue;
        }

        if (!$verify->getCodeRequest()->isAccepted()) {
            $msg = $verify->getCodeRequest()->getMessage();
        
            // ✅ CASO FINAL: rango válido pero sin CFDIs
            if (str_contains(strtolower($msg), 'no se encontró la información')) {
        
                echo "⚠ Solicitud FINAL sin CFDIs (SAT confirmó 0 resultados)\n";
        
                $stmt = $pdo->prepare("
                    UPDATE cfdi_webservice_requests
                    SET status = 'empty',
                        error_message = ?,
                        updated_at = NOW()
                    WHERE id = ?
                ");
                $stmt->execute([$msg, $id]);
        
                continue;
            }
        
            // ⏳ Cualquier otro rechazo → transitorio
            echo "⏳ Solicitud aún no aceptada por SAT: $msg\n";
        
            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'pending',
                    error_message = ?,
                    updated_at = NOW()
                WHERE id = ?
            ");
            $stmt->execute([$msg, $id]);
        
            continue;
        }

        $statusRequest = $verify->getStatusRequest();
        $packages = $verify->getPackagesIds();
        
        // 🔹 CASO 1: SAT terminó pero NO generó paquetes (0 resultados)
        if ($statusRequest->isFinished() && empty($packages)) {
        
            echo "⚠ Solicitud FINALIZADA sin paquetes (0 CFDIs)\n";
        
            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'empty',
                    updated_at = NOW()
                WHERE id = ?
            ");
            $stmt->execute([$id]);
        
            continue;
        }
        
        // 🔹 CASO 2: SAT aún procesando
        if (!$statusRequest->isFinished()) {
        
            echo "⏳ SAT procesando — Estado: " . $statusRequest->getMessage() . "\n";
        
            $stmt = $pdo->prepare("
                UPDATE cfdi_webservice_requests
                SET status = 'in_progress', updated_at = NOW()
                WHERE id = ? AND status = 'pending'
            ");
            $stmt->execute([$id]);
        
            continue;
        }
        
        // 🔹 CASO 3: SAT terminó y hay paquetes
        echo "✅ Solicitud lista — Paquetes: " . count($packages) . "\n";
        
        $stmt = $pdo->prepare("
            UPDATE cfdi_webservice_requests
            SET status = 'ready', zip_count = ?, updated_at = NOW()
            WHERE id = ?
        ");
        $stmt->execute([count($packages), $id]);


        // Guardar paquetes en tabla aparte (si deseas)
        foreach ($packages as $pkgId) {
            echo "   📦 Paquete: $pkgId\n";
        }

    } catch (Exception $e) {
        echo "⚠ Error transitorio RFC $rfc → {$e->getMessage()}\n";
       
        $stmt = $pdo->prepare("
            UPDATE cfdi_webservice_requests
            SET status = 'pending',
                error_message = ?,
                updated_at = NOW()
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
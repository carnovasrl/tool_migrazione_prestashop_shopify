<?php
declare(strict_types=1);

/*
 * CRON WEB SAFE (no-timeout)
 * Chiama index.php?action=run a tranche di tempo (max_seconds)
 */

error_reporting(E_ALL);
ini_set('display_errors', '0');

set_time_limit(0);
ignore_user_abort(true);

header('Content-Type: application/json; charset=utf-8');

$LOG_FILE = __DIR__ . '/log.txt';

/* ================= CONFIG ================= */
$CRON_TOKEN = 'dwvwergwergewrgewrfeqrfdbgwfjh';
$TARGET_HOST = 'migration.undomus.com';
$TARGET_IP   = '217.182.89.68';
$PORT = 443;
/* ========================================= */

$maxSeconds = isset($_GET['max_seconds']) ? (int)$_GET['max_seconds'] : 25;
$startedAt = microtime(true);

/* ---------------- TOKEN ---------------- */
$token = (string)($_GET['token'] ?? '');
if ($token === '' || !hash_equals($CRON_TOKEN, $token)) {
    http_response_code(403);
    echo json_encode(['ok' => false, 'error' => 'Forbidden']);
    exit;
}

/* ---------------- PARAMS ---------------- */
$params = [
    'brand_id'          => $_GET['brand_id']          ?? null,
    'product_ids'       => $_GET['product_ids']       ?? '',
    'limit'             => $_GET['limit']             ?? null,
    'offset'            => $_GET['offset']            ?? 0,
    'cursor'            => (int)($_GET['cursor'] ?? 0),
    'sync_variants'     => (int)($_GET['sync_variants']     ?? 1),
    'sync_inventory'    => (int)($_GET['sync_inventory']    ?? 1),
    'sync_translations' => (int)($_GET['sync_translations'] ?? 1),
    'sync_images'       => (int)($_GET['sync_images']       ?? 1),
];

$baseUrl = "https://{$TARGET_HOST}/home/index.php";

logLine($LOG_FILE, "==== CRON START cursor={$params['cursor']} ====");

$loops = 0;
$totalProcessed = 0;
$totalFailed = 0;
$cursor = (int)$params['cursor'];
$last = null;

while (true) {
    $loops++;

    $q = buildQueryParams($params);
    $q['action'] = 'run';
    $q['cursor'] = $cursor;

    $res = httpJson($baseUrl, $q, $TARGET_HOST, $TARGET_IP, $PORT, $LOG_FILE);
    $last = $res;

    if (!$res['ok']) {
        logLine($LOG_FILE, "[ERROR] " . json_encode($res));
        echo json_encode(['ok'=>false,'error'=>'RUN failed','detail'=>$res]);
        exit;
    }

    $totalProcessed += (int)($res['processed'] ?? 0);
    $totalFailed    += (int)($res['failed'] ?? 0);

    $cursorNext = (int)($res['cursor_next'] ?? ($cursor+1));
    $finished   = (bool)($res['finished'] ?? false);

    logLine($LOG_FILE, "[BATCH] cursor={$cursor} -> {$cursorNext} finished=" . ($finished?'true':'false'));

    if ($finished) {
        break;
    }

    $cursor = $cursorNext;

    /* --------- STOP BEFORE TIMEOUT --------- */
    if ((microtime(true) - $startedAt) >= $maxSeconds) {
        echo json_encode([
            'ok' => true,
            'partial' => true,
            'finished' => false,
            'cursor_next' => $cursor,
            'loops' => $loops,
            'total_processed' => $totalProcessed,
            'total_failed' => $totalFailed,
            'last' => $last
        ]);
        exit;
    }
}

logLine($LOG_FILE, "==== CRON END ====");

echo json_encode([
    'ok' => true,
    'finished' => true,
    'cursor_next' => $cursor,
    'loops' => $loops,
    'total_processed' => $totalProcessed,
    'total_failed' => $totalFailed,
    'last' => $last
]);

/* ================= HELPERS ================= */

function buildQueryParams(array $p): array {
    $q = [];
    if ($p['brand_id'] !== null) $q['brand_id'] = $p['brand_id'];
    if ($p['product_ids'] !== '') $q['product_ids'] = $p['product_ids'];
    if ($p['limit'] !== null) $q['limit'] = $p['limit'];
    if ((int)$p['offset'] > 0) $q['offset'] = (int)$p['offset'];

    $q['sync_variants']     = (int)$p['sync_variants'];
    $q['sync_inventory']    = (int)$p['sync_inventory'];
    $q['sync_translations'] = (int)$p['sync_translations'];
    $q['sync_images']       = (int)$p['sync_images'];

    return $q;
}

function httpJson(string $baseUrl, array $params, string $host, string $ip, int $port, string $logFile): array {
    $url = $baseUrl . '?' . http_build_query($params);

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT        => 60,
        CURLOPT_HTTPHEADER     => ['Accept: application/json'],
        CURLOPT_RESOLVE        => ["{$host}:{$port}:{$ip}"],
		CURLOPT_SSL_VERIFYHOST => 0,
CURLOPT_SSL_VERIFYPEER => false,

    ]);

    $raw = curl_exec($ch);
    $err = curl_error($ch);
    $code = (int)curl_getinfo($ch, CURLINFO_HTTP_CODE);
    curl_close($ch);

    if ($raw === false) {
        return ['ok'=>false,'error'=>$err,'http_code'=>$code];
    }

    // ripulisce eventuale HTML prima del JSON
    $pos = strpos($raw, '{');
    if ($pos !== false && $pos > 0) {
        $raw = substr($raw, $pos);
    }

    $json = json_decode($raw, true);
    if (!is_array($json)) {
        logLine($logFile, "[INVALID JSON] {$raw}");
        return ['ok'=>false,'error'=>'Invalid JSON','raw'=>$raw];
    }

    return $json;
}

function logLine(string $file, string $line): void {
    $ts = date('Y-m-d H:i:s');
    file_put_contents($file, "[{$ts}] {$line}\n", FILE_APPEND);
}

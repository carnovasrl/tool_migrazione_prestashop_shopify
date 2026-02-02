<?php
declare(strict_types=1);

/*
 * cron_cli.php (CLI)
 * Richiama index.php?action=run in loop.
 * Log sintetico: una riga per prodotto -> OK <id_ps> | KO <id_ps> <errore>
 */

error_reporting(E_ALL);
ini_set('display_errors', '0');
ini_set('log_errors', '1');
set_time_limit(0);

$LOG_FILE = __DIR__ . '/log.txt';

// === CONFIG HOST/IP (bypass DNS + SSL ok) ===
$HOST = 'migration.undomus.com';
$IP   = '217.182.89.68';
$PORT = 443;

$BASE_URL = "https://{$HOST}/home/index.php";

// ---- args CLI (filtri) ----
$args = parseArgs($argv);

$cursor   = (int)($args['cursor'] ?? 0);
$maxLoops = (int)($args['max_loops'] ?? 200000);
$sleepMs  = (int)($args['sleep_ms'] ?? 150);

for ($i=0; $i<$maxLoops; $i++) {
    $q = buildQuery($args);
    $q['action'] = 'run';
    $q['cursor'] = $cursor;

    $res = httpJson($BASE_URL, $q, $HOST, $IP, $PORT);

    if (!is_array($res) || ($res['ok'] ?? false) !== true) {
        logLine($LOG_FILE, "KO {$cursor} " . oneLine((string)($res['error'] ?? 'RUN failed')));
        exit(1);
    }

    $items = $res['items'] ?? [];
    if (is_array($items)) {
        foreach ($items as $it) {
            if (!is_array($it)) continue;
            $id = (string)($it['id_product'] ?? $it['id'] ?? '?');
            if (($it['ok'] ?? false) === true) {
                logLine($LOG_FILE, "OK {$id}");
            } else {
                $err = (string)($it['error'] ?? 'Unknown error');
                logLine($LOG_FILE, "KO {$id} " . oneLine($err));
            }
        }
    }

    $finished   = (bool)($res['finished'] ?? false);
    $cursorNext = isset($res['cursor_next']) ? (int)$res['cursor_next'] : ($cursor + 1);

    if ($finished) break;

    if ($cursorNext === $cursor) {
        logLine($LOG_FILE, "KO {$cursor} cursor_stuck");
        break;
    }

    $cursor = $cursorNext;

    if ($sleepMs > 0) usleep($sleepMs * 1000);
}

exit(0);

/* ================= helpers ================= */

function buildQuery(array $a): array {
    $q = [];
    if (isset($a['brand_id']) && $a['brand_id'] !== null) $q['brand_id'] = (int)$a['brand_id'];
    if (!empty($a['product_ids'])) $q['product_ids'] = (string)$a['product_ids'];
    if (isset($a['limit']) && $a['limit'] !== null) $q['limit'] = (int)$a['limit'];
    if (!empty($a['offset'])) $q['offset'] = (int)$a['offset'];

    $q['sync_variants']     = (int)($a['sync_variants'] ?? 1);
    $q['sync_inventory']    = (int)($a['sync_inventory'] ?? 1);
    $q['sync_translations'] = (int)($a['sync_translations'] ?? 1);
    $q['sync_images']       = (int)($a['sync_images'] ?? 1);

    return $q;
}

function httpJson(string $baseUrl, array $params, string $host, string $ip, int $port): array {
    $url = $baseUrl . '?' . http_build_query($params);

    $ch = curl_init($url);
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_FOLLOWLOCATION => true,
        CURLOPT_CONNECTTIMEOUT => 10,
        CURLOPT_TIMEOUT        => 120,
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
        return ['ok'=>false,'error'=>"curl: {$err}",'http_code'=>$code];
    }

    $pos = strpos($raw, '{');
    if ($pos !== false && $pos > 0) $raw = substr($raw, $pos);

    $json = json_decode($raw, true);
    if (!is_array($json)) {
        return ['ok'=>false,'error'=>'Invalid JSON','http_code'=>$code,'raw'=>substr($raw,0,2000)];
    }

    return $json;
}

function parseArgs(array $argv): array {
    $out = [
        'brand_id' => null,
        'product_ids' => '',
        'limit' => null,
        'offset' => 0,
        'cursor' => 0,
        'sync_variants' => 1,
        'sync_inventory' => 1,
        'sync_translations' => 1,
        'sync_images' => 1,
        'sleep_ms' => 150,
        'max_loops' => 200000,
    ];

    for ($i=1; $i<count($argv); $i++) {
        $a = $argv[$i];
        if (strncmp($a, '--', 2) !== 0) continue;
        $a = substr($a, 2);

        $k = $a; $v = '1';
        if (strpos($a, '=') !== false) {
            [$k,$v] = explode('=', $a, 2);
        } elseif (isset($argv[$i+1]) && strncmp($argv[$i+1], '--', 2) !== 0) {
            $v = $argv[++$i];
        }

        if (!array_key_exists($k, $out)) continue;

        if (in_array($k, ['offset','cursor','sync_variants','sync_inventory','sync_translations','sync_images','sleep_ms','max_loops'], true)) {
            $out[$k] = (int)$v;
        } elseif (in_array($k, ['brand_id','limit'], true)) {
            $out[$k] = ($v === '' || strtolower((string)$v) === 'null') ? null : (int)$v;
        } else {
            $out[$k] = (string)$v;
        }
    }

    return $out;
}

function logLine(string $file, string $line): void {
    $ts = date('Y-m-d H:i:s');
    file_put_contents($file, "[{$ts}] {$line}
", FILE_APPEND);
}

function oneLine(string $s): string {
    $s = trim(preg_replace('/\s+/', ' ', $s));
    if (strlen($s) > 300) $s = substr($s, 0, 300) . '...';
    return $s;
}

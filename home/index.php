<?php
declare(strict_types=1);

error_reporting(E_ALL);
ini_set('display_errors','0');

require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/src/Prestashop.php';
require __DIR__ . '/src/ShopifyClient.php';

use App\Prestashop;
use App\ShopifyClient;

$config   = require __DIR__ . '/config.php';
$psCfg    = $config['ps'];
$shopCfg  = $config['shopify'];
$invCfg   = $config['inventory'];
$dryRun   = (bool)$config['dry_run'];

$BATCH_SIZE = 1;

function json_out($arr, int $status=200): never {
  http_response_code($status);
  header('Content-Type: application/json; charset=utf-8');
  echo json_encode($arr, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES);
  exit;
}

/* -------- API: init (stateless) -------- */
if (($_GET['action'] ?? '') === 'init') {
  try {
    $ps      = new Prestashop($psCfg);
    $brandId = (isset($_GET['brand_id']) && $_GET['brand_id'] !== '') ? (int)$_GET['brand_id'] : null;
    $productIdsRaw = isset($_GET['product_ids']) ? (string)$_GET['product_ids'] : '';
    $productIds = [];
    if (trim($productIdsRaw) !== '') {
      $parts = preg_split('/[^0-9]+/', trim($productIdsRaw));
      $productIds = array_values(array_unique(array_filter(array_map('intval', $parts), fn($v)=>$v>0)));
    }
    $limit   = (isset($_GET['limit'])    && $_GET['limit']    !== '') ? (int)$_GET['limit']    : null;
    $offset  = (isset($_GET['offset'])   && $_GET['offset']   !== '') ? (int)$_GET['offset']   : 0;

    // totale stimato (facoltativo)
    $total = null;
    // Se ho un elenco di ID, il totale è deterministico e non richiede query di conteggio.
    if ($productIds) {
      $total = count($productIds);
    } else {
      $total = $ps->countProductsFiltered($brandId);
    }
	  
	//  echo $total; exit();
	  
    if ($offset > 0) $total = max(0, $total - $offset);
    if ($limit !== null)  $total = min($total, max(0, $limit));
   

    json_out([
      'ok'        => true,
      'total'     => $total,          // può essere null se non disponibile
      'batchSize' => $BATCH_SIZE,
      'message'   => 'Init ok (stateless)'
    ]);
  } catch (Throwable $e) {
    json_out(['ok'=>false,'error'=>$e->getMessage()], 500);
  }
}

/* -------- API: run (stateless) -------- */
if (($_GET['action'] ?? '') === 'run') {
  try {
    @set_time_limit(0);

    $brandId = (isset($_GET['brand_id']) && $_GET['brand_id'] !== '') ? (int)$_GET['brand_id'] : null;
    $productIdsRaw = isset($_GET['product_ids']) ? (string)$_GET['product_ids'] : '';
    $productIds = [];
    if (trim($productIdsRaw) !== '') {
      $parts = preg_split('/[^0-9]+/', trim($productIdsRaw));
      $productIds = array_values(array_unique(array_filter(array_map('intval', $parts), fn($v)=>$v>0)));
    }
    $limit   = (isset($_GET['limit'])    && $_GET['limit']    !== '') ? (int)$_GET['limit']    : null; // cap massimo totale
    $offset  = (isset($_GET['offset'])   && $_GET['offset']   !== '') ? (int)$_GET['offset']   : 0;    // offset di partenza
    $cursor  = (isset($_GET['cursor'])   && $_GET['cursor']   !== '') ? (int)$_GET['cursor']   : 0;    // avanza lato client

    // opzioni di sincronizzazione (di default: tutto ON)
    $syncVariants     = !isset($_GET['sync_variants'])     || $_GET['sync_variants']     === '1';
    $syncInventory    = !isset($_GET['sync_inventory'])    || $_GET['sync_inventory']    === '1';
    $syncTranslations = !isset($_GET['sync_translations']) || $_GET['sync_translations'] === '1';
    $syncImages       = !isset($_GET['sync_images'])       || $_GET['sync_images']       === '1';

    // Se ho un elenco di ID, offset+cursor sono applicati all'elenco; altrimenti sono applicati al DB.
    $dbOffset = $offset + $cursor;

    // rispetto un limite totale "hard" (se passato)
    $take = $BATCH_SIZE;
    if ($limit !== null) {
      $remaining = max(0, $limit - $cursor);
      if ($remaining <= 0) {
        json_out(['ok'=>true,'items'=>[],'processed'=>0,'failed'=>0,'cursor_next'=>$cursor,'finished'=>true]);
      }
      $take = min($BATCH_SIZE, $remaining);
    }

    $ps = new Prestashop($psCfg);

    // prendo il "pezzo" da PS
    $requestedIds = [];
    if ($productIds) {
      // Applico offset + cursor sull'elenco di id passati dall'utente.
      $listOffset = $offset + $cursor;
      if ($listOffset >= count($productIds)) {
        json_out(['ok'=>true,'had_errors'=>false,'items'=>[],'processed'=>0,'failed'=>0,'cursor_next'=>$cursor,'finished'=>true]);
      }
      $requestedIds = array_slice($productIds, $listOffset, $take);
      if (!$requestedIds) {
        json_out(['ok'=>true,'had_errors'=>false,'items'=>[],'processed'=>0,'failed'=>0,'cursor_next'=>$cursor,'finished'=>true]);
      }
      $base = $ps->getBaseProductsByIds($requestedIds);
    } else {
      $base = $ps->getBaseProductsFiltered($take, $dbOffset, $brandId);
      if (!$base) {
        json_out(['ok'=>true,'had_errors'=>false,'items'=>[],'processed'=>0,'failed'=>0,'cursor_next'=>$cursor,'finished'=>true]);
      }
    }

    $shop = new ShopifyClient($shopCfg, (bool)$invCfg['track_inventory']);

    $langMap       = $psCfg['lang_map'];
    $primaryLangId = (int)$psCfg['primary_lang_id'];
    $primaryLocale = $langMap[$primaryLangId] ?? array_values($langMap)[0];

	/*  
    $locationId = $shop->resolveDefaultLocationId(
      $shopCfg['PreferredLocationId'] ?? null,
      $shopCfg['PreferredLocationName'] ?? null
    );
	*/
	  
	$locationId = $shopCfg['locationId'];

    $items   = [];
    $okCount = 0;
    $koCount = 0;

    // Indicizzo le righe base (serve per gestire ID richiesti ma non trovati)
    $baseById = [];
    foreach ($base as $r) {
      $baseById[(int)$r['id_product']] = $r;
    }

    $loopIds = $productIds ? $requestedIds : array_keys($baseById);
    foreach ($loopIds as $pid) {
      $idp    = (int)$pid;
      $start  = microtime(true);
      $status = ['id_product'=>$idp, 'ok'=>false, 'time'=>0, 'report'=>null, 'error'=>null];

      if (!isset($baseById[$idp])) {
        $status['ok'] = false;
        $status['error'] = 'Prodotto non trovato in PrestaShop (id_product=' . $idp . ')';
        $status['time'] = round(microtime(true) - $start, 2);
        $items[] = $status;
        $koCount++;
        continue;
      }

      $row = $baseById[$idp];
		
	  //aggiungo l'iva
		$row['base_price'] = $row['base_price'] * 1.22;
		if(isset($row['price']))
			$row['price'] = $row['price'] * 1.22;

      try {
        // normalizzo le chiavi attese dal builder
        $psProdNorm = [
          'id_product' => $idp,
          'reference'  => $row['reference'] ?? null,
          'base_price' => isset($row['base_price']) ? $row['base_price'] : ($row['price'] ?? 0),
          'weight'     => $row['weight'] ?? 0,
          'brand'      => $row['brand']  ?? null,
          'quantity'   => $row['quantity'] ?? 0,
        ];

        $textsByLocale = $ps->getTextsByLocale($idp, $langMap);
        if (!$syncTranslations) {
          // mantengo solo la lingua primaria (nessuna traduzione su Shopify)
          $textsByLocale = isset($textsByLocale[$primaryLocale]) ? [$primaryLocale => $textsByLocale[$primaryLocale]] : [];
        }
        $primaryData   = $textsByLocale[$primaryLocale] ?? [
          'name'=>"Product $idp",'handle'=>'','description'=>'','meta_title'=>'','meta_description'=>''
        ];

        // traduzioni: se disattivate, invio solo la lingua primaria
        if (!$syncTranslations) {
          $textsByLocale = [$primaryLocale => $primaryData];
        }

        // immagini: se disattivate, non invio immagini a Shopify
        $imageUrls = $syncImages ? $ps->getImageUrls($idp) : [];

        // varianti: se disattivate, non carico combinazioni
        $combinations = [];
        if ($syncVariants && $ps->hasCombinations($idp)) {
          $anyLangId    = (int)array_key_first($langMap);
          $combinations = $ps->getCombinations($idp, $anyLangId);
        }

        $payload = $shop->buildProductPayload($psProdNorm, $imageUrls, $primaryData, $combinations);

        $logger = function ($m) use (&$status) { $status['log'][] = '['.date('H:i:s')."] ".$m; };
        $onlyinsert    = false;
        $doInventory = $syncInventory;


        $report = $shop->upsertProduct(
          $payload, $textsByLocale, $primaryLocale, $locationId,
          $invCfg['default_qty_if_in_stock'], $dryRun, true, $logger, $ps, $idp,
          $onlyinsert, $doInventory
        );

        $status['report'] = $report;
        if (!empty($report['error'])) {
          $status['ok']    = false;
          $status['error'] = $report['error'];
          $koCount++;
        } else {
          $status['ok'] = true;
          $okCount++;
        }
      } catch (Throwable $e) {
        $status['ok']    = false;
        $status['error'] = $e->getMessage();
        $koCount++;
      } finally {
        $status['time'] = round(microtime(true) - $start, 2);
        $items[] = $status;
      }
    }

    $cursorNext = $cursor + ($productIds ? count($requestedIds) : count($base));

    json_out([
      'ok'          => true,
      'had_errors'  => ($koCount > 0),
      'items'       => $items,
      'processed'   => $okCount,
      'failed'      => $koCount,
      'cursor_next' => $cursorNext,
      // finished se ho preso meno del richiesto (fine lista) oppure ho raggiunto il cap totale
      'finished'    => ($productIds ? (count($requestedIds) < $take) : (count($base) < $take))
                      || ($limit !== null && $cursorNext >= $limit)
    ]);

  } catch (Throwable $e) {
    json_out(['ok'=>false, 'error'=>$e->getMessage()], 500);
  }
}

/* -------- UI -------- */
?>
<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8"/>
  <title>Sync PrestaShop → Shopify</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Cantarell,"Helvetica Neue",Arial,sans-serif;margin:24px;color:#111}
    h1{font-size:20px;margin:0 0 12px}
    .row{margin:8px 0}
    .btn{display:inline-block;padding:8px 12px;border-radius:8px;border:1px solid #ccc;background:#f7f7f7;cursor:pointer}
    .btn[disabled]{opacity:.5;cursor:not-allowed}
    .progress{height:12px;background:#eee;border-radius:6px;overflow:hidden}
    .bar{height:100%;width:0;background:#2ea44f;transition:width .2s}
    .stats{font-size:13px;color:#555;margin-top:6px}
    .grid{display:grid;grid-template-columns:1fr 340px;gap:16px;margin-top:16px}
    .card{border:1px solid #ddd;border-radius:10px;padding:12px;background:#fff}
    .log{height:420px;overflow:auto;background:#0b1426;color:#e6edf3;font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding:12px;border-radius:8px;font-size:12px;white-space:pre-wrap}
    .item{border-bottom:1px solid #eee;padding:8px 0}
    .ok{color:#2e7d32}
    .fail{color:#c62828}
    .muted{color:#777;font-size:12px}
	  .error{color: red;}
    label{margin-right:12px}
    input[type=number]{width:110px}
  </style>
  <script src="https://code.jquery.com/jquery-3.7.1.min.js"></script>
</head>
<body>
  <h1>Sincronizzazione PrestaShop → Shopify</h1>

  <div class="row">
    <label>Brand ID: <input id="brand" type="number" placeholder="(facoltativo)"></label>
    <label style="display:inline-block;min-width:360px">ID prodotto (virgola):
      <input id="productIds" type="text" placeholder="es. 123,456,789" style="width:320px">
    </label>
    <label>Offset iniziale: <input id="offset" type="number" value="0"></label>
    <label>Limit totale: <input id="limit" type="number" placeholder="(facoltativo)"></label>
  </div>

  <div class="row">
    <label><input id="optVariants" type="checkbox" checked> Varianti</label>
    <label><input id="optInventory" type="checkbox" checked> Inventario</label>
    <label><input id="optTranslations" type="checkbox" checked> Traduzioni</label>
    <label><input id="optImages" type="checkbox" checked> Immagini</label>
  </div>

  <div class="row">
    <button id="start" class="btn">Avvia sincronizzazione</button>
    <button id="stop"  class="btn" disabled>Interrompi</button>
  </div>

  <div class="row">
    <div class="progress"><div id="bar" class="bar"></div></div>
    <div id="stats" class="stats">In attesa…</div>
  </div>

  <div class="grid">
    <div class="card"><div id="items"></div></div>
    <div class="card">
      <div class="muted">Log</div>
      <div id="log" class="log"></div>
    </div>
  </div>

<script>
(function(){
  let running=false, total=null, done=0, failed=0, cursor=0;

  // parametri di questa “sessione” (solo JS)
  let brandId=null, baseOffset=0, hardLimit=null;
  let productIdsStr='';

  // opzioni sync (solo JS)
  let optVariants=true, optInventory=true, optTranslations=true, optImages=true;

  function log(msg){
    const $log = $('#log');
    $log.append(msg + "\n");
    $log.scrollTop($log[0].scrollHeight);
  }

  function setStats(){
    // se total è noto, mostro percentuale
    if(total !== null){
      const processed = done + failed;
      const left = Math.max(0, total - processed);
      const pct = total ? Math.round(processed / total * 100) : 0;
      $('#bar').css('width', pct+'%');
      $('#stats').text(`Totale: ${total} | Completati: ${done} | Errori: ${failed} | Rimasti: ${left} | ${pct}%`);
    } else {
      // totale ignoto → mostra solo progress contatori
      $('#bar').css('width','0%');
      $('#stats').text(`Completati: ${done} | Errori: ${failed} | (totale non disponibile)`);
    }
  }

  function addItemRow(item){
    const cls = item.ok ? 'ok' : 'fail';
    const title = item.ok ? 'OK' : 'ERRORE';
	
	  
    let html = `<div class="item">
      <div><strong>#${item.id_product}</strong> <span class="${cls}">${title}</span> <span class="muted">(${item.time}s)</span></div>`;
    if(item.error){
      html += `<div class="muted">${$('<div/>').text(item.error).html()}</div>`;
    }
    if(item.log && item.log.length){
      html += `<details><summary>Log</summary><pre>${$('<div/>').text(item.log.join("\n")).html()}</pre></details>`;
    }
    if(item.report){
      const action = item.report.result?.action || '';
      const pid    = item.report.result?.productId || '';
      html += `<div class="muted">Action: ${action} | ProductId: ${pid}</div>`;
    }
    html += `</div>`;
    $('#items').append(html);
  }

  function toggle(disabled){
    $('#start').prop('disabled', disabled);
    $('#stop').prop('disabled', !disabled);
  }

  async function ajax(action, extra={}){
    return $.ajax({url:'index.php', method:'GET', dataType:'json', data:{action, ...extra}});
  }

  async function runLoop(){
    while(running){
      const res = await ajax('run', {
        brand_id: brandId ?? '',
        product_ids: productIdsStr || '',
        limit:    hardLimit ?? '',
        offset:   baseOffset,
        cursor:   cursor,
        sync_variants:     optVariants ? 1 : 0,
        sync_inventory:    optInventory ? 1 : 0,
        sync_translations: optTranslations ? 1 : 0,
        sync_images:       optImages ? 1 : 0
      }).catch(err=>{
        running=false; toggle(false);
        log('❌ Errore batch: ' + (err.responseText || err.statusText));
      });
      if(!running || !res) break;
      if(!res.ok){
        running=false; toggle(false);
        log('❌ ' + (res.error || 'errore sconosciuto'));
        break;
      }

      (res.items||[]).forEach(addItemRow);
      done    += res.processed || 0;
      failed  += res.failed    || 0;
      cursor   = res.cursor_next ?? cursor;

      setStats();

      if(res.finished){
        running=false; toggle(false); log('✅ Finito.');
        break;
      }
    }
  }

  $('#start').on('click', async function(){
    toggle(true); running = true;
    $('#items').empty(); $('#log').empty();
    $('#bar').css('width','0%'); $('#stats').text('Inizializzo…');
    log('▶ Inizializzazione…');

    // leggo i parametri dall'UI
    brandId    = $('#brand').val() ? parseInt($('#brand').val(),10) : null;
    productIdsStr = ($('#productIds').val() || '').trim();
    baseOffset = $('#offset').val() ? parseInt($('#offset').val(),10) : 0;
    hardLimit  = $('#limit').val()  ? parseInt($('#limit').val(),10)  : null;

    optVariants     = $('#optVariants').is(':checked');
    optInventory    = $('#optInventory').is(':checked');
    optTranslations = $('#optTranslations').is(':checked');
    optImages       = $('#optImages').is(':checked');

    // reset contatori
    total=null; done=0; failed=0; cursor=0;

    // init (per avere un totale stimato, se disponibile)
    const res = await ajax('init', {
      brand_id: brandId ?? '',
      product_ids: productIdsStr || '',
      limit:    hardLimit ?? '',
      offset:   baseOffset
    }).catch(err=>{
      running=false; toggle(false);
      log('❌ Init error: ' + (err.responseText || err.statusText));
    });
    if(!running || !res) return;
    if(!res.ok){ running=false; toggle(false); log('❌ ' + (res.error||'init fallita')); return; }

    total = (typeof res.total === 'number') ? res.total : null;
    setStats();
    log(`✓ Init ok${total!==null ? ' (totale stimato: '+total+')' : ''}. Batch=${res.batchSize||'?'}`);

    runLoop();
  });

  $('#stop').on('click', function(){
    running=false; toggle(false); log('⏹ Interrotto dall’utente.');
  });
})();
</script>
</body>
</html>

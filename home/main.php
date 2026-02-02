<?php
declare(strict_types=1);

error_reporting(E_ALL);
ini_set('display_errors','1');

require __DIR__ . '/vendor/autoload.php';
require __DIR__ . '/src/Prestashop.php';
require __DIR__ . '/src/ShopifyClient.php';

use App\Prestashop;
use App\ShopifyClient;

// Config
$config   = require __DIR__ . '/config.php';
$psCfg    = $config['ps'];
$shopCfg  = $config['shopify'];
$invCfg   = $config['inventory'];
$dryRun   = (bool)$config['dry_run'];

// Istanza servizi
$ps   = new Prestashop($psCfg);
$shop = new ShopifyClient($shopCfg, (bool)$invCfg['track_inventory']);

// Locale primaria
$langMap       = $psCfg['lang_map'];
$primaryLangId = (int)$psCfg['primary_lang_id'];
$primaryLocale = $langMap[$primaryLangId] ?? array_values($langMap)[0];

// Location per inventario
/*
$locationId = $shop->resolveDefaultLocationId(
    $shopCfg['PreferredLocationId'] ?? null,
    $shopCfg['PreferredLocationName'] ?? null
);
*/
$locationId = $shopCfg['locationId'];

// Carica prodotti PS
$products = $ps->getBaseProducts((int)$psCfg['limit']);
echo "Trovati ".count($products)." prodotti base\n";

// Loop
foreach ($products as $psProd) {
    $idp = (int)$psProd['id_product'];

    $textsByLocale = $ps->getTextsByLocale($idp, $langMap);
    $primaryData   = $textsByLocale[$primaryLocale] ?? [
        'name' => "Product $idp", 'handle'=>'', 'description'=>'', 'meta_title'=>'', 'meta_description'=>''
    ];

    $imageUrls = $ps->getImageUrls($idp);

    $combinations = [];
    if ($ps->hasCombinations($idp)) {
        $anyLangId   = (int)array_key_first($langMap);
        $combinations= $ps->getCombinations($idp, $anyLangId);
    }

    $payload = $shop->buildProductPayload($psProd, $imageUrls, $primaryData, $combinations);

    try {
        $report = $shop->upsertProduct(
			$payload,
			$textsByLocale,
			$primaryLocale,
			$locationId,
			$invCfg['default_qty_if_in_stock'],
			$dryRun,
			true, // <- debug ON
			function ($m) { echo date('[H:i:s] ') . $m . PHP_EOL; }, // logger,
			$ps,           
    		$idp,
			false,
			false
		);
		
		//$trs = $shop->debugFetchTranslations($idp, 'en');
		//echo "LINGUE: <pre>".json_encode($trs, JSON_PRETTY_PRINT|JSON_UNESCAPED_UNICODE)."</pre>";
		
		echo "<pre>".json_encode($report, JSON_PRETTY_PRINT|JSON_UNESCAPED_SLASHES|JSON_UNESCAPED_UNICODE)."</pre>";
		
		
		
    } catch (\Throwable $e) {
        echo "ERRORE PS {$idp}: ".$e->getMessage()."\n";
    }
}

echo "Fatto.\n";

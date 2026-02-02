<?php
return [
	
    
	
	'ps' => [
        'dsn'      => 'mysql:host=localhost;dbname=prestashop_9;charset=utf8mb4',
        'user'     => 'migrazione',
        'password' => '[passw]',
        'prefix'   => 'prstshp_',
        'id_shop'  => 1,
        'base_url' => 'https://www.undomus.com',
        // mappa ID lingua PrestaShop -> locale Shopify
        // es. 1=it, 2=en, 3=fr (adatta ai tuoi ID)
        'lang_map' => [
            1 => 'en',
          //  1 => 'it',
          //  2 => 'en',
        ],
        // quale lingua usare per CREARE il prodotto (contenuti di default)
        'primary_lang_id' => 1,
		 'limit'    => 10000, // prodotti per run
		'download_dir' => '/var/www/vhosts/undomus.com/httpdocs/download',
    ],
    // --- Shopify ---
    'shopify' => [
        'ShopUrl'     => 'undomus.myshopify.com',
        'AccessToken' => '[tocken]',
        'ApiVersion' => '2025-07',
		'PreferredLocationId'   => null,
        'PreferredLocationName' => null,
		'categoryDefault' => 'gid://shopify/TaxonomyCategory/fr',
		'onlineStorePublicationId' => 'gid://shopify/Publication/326665765194',
		'inventoryPolicy' => 'CONTINUE', //oppure DENY
		'locationId' => 117666251082,
    ],
	
    'inventory' => [
        'default_qty_if_in_stock' => null,
        'track_inventory'         => true,
    ],
    'dry_run' => false,
];

<?php


require __DIR__ . '/vendor/autoload.php';




class shopify {
    private $apiUrl = "";
	private $shopifyClass;
	

    // Costruttore che accetta l'endpoint base con autenticazione
    public function __construct() {
		
		
		$conf = new configurazione();
		$param = $conf->getParam();
		
		
		
       
		$this->apiUrl = "https://" . $param["ecommerce_shopify_apikey"] . ":" . $param["ecommerce_shopify_accesstoken"] . "@" . $param["ecommerce_shopify_apiurl"];
		$this->accessToken = $param["ecommerce_shopify_accesstoken"];

		
		
		$config = array(
			'ShopUrl' => $param["ecommerce_shopify_shopurl"],
		  
			'AccessToken' => $param["ecommerce_shopify_accesstoken"],
			'ApiVersion'  => '2025-07',
			'Curl'        => [
				// ↑ aumentiamo i tempi
				CURLOPT_TIMEOUT        => 60,   // da 10 → 60s
				CURLOPT_CONNECTTIMEOUT => 15,
				// forza IPv4 (alcuni ambienti hanno IPv6 lento)
				CURLOPT_IPRESOLVE      => CURL_IPRESOLVE_V4,
				CURLOPT_FOLLOWLOCATION => true,
				// facoltativo ma a volte utile:
				// CURLOPT_HTTP_VERSION   => CURL_HTTP_VERSION_1_1,
			],
			//'ApiVersion'  => '2025-07', // <— aggiungi questa riga
		);

		PHPShopify\ShopifySDK::config($config);
		
		$this->shopifyClass = new PHPShopify\ShopifySDK($config);
    }

    // Metodo per fare una richiesta GET all'API
    private function makeApiCall($url) {
        $ch = curl_init();

        curl_setopt($ch, CURLOPT_URL, $url);
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_HEADER, true);
        
		curl_setopt($ch, CURLOPT_HTTPHEADER, array(
            'Content-Type: application/json',
            'X-Shopify-Access-Token: ' . $this->accessToken // Aggiungi il token nell'intestazione
        ));

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
		
		$headerSize = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
		$headers = safe_substr($response, 0, $headerSize);
		$body = safe_substr($response, $headerSize);
		//print_r($headers);

        curl_close($ch);

        if ($httpCode != 200) {
            throw new Exception("API $url request failed with response code: $httpCode");
        }

		return ['body' => json_decode($body, true), 'headers' => $headers];
        //return json_decode($response, true);
    }

    // Metodo per ottenere tutti i prodotti gestendo la paginazione
	
	
	
	function getAllProducts(array $filters = [], $max_product = 1000) {
		
		$limit = 250; // Shopify consente un massimo di 250 prodotti per richiesta
		$products = [];
		//$params = ['limit' => $limit];
		$params = array_merge(['limit' => $limit], $filters); // Unisce i filtri ai parametri API


		do {
			$response = $this->shopifyClass->Product->get($params); // Recupera i prodotti
			$products = array_merge($products, $response);

			// Verifica se esiste una pagina successiva
			$nextPageLink = $this->shopifyClass->Product->getNextPageParams();
			$params = $nextPageLink; // Imposta i parametri per la prossima richiesta

		} while ($nextPageLink && count($products) < $max_product);

		// Se abbiamo superato il massimo richiesto, tronchiamo l'array
		return array_slice($products, 0, $max_product);
	}
	
	
	private function getNextPageUrl($headers) {
        if (preg_match('/<([^>]+)>; rel="next"/', $headers, $matches)) {
            return $matches[1];  // Restituisce l'URL della prossima pagina
        }
        return null;
    }
	
	
	 // Metodo per generare la tabella HTML con i prodotti
    // Metodo per generare la tabella HTML con i prodotti e le opzioni valorizzate
    public function generateProductsTable($products) {
        $html = '';
		
		$articolo = new articolo();
       
        foreach ($products as $product) {
			$i++;
            foreach ($product['variants'] as $variant) {
                // Aggiungi le opzioni valorizzate al titolo del prodotto
                $optionValues = [];
                if (!empty($variant['option1'])) {
                    $optionValues[] = $variant['option1'];
                }
                if (!empty($variant['option2'])) {
                    $optionValues[] = $variant['option2'];
                }
                if (!empty($variant['option3'])) {
                    $optionValues[] = $variant['option3'];
                }

                // Se ci sono opzioni valorizzate, aggiungile al titolo
                $productTitle = $product['title'];
                if (!empty($optionValues)) {
                    $productTitle .= ' (' . implode(', ', $optionValues) . ')';
                }

                // Aggiungi la riga alla tabella HTML
                $html .= '<tr>';
				 $html .= '<td>' . $i . "<input type=\"checkbox\" data-val=\"" . htmlspecialchars(json_encode($variant), ENT_QUOTES, 'UTF-8') . "\" class=\"checkbox_class\" name=\"check_". $product['id'] . '-' . $variant['id'] ."\" value=\"".$product['id'] . '-' . $variant['id']."\" id=\"check_".$product['id'] . '-' . $variant['id']."\">" . '</td>';
				$html .= '<td><a href="' . $product["image"]["src"] . '" target="_blank"><img src="' . $product["image"]["src"] . '" style="height:100px; width:auto;"></a></td>';
                
                $html .= '<td>' . $product['id'] . '-' . $variant['id'] . ' </td>';
               
                $html .= '<td>' . $variant['sku'] . '</td>';
				 $html .= '<td>' . $productTitle . '</td>';
				$html .= '<td>' . $product["vendor"] . '</td>';
				$html .= '<td>' . $product["product_type"] . '</td>';
				$html .= '<td>' . $variant['price'] . '</td>';
				
				$match = $articolo->findMatchIdEcommerce($product['id'], $variant['id'], $variant['sku']);
				if($match) $match = "si"; else $match = "no";
				 $html .= '<td>' . $match . '</td>';
                $html .= '</tr>';
            }
        }

       
        return $html;
    }
	
	
	public function createDataByIdProduct($id){
		
		$articolo = new articolo();
		
		$artInfo = $articolo->getInfo($id);
		
		$tags = array();
		$tags[] = $artInfo["categoria"];
		$collections = array();
		$collections[] = $artInfo["categoria"];
		$collections[] = $artInfo["codice_stat"];
		
		$combinazioni = $articolo->getCombinazioniByIdArticolo($id);
		$variants = array();
		
		$attr = array();
		
		$resAttr = $articolo->getAttributiByArticolo($id);
		$resAttr2 = $resAttr;
		
		$datiAttributi = array();
		
		while($dati = mysql_fetch_array($resAttr)){
			$attr[] = $dati["gruppo"];
			$datiAttributi[] = $dati;
			
		}
		
		$attrNames = array_unique($attr);
		
		
		
		
		
		
		
		//echo print_r($opts); exit();
		if(mysql_num_rows($combinazioni) > 0){
			
			$opts = array();
		
			foreach($attrNames as $attrName){

				$opt = array();
				$values = array();



				foreach($datiAttributi as $dati){

					if($dati["gruppo"] == $attrName){
						//$values["name"] = $dati["gruppo"];
						$values[] = $dati["attributo"];
					}
				}

				$opt["name"] = $attrName;
				$opt["values"] =array_unique($values);
				$opts[] = $opt;
			}
			
			foreach($combinazioni as $combinazione){
				$a = array();
				$i = array();
				$a["sku"] = $combinazione["reference"];
				$a["price"] = $combinazione["prezzo"] + $combinazione["price"];
				$a["taxable"] = true;
				$a["inventory_management"] = "shopify";


				$a["barcode"] =  $combinazione["ean13"]; 
				
				if($combinazione["id_ecommerce"] != ""){
					$a["id"] = $combinazione["id_ecommerce"];
				}

				$options = array();

				$attributi = $articolo->getAttributiByCombinazione($combinazione["id_product_attribute"]);
				foreach($attributi as $attributo){
					$options[] = $attributo["attributo"];
				}

				$a["options"] = $options;

				$variants[] = $a;


			}
			
		}
		else{
			
			$a = array();
			$a["sku"] = $artInfo["codice"];
			$a["price"] = $artInfo["prezzo"] ;
			$a["taxable"] = true;
			$a["inventory_management"] = "shopify";
			$a["barcode"] =  $artInfo["ean13_e"] ;
			$variants[] = $a;
		}
		
		
		
		$data = [
		  'id' => $artInfo["id_ecommerce"],
		  'id_siriocloud' => $artInfo["id"],
		  'title' => $artInfo["descrizione"],
		  'body_html' => $artInfo["descrizione"],
		  'vendor' => $artInfo["nome_produttore"],
		  'product_type' => $artInfo["categoria"],
		  'status' => 'active', // oppure 'draft'
		  'tags' => $tags,

			
		  'options' => $opts,
		  'variants' => $variants,
			

		  // Metafield di prodotto (opzionale)
			/*
		  'metafields' => [
			['namespace'=>'custom','key'=>'materiale','type'=>'single_line_text_field','value'=>'Poliestere 100%'],
		  ],
		  */
		  'collections'    => $collections, // descrittori
		  'collections_by' => 'title', // oppure 'handle' (default: 'title')
		  'collections_publish'=> true, 
		];
		
		
		
		
		
		return $data;

	}
	
	
	public function updateIdEcommerce($id_product, $productShopify){
		
		$articolo = new articolo();
		$attributo = new attributo();
		
		$articolo->updateVal("id_ecommerce",$productShopify["id"],$id_product);
		
		$variants = $productShopify["variants"];
		
		foreach($variants as $variant){
			if($variant["sku"]!=""){
				$attributo->setIdEcommerceByReference($variant["sku"], $variant["id"]);
			}
		}
		
	}
	
	
	
	
	
	public function putProduct(array $data)
	{
		echo date("Y-m-d H:i:s") . " PUT PRODUCT\n\r";
		
		if (empty($data['title'])) {
			throw new \InvalidArgumentException("title è obbligatorio");
		}

		// Base product
		$product = [
			'id' => $data['id'],
			'title'        => (string)$data['title'],
			'body_html'    => $data['body_html'] ?? null,
			'vendor'       => $data['vendor'] ?? null,
			'product_type' => $data['product_type'] ?? null,
			// dal 2020+ REST supporta 'status': 'active'|'draft'|'archived'
			'status'       => strtolower($data['status'] ?? 'draft'),
		];

		// Tags (REST accetta una stringa separata da virgole)
		if (isset($data['tags'])) {
			$product['tags'] = is_array($data['tags'])
				? implode(',', array_map('strval', $data['tags']))
				: (string)$data['tags'];
		}

		// Opzioni (solo nomi per creare le colonne)
		if (!empty($data['options']) && is_array($data['options'])) {
			$product['options'] = [];
			foreach ($data['options'] as $opt) {
				if (is_array($opt)) {
					// es. ['name' => 'Colore', 'values' => ['Nero','Blu']]
					// In REST i 'values' non sono obbligatori qui: creiamo solo le colonne.
					$product['options'][] = ['name' => (string)($opt['name'] ?? 'Option')];
				} else {
					$product['options'][] = ['name' => (string)$opt];
				}
			}
		}

		// Varianti (price come stringa decimale; opzioni su option1/2/3)
		if (!empty($data['variants']) && is_array($data['variants'])) {
			$variants = [];
			foreach ($data['variants'] as $v) {
				$variant = [];
				
				if($v["id_ecommerce"]!= ""){
					$variant["id"] = $v["id_ecommerce"];
				}

				foreach ([
					'sku','price','compare_at_price','barcode',
					'taxable','requires_shipping','weight','weight_unit',
					'inventory_management','inventory_policy','grams','position'
				] as $k) {
					if (array_key_exists($k, $v)) $variant[$k] = $v[$k];
				}

				if (!empty($v['options']) && is_array($v['options'])) {
					if (isset($v['options'][0])) $variant['option1'] = (string)$v['options'][0];
					if (isset($v['options'][1])) $variant['option2'] = (string)$v['options'][1];
					if (isset($v['options'][2])) $variant['option3'] = (string)$v['options'][2];
				} else {
					foreach (['option1','option2','option3'] as $ok) {
						if (isset($v[$ok])) $variant[$ok] = (string)$v[$ok];
					}
				}

				// NB: la quantità NON si imposta qui (va fatto via Inventory)
				$variants[] = $variant;
			}
			if ($variants) $product['variants'] = $variants;
		}
		
		//echo json_encode($product); exit();
		

		// Ripulisci null/empty
		$product = array_filter($product, fn($v) => !is_null($v) && $v !== '' && $v !== []);
		
		$nontrovato = false;
		
		if(isset($product["id"])){
			
			echo "faccio aggiornamento\n\r";
			
			try {
				$resp = $this->shopifyClass->Product($product["id"])->put($product);
			} catch (\PHPShopify\Exception\ApiException $e) {
				
				if(stripos($e->getMessage(), 'Not Found')){
					$nontrovato = true;
				}
				
				echo "eccezione: " . $e->getMessage();
				// se il tuo endpoint pretende il wrapper {"product":{...}}
				if (stripos($e->getMessage(), 'product') !== false || stripos($e->getMessage(), 'missing') !== false) {
					$resp = $this->shopifyClass->Product($product["id"])->put(['product' => $product]);
				} else {
					//throw $e;
					echo "eccezione: " . $e->getMessage();
				}
			}
			
			//echo json_encode($resp);

			if (empty($resp['id'])) {
				
				//throw new \RuntimeException('Aggiornamento prodotto REST fallito: risposta inattesa');
				echo 'Aggiornamento prodotto REST fallito: risposta inattesa\n\r';
				$nontrovato = true;
			}
			else{
				echo 'Prodotto aggiornato con id:' . $resp['id'] . "\n\r";
				$productId = $resp['id'];
			}
			
			
		}
		
		//echo $nontrovato;
		
		if(!isset($product["id"]) || $nontrovato ){
		
			echo "faccio creazione";
			
			try {
				$resp = $this->shopifyClass->Product->post($product);
			} catch (\PHPShopify\Exception\ApiException $e) {
				// se il tuo endpoint pretende il wrapper {"product":{...}}
				if (stripos($e->getMessage(), 'product') !== false || stripos($e->getMessage(), 'missing') !== false) {
					$resp = $this->shopifyClass->Product->post(['product' => $product]);
				} else {
					throw $e;
				}
			}

			if (empty($resp['id'])) {
				echo 'Creazione prodotto REST fallita: risposta inattesa';
				//throw new \RuntimeException('Creazione prodotto REST fallita: risposta inattesa');
			}

			$productId = $resp['id'];

			$this->updateIdEcommerce($data["id_siriocloud"], $resp);
			
		}

		// CREA il prodotto (REST)
		// Con phpclassic/php-shopify puoi passare sia $product "puro" sia ['product' => $product].
		// Di norma funziona il payload "puro", ma gestiamo entrambi i casi per compatibilità.
		
		
		//echo json_encode($resp);

		// Metafield di PRODOTTO (opzionale) via REST
		/*
		if (!empty($data['metafields']) && is_array($data['metafields'])) {
			foreach ($data['metafields'] as $mf) {
				if (!isset($mf['namespace'], $mf['key'], $mf['value'])) continue;
				$mfPayload = [
					'namespace'  => (string)$mf['namespace'],
					'key'        => (string)$mf['key'],
					// in REST moderno usa 'type' quando disponibile; se non supportato, la libreria accetta anche 'value' + 'value_type'
					'type'       => $mf['type'] ?? 'single_line_text_field',
					'value'      => (string)$mf['value'],
				];
				try {
					$this->shopifyClass->Product($productId)->Metafield->post($mfPayload);
				} catch (\PHPShopify\Exception\ApiException $e) {
					// fallback legacy senza 'type'
					if (stripos($e->getMessage(), 'type') !== false) {
						$legacy = [
							'namespace'  => $mfPayload['namespace'],
							'key'        => $mfPayload['key'],
							'value'      => $mfPayload['value'],
							'value_type' => 'string',
						];
						$this->shopifyClass->Product($productId)->Metafield->post( $legacy);
					} else {
						throw $e;
					}
				}
			}
		}
		*/
		
		// Aggiunta/creazione collezioni manuali (Custom Collections) e collegamento via Collect
		
		
		// --- COLLECT: collega il prodotto alle Custom Collections ---
		if (!empty($data['collections']) && is_array($data['collections'])) {
			$by         = isset($data['collections_by']) ? strtolower((string)$data['collections_by']) : 'title';
			$publishNew = array_key_exists('collections_publish', $data) ? (bool)$data['collections_publish'] : true;

			$collectionIds = $this->resolveOrCreateCustomCollectionIds($data['collections'], $by, $publishNew);

			// sanity check
			if (empty($productId) || !is_numeric($productId)) {
				throw new \RuntimeException("Collect: productId mancante/non numerico: " . var_export($productId, true));
			}

			foreach ($collectionIds as $cid) {
				$cid = (int)$cid;
				if ($cid <= 0) continue;

				echo "faccio post collection: $cid di product_id: $productId \n\r";

				$payload = [
					'product_id'    => (int)$productId,
					'collection_id' => $cid,
				];

				try {
					$this->shopifyClass->Collect->post($payload); // ← no wrapper
				} catch (\PHPShopify\Exception\ApiException $e) {
					$msg = $e->getMessage();

					// già collegato
					if (stripos($msg, 'has already been taken') !== false) {
						echo "Collect già esistente\n\r";
						continue;
					}

					// diagnostica veloce: prova a leggere risorse
					try { $this->shopifyClass->Product((int)$productId)->get(); } catch (\Exception $ee) {}
					try { $this->shopifyClass->CustomCollection((int)$cid)->get(); } catch (\Exception $ee) {}
					
				}
			}

		}

		

		return $resp; // contiene id, handle, variants, ecc.
	}
	
	
	
	private function getAllCustomCollections(): array
	{
		$limit = 250;
		$params = ['limit' => $limit];
		$out = [];
		do {
			$chunk = $this->shopifyClass->CustomCollection->get($params);
			if (is_array($chunk)) $out = array_merge($out, $chunk);
			$next = $this->shopifyClass->CustomCollection->getNextPageParams();
			$params = $next ?: [];
		} while (!empty($next));
		return $out;
	}

	private function slugifyHandle(string $s): string
	{
		$s = mb_strtolower($s, 'UTF-8');
		$s = preg_replace('~[^\pL\d]+~u', '-', $s);
		$s = trim($s, '-');
		$s = preg_replace('~[^-\w]+~', '', $s);
		$s = preg_replace('~-+~', '-', $s);
		return $s ?: '';
	}

	private function humanizeFromHandle(string $handle): string
	{
		$s = str_replace('-', ' ', $handle);
		return mb_convert_case($s, MB_CASE_TITLE, 'UTF-8');
	}

	private function createCustomCollection(string $title, ?string $handle = null, bool $published = true): int
	{
		$payload = ['title' => $title];
		if ($handle)    $payload['handle']    = $handle;
		if ($published) $payload['published'] = true;

		// alcune versioni vogliono il wrapper, altre no → tentiamo entrambi
		try {
			$cc = $this->shopifyClass->CustomCollection->post(['custom_collection' => $payload]);
		} catch (\PHPShopify\Exception\ApiException $e) {
			$cc = $this->shopifyClass->CustomCollection->post($payload);
		}

		return (int)($cc['id'] ?? 0);
	}

	/**
	 * Risolve gli ID delle Custom Collections a partire da descrittori (title/handle).
	 * Se non trovate, le crea (published = true di default).
	 */
	private function resolveOrCreateCustomCollectionIds(array $descriptors, string $by = 'title', bool $publishNew = true): array
	{
		$by = strtolower($by);
		if (!in_array($by, ['title','handle'], true)) $by = 'title';

		$all = $this->getAllCustomCollections();

		// indicizza per match veloce
		$byTitle  = []; // chiave: titolo in lowercase → [ids]
		$byHandle = []; // chiave: handle         → [ids]
		foreach ($all as $c) {
			if (!empty($c['title']))  $byTitle[mb_strtolower($c['title'],'UTF-8')][] = (int)$c['id'];
			if (!empty($c['handle'])) $byHandle[(string)$c['handle']][]             = (int)$c['id'];
		}

		$ids = [];
		foreach ($descriptors as $d) {
			if (!is_string($d) || $d === '') continue;

			if ($by === 'handle') {
				$handle = $this->slugifyHandle($d);
				if ($handle === '') continue;

				if (!empty($byHandle[$handle])) {
					// esiste già
					foreach ($byHandle[$handle] as $id) $ids[] = $id;
					continue;
				}

				// non esiste: crea, titolando in modo umano dal handle
				$title = $this->humanizeFromHandle($handle);
				$newId = $this->createCustomCollection($title, $handle, $publishNew);
				if ($newId) $ids[] = $newId;
			} else {
				// by title (case-insensitive)
				$key = mb_strtolower($d, 'UTF-8');
				if (!empty($byTitle[$key])) {
					foreach ($byTitle[$key] as $id) $ids[] = $id;
					continue;
				}

				// non esiste: crea; lascia che Shopify generi l'handle
				$newId = $this->createCustomCollection($d, null, $publishNew);
				if ($newId) $ids[] = $newId;
			}
		}

		return array_values(array_unique($ids));
	}
	
	
	/**
 * Imposta la quantità **assoluta** per una lista di articoli su una location.
 *
 * Struttura $data:
 * [
 *   // opzionale: id o nome location; se omesso e c’è UNA sola location attiva, usa quella
 *   'location' => 1234567890 | 'Magazzino Principale',
 *
 *   // elenco righe da impostare
 *   'items' => [
 *     ['variant_id' => 111, 'quantity' => 25],
 *     ['inventory_item_id' => 222, 'quantity' => 10],
 *   ],
 * ]
 */
	/*
		public function setInventory(array $data): array
		{
			if (empty($data['items']) || !is_array($data['items'])) {
				throw new \InvalidArgumentException('setInventory: "items" è obbligatorio e deve essere un array.');
			}

			// 1) risolvi location
			if (!empty($data['location'])) {
				$locationId = $this->resolveLocationId($data['location']);
			} else {
				$locationId = $this->resolveDefaultLocationId(); // usa la sola location attiva, se unica
			}

			// 2) helper per ottenere inventory_item_id
			$toInventoryItemId = function(array $line): int {
				if (!empty($line['inventory_item_id'])) {
					return (int)$line['inventory_item_id'];
				}
				if (!empty($line['variant_id'])) {
					//$variant = $this->shopifyClass->Variant((int)$line['variant_id'])->get();
					$variant = $this->shopifyClass->ProductVariant((int)$line['variant_id'])->get();
					if (empty($variant['inventory_item_id'])) {
						throw new \RuntimeException("Variant {$line['variant_id']} senza inventory_item_id.");
					}
					return (int)$variant['inventory_item_id'];
				}
				throw new \InvalidArgumentException('Ogni riga deve avere "variant_id" oppure "inventory_item_id".');
			};

			$results = [];

			foreach ($data['items'] as $line) {
				if (!isset($line['quantity'])) {
					throw new \InvalidArgumentException('Ogni riga deve avere "quantity".');
				}
				$invItemId = $toInventoryItemId($line);

				// connect (idempotente: se già connesso, Shopify lo ignora)
				
				try {
					$this->shopifyClass->InventoryLevel->connect([
					  'location_id'       => $locationId,
					  'inventory_item_id' => $invItemId,
					]);
				} catch (\PHPShopify\Exception\ApiException $e) {
					// se già connesso o non necessario, ignoriamo
				}
				
				//echo "dati: $locationId - $invItemId -" . $line['quantity'];

				// set assoluto
				$payload = [
					'location_id'       => $locationId,
					'inventory_item_id' => $invItemId,
					'available'         => (int)$line['quantity'],
				];
				$res = $this->shopifyClass->InventoryLevel->set([
				  'location_id'       => $locationId,
				  'inventory_item_id' => $invItemId,
				  'available'         => (int)$line['quantity'],
				]);
				$results[] = $res;
			}

			return $results;
		}
	*/
	
	/*
	private function resolveLocationId($descriptor): int {
		if (is_numeric($descriptor)) return (int)$descriptor;
		$locs = $this->shopifyClass->Location->get(['limit' => 250]);
		foreach ($locs as $loc) {
			if (!empty($loc['name']) && mb_strtolower($loc['name'],'UTF-8') === mb_strtolower((string)$descriptor,'UTF-8')) {
				return (int)$loc['id'];
			}
		}
		throw new \RuntimeException("Location non trovata: {$descriptor}");
	}

	private function resolveDefaultLocationId(): int {
		$locs = $this->shopifyClass->Location->get(['limit' => 2, 'active' => true]);
		
		//print_r($locs);
		
		if (is_array($locs) && count($locs) === 1) {
			return (int)$locs[0]['id'];
		}
		throw new \RuntimeException('Location non specificata e multiple attive presenti: specificane una.');
	}
	*/
	
	public function getDefaultVariantByProductId(int $productId): array
	{
		// Restituisce la prima variante del prodotto
		$p = $this->shopifyClass->Product($productId)->get();      // GET /products/{id}.json
		if (empty($p['variants']) || !is_array($p['variants'])) {
			throw new \RuntimeException("Prodotto {$productId} senza varianti.");
		}
		return $p['variants'][0]; // default / unica variante
	}

	
	
}


?>

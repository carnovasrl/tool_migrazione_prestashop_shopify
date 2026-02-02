<?php
declare(strict_types=1);

namespace App;

use PHPShopify\ShopifySDK;
use RuntimeException;

final class ShopifyClient
{
    private ShopifySDK $sdk;
    private array $cfg;
    private bool $trackInventory;
	
	
	    // --- Rate limiting / retry (REST & GraphQL) ---
    private static float $lastInvCallTs = 0.0;
    private static float $lastRestCallTs = 0.0;
    private float $invMinIntervalSec = 0.60;   // prudente: <2 req/sec sulle inventory endpoints
    private float $restMinIntervalSec = 0.00;  // opzionale: 0 = nessun throttle globale
    private int $maxRetries = 6;

private bool $collectionsCacheLoaded = false;
	private array $collectionsByHandle = [];   // handle => ['id'=>int,'title'=>string]
	private array $collectCache = [];          // "productId:collectionId" => true

	// --- Variant swatch texture (file_reference metafield) ---
	private array $fileIdByUrl = []; // texture_url => File GID
	
	// --- Product attachments (PDF/manuals) ---
	private array $genericFileIdByPsHash = []; // ps attachment hash => Shopify File GID

	/**
	 * Debug logger (sempre attivo): stampa righe con prefisso [DBG].
	 * NON altera la logica applicativa.
	 */
	// Nota: niente type-hint "mixed" per compatibilita' con PHP < 8.0
	private function dbg(string $msg, $data = null): void
	{
		if ($data !== null) {
			$json = json_encode($data, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
			if ($json === false) {
				$json = '<<json_encode failed>>';
			}
			error_log('[DBG] ' . $msg . ' ' . $json);
			return;
		}
		error_log('[DBG] ' . $msg);
	}

	/**
	 * Polyfill per PHP < 8 (e per evitare la risoluzione nel namespace App).
	 */
	private function startsWith(string $haystack, string $needle): bool
	{
		if ($needle === '') {
			return true;
		}
		return strncmp($haystack, $needle, strlen($needle)) === 0;
	}

	/**
	 * Ritorna un URL normalizzato per caching (evita duplicati per querystring irrilevanti).
	 */
	private function normalizeUrlForCache(string $url): string
	{
		$url = trim($url);
		if ($url === '') return '';
		// Togliamo frammenti (#...) che non cambiano la risorsa
		$hashPos = strpos($url, '#');
		if ($hashPos !== false) $url = substr($url, 0, $hashPos);
		return $url;
	}

	/**
	 * Carica (o riusa) un file Shopify a partire da un URL pubblico.
	 * Ritorna il File GID (es. gid://shopify/File/123).
	 */
	private function ensureShopifyFileFromUrl(string $url, bool $debug = false, ?callable $say = null): string
	{
		$url = $this->normalizeUrlForCache($url);
		if ($url === '') {
			throw new RuntimeException('Texture URL vuoto');
		}
		if (isset($this->fileIdByUrl[$url])) {
			return $this->fileIdByUrl[$url];
		}

		// 1) Tentativo rapido: Shopify fileCreate con originalSource pubblico.
		// Nota: alcuni host bloccano i bot Shopify (403/hotlink protection). In quel caso
		// Shopify crea il record File ma lo lascia in stato FAILED, e l'immagine risulta vuota.
		try {
			$fileId = $this->fileCreateByOriginalSource($url);
			$status = $this->getFileStatus($fileId);
			if ($status === 'FAILED') {
				throw new RuntimeException('fileCreate originalSource FAILED (probabile hotlink/protezione)');
			}
			// Anche se PROCESSING, salviamo e ritorniamo: in genere compare dopo pochi secondi.
			$this->fileIdByUrl[$url] = $fileId;
			if ($debug && $say) {
				$say("  [SWATCH] fileCreate originalSource ok url={$url} fileId={$fileId} status={$status}");
			}
			return $fileId;
		} catch (\Throwable $e) {
			// 2) Fallback robusto: download + stagedUploadsCreate + upload + fileCreate.
			$this->dbg('ensureShopifyFileFromUrl fallback staged upload', ['url' => $url, 'err' => $e->getMessage()]);
			$fileId = $this->fileCreateByStagedUploadFromUrl($url);
			$this->fileIdByUrl[$url] = $fileId;
			if ($debug && $say) {
				$say("  [SWATCH] fileCreate stagedUpload ok url={$url} fileId={$fileId}");
			}
			return $fileId;
		}
	}

	/**
	 * Crea un File Shopify usando originalSource.
	 */
	private function fileCreateByOriginalSource(string $url): string
	{
		$q = <<<'GQL'
mutation fileCreate($files: [FileCreateInput!]!) {
  fileCreate(files: $files) {
    files { id }
    userErrors { field message }
  }
}
GQL;
		$vars = [
			'files' => [[
				'originalSource' => $url,
				'contentType'    => 'IMAGE',
			]],
		];
		$res = $this->graphql($q, $vars);
		$ue = $res['data']['fileCreate']['userErrors'] ?? [];
		if (!empty($ue)) {
			throw new RuntimeException('fileCreate userErrors: ' . json_encode($ue));
		}
		$fileId = $res['data']['fileCreate']['files'][0]['id'] ?? '';
		if ($fileId === '') {
			throw new RuntimeException('fileCreate: risposta inattesa: ' . json_encode($res));
		}
		return $fileId;
	}

	/**
	 * Legge lo stato del File (READY/PROCESSING/FAILED...).
	 */
	private function getFileStatus(string $fileGid): string
	{
		$q = <<<'GQL'
query nodeFile($id: ID!) {
  node(id: $id) {
    ... on File {
      id
      fileStatus
    }
  }
}
GQL;
		$r = $this->graphql($q, ['id' => $fileGid]);
		$node = $r['data']['node'] ?? null;
		return (string)($node['fileStatus'] ?? '');
	}

	/**
	 * Fallback: scarica l'immagine e la carica su Shopify via stagedUploadsCreate.
	 * Questo aggira protezioni hotlink / blocchi UA sui file remoti.
	 */
	private function fileCreateByStagedUploadFromUrl(string $url): string
	{
		$tmp = tempnam(sys_get_temp_dir(), 'swatch_');
		if ($tmp === false) {
			throw new RuntimeException('Impossibile creare file temporaneo');
		}
		$bin = $this->httpGetBinary($url);
		file_put_contents($tmp, $bin);
		$size = filesize($tmp);
		if ($size === false || $size <= 0) {
			@unlink($tmp);
			throw new RuntimeException('Download texture fallito o vuoto');
		}
		$filename = basename(parse_url($url, PHP_URL_PATH) ?: 'swatch.jpg');
		$mime = $this->detectMime($tmp);
		if ($mime === '') $mime = 'image/jpeg';

		// 1) stagedUploadsCreate
		$qStage = <<<'GQL'
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets {
      url
      resourceUrl
      parameters { name value }
    }
    userErrors { field message }
  }
}
GQL;
		$vars = [
			'input' => [[
				'resource' => 'FILE',
				'filename' => $filename,
				'mimeType'  => $mime,
				'fileSize'  => (string)$size,
			]],
		];
		$r = $this->graphql($qStage, $vars);
		$ue = $r['data']['stagedUploadsCreate']['userErrors'] ?? [];
		if (!empty($ue)) {
			@unlink($tmp);
			throw new RuntimeException('stagedUploadsCreate userErrors: ' . json_encode($ue));
		}
		$target = $r['data']['stagedUploadsCreate']['stagedTargets'][0] ?? null;

		if (!$target || empty($target['url']) || empty($target['resourceUrl'])) {
			@unlink($tmp);
			throw new RuntimeException('stagedUploadsCreate: risposta inattesa: ' . json_encode($r));
		}

		// 2) Upload (multipart/form-data) verso URL firmato
		$this->multipartPostFile($target['url'], $target['parameters'] ?? [], $tmp);
		@unlink($tmp);

		// 3) fileCreate usando resourceUrl come originalSource
		$fileId = $this->fileCreateByOriginalSource((string)$target['resourceUrl']);
		$status = $this->getFileStatus($fileId);
		if ($status === 'FAILED') {
			throw new RuntimeException('fileCreate stagedUpload FAILED');
		}
		return $fileId;
	}

	private function httpGetBinary(string $url): string
	{
		$ch = curl_init($url);
		curl_setopt_array($ch, [
			CURLOPT_RETURNTRANSFER => true,
			CURLOPT_FOLLOWLOCATION => true,
			CURLOPT_TIMEOUT => 30,
			CURLOPT_CONNECTTIMEOUT => 10,
			CURLOPT_USERAGENT => 'Mozilla/5.0',
		]);
		$bin = curl_exec($ch);
		$code = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
		$err = curl_error($ch);
		curl_close($ch);
		if ($bin === false || $code >= 400) {
			throw new RuntimeException("HTTP GET failed code={$code} err={$err}");
		}
		return (string)$bin;
	}

	private function detectMime(string $path): string
	{
		if (function_exists('finfo_open')) {
			$f = finfo_open(FILEINFO_MIME_TYPE);
			if ($f) {
				$m = finfo_file($f, $path);
				finfo_close($f);
				return is_string($m) ? $m : '';
			}
		}
		return '';
	}

	private function multipartPostFile(string $url, array $params, string $filePath): void
	{
		$fields = [];
		foreach ($params as $p) {
			if (!isset($p['name'], $p['value'])) continue;
			$fields[(string)$p['name']] = (string)$p['value'];
		}
		$fields['file'] = new \CURLFile($filePath);
		$ch = curl_init($url);
		curl_setopt_array($ch, [
			CURLOPT_POST => true,
			CURLOPT_POSTFIELDS => $fields,
			CURLOPT_RETURNTRANSFER => true,
			CURLOPT_TIMEOUT => 60,
		]);
		$resp = curl_exec($ch);
		$code = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
		$err = curl_error($ch);
		curl_close($ch);
		if ($resp === false || $code >= 400) {
			throw new RuntimeException("staged upload POST failed code={$code} err={$err}");
		}
	}

	/**
	 * Imposta un metafield file_reference sulla variante.
	 * Default: namespace=swatch, key=texture
	 */
	private function setVariantTextureMetafield(int $variantId, string $fileGid, string $namespace = 'swatch', string $key = 'texture'): void
	{
		$ownerId = "gid://shopify/ProductVariant/{$variantId}";
		$q = <<<'GQL'
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
GQL;
		$vars = [
			'metafields' => [[
				'ownerId'   => $ownerId,
				'namespace' => $namespace,
				'key'       => $key,
				'type'      => 'file_reference',
				'value'     => $fileGid,
			]],
		];
		$res = $this->graphql($q, $vars);
		$ue = $res['data']['metafieldsSet']['userErrors'] ?? [];
		if (!empty($ue)) {
			throw new RuntimeException('metafieldsSet userErrors: ' . json_encode($ue));
		}
	}

	/**
	 * Applica le texture alle varianti leggendo un campo dal payload della migrazione.
	 * Convenzione: per ogni item in payload['variants'], se presente una chiave:
	 *  - _ps_texture_url (o _ps_texture) => URL pubblico dell'immagine texture
	 * allora viene creato/riusato un File Shopify e impostato su metafield file_reference della variante.
	 */
	private function applyVariantTextureMetafields(int $productId, array $payload, bool $debug = false, ?callable $say = null): void
	{
		$variants = $payload['variants'] ?? [];
		if (!is_array($variants) || !$variants) return;

		// Mappa SKU -> URL texture (solo per varianti che la hanno)
		$skuToTexture = [];
		foreach ($variants as $v) {
			$sku = (string)($v['sku'] ?? '');
			$tex = (string)($v['_ps_texture_url'] ?? ($v['_ps_texture'] ?? ''));
			$tex = trim($tex);
			if ($sku !== '' && $tex !== '') {
				$skuToTexture[$sku] = $tex;
			}
		}
		if (!$skuToTexture) return;

		// Recupera varianti fresche per avere ID numerici Shopify (necessari per ownerId gid)
		$fresh = $this->getProduct($productId);
		$shopVariants = $fresh['variants'] ?? [];
		$skuToVariantId = [];
		foreach ($shopVariants as $sv) {
			$sku = (string)($sv['sku'] ?? '');
			$vid = (int)($sv['id'] ?? 0);
			if ($sku !== '' && $vid > 0) $skuToVariantId[$sku] = $vid;
		}

		foreach ($skuToTexture as $sku => $texUrl) {
			$vid = $skuToVariantId[$sku] ?? 0;
			if ($vid <= 0) {
				if ($debug && $say) $say("  [SWATCH] skip sku={$sku}: variantId non trovato su Shopify");
				continue;
			}
			$fileGid = $this->ensureShopifyFileFromUrl($texUrl, $debug, $say);
			$this->setVariantTextureMetafield($vid, $fileGid);
			if ($debug && $say) $say("  [SWATCH] set metafield variantId={$vid} sku={$sku}");
		}
	}
	
	

    public function __construct(array $shopCfg, bool $trackInventory)
    {
        $this->cfg = $shopCfg;
        $this->trackInventory = $trackInventory;
        $this->sdk = new ShopifySDK([
            'ShopUrl'     => $shopCfg['ShopUrl'],
            'AccessToken' => $shopCfg['AccessToken'],
            'ApiVersion'  => $shopCfg['ApiVersion'],
        ]);
    }
	
	
	private function rest(string $method, string $path, array $body = null): array
	{
		// Throttle più stretto per inventory (Shopify: 2 req/sec per client su alcuni endpoint)
		$normPath = ltrim($path, '/');
		$isInventory = $this->startsWith($normPath, 'inventory_levels/');
		$minInterval = $isInventory ? $this->invMinIntervalSec : $this->restMinIntervalSec;
		if ($minInterval > 0) {
			$now = microtime(true);
			$last = $isInventory ? self::$lastInvCallTs : self::$lastRestCallTs;
			$next = $last + $minInterval;
			if ($now < $next) usleep((int)(($next - $now) * 1_000_000));
			$now2 = microtime(true);
			if ($isInventory) self::$lastInvCallTs = $now2; else self::$lastRestCallTs = $now2;
		}

		$url = "https://{$this->cfg['ShopUrl']}/admin/api/{$this->cfg['ApiVersion']}/" . $normPath;

		$attempt = 0;
		$delayMs = 800;

		while (true) {
			$respHeaders = [];
			$ch = curl_init($url);
			$headers = [
				"Content-Type: application/json",
				"Accept: application/json",
				"X-Shopify-Access-Token: {$this->cfg['AccessToken']}",
			];
			$opts = [
				CURLOPT_RETURNTRANSFER => true,
				CURLOPT_TIMEOUT        => 60,
				CURLOPT_HTTPHEADER     => $headers,
				CURLOPT_CUSTOMREQUEST  => strtoupper($method),
				// cattura headers per Retry-After / rate limit
				CURLOPT_HEADERFUNCTION => function($ch, $headerLine) use (&$respHeaders) {
					$len = strlen($headerLine);
					$parts = explode(':', $headerLine, 2);
					if (count($parts) === 2) {
						$k = strtolower(trim($parts[0]));
						$v = trim($parts[1]);
						$respHeaders[$k] = $v;
					}
					return $len;
				},
			];
			if ($body !== null) $opts[CURLOPT_POSTFIELDS] = json_encode($body);
			curl_setopt_array($ch, $opts);

			$resp = curl_exec($ch);
			if ($resp === false) {
				$err = curl_error($ch);
				curl_close($ch);
				throw new RuntimeException("REST cURL error: {$err}");
			}
			$status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
			curl_close($ch);

			$data = json_decode($resp, true);
			// Shopify a volte ritorna stringa non-JSON in errore: preserviamo raw
			if ($data === null && trim($resp) !== '' && json_last_error() !== JSON_ERROR_NONE) {
				$data = ['_raw' => $resp];
			}

			if ($status < 400) {
				return is_array($data) ? $data : [];
			}

			// Retry su 429 (rate limit) e su alcuni 5xx transitori
			$is429 = ($status === 429);
			$is5xx = ($status >= 500 && $status <= 599);

			if (($is429 || $is5xx) && $attempt < $this->maxRetries) {
				$attempt++;

				// se Shopify manda Retry-After, usiamolo
				$retryAfter = $respHeaders['retry-after'] ?? null;
				if ($retryAfter !== null && is_numeric($retryAfter)) {
					$sleepMs = max(0, (int)($retryAfter * 1000));
				} else {
					$sleepMs = $delayMs;
					$delayMs = min((int)($delayMs * 1.7), 8000);
				}

				usleep($sleepMs * 1000);
				continue;
			}

			$err = is_array($data) ? json_encode($data) : $resp;
			throw new RuntimeException("REST HTTP {$status} on {$method} {$normPath}: {$err}");
		}
	}

	

    // ---------- Utils ----------
    private static function slugify(string $s): string {
        $s = trim(mb_strtolower($s, 'UTF-8'));
        $s = preg_replace('~[^\pL0-9]+~u', '-', $s);
        $s = trim($s, '-');
        return $s === '' ? 'item' : $s;
    }
    private static function tick(): void { 
		//usleep(250000); 
		
	} // 0.25s
	
	
	private static function normalizeBarcode($s): ?string {
		$s = trim((string)$s);
		if ($s === '') return null;
		// niente formattazioni, solo cifre/lettere (Shopify accetta stringa libera)
		$s = preg_replace('~[^0-9A-Za-z]+~', '', $s);
		return $s !== '' ? $s : null;
	}

    // ---------- GraphQL ----------
    private function graphql(string $query, array $variables = []): array
    {
        // Facoltativo: throttle globale anche sul GraphQL
        if ($this->restMinIntervalSec > 0) {
            $now = microtime(true);
            $next = self::$lastRestCallTs + $this->restMinIntervalSec;
            if ($now < $next) usleep((int)(($next - $now) * 1_000_000));
            self::$lastRestCallTs = microtime(true);
        }

        $url = "https://{$this->cfg['ShopUrl']}/admin/api/{$this->cfg['ApiVersion']}/graphql.json";

        $attempt = 0;
        $delayMs = 800;

        while (true) {
            $respHeaders = [];
            $ch = curl_init($url);
            curl_setopt_array($ch, [
                CURLOPT_POST           => true,
                CURLOPT_HTTPHEADER     => [
                    "Content-Type: application/json",
                    "X-Shopify-Access-Token: {$this->cfg['AccessToken']}",
                ],
                CURLOPT_POSTFIELDS     => json_encode(['query'=>$query,'variables'=>$variables]),
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT        => 60,
                CURLOPT_HEADERFUNCTION => function($ch, $headerLine) use (&$respHeaders) {
                    $len = strlen($headerLine);
                    $parts = explode(':', $headerLine, 2);
                    if (count($parts) === 2) {
                        $k = strtolower(trim($parts[0]));
                        $v = trim($parts[1]);
                        $respHeaders[$k] = $v;
                    }
                    return $len;
                },
            ]);
            $resp = curl_exec($ch);
            if ($resp === false) {
                $err = curl_error($ch);
                curl_close($ch);
                throw new RuntimeException("GraphQL cURL error: {$err}");
            }
            $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
            curl_close($ch);

            $data = json_decode($resp, true);
            if ($status < 400) {
                // GraphQL può ritornare errors anche con 200
                if (isset($data['errors']) && is_array($data['errors']) && count($data['errors']) > 0) {
                    throw new RuntimeException("GraphQL errors: " . json_encode($data['errors']));
                }
                return $data;
            }

            $is429 = ($status === 429);
            $is5xx = ($status >= 500 && $status <= 599);

            if (($is429 || $is5xx) && $attempt < $this->maxRetries) {
                $attempt++;
                $retryAfter = $respHeaders['retry-after'] ?? null;
                if ($retryAfter !== null && is_numeric($retryAfter)) {
                    $sleepMs = max(0, (int)($retryAfter * 1000));
                } else {
                    $sleepMs = $delayMs;
                    $delayMs = min((int)($delayMs * 1.7), 8000);
                }
                usleep($sleepMs * 1000);
                continue;
            }

            throw new RuntimeException("GraphQL HTTP $status: $resp");
        }
    }

    public function findProductByHandle(string $handle): ?array
    {
        $q = <<<'GQL'
query($handle:String!) {
  productByHandle(handle:$handle) {
    id
    handle
    variants(first:250){ edges{ node{ id sku } } }
    images(first:250){ edges{ node{ src } } }
  }
}
GQL;
        $r = $this->graphql($q, ['handle'=>$handle]);
        $p = $r['data']['productByHandle'] ?? null;
        if (!$p) return null;

        $variants = [];
        foreach ($p['variants']['edges'] as $e) {
            $n = $e['node'];
            $variants[$n['sku'] ?: $n['id']] = $n['id'];
        }
        $images = [];
        foreach ($p['images']['edges'] as $e) $images[] = $e['node']['src'];

        return [
            'id'       => (int)preg_replace('~\D+~','',$p['id']),
            'handle'   => $p['handle'],
            'variants' => $variants,
            'images'   => $images,
        ];
    }
	
	// --- Helper: legge i digest per tutte le chiavi traducibili della risorsa ---
	private function fetchTranslatableDigests(string $gid): array
	{
		$q = <<<'GQL'
	query GetDigests($id: ID!) {
	  translatableResource(resourceId: $id) {
		resourceId
		translatableContent {
		  key
		  digest
		}
	  }
	}
	GQL;
		$res = $this->graphql($q, ['id' => $gid]);
		if (!empty($res['errors'])) {
			// raccogli i messaggi in modo leggibile
			$msgs = array_map(fn($e) => $e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (GetDigests): " . implode(' | ', $msgs));
		}
		$map = [];
		foreach (($res['data']['translatableResource']['translatableContent'] ?? []) as $row) {
			if (!empty($row['key']) && isset($row['digest'])) {
				$map[$row['key']] = $row['digest'];
			}
		}
		return $map;
	}

	public function registerProductTranslations(int $productId, string $locale, array $data, bool $debug = false): void
	{
		$gid = "gid://shopify/Product/{$productId}";

		// Traduco solo titolo e body
		$want = [];
		if (!empty($data['name']))        $want['title']     = $data['name'];
		if (!empty($data['description'])) $want['body_html'] = $data['description'];
		if (!empty($data['handle']))      $want['handle']    = $this->slugify((string)$data['handle']);
    
		if (!$want) return;

		$digests = $this->fetchTranslatableDigests($gid);

		$translations = [];
		foreach ($want as $key => $value) {
			$digest = $digests[$key] ?? null;
			if ($digest === null) {
				throw new \RuntimeException("Missing digest for key '{$key}' on {$gid}.");
			}
			$translations[] = [
				'key'                       => $key,
				'value'                     => $value,
				'locale'                    => $locale,
				'translatableContentDigest' => $digest,
			];
		}

		$mutation = <<<'GQL'
	mutation TranslationsRegister($id: ID!, $translations: [TranslationInput!]!) {
	  translationsRegister(resourceId: $id, translations: $translations) {
		userErrors { field message }
	  }
	}
	GQL;

		$res = $this->graphql($mutation, ['id' => $gid, 'translations' => $translations]);

		if (!empty($res['errors'])) {
			$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (translationsRegister): " . implode(' | ', $msgs));
		}
		$ue = $res['data']['translationsRegister']['userErrors'] ?? [];
		if ($ue) {
			$msgs = array_map(function($e){
				$f = isset($e['field']) ? implode('.', (array)$e['field']) : '';
				return ($f ? "{$f}: " : '') . ($e['message'] ?? 'unknown');
			}, $ue);
			throw new \RuntimeException("translationsRegister userErrors: " . implode(' | ', $msgs));
		}
	}


	
	private function createProduct(array $payload): array {
        $variantsCount = isset($payload['variants']) && is_array($payload['variants']) ? count($payload['variants']) : 0;
            return $this->createProductGraphQLLinked($payload);

    }

	

    private function isApiVersionAtLeast(string $min): bool
    {
        // formato atteso: YYYY-MM
        $cur = (string)($this->cfg['ApiVersion'] ?? '');
        $norm = function(string $v): int {
            if (!preg_match('~^(\d{4})-(\d{2})~', $v, $m)) return 0;
            return ((int)$m[1]) * 100 + (int)$m[2];
        };
        return $norm($cur) >= $norm($min);
    }



    // ---------- Linked options (Metaobjects + Metafields) ----------
/*
    private function slugKey(string $s): string
    {
        $s = trim(mb_strtolower($s, 'UTF-8'));
        $s = preg_replace('~[^\pL0-9]+~u', '_', $s);
        $s = trim($s, '_');
        return $s === '' ? 'item' : $s;
    }
	*/
	
	private function slugKey(string $s): string
	{
		$s = trim($s);

		// Traslittera in ASCII (Bouclé -> Boucle)
		$ascii = @iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $s);
		if ($ascii !== false) {
			$s = $ascii;
		}

		$s = strtolower($s);

		// spazi/underscore -> trattino
		$s = str_replace([' ', '_'], '_', $s);

		// lascia solo [a-z0-9-]
		$s = preg_replace('~[^a-z0-9-]+~', '_', $s);
		$s = preg_replace('~-+~', '_', $s);
		$s = trim($s, '-');

		return $s;
	}
	

    private function ensureMetaobjectDefinition(string $type, string $name): array
    {
        // Campi richiesti per lo swatch metaobject (come da UI Shopify):
        // - Etichetta (testo)
        // - Colore (color)
        // - Immagine (file)
        // - Color di base (list product taxonomy value)
        // - Pattern di base (product taxonomy value)
        // Nota: rendiamo obbligatoria solo "Etichetta" per non bloccare la creazione
        // quando il dato non e' disponibile lato Prestashop.
        $wanted = [
            [
                'name' => 'Etichetta',
                'key'  => 'etichetta',
                'type' => 'single_line_text_field',
                'required' => true,
            ],
            [
                'name' => 'Colore',
                'key'  => 'color',
                'type' => 'color',
                'required' => false,
            ],
            [
                'name' => 'Immagine',
                'key'  => 'image',
                'type' => 'file_reference',
                'required' => false,
				'validations' => [
					[
					  'name'  => 'file_type_options',
					  'value' => '["Image"]'
					]
				],
            ],
            // NOTE IMPORTANTE:
            // I tipi *product_taxonomy_value_reference* richiedono che nella definition
            // venga selezionato un *product taxonomy attribute* (impostazione store-specific).
            // In assenza di quell'ID, Shopify blocca la creazione con:
            // "Validations require that you select a product taxonomy attribute.".
            // Per mantenere la compatibilita' out-of-the-box, modelliamo questi due campi
            // come testo (potrai poi migrare a taxonomy_reference impostandolo manualmente
            // nello store, una volta noto l'attributo).
            [
                'name' => 'Color di base',
                'key'  => 'color_di_base',
                'type' => 'list.single_line_text_field',
                'required' => false,
            ],
            [
                'name' => 'Pattern di base',
                'key'  => 'pattern_di_base',
                'type' => 'single_line_text_field',
                'required' => false,
            ],
        ];

        $qGet = <<<'GQL'
query defByType($type: String!) {
  metaobjectDefinitionByType(type: $type) {
    id
    name
    type
    fieldDefinitions {
      name
      key
      required
      type { name }
    }
  }
}
GQL;
        $r = $this->graphql($qGet, ['type' => $type]);
        $def = $r['data']['metaobjectDefinitionByType'] ?? null;

        if ($def && !empty($def['id'])) {
            // Se esiste gia', proviamo ad aggiungere eventuali fieldDefinitions mancanti.
            $existing = [];
            foreach (($def['fieldDefinitions'] ?? []) as $fd) {
                $k = (string)($fd['key'] ?? '');
                if ($k !== '') $existing[$k] = $fd;
            }

            $missing = [];
            foreach ($wanted as $w) {
                if (!isset($existing[$w['key']])) {
                    $missing[] = $w;
                }
            }

            if ($missing) {
                // Shopify richiede in update l'elenco completo delle fieldDefinitions.
                $merged = [];
                // Manteniamo l'ordine: prima quelle esistenti, poi quelle nuove.
                foreach ($existing as $k => $fd) {
                    $merged[] = [
                        'name' => (string)($fd['name'] ?? $k),
                        'key'  => $k,
                        'type' => (string)($fd['type']['name'] ?? 'single_line_text_field'),
                        'required' => (bool)($fd['required'] ?? false),
                    ];
                }
                foreach ($missing as $m) {
                    $merged[] = $m;
                }

                $qUpd = <<<'GQL'
mutation defUpdate($id: ID!, $definition: MetaobjectDefinitionUpdateInput!) {
  metaobjectDefinitionUpdate(id: $id, definition: $definition) {
    metaobjectDefinition { id name type }
    userErrors { field message }
  }
}
GQL;
                $varsUpd = [
                    'id' => (string)$def['id'],
                    'definition' => [
                        'fieldDefinitions' => $merged,
                    ],
                ];
                $rUpd = $this->graphql($qUpd, $varsUpd);
                $ue = $rUpd['data']['metaobjectDefinitionUpdate']['userErrors'] ?? [];
                if ($ue) {
                    throw new RuntimeException('metaobjectDefinitionUpdate userErrors: ' . json_encode($ue));
                }
                $def2 = $rUpd['data']['metaobjectDefinitionUpdate']['metaobjectDefinition'] ?? null;
                if ($def2 && !empty($def2['id'])) {
                    return $def2;
                }
            }

            return $def;
        }

        $qCreate = <<<'GQL'
mutation defCreate($definition: MetaobjectDefinitionCreateInput!) {
  metaobjectDefinitionCreate(definition: $definition) {
    metaobjectDefinition { id name type }
    userErrors { field message }
  }
}
GQL;
        $vars = [
            'definition' => [
                'name' => $name,
                'type' => $type,
                'fieldDefinitions' => $wanted,
                // NB: NON impostare access.admin (consentito solo per app-reserved type)
                'access' => [
                    'storefront' => 'PUBLIC_READ',
                ],
            ],
        ];
        $r2 = $this->graphql($qCreate, $vars);
        $ue = $r2['data']['metaobjectDefinitionCreate']['userErrors'] ?? [];
        if ($ue) {
            throw new RuntimeException('metaobjectDefinitionCreate userErrors: ' . json_encode($ue));
        }
        $def2 = $r2['data']['metaobjectDefinitionCreate']['metaobjectDefinition'] ?? null;
        if (!$def2 || empty($def2['id'])) {
            throw new RuntimeException('metaobjectDefinitionCreate: risposta inattesa: ' . json_encode($r2));
        }
        return $def2;
    }

    private function ensureMetafieldDefinitionForOption(string $namespace, string $key, string $metaobjectDefinitionId): array
    {
        // NOTE: In recent Admin GraphQL versions, metafieldDefinition is typically fetched by ID.
        // To look up by namespace/key, use metafieldDefinitions with filters.
        $qGet = <<<'GQL'
query mfd($namespace: String!) {
  metafieldDefinitions(ownerType: PRODUCT, namespace: $namespace, first: 250) {
    edges {
      node {
        id
        name
        namespace
        key
        type { name }
      }
    }
  }
}
GQL;
        $r = $this->graphql($qGet, ['namespace' => $namespace]);
        foreach (($r['data']['metafieldDefinitions']['edges'] ?? []) as $e) {
            $node = $e['node'] ?? null;
            if ($node && ($node['key'] ?? null) === $key && ($node['namespace'] ?? null) === $namespace) {
                return $node;
            }
        }

        $qCreate = <<<'GQL'
mutation mfdCreate($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition { id namespace key type { name } }
    userErrors { field message }
  }
}
GQL;
        $vars = [
            'definition' => [
                'name' => $key,
                'namespace' => $namespace,
                'key' => $key,
                'ownerType' => 'PRODUCT',
                'type' => 'list.metaobject_reference',
                'validations' => [
                    [
                        'name' => 'metaobject_definition_id',
                        'value' => $metaobjectDefinitionId,
                    ],
                ],
            ],
        ];
        $r2 = $this->graphql($qCreate, $vars);
        $ue = $r2['data']['metafieldDefinitionCreate']['userErrors'] ?? [];
        if ($ue) {
            throw new RuntimeException('metafieldDefinitionCreate userErrors: ' . json_encode($ue));
        }
        $out = $r2['data']['metafieldDefinitionCreate']['createdDefinition'] ?? null;
        if (!$out || empty($out['id'])) {
            throw new RuntimeException('metafieldDefinitionCreate: risposta inattesa: ' . json_encode($r2));
        }
        return $out;
    }

    private function ensureMetaobjectEntry(string $type, string $handle, string $label, array $meta): string
    {
        $qGet = <<<'GQL'
query moByHandle($handle: MetaobjectHandleInput!) {
  metaobjectByHandle(handle: $handle) { id }
}
GQL;
        $r = $this->graphql($qGet, ['handle' => ['type' => $type, 'handle' => $handle]]);
        $mo = $r['data']['metaobjectByHandle'] ?? null;
        if ($mo && !empty($mo['id'])) return (string)$mo['id'];

        $qCreate = <<<'GQL'
mutation moCreate($metaobject: MetaobjectCreateInput!) {
  metaobjectCreate(metaobject: $metaobject) {
    metaobject { id }
    userErrors { field message }
  }
}
GQL;
        // NB: i nomi dei campi devono combaciare con le fieldDefinitions create/aggiornate.
        // Scriviamo solo i campi per cui abbiamo un valore.
        $fields = [
            ['key' => 'etichetta', 'value' => $label],
        ];

        $color = (string)($meta['color'] ?? '');
        $color = trim($color);
        if ($color !== '') {
            $fields[] = ['key' => 'colore', 'value' => $color];
        }

        $texUrl = (string)($meta['texture_url'] ?? '');
        $texUrl = trim($texUrl);
        if ($texUrl !== '') {
            // Carichiamo il file su Shopify e usiamo un file_reference.
            $fileGid = $this->ensureShopifyFileFromUrl($texUrl);
            $fields[] = ['key' => 'image', 'value' => $fileGid];
        }

        // Taxonomy fields (se in futuro verranno popolati lato Prestashop/mapping).
        $baseColors = $meta['color_di_base'] ?? null;
        if (is_array($baseColors) && !empty($baseColors)) {
            // list.product_taxonomy_value_reference vuole una lista JSON di GID
            $fields[] = ['key' => 'color_di_base', 'value' => json_encode(array_values($baseColors), JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE)];
        }
        $basePattern = (string)($meta['pattern_di_base'] ?? '');
        $basePattern = trim($basePattern);
        if ($basePattern !== '') {
            $fields[] = ['key' => 'pattern_di_base', 'value' => $basePattern];
        }
        $vars = [
            'metaobject' => [
                'type' => $type,
                'handle' => $handle,
                'fields' => $fields,
            ],
        ];
        $r2 = $this->graphql($qCreate, $vars);
        $ue = $r2['data']['metaobjectCreate']['userErrors'] ?? [];
        if ($ue) {
            throw new RuntimeException('metaobjectCreate userErrors: ' . json_encode($ue));
        }
        $id = $r2['data']['metaobjectCreate']['metaobject']['id'] ?? '';
        if (!$id) {
            throw new RuntimeException('metaobjectCreate: risposta inattesa: ' . json_encode($r2));
        }
        return (string)$id;
    }

    private function createProductGraphQLLinked(array $payload): array
    {
        if (!$this->isApiVersionAtLeast('2024-04')) {
            throw new RuntimeException("Linked options richiedono GraphQL, ma ApiVersion={$this->cfg['ApiVersion']} (<2024-04). Imposta ApiVersion >= 2024-04.");
        }

        $optionMeta = $payload['_ps_option_meta'] ?? [];
        
		/*
		if (!is_array($optionMeta) || !$optionMeta) {
            // fallback
            return $this->createProductGraphQL($payload);
        }
		*/

        // 1) Crea prodotto base.
        // Importante: se non specifichiamo opzioni in creazione, Shopify crea di default l'opzione "Title"
        // con una variante "Default Title". Con linked options questo spesso impedisce/complica la creazione
        // delle varianti reali (Shopify si aspetta anche l'opzione "Title").
        // Strategia: creiamo il prodotto, rileviamo la variante di default e la eliminiamo dopo aver creato
        // le opzioni reali, prima di eseguire productVariantsBulkCreate.
		
		
		
        $productInput = [
            'title'       => $payload['title'] ?? 'Item',
            'handle'      => $payload['handle'] ?? null,
            'vendor'      => $payload['vendor'] ?? null,
            'productType' => $payload['product_type'] ?? null,
            'tags'        => isset($payload['tags']) ? (is_array($payload['tags']) ? implode(', ', $payload['tags']) : (string)$payload['tags']) : null,
            'descriptionHtml' => $payload['body_html'] ?? null,
			'category' => $this->cfg['categoryDefault'],
        ];
        $productInput = array_filter($productInput, fn($v) => $v !== null && $v !== '');

        $qCreate = <<<'GQL'
mutation productCreate($input: ProductInput!) {
  productCreate(input: $input) {
    product { id handle legacyResourceId }
    userErrors { field message }
  }
}
GQL;
        $rCreate = $this->graphql($qCreate, ['input' => $productInput]);
		
		
		
        $errs = $rCreate['data']['productCreate']['userErrors'] ?? [];
        if ($errs) {
            throw new RuntimeException('GraphQL productCreate userErrors: ' . json_encode($errs));
        }
        $pNode = $rCreate['data']['productCreate']['product'] ?? null;
        if (!$pNode || empty($pNode['id'])) {
            throw new RuntimeException('GraphQL productCreate: risposta inattesa: ' . json_encode($rCreate));
        }
        $productGid = (string)$pNode['id'];
		
		
		// --- Metafield ps_id (Prestashop product id) in creazione ---
		$psId = $payload['ps_id'] ?? null;
		if ($psId !== null && $psId !== '') {
			$this->metafieldsSet($productGid, [[
				'namespace' => 'prestashop',
				'key'       => 'ps_id',
				'type'      => 'single_line_text_field',
				'value'     => (string)$psId,
			]]);
		}
		
		$psDescriptionShort = $payload['description_short'] ?? null;

		if ($psDescriptionShort !== null && $psDescriptionShort !== '') {
			$this->metafieldsSet($productGid, [[
				'namespace' => 'custom',
				'key'       => 'description_short',
				'type'      => 'multi_line_text_field',
				'value'     => strip_tags((string)$psDescriptionShort),
			]]);
		}
		
		
		// --- Metafield ps_id (Prestashop product id) in creazione ---
		$psReference = $payload['reference'] ?? null;
		if ($psReference !== null && $psReference !== '') {
			$this->metafieldsSet($productGid, [[
				'namespace' => 'prestashop',
				'key'       => 'ps_reference',
				'type'      => 'single_line_text_field',
				'value'     => (string)$psReference,
			]]);
		}
		
		//modifica carlo pubblico i prodotti
		$this->publishProductToChannel(
			$productGid,
			$this->cfg['onlineStorePublicationId']
		);
		
		
		
        $handle = $pNode['handle'] ?? ($payload['handle'] ?? null);
        $productLegacyId = (int)($pNode['legacyResourceId'] ?? 0);

        // Legge la variante di default (se presente) per eliminarla più avanti.
        $defaultVariantGid = null;
        try {
            $qDefV = <<<'GQL'
query pv($id: ID!) {
  product(id: $id) {
    variants(first: 5) {
      edges { node { id title sku legacyResourceId } }

    }
  }
}
GQL;
            $rDefV = $this->graphql($qDefV, ['id' => $productGid]);
            $edges = $rDefV['data']['product']['variants']['edges'] ?? [];
            if (count($edges) === 1) {
                $n = $edges[0]['node'] ?? null;
                if ($n && ($n['title'] ?? '') === 'Default Title') {
                    $defaultVariantGid = (string)$n['id'];
                }
            }
        } catch (\Throwable $e) {
            // non fatale
            $defaultVariantGid = null;
        }
		
		// Se NON ci sono combinazioni/opzioni, resta solo la variante Default Title.
		// In questo caso dobbiamo aggiornare la variante di default con SKU (e altri campi) manualmente.
		if (empty($optionMeta)) {
			// Prendiamo i dati dalla prima "variant" del payload (se presente)
			$v0 = $payload['variants'][0] ?? [];

			$sku = (string)($v0['sku'] ?? ($payload['reference'] ?? ''));
			$sku = trim($sku);

			// Se non c'è SKU non facciamo nulla
			if ($sku !== '') {
				// Se hai già letto legacyResourceId nella query pv, prendilo da lì:
				$defaultLegacyVariantId = 0;
				try {
					$rDefV = $this->graphql($qDefV, ['id' => $productGid]);
					$edges = $rDefV['data']['product']['variants']['edges'] ?? [];
					if (count($edges) === 1) {
						$n = $edges[0]['node'] ?? null;
						if ($n && ($n['title'] ?? '') === 'Default Title') {
							$defaultLegacyVariantId = (int)($n['legacyResourceId'] ?? 0);
						}
					}
				} catch (\Throwable $e) {}

				if ($defaultLegacyVariantId > 0) {
					$fields = [
						'sku' => $sku,
					];
					if (isset($v0['price'])) $fields['price'] = (string)$v0['price'];
					if (array_key_exists('barcode', $v0)) $fields['barcode'] = (string)($v0['barcode'] ?? '');
					// se vuoi forzare tracking inventario:
					$fields['inventory_management'] = 'shopify';
					$fields['inventory_policy'] = 'continue'; // <-- QUESTO

					$this->updateVariant($defaultLegacyVariantId, $fields);
				}
			}

			// Return subito: niente linked options / bulk create
			return [
				'id' => $productLegacyId ?: 0,
				'handle' => $handle,
				'variants' => [
					[
						'id' => $defaultLegacyVariantId ?? 0,
						'sku' => $sku ?: null,
						'inventory_item_id' => 0, // se ti serve lo puoi rileggere con una query successiva
					]
				],
			];
		}


        // 2) Per ogni opzione: definizione metaobject + metafield definition + entries
        // Shopify richiede namespace di almeno 3 caratteri.
        // Usiamo un namespace dedicato alla migrazione PrestaShop.
        $namespace = 'psm';
        $valueGidByOption = []; // optionName => valueText => metaobjectGid
        $linkedMetafieldByOption = []; // optionName => ['namespace'=>..,'key'=>..]

        foreach ($optionMeta as $optName => $valuesMeta) {
            if (!is_array($valuesMeta) || !$valuesMeta) continue;
            $optName = (string)$optName;
            if ($optName === '' || $optName === 'Title') continue;

            $key = $this->slugKey($optName);
            $type = 'ps_' . $key;
			
			$namespace = 'psm';

			// key unica per vendor + option (evita collisioni tipo psm.voltage per vendor diversi)
			$vendorKey = $this->slugKey((string)($payload['vendor'] ?? 'vendor'));
			$optKey    = $this->slugKey($optName);
			$key_metafield = $vendorKey . '_' . $optKey;

			// metaobject TYPE per vendor (va bene come avevi fatto)
			$type = $vendorKey;

			// definizione metaobject (type = vendorKey)
			$def = $this->ensureMetaobjectDefinition($type, $vendorKey);
			$defId = (string)$def['id'];

			// metafield definition: psm.<vendor_opt> -> list.metaobject_reference validato sul defId
			$this->ensureMetafieldDefinitionForOption($namespace, $key_metafield, $defId);

			// IMPORTANTISSIMO: salva la key giusta (non $key vecchio)
			$linkedMetafieldByOption[$optName] = ['namespace' => $namespace, 'key' => $key_metafield];

            $valueGidByOption[$optName] = [];

            foreach ($valuesMeta as $valName => $meta) {
                $valName = (string)$valName;
                $idAttr = is_array($meta) ? (int)($meta['id_attribute'] ?? 0) : 0;
                $handleMo = $this->slugKey($valName) . ($idAttr > 0 ? '_' . $idAttr : '');
                $gid = $this->ensureMetaobjectEntry($type, $handleMo, $valName, is_array($meta) ? $meta : []);
                $valueGidByOption[$optName][$valName] = $gid;
            }
        }

        // 2.5) Popola i metafield (list.metaobject_reference) sul prodotto.
        // Shopify, quando crei un'opzione "linked" a un metafield, richiede che
        // il metafield abbia gia' dei valori sul prodotto oppure che tali valori
        // vengano passati nella stessa mutation. Noi li settiamo prima, usando
        // gli stessi GID dei metaobject creati sopra.
        // Questo evita l'errore:
        // "The 'psm.<key>' metafield has no values and none were passed in for this linked option."
        $mfEntries = [];
        foreach ($linkedMetafieldByOption as $optName => $lm) {
            $valsMap = $valueGidByOption[$optName] ?? [];
            if (!$valsMap) continue;
            $gids = array_values($valsMap);
            // deduplica
            $gids = array_values(array_unique(array_filter($gids, fn($x) => is_string($x) && $x !== '')));
            if (!$gids) continue;
            $mfEntries[] = [
                'namespace' => (string)$lm['namespace'],
                'key'       => (string)$lm['key'],
                'type'      => 'list.metaobject_reference',
                'value'     => json_encode($gids, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
            ];
        }
        if ($mfEntries) {
            $this->metafieldsSet($productGid, $mfEntries);
        }

        // 3) Crea opzioni prodotto collegate
        // NOTA: chiediamo anche le optionValues generate, per poter mappare subito gli ID.
        // Alcuni store NON espongono linkedMetafieldValue sulle optionValues; in quel caso
        // useremo un mapping per posizione, basato sull'ordine dei GID che abbiamo appena
        // settato nel metafield list.metaobject_reference.
        $qOptsLinked = <<<'GQL'
mutation productOptionsCreate($productId: ID!, $options: [OptionCreateInput!]!) {
  productOptionsCreate(productId: $productId, options: $options) {
    product {
      id
      options {
        id
        name
        optionValues {
          id
          name
          linkedMetafieldValue
        }
      }
    }
    userErrors { field message }
  }
}
GQL;
        // Fallback se lo schema dello store non espone linkedMetafieldValue.
        $qOptsNoLinked = <<<'GQL'
mutation productOptionsCreate($productId: ID!, $options: [OptionCreateInput!]!) {
  productOptionsCreate(productId: $productId, options: $options) {
    product {
      id
      options {
        id
        name
        optionValues {
          id
          name
        }
      }
    }
    userErrors { field message }
  }
}
GQL;
        $optsInput = [];
        // manteniamo ordine da payload['options']
        foreach (($payload['options'] ?? []) as $o) {
            $optName = (string)($o['name'] ?? '');
            if ($optName === '' || $optName === 'Title') continue;

            if (isset($linkedMetafieldByOption[$optName])) {
                $lm = $linkedMetafieldByOption[$optName];
				// IMPORTANT:
				// Per le opzioni collegate a metafield/metaobject, Shopify NON consente di
				// creare esplicitamente i values con productOptionsCreate ("Cannot combine linked metafield and option values").
				// I valori verranno creati implicitamente quando creiamo le varianti con
				// productVariantsBulkCreate passando linkedMetafieldValue.
				$optsInput[] = [
					'name' => $optName,
					'linkedMetafield' => $lm,
				];
            } else {
                // fallback non-linked
                $vals = [];
                foreach (($o['values'] ?? []) as $v) $vals[] = ['name' => (string)$v];
                $optsInput[] = [
                    'name' => $optName,
                    'values' => $vals,
                ];
            }
        }

        $rOpts = null;
        if ($optsInput) {
            $rOpts = $this->graphql($qOptsLinked, ['productId' => $productGid, 'options' => $optsInput]);
            if (!empty($rOpts['errors'])) {
                // Probabile errore di schema: riprova senza linkedMetafieldValue.
                $rOpts = $this->graphql($qOptsNoLinked, ['productId' => $productGid, 'options' => $optsInput]);
            }
            if (!empty($rOpts['errors'])) {
                throw new RuntimeException('productOptionsCreate GraphQL errors: ' . json_encode($rOpts['errors']));
            }
            $ue = $rOpts['data']['productOptionsCreate']['userErrors'] ?? [];
            if ($ue) {
                throw new RuntimeException('productOptionsCreate userErrors: ' . json_encode($ue));
            }
        }

        // 3.25) Dopo la creazione delle opzioni, leggiamo gli optionValues reali generati da Shopify.
        // Per le opzioni collegate (linkedMetafield) NON possiamo passare "name" in productVariantsBulkCreate
        // (Shopify risponde: "Cannot set name for an option value linked to a metafield").
        // Quindi creiamo le varianti indicando l'ID dell'option value.
        // Per opzioni linked vogliamo mappare: optionName => metaobjectGid(linkedMetafieldValue) => optionValueId
        // Per opzioni non-linked manteniamo: optionName => valueName => optionValueId
        $optionValueIdByOption = [];
        $optionValueNameByOption = []; // optionName => metaGid(or name) => optionValueName
        $optionIdByName = [];
        try {
            // Costruiamo il mapping usando la risposta di productOptionsCreate.
            // Nota: a seconda dello schema/API version, `product.options` puo' essere una lista semplice
            // (array di ProductOption) oppure una connection (edges/nodes). Gestiamo entrambi.
            $optContainer = $rOpts['data']['productOptionsCreate']['product']['options'] ?? [];
            $optList = [];
            if (is_array($optContainer) && array_key_exists('edges', $optContainer)) {
                foreach (($optContainer['edges'] ?? []) as $oe) {
                    if (!empty($oe['node'])) $optList[] = $oe['node'];
                }
            } elseif (is_array($optContainer)) {
                $optList = $optContainer;
            }

            foreach ($optList as $on) {
                if (!is_array($on)) continue;
                $oName = (string)($on['name'] ?? '');
                if ($oName === '') continue;
                if (!empty($on['id'])) $optionIdByName[$oName] = (string)$on['id'];

                $valsContainer = $on['optionValues'] ?? [];
                $valsList = [];
                if (is_array($valsContainer) && array_key_exists('edges', $valsContainer)) {
                    foreach (($valsContainer['edges'] ?? []) as $ve) {
                        if (!empty($ve['node'])) $valsList[] = $ve['node'];
                    }
                } elseif (is_array($valsContainer)) {
                    $valsList = $valsContainer;
                }

                $idsInOrder = [];
                foreach ($valsList as $vn) {
                    if (!is_array($vn)) continue;
                    $vId = (string)($vn['id'] ?? '');
                    if ($vId === '') continue;
                    $vName = (string)($vn['name'] ?? '');
                    $lmv = (string)($vn['linkedMetafieldValue'] ?? '');
                    $idsInOrder[] = ['id' => $vId, 'name' => $vName, 'lmv' => $lmv];

                    if ($lmv !== '') {
                        $optionValueIdByOption[$oName][$lmv] = $vId;
                        if ($vName !== '') $optionValueNameByOption[$oName][$lmv] = $vName;
                    } elseif ($vName !== '') {
                        $optionValueIdByOption[$oName][$vName] = $vId;
                        $optionValueNameByOption[$oName][$vName] = $vName;
                    }
                }

                // Fallback per opzioni linked: se linkedMetafieldValue non e' disponibile nello schema,
                // mappiamo per posizione usando l'ordine dei GID impostati nel metafield list.
                if (isset($linkedMetafieldByOption[$oName])) {
                    $gids = array_values($valueGidByOption[$oName] ?? []);
                    $gids = array_values(array_unique(array_filter($gids, fn($x) => is_string($x) && $x !== '')));
                    if ($gids && count($gids) === count($idsInOrder)) {
                        for ($i = 0; $i < count($gids); $i++) {
                            $gid = (string)$gids[$i];
                            $vid = (string)($idsInOrder[$i]['id'] ?? '');
                            $vnm = (string)($idsInOrder[$i]['name'] ?? '');
                            if ($gid !== '' && $vid !== '') {
                                $optionValueIdByOption[$oName][$gid] = $vid;
                                if ($vnm !== '') $optionValueNameByOption[$oName][$gid] = $vnm;
                            }
                        }
                    }
                }
            }
        } catch (\Throwable $e) {
            $optionValueIdByOption = [];
        }

        // [DBG] Option / OptionValues reali restituiti da Shopify dopo productOptionsCreate
        $this->dbg('OPTIONS after productOptionsCreate optionIdByName', $optionIdByName);
        $this->dbg('OPTIONS after productOptionsCreate optionValueIdByOption', $optionValueIdByOption);

        // Se le opzioni sono collegate a metafield, Shopify richiede che i valori linked esistano
        // sull'opzione PRIMA di poterli usare nelle varianti. In alcuni flussi (es. creazione prodotto via REST)
        // i valori potrebbero non risultare presenti subito. In tal caso li aggiungiamo con productOptionUpdate.
        if (!empty($linkedMetafieldByOption) && !empty($optionIdByName)) {
            $missingByOption = [];
            foreach ($linkedMetafieldByOption as $optName => $_lm) {
                $valsMap = $valueGidByOption[$optName] ?? [];
                foreach ($valsMap as $valName => $gid) {
                    $gid = (string)$gid;
                    if ($gid === '') continue;
                    if (empty($optionValueIdByOption[$optName][$gid])) {
                        $missingByOption[$optName][$gid] = true;
                    }
                }
            }

            foreach ($missingByOption as $optName => $gidsSet) {
                $optId = $optionIdByName[$optName] ?? null;
                if (!$optId) continue;
                $toAdd = [];
                foreach (array_keys($gidsSet) as $gid) {
                    $toAdd[] = ['linkedMetafieldValue' => $gid];
                }
                if (!$toAdd) continue;

                $qUpd = <<<'GQL'
mutation AddLinkedOptionValues($productId: ID!, $optionId: ID!, $optionValuesToAdd: [OptionValueCreateInput!]!) {
  productOptionUpdate(productId: $productId, option: { id: $optionId }, optionValuesToAdd: $optionValuesToAdd) {
    product {
      options {
        id
        name
        linkedMetafield { namespace key }
        optionValues {
          id
          name
          linkedMetafieldValue
        }
      }
    }
    userErrors { field message }
  }
}
GQL;

                $rUpd = $this->graphql($qUpd, [
                    'productId' => $productGid,
                    'optionId' => $optId,
                    'optionValuesToAdd' => $toAdd,
                ]);

                $ue = $rUpd['data']['productOptionUpdate']['userErrors'] ?? [];
                if (!empty($ue)) {
                    throw new RuntimeException('[createProduct] productOptionUpdate userErrors: ' . json_encode($ue));
                }

                // Aggiorna mapping da risposta
                $optsList = $rUpd['data']['productOptionUpdate']['product']['options'] ?? [];
                foreach ($optsList as $o) {
                    $n = (string)($o['name'] ?? '');
                    if ($n !== $optName) continue;
                    $optionIdByName[$n] = (string)($o['id'] ?? $optId);
                    foreach (($o['optionValues'] ?? []) as $ov) {
                        $ovId = (string)($ov['id'] ?? '');
                        $lmv = (string)($ov['linkedMetafieldValue'] ?? '');
                        $nm = (string)($ov['name'] ?? '');
                        if ($ovId === '') continue;
                        if ($lmv !== '') {
                            $optionValueIdByOption[$n][$lmv] = $ovId;
                            if ($nm !== '') $optionValueNameByOption[$n][$lmv] = $nm;
                        } elseif ($nm !== '') {
                            $optionValueIdByOption[$n][$nm] = $ovId;
                            $optionValueNameByOption[$n][$nm] = $nm;
                        }
                    }
                }

                // [DBG] Mapping dopo productOptionUpdate per l'opzione corrente
                $this->dbg('OPTIONS after productOptionUpdate option=' . $optName . ' optionId=' . $optId, [
                    'optionIdByName' => $optionIdByName[$optName] ?? null,
                    'optionValueIdByOption' => $optionValueIdByOption[$optName] ?? [],
                    'optionValueNameByOption' => $optionValueNameByOption[$optName] ?? [],
                ]);
            }
        }



        // 4) Crea varianti bulk usando linkedMetafieldValue per le opzioni collegate
        $variants = $payload['variants'] ?? [];
        $variantInputs = [];
        // Mappa posizione option1/2/3 -> nome opzione, SENZA buchi.
        // (Se in payload compare "Title" o opzioni vuote, l'indicizzazione basata su $idx+1
        // può generare gap e quindi varianti senza optionValues -> Shopify crea 0 varianti senza userErrors chiari.)
        $optionNamesByIndex = [];
        $pos = 1;
        foreach (($payload['options'] ?? []) as $opt) {
            $name = (string)($opt['name'] ?? '');
            if ($name === '' || $name === 'Title') continue;
            $optionNamesByIndex[$pos] = $name;
            $pos++;
            if ($pos > 3) break;
        }

        // [DBG] Mappa posizioni -> nomi opzioni realmente usate
        $this->dbg('createProductGraphQLLinked optionNamesByIndex', $optionNamesByIndex);

        // Se Shopify ha gia' generato automaticamente le varianti dopo la creazione/aggiornamento delle opzioni
        // (comportamento comune quando si aggiungono option values), evitiamo productVariantsBulkCreate
        // e aggiorniamo invece le varianti esistenti (SKU, price, barcode, taxable).
        $existingVariantsByKey = []; // key -> legacyVariantId
        try {
            $qExist = <<<'GQL'
query existingVariants($id: ID!) {
  product(id: $id) {
    variants(first: 250) {
      edges {
        node {
          legacyResourceId
          selectedOptions { name value }
        }
      }
    }
  }
}
GQL;
            $rExist = $this->graphql($qExist, ['id' => $productGid]);
            $edges = $rExist['data']['product']['variants']['edges'] ?? [];
            $this->dbg('existingVariants query edges.count=' . count($edges));
            foreach ($edges as $e) {
                $n = $e['node'] ?? null;
                if (!$n) continue;
                $legacyVid = (int)($n['legacyResourceId'] ?? 0);
                if ($legacyVid <= 0) continue;
                $sel = $n['selectedOptions'] ?? [];

                // [DBG] Varianti esistenti (id + selectedOptions)
                $this->dbg('EXIST variant legacyResourceId=' . $legacyVid, $sel);

                $selMap = [];
                foreach ($sel as $so) {
                    $sn = (string)($so['name'] ?? '');
                    $sv = (string)($so['value'] ?? '');
                    if ($sn !== '') $selMap[mb_strtolower(trim($sn))] = trim($sv);
                }
                $parts = [];
                for ($i=1; $i<=3; $i++) {
                    $on = $optionNamesByIndex[$i] ?? null;
                    if (!$on) continue;
                    $val = $selMap[mb_strtolower(trim($on))] ?? '';
                    $parts[] = mb_strtolower(trim($val));
                }
                $key = implode('||', $parts);
                if ($key !== '') $existingVariantsByKey[$key] = $legacyVid;

                // [DBG] Chiave calcolata per variante esistente
                $this->dbg('EXIST key=' . $key . ' -> legacyVid=' . $legacyVid);
            }
        } catch (\Throwable $e) {
            $existingVariantsByKey = [];
            $this->dbg('existingVariants query FAILED: ' . $e->getMessage());
        }

        // [DBG] Riepilogo varianti esistenti
        $this->dbg('existingVariantsByKey.count=' . count($existingVariantsByKey));

        foreach ($variants as $v) {
            $inv = [
                'tracked' => true,
                // SKU va impostato su InventoryItemInput (sku su ProductVariantsBulkInput non esiste)
                'sku' => $v['sku'] ?? null,
                'requiresShipping' => isset($v['requires_shipping']) ? (bool)$v['requires_shipping'] : true,
            ];
            // Peso: in GraphQL moderno il peso sta su inventoryItem.measurement.weight
            $grams = isset($v['grams']) ? (float)$v['grams'] : null;
            if ($grams !== null && $grams > 0) {
                $inv['measurement'] = ['weight' => ['unit' => 'GRAMS', 'value' => $grams]];
            } elseif (isset($v['weight']) && $v['weight'] !== null && $v['weight'] !== '') {
                $inv['measurement'] = ['weight' => ['unit' => 'KILOGRAMS', 'value' => (float)$v['weight']]];
            }

            $vi = [
                'barcode'  => $v['barcode'] ?? null,
                'price'    => isset($v['price']) ? (string)$v['price'] : null,
                'compareAtPrice' => isset($v['compare_at_price']) ? (string)$v['compare_at_price'] : null,
                'taxable'  => isset($v['taxable']) ? (bool)$v['taxable'] : null,
                'inventoryPolicy' => $this->cfg['inventoryPolicy'],
                'inventoryItem' => $inv,
            ];
			
			
			

            // Se abbiamo gia' la variante esistente, aggiorniamo via REST e saltiamo la creazione GraphQL.
            //if (!empty($existingVariantsByKey) && $productLegacyId > 0) {
            
			
			if (!empty($existingVariantsByKey) && $productLegacyId > 0) {
                $parts = [];
                for ($i=1; $i<=3; $i++) {
                    $optName = $optionNamesByIndex[$i] ?? null;
                    if (!$optName) continue;
                    $val = (string)($v['option'.$i] ?? '');
                    if ($val === '') { $parts[] = ''; continue; }
                    if (isset($valueGidByOption[$optName][$val])) {
                        $metaGid = (string)$valueGidByOption[$optName][$val];
                        $disp = (string)($optionValueNameByOption[$optName][$metaGid] ?? $val);
                        $parts[] = mb_strtolower(trim($disp));
                    } else {
                        $parts[] = mb_strtolower(trim($val));
                    }
                }
                $k = implode('||', $parts);
                $legacyVid = $existingVariantsByKey[$k] ?? null;

                // [DBG] Chiave calcolata per variante desiderata + esito match
                $this->dbg('WANT key=' . $k . ' sku=' . (string)($v['sku'] ?? '') . ' found=' . ($legacyVid ? 'YES' : 'NO'));
                if ($legacyVid) {
                    try {
                        $restFields = [];
                        if (isset($v['sku'])) $restFields['sku'] = (string)$v['sku'];
                        if (isset($v['price'])) $restFields['price'] = (string)$v['price'];
                        if (array_key_exists('barcode', $v)) $restFields['barcode'] = $v['barcode'];
                        if (array_key_exists('taxable', $v)) $restFields['taxable'] = (bool)$v['taxable'];
                        // inventory_policy su REST e' "deny"/"continue"
                        if (isset($v['inventory_policy'])) $restFields['inventory_policy'] = (string)$v['inventory_policy'];
                        $restFields = array_filter($restFields, fn($x) => $x !== null);
                        if ($restFields) {
                            $this->updateVariant( (int)$legacyVid, $restFields);
                        }
                    } catch (\Throwable $e) {
						
						echo 'XXX non si aggiorna: ' . $e->getMessage();
						
                        // best-effort: se non riusciamo ad aggiornare, tentiamo comunque la creazione GraphQL
                    }
                    // rimuovi dalla mappa per non aggiornarla piu' volte
                 //   unset($existingVariantsByKey[$k]);
                 //   continue;
                }
            }
			
			
            $optVals = [];
            $keyParts = [];
            for ($i=1; $i<=3; $i++) {
                $optName = $optionNamesByIndex[$i] ?? null;
                if (!$optName) continue;
                $val = (string)($v['option'.$i] ?? '');
                if ($val === '') continue;
                if (isset($valueGidByOption[$optName][$val])) {
                    // Opzione collegata: NON possiamo impostare "name"; dobbiamo passare l'ID dell'option value.
                    // L'ID viene ricavato leggendo le optionValues generate da Shopify dopo productOptionsCreate.
                    $metaGid = (string)$valueGidByOption[$optName][$val];
                    $ovId = $optionValueIdByOption[$optName][$metaGid] ?? null;
                    if (!$ovId && !empty($optionValueIdByOption[$optName])) {
                        // fallback case-insensitive / trim sul nome (per store dove linkedMetafieldValue non e' esposto)
                        $k = mb_strtolower(trim($val));
                        foreach ($optionValueIdByOption[$optName] as $n => $idTmp) {
                            if (mb_strtolower(trim((string)$n)) === $k) { $ovId = $idTmp; break; }
                        }
                    }
                    if ($ovId) {
                        $optVals[] = [
                            'optionName' => $optName,
                            'id' => $ovId,
                        ];
                        $disp = $optionValueNameByOption[$optName][$metaGid] ?? $val;
                        $keyParts[] = mb_strtolower(trim((string)$disp));
                    } else {
                        // Non possiamo impostare "name" su option values linked; se non troviamo l'id,
                        // fermiamoci con un errore esplicito (cosi' vediamo subito il mapping mancante).
                        throw new RuntimeException("Linked optionValue id non trovato per option='" . $optName . "' value='" . $val . "' metaGid='" . $metaGid . "'. Verifica che productOptionsCreate abbia generato i valori e che il mapping linkedMetafieldValue sia disponibile.");
                    }
                } else {
                    $optVals[] = ['optionName' => $optName, 'name' => $val];
                    $keyParts[] = mb_strtolower(trim($val));
                }
            }
            if ($optVals) $vi['optionValues'] = $optVals;
            $vi = array_filter($vi, fn($x) => $x !== null && $x !== '');

            // Se esiste gia' una variante con queste option values, aggiorna via REST e non crearla.
            $key = implode('||', $keyParts);
            if ($key !== '' && isset($existingVariantsByKey[$key])) {
				
				
                $legacyVid = (int)$existingVariantsByKey[$key];
                $fields = [];
                if (isset($v['price'])) $fields['price'] = (string)$v['price'];
                if (!empty($v['sku'])) $fields['sku'] = (string)$v['sku'];
                if (array_key_exists('barcode', $v) && $v['barcode'] !== null && $v['barcode'] !== '') $fields['barcode'] = (string)$v['barcode'];
                if (isset($v['taxable'])) $fields['taxable'] = (bool)$v['taxable'];
				if (isset($v['inventory_policy'])) $fields['inventory_policy'] = (string)$v['inventory_policy'];
				
				$fields['inventory_management'] = 'shopify';
				
				
                if (!empty($fields) && $productLegacyId > 0) {
                    try {
                        $this->updateVariant( (int)$legacyVid, $fields);
						
                    } catch (\Throwable $e) {
						
						echo "errore aggiornamento variante";
						
                        // non fatale: continuiamo con eventuale creazione
                    }
                }
            } else {
                $variantInputs[] = $vi;
            }
        }

        // Se tutte le varianti sono gia' presenti (auto-generate), non eseguiamo bulkCreate.
        if (empty($variantInputs)) {
            // Continua con lettura varianti e ritorno.
            $variantInputs = [];
        }

        // [DBG] Decisione chiamata bulkCreate + payload di esempio
        $this->dbg('DECISION existingCount=' . count($existingVariantsByKey) . ' desiredCount=' . count($variants) . ' bulkCount=' . count($variantInputs) . ' willCallBulkCreate=' . (!empty($variantInputs) ? 'YES' : 'NO'));
        if (!empty($variantInputs)) {
            $this->dbg('BULK bulk[0]', $variantInputs[0]);
        }

        $qBulk = <<<'GQL'
mutation productVariantsBulkCreate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkCreate(productId: $productId, variants: $variants) {
    product { id }
    productVariants { id sku legacyResourceId inventoryItem { legacyResourceId } }
    userErrors { field message }
  }
}
GQL;
        if (!empty($variantInputs)) {
            $chunks = array_chunk($variantInputs, 250);
            foreach ($chunks as $chunk) {
                $rBulk = $this->graphql($qBulk, ['productId' => $productGid, 'variants' => $chunk]);
                $ue = $rBulk['data']['productVariantsBulkCreate']['userErrors'] ?? [];
                // [DBG] UserErrors restituiti da Shopify in bulkCreate (prima del filtro)
                if (!empty($ue)) {
                    $this->dbg('BULK userErrors.raw', $ue);
                }
                // Se alcune varianti esistono gia' (create automaticamente da Shopify), ignoriamo l'errore "already exists"
                // e proseguiamo; gli aggiornamenti SKU/price sono gia' stati tentati via REST sopra.
                if ($ue) {
                    $filtered = [];
                    foreach ($ue as $err) {
                        $msg = (string)($err['message'] ?? '');
                        if (stripos($msg, 'already exists') !== false) {
                            continue;
                        }
                        $filtered[] = $err;
                    }
                    // [DBG] UserErrors dopo filtro
                    $this->dbg('BULK userErrors.filteredCount=' . count($filtered));
                    if (!empty($filtered)) {
                        throw new RuntimeException('productVariantsBulkCreate userErrors: ' . json_encode($filtered));
                    }
                }
            }
        }

        // 5) Rilegge varianti per inventory levels
        $p = null;
        if ($handle) {
            $qRead = <<<'GQL'
query productByHandle($handle: String!) {
  productByHandle(handle: $handle) {
    legacyResourceId
    handle
    variants(first:250) { edges { node { sku legacyResourceId inventoryItem { legacyResourceId } } } }
  }
}
GQL;
            $rRead = $this->graphql($qRead, ['handle' => $handle]);
            $p = $rRead['data']['productByHandle'] ?? null;
        }
        if (!$p) {
            $qRead = <<<'GQL'
query product($id: ID!) {
  product(id: $id) {
    legacyResourceId
    handle
    variants(first:250) { edges { node { sku legacyResourceId inventoryItem { legacyResourceId } } } }
  }
}
GQL;
            $rRead = $this->graphql($qRead, ['id' => $productGid]);
            $p = $rRead['data']['product'] ?? null;
        }
        if (!$p) {
            return ['id' => $productLegacyId ?: 0, 'variants' => []];
        }
        $outVariants = [];
        foreach (($p['variants']['edges'] ?? []) as $e) {
            $n = $e['node'] ?? [];
            $outVariants[] = [
                'id' => (int)($n['legacyResourceId'] ?? 0),
                'sku' => $n['sku'] ?? null,
                'inventory_item_id' => (int)($n['inventoryItem']['legacyResourceId'] ?? 0),
            ];
        }
		
	//	echo "VARIANTI" . json_encode($outVariants); 
		
		
        return [
            'id' => (int)($p['legacyResourceId'] ?? $productLegacyId),
            'handle' => $p['handle'] ?? $handle,
            'variants' => $outVariants,
        ];
    }
	
	
	
	
	
	
	
    private function updateProduct(int $productId, array $fields): array {
        $resp = $this->sdk->Product($productId)->put($fields);
        self::tick();
        return $resp['product'] ?? $resp;
    }
    private function getProduct(int $productId): array {
        $resp = $this->sdk->Product($productId)->get();
        self::tick();
        return $resp;
    }
	
	
	
	private function updateVariant(int $variantId, array $fields): array
	{
		$payload = ['id' => $variantId] + $fields;
		// Endpoint corretto REST: PUT /variants/{id}.json
		$resp = $this->sdk->ProductVariant($variantId)->put($payload);
		

		self::tick();
		return $resp['variant'] ?? $resp;
	}
	
	
	
	
	
	private function createVariant(int $productId, array $fields, bool $debug = false, ?callable $say = null): array
	{
		$say = $say ?? function ($m) {};
		
		
		$say(" XXXXX sto creando la variante" . json_encode($fields, JSON_UNESCAPED_UNICODE));

		try {
			// Il tuo SDK: spesso vuole wrapper { "variant": {...} }
			// Se tu già passi fields col wrapper, lascia così.
			// Se invece passi i campi nudi, usa: ['variant' => $fields]
			$resp = $this->sdk->Product($productId)->Variant->post($fields);

			if ($debug) {
				$say("  [VAR-CREATE] resp=" . json_encode($resp, JSON_UNESCAPED_UNICODE));
			}

			self::tick();
			return $resp['variant'] ?? $resp;

		} catch (\Throwable $e) {
			// Log completo e rilancio (così lo vedi anche a monte)
			$msg = $e->getMessage();

			// Se l’eccezione contiene response/body (dipende dall’SDK), prova a estrarlo
			$extra = [];
			if (method_exists($e, 'getResponse') && $e->getResponse()) {
				try {
					$extra['status'] = $e->getResponse()->getStatusCode();
					$extra['body']   = (string)$e->getResponse()->getBody();
				} catch (\Throwable $ignored) {}
			}

			if ($debug) {
				$say("  [VAR-CREATE] EXCEPTION: {$msg}" . ($extra ? " extra=" . json_encode($extra, JSON_UNESCAPED_UNICODE) : ""));
				$say("  [VAR-CREATE] payload=" . json_encode($fields, JSON_UNESCAPED_UNICODE));
			}

			throw $e;
		}
	}

	
	
	
	
    private function addImage(int $productId, string $src, ?int $position=null): array {
        $payload = ['src'=>$src];
        if ($position) $payload['position'] = $position;
        $resp = $this->sdk->Product($productId)->Image->post($payload);
        self::tick();
        return $resp['image'] ?? $resp;
    }

   
	
	private function setInventoryGraph(array $product, int $locationId, ?int $forcedQty, bool $debug = false, ?callable $log = null): void
	{
		$say = $log ?? function ($m) { echo $m . PHP_EOL; };

		// Location GID
		$locationGid = "gid://shopify/Location/{$locationId}";

		// Costruisci lista quantità (InventoryQuantityInput)
		$rows = [];
		
		
		
		foreach ($product['variants'] as $v) {
			$inventoryItemId = $v['inventory_item_id'] ?? null;
			if (!$inventoryItemId) {
				if ($debug) $say("  [INV] skip: variant senza inventory_item_id");
				continue;
			}

			$qty = $forcedQty;
			if ($qty === null && isset($v['_ps_qty'])) $qty = (int)$v['_ps_qty'];
			if ($qty === null) $qty = 0;

			$rows[] = [
				'inventoryItemId'     => "gid://shopify/InventoryItem/{$inventoryItemId}",
				'locationId'          => $locationGid,
				'quantity'            => (int)$qty,
				// Opt-out compare check in modo “moderno”:
				//'changeFromQuantity'  => null,
			];
			
			
		}
		
	
		if (!$rows) return;

		// Mutation: inventorySetQuantities
		$mSet = <<<'GQL'
	mutation invSet($input: InventorySetQuantitiesInput!) {
	  inventorySetQuantities(input: $input) {
		userErrors { code field message }
	  }
	}
	GQL;

		// Mutation: inventoryActivate (fallback per ITEM_NOT_STOCKED_AT_LOCATION)
		$mAct = <<<'GQL'
	mutation invAct($inventoryItemId: ID!, $locationId: ID!, $available: Int) {
	  inventoryActivate(inventoryItemId: $inventoryItemId, locationId: $locationId, available: $available) {
		userErrors { field message }
	  }
	}
	GQL;

		// Chunking: 50–100 è un buon compromesso (dipende dal cost e dalla dimensione media payload)
		$chunkSize = 75;

		for ($offset = 0; $offset < count($rows); $offset += $chunkSize) {
			$chunk = array_slice($rows, $offset, $chunkSize);

			$input = [
				'name'       => 'available',
				'reason'     => 'correction',
				'quantities' => $chunk,
				'ignoreCompareQuantity' => true,  // <-- FIX
			];

			// 1) Prova set in batch
			$out = $this->graphql($mSet, ['input' => $input]);
			$errs = $out['data']['inventorySetQuantities']['userErrors'] ?? [];

			if ($debug) {
				$say("  [INV-GQL] setQuantities chunk=".(int)($offset/$chunkSize+1)." rows=".count($chunk)." userErrors=".count($errs));
			}

			if (!$errs) {
				self::tick();
				continue;
			}

			// 2) Se errore = item non stockato in location, attiva e riprova SOLO per quelli
			$toRetry = [];
			foreach ($errs as $e) {
				$code = $e['code'] ?? '';
				$field = $e['field'] ?? [];

				// Pattern tipico: ["input","quantities","12","locationId"] oppure simili
				$idx = null;
				foreach ($field as $p) {
					if (is_numeric($p)) { $idx = (int)$p; break; }
				}

				if ($code === 'ITEM_NOT_STOCKED_AT_LOCATION' && $idx !== null && isset($chunk[$idx])) {
					$toRetry[$idx] = $chunk[$idx];
				} elseif ($debug) {
					$say("  [INV-GQL] non-retriable userError: ".json_encode($e, JSON_UNESCAPED_UNICODE));
				}
			}

			if ($toRetry) {
				foreach ($toRetry as $idx => $row) {
					try {
						if ($debug) $say("  [INV-GQL] activate item={$row['inventoryItemId']} @loc={$row['locationId']} qty={$row['quantity']}");
						$act = $this->graphql($mAct, [
							'inventoryItemId' => $row['inventoryItemId'],
							'locationId'      => $row['locationId'],
							'available'       => (int)$row['quantity'],
						]);

						$ue = $act['data']['inventoryActivate']['userErrors'] ?? [];
						if ($ue && $debug) {
							$say("  [INV-GQL] activate userErrors: ".json_encode($ue, JSON_UNESCAPED_UNICODE));
						}
						self::tick();
					} catch (\Throwable $ex) {
						if ($debug) $say("  [INV-GQL] activate FAILED: ".$ex->getMessage());
					}
				}

				// retry setQuantities solo per quelli attivati
				$retryChunk = array_values($toRetry);
				$retryInput = $input;
				$retryInput['quantities'] = $retryChunk;

				$out2 = $this->graphql($mSet, ['input' => $retryInput]);
				$errs2 = $out2['data']['inventorySetQuantities']['userErrors'] ?? [];

				if ($errs2) {
					// Non faccio throw: restiamo best-effort ma logghiamo per debugging
					if ($debug) $say("  [INV-GQL] retry userErrors: ".json_encode($errs2, JSON_UNESCAPED_UNICODE));
				}

				self::tick();
			}
		}
	}
	
	


	/**
	 * Abilita il tracking ("Track quantity") su uno specifico InventoryItem.
	 * Usato esclusivamente come fallback quando inventory_levels/set.json restituisce 422.
	 */
	private function ensureInventoryItemTracked(int $inventoryItemLegacyId): void
	{
		$gid = 'gid://shopify/InventoryItem/' . $inventoryItemLegacyId;
		$q = <<<'GQL'
mutation inventoryItemUpdate($id: ID!, $input: InventoryItemInput!) {
  inventoryItemUpdate(id: $id, input: $input) {
    inventoryItem { id tracked }
    userErrors { field message }
  }
}
GQL;
		$r = $this->graphql($q, ['id' => $gid, 'input' => ['tracked' => true]]);
		$ue = $r['data']['inventoryItemUpdate']['userErrors'] ?? [];
		if ($ue) {
			throw new RuntimeException('inventoryItemUpdate userErrors: ' . json_encode($ue));
		}
	}

    // ---------- BUILD / MAPPING ----------
    private static function buildOptionsFromCombinations(array $combinations): array
    {
        $groups = [];
        foreach ($combinations as $c) {
            foreach ($c['option_pairs'] as $g => $v) $groups[$g][$v] = true;
        }
        $options = [];
        foreach (array_slice(array_keys($groups), 0, 3) as $name) {
            $options[] = ['name'=>$name, 'values'=>array_keys($groups[$name])];
        }
        return $options;
    }
	
	

    /**
     * Costruisce le varianti garantendo l'ordine coerente delle option1/option2/option3.
     *
     * Problema: per alcune combinazioni Prestashop può fornire option_pairs con ordine diverso
     * (es. ["Agape versions"=>..., "Agape finishes"=>...] invece di ["Agape finishes"=>..., "Agape versions"=>...]).
     * Se ci basiamo sui primi 2/3 elementi dell'array, Shopify riceve option1/option2 invertite.
     *
     * Soluzione: imponiamo l'ordine basandoci sulla sequenza delle options calcolate (options[0..2]).
     */
    private function buildVariantsFromCombinations(array $psProduct, array $combinations, array $optionsOrder): array
    {
        $basePrice  = (float)$psProduct['base_price'];
        $baseWeight = (float)$psProduct['weight'];
        $variants   = [];

        // Ordine option names (max 3) come inviato a Shopify.
        $oNames = [];
        foreach (array_slice($optionsOrder, 0, 3) as $o) {
            $name = (string)($o['name'] ?? '');
            if ($name !== '') $oNames[] = $name;
        }

        foreach ($combinations as $c) {
            $pairs = $c['option_pairs'] ?? [];

            // Se non abbiamo names (caso anomalo), fallback all'ordine nativo.
            if (empty($oNames)) {
                $keys = array_keys($pairs);
                $oNames = array_slice($keys, 0, 3);
            }

            $v1 = $oNames[0] ?? null;
            $v2 = $oNames[1] ?? null;
            $v3 = $oNames[2] ?? null;

            $variants[] = [
                'sku'                  => $c['reference'] ?: null,
                'price'                => number_format($basePrice + (float)$c['price_impact'], 2, '.', ''),
                'inventory_management' => $this->trackInventory ? 'shopify' : null,
                'inventory_policy'     =>  $this->cfg['inventoryPolicy'],
                'taxable'              => true,
                'requires_shipping'    => true,
                'grams'                => (int)round(($baseWeight + (float)$c['weight_impact']) * 1000),
                'option1'              => (string)($v1 ? ($pairs[$v1] ?? 'Default') : 'Default'),
                'option2'              => $v2 ? (string)($pairs[$v2] ?? '') : null,
                'option3'              => $v3 ? (string)($pairs[$v3] ?? '') : null,
                '_ps_qty'              => (int)$c['quantity'],
				//'barcode'              => (string)($psProduct['ean13'] ?? ''),
            ];
        }
        return $variants;
    }

    private static function sanitizePayload(array $payload): array
    {
        if (!empty($payload['options'])) {
            foreach ($payload['options'] as &$opt) {
                $opt['name']   = (string)($opt['name'] ?? '');
                $opt['values'] = array_map(fn($v)=>(string)$v, $opt['values'] ?? []);
            }
            unset($opt);
        }
        if (!empty($payload['variants'])) {
            // Safety net: se qualche variante arriva con option1/option2 invertite rispetto a payload['options'],
            // riallineiamo i valori in base agli insiemi di valori dichiarati nelle options.
            // Questo evita casi limite dovuti a dati sporchi o a mapping esterni.
            $optValues = [];
            if (!empty($payload['options'][0]['values'])) $optValues[1] = array_flip($payload['options'][0]['values']);
            if (!empty($payload['options'][1]['values'])) $optValues[2] = array_flip($payload['options'][1]['values']);
            if (!empty($payload['options'][2]['values'])) $optValues[3] = array_flip($payload['options'][2]['values']);

            foreach ($payload['variants'] as &$v) {
                // Se abbiamo almeno 2 options, proviamo a correggere eventuale swap tra option1 e option2.
                if (isset($optValues[1], $optValues[2], $v['option1'], $v['option2'])) {
                    $o1 = (string)$v['option1'];
                    $o2 = (string)$v['option2'];
                    $o1In1 = isset($optValues[1][$o1]);
                    $o1In2 = isset($optValues[2][$o1]);
                    $o2In1 = isset($optValues[1][$o2]);
                    $o2In2 = isset($optValues[2][$o2]);

                    // Caso tipico: o1 appartiene al set della option2 e o2 appartiene al set della option1.
                    if (!$o1In1 && $o1In2 && $o2In1 && !$o2In2) {
                        $v['option1'] = $o2;
                        $v['option2'] = $o1;
                    }
                }

                if (isset($v['option1'])) $v['option1'] = (string)$v['option1'];
                if (isset($v['option2']) && $v['option2'] !== null) $v['option2'] = (string)$v['option2']; else unset($v['option2']);
                if (isset($v['option3']) && $v['option3'] !== null) $v['option3'] = (string)$v['option3']; else unset($v['option3']);
                if (isset($v['price']))   $v['price']   = number_format((float)$v['price'], 2, '.', '');
            }
            unset($v);
        }
        return $payload;
    }

    /** Costruisce il payload prodotto partendo dai dati PS */
    public function buildProductPayload(array $psProduct, array $imageUrls, array $primaryData, array $combinations = []): array
    {
		$say = $log ?? function ($m) { echo $m . PHP_EOL; };
		
        $title   = $primaryData['name'] ?? ('Product '.$psProduct['id_product']);
        $handle  = $primaryData['handle'] ?? '';
        $images  = [];
        foreach ($imageUrls as $i=>$url) $images[] = ['src'=>$url, 'position'=>$i+1];

        if ($combinations) {
            $options  = self::buildOptionsFromCombinations($combinations);
            $variants = $this->buildVariantsFromCombinations($psProduct, $combinations, $options);
			//$say("VARIANTI: " . json_encode($variants ));
        } else {
            $options  = [['name'=>'Title','values'=>['Default Title']]];
            $variants = [[
                'sku'                  => $psProduct['reference'] ?: null,
                'price'                => number_format((float)$psProduct['base_price'], 2, '.', ''),
                'inventory_management' => $this->trackInventory ? 'shopify' : null,
                'inventory_policy'     =>  $this->cfg['inventoryPolicy'],
                'taxable'              => true,
                'requires_shipping'    => true,
                'grams'                => (int)round(((float)$psProduct['weight'] ?? 0) * 1000),
                'option1'              => 'Default Title',
                '_ps_qty'              => (int)($psProduct['quantity'] ?? 0),
			//	'barcode'              => (string)($psProduct['ean13'] ?? '') ,
            ]];
        }



        // Meta per linked options (Metaobjects/Metafields) ricavate da PrestaShop
        // Struttura: optionName => valueName => {id_attribute, id_attribute_group, is_color_group, texture_url}
        $psOptionMeta = [];
        if ($combinations) {
            foreach ($combinations as $c) {
                if (!empty($c['_option_meta']) && is_array($c['_option_meta'])) {
                    foreach ($c['_option_meta'] as $groupName => $values) {
                        if (!isset($psOptionMeta[$groupName])) $psOptionMeta[$groupName] = [];
                        foreach ((array)$values as $valueName => $meta) {
                            if (!isset($psOptionMeta[$groupName][$valueName])) {
                                $psOptionMeta[$groupName][$valueName] = $meta;
                            }
                        }
                    }
                }
            }
        }
        $payload = [
            'title'     => $title,
            //'handle'    => $handle ?: self::slugify($title),
			'handle'    => $handle ?: self::slugify($title )  . "_" . $psProduct['id_product'],
			
            'body_html' => $primaryData['description'] ?? '',
			'description_short' => $primaryData['description_short'] ?? '',
            'vendor'    => $psProduct['brand'] ?? null,
			'reference' => $psProduct['reference'],
            'status'    => 'active',
            'images'    => $images,
            'options'   => $options,
            'variants'  => $variants,
        ];

        if (!empty($psOptionMeta)) {
            $payload['_ps_option_meta'] = $psOptionMeta;
        }
        return self::sanitizePayload($payload);
    }

	
	
	
	public function upsertProduct(
		array $payload,
		array $textsByLocale,
		string $primaryLocale,
		int $locationId,
		?int $defaultQtyIfInStock = null,
		bool $dryRun = false,
		bool $debug = false,
		?callable $log = null,
		?Prestashop $ps = null,
		?int $psProductId = null,
		$onlyinsert = true,
		$syncInventory = false
	): array {
		
		
		$handle = $payload['handle'] ?? self::slugify($payload['title'] ?? 'item');
		$cfg    = $ps->getConfig();
		
		if ($psProductId !== null) {
			$payload['ps_id'] = (string)$psProductId; // key metafield desiderata
		}
		
		$cats = $ps->getCategoriesForProductSimple($psProductId, $cfg['lang_map'], (int)$cfg['primary_lang_id']);
		
		//echo json_encode($cats); exit();
		
		$payload['tags'] = isset($cats) ? (is_array($cats) ? implode(', ', $cats) : (string)$cats)  : null;
		
		
		$report = [
			'handle'  => $handle,
			'steps'   => [],
			'result'  => null,
			'error'   => null,
			'timings' => [],
		];
		$say = $log ?? function ($m) { echo $m . PHP_EOL; };

		// --- helpers timing ---
		$tStart = microtime(true);
		$lastT  = $tStart;
		$ms = fn(float $seconds) => (int)round($seconds * 1000);

		$addStep = function(string $name, bool $ok, array $extra = []) use (&$report, &$lastT, $tStart, $ms, $debug, $say) {
			$now   = microtime(true);
			$durMs = $ms($now - $lastT);
			$fromStartMs = $ms($now - $tStart);
			$lastT = $now;

			$row = ['step' => $name, 'ok' => $ok, 'duration_ms' => $durMs, 't_from_start_ms' => $fromStartMs] + $extra;
			$report['steps'][] = $row;
			if ($debug) $say(sprintf("  [TIME] %-28s %5d ms (t+%d)", $name, $durMs, $fromStartMs));
		};

		$logPayload = function (string $title, array $data) use ($say, $debug) {
			if (!$debug) return;
			$json = json_encode($data, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES);
			if (strlen($json) > 4096) $json = substr($json, 0, 4096) . '... [truncated]';
			$say("  {$title}: $json");
		};

		try {
			if ($dryRun) {
				$say("[UPSERT][DRY RUN] handle={$handle}");
				$logPayload('payload', $payload);
				$addStep('dry-run', true);
				$report['result'] = ['action' => 'dry-run'];
				$report['timings']['total_ms'] = $ms(microtime(true) - $tStart);
				return $report;
			}

			// 1) CHECK ESISTENZA
			$say("[UPSERT] handle={$handle}");
			$existing = null;
			
			if(!$onlyinsert){
				
				try {
					$existing = $this->findProductByHandle($handle);
					$addStep('findProductByHandle', true, ['exists' => (bool)$existing]);
					if ($debug) $say("  findProductByHandle: " . ($existing ? "FOUND id={$existing['id']}" : "NOT FOUND"));
				} catch (\Throwable $e) {
					$addStep('findProductByHandle', false, ['error' => $e->getMessage()]);
					throw new \RuntimeException("[findProductByHandle] " . $e->getMessage(), 0, $e);
				}

			}
			
			

			if (!$existing || $onlyinsert) {
				// ---- CREATE ----
				try {
					
					
					
					if ($debug) { $say("  CREATE /admin/api/{$this->cfg['ApiVersion']}/products.json"); $logPayload('product', $payload); }
					$created = $this->createProduct($payload);
					$pid = (int)$created['id'];
					$addStep('createProduct', true, ['productId' => $pid]);
					if ($debug) $say("  CREATED pid={$pid}");
				} catch (\Throwable $e) {
					$addStep('createProduct', false, ['error' => $e->getMessage(), 'hint' => 'Controlla fields product/options/variants/handle']);
					throw new \RuntimeException("[createProduct] " . $e->getMessage(), 0, $e);
				}

				

				// INVENTARIO (solo dopo create)
				
				if($syncInventory == true){
					
					try {
						
						$fresh = $this->getProduct($pid);
						$skuToQty = [];
						
						foreach ($payload['variants'] as $idx=>$pv) $skuToQty[$pv['sku'] ?? ("idx_$idx")] = $pv['_ps_qty'] ?? null;
						
						foreach ($fresh['variants'] as &$fv) {
							$sku = $fv['sku'] ?: ("id_".$fv['id']);
							if (isset($skuToQty[$sku]) && $skuToQty[$sku] !== null) $fv['_ps_qty'] = (int)$skuToQty[$sku];
						} unset($fv);
						
						if ($debug) $say("  INVENTORY set / InventoryLevel POST");
						//$this->setInventory($created, $locationId, $defaultQtyIfInStock, $debug, $say);
						$this->setInventoryGraph($fresh, $locationId, null, $debug, $say);
						//$addStep('setInventory_update', true);
						$addStep('setInventory_create', true);
					} catch (\Throwable $e) {
						$addStep('setInventory_create', false, ['error' => $e->getMessage()]);
						throw new \RuntimeException("[setInventory_create] " . $e->getMessage(), 0, $e);
					}
					
				}
				
				
				// Immagini: Shopify spesso non scarica subito le immagini passate in create (src remoto).
				// Per evitare il "devo lanciare due volte" facciamo un sync esplicito post-create.
				try {
					$fresh = $this->getProduct($pid);
					$already = $fresh['images'] ?? [];
					$pos = count($already) + 1;
					foreach (($payload['images'] ?? []) as $img) {
						$src = (string)($img['src'] ?? '');
						if ($src === '' || in_array($src, $already, true)) continue;
						if ($debug) { $say("  ADD IMAGE (post-create) /products/{$pid}/images.json"); $logPayload('image', ['src'=>$src,'position'=>$pos]); }
						$this->addImage($pid, $src, $pos++);
					}
					$addStep('addImages_create', true, ['existing' => count($already), 'sent' => $pos - (count($already) + 1)]);
				} catch (\Throwable $e) {
					$addStep('addImages_create', false, ['error' => $e->getMessage()]);
					// Non blocchiamo la migrazione: se falliscono le immagini, il resto deve andare avanti.
					if ($debug) $say('  [IMG] warn: '.$e->getMessage());
				}
				
				
				try {
					$this->ensureProductLegacyRedirects($psProductId, $textsByLocale, $handle, $primaryLocale);
				} catch (\Throwable $e) {
					if ($debug) $say('[REDIR] warn: '.$e->getMessage());
				}
				
				

				// TRADUZIONI prodotto
				try {
					foreach ($textsByLocale as $loc => $data) {
						if ($loc === $primaryLocale) continue;
						if ($debug) { $say("  TRANSLATE {$loc} (product {$pid})"); $logPayload('translations', $data); }
						$this->registerProductTranslations($pid, $loc, $data);
					}
					$addStep('translations_create', true);
				} catch (\Throwable $e) {
					$addStep('translations_create', false, ['error' => $e->getMessage()]);
				}

				// I18N opzioni/valori
				try {
					if ($ps && $psProductId) {
						
						$groups = $ps->getOptionGroupTranslations($psProductId, $cfg['lang_map'], (int)$cfg['primary_lang_id']);
						$titles = $ps->getVariantTitlesByLocaleBySku($psProductId, $cfg['lang_map']);
						if ($debug) { $say("  I18N options/values"); $logPayload('groups', $groups); $logPayload('titlesBySku', $titles); }
						$this->translateOptionsAndValuesInternal($pid, $primaryLocale, $groups, $titles, $debug, $say);
					}
					$addStep('i18n_options_values_create', true);
				} catch (\Throwable $e) {
					$addStep('i18n_options_values_create', false, ['error' => $e->getMessage()]);
				}

				// Variant swatch textures (metafield file_reference) - non fatale
				try {
					$this->applyVariantTextureMetafields($pid, $payload, $debug, $say);
					$addStep('variant_textures_create', true);
				} catch (\Throwable $e) {
					$addStep('variant_textures_create', false, ['error' => $e->getMessage()]);
					if ($debug) $say('  [SWATCH] warn: ' . $e->getMessage());
				}

				// COLLECTIONS
				try {
					if ($ps && $psProductId) {
						$cfg  = $ps->getConfig();
						$cats = $ps->getCategoriesForProduct($psProductId, $cfg['lang_map'], (int)$cfg['primary_lang_id']);
						if ($debug) { $say("  [COLL] sync from PS"); $logPayload('psCategories', $cats); }
						$this->syncCollectionsFromPrestashop($pid, $cats, $primaryLocale, $textsByLocale, $debug, $say);
					}
					$addStep('collections_create', true);
				} catch (\Throwable $e) {
					$addStep('collections_create', false, ['error' => $e->getMessage()]);
				}

				// FEATURES -> Metafields
				try {
					if ($ps && $psProductId) {
						$cfg        = $ps->getConfig();
						$featBundle = $ps->getFeaturesBundle($psProductId, $cfg['lang_map'], (int)$cfg['primary_lang_id']);
						if ($debug) { $say("  FEATURES bundle"); $logPayload('featuresBundle', $featBundle); }
						$this->upsertPrestashopFeaturesMetafields($pid, $featBundle, $primaryLocale, $debug, $say);
					}
					$addStep('features_metafields_create', true);
				} catch (\Throwable $e) {
					$addStep('features_metafields_create', false, ['error' => $e->getMessage()]);
				}
				
				
				
				
				// ATTACHMENTS (PDF/manuals) -> Shopify Files + product metafield list.file_reference
				try {
					if ($ps && $psProductId) {
						$this->syncAttachmentsFromPrestashop($pid, $ps, $psProductId, $primaryLocale, $debug, $say);
					}
					$addStep('attachments_create', true);
				} catch (\Throwable $e) {
					$addStep('attachments_create', false, ['error' => $e->getMessage()]);
					if ($debug) $say('  [ATT] warn: ' . $e->getMessage());
				}

				
				
				
				

				$report['result'] = ['action' => 'created', 'productId' => $pid];
				$report['timings']['total_ms'] = $ms(microtime(true) - $tStart);
				return $report;

			} else {
				
				
				
				// ---- UPDATE ----
				$pid = (int)$existing['id'];

				try {
					$baseUpd = [
						'title'     => $payload['title']     ?? null,
						'body_html' => $payload['body_html'] ?? null,
						'vendor'    => $payload['vendor']    ?? null,
						'status'    => $payload['status']    ?? null,
						'handle'    => $handle,
					];
					$baseUpd = array_filter($baseUpd, fn($v)=> !is_null($v));
					if ($baseUpd) {
						if ($debug) { $say("  UPDATE /admin/api/{$this->cfg['ApiVersion']}/products/{$pid}.json"); $logPayload('product', $baseUpd); }
						$this->updateProduct($pid, $baseUpd);
					}
					$addStep('updateProduct', true);
				} catch (\Throwable $e) {
					$addStep('updateProduct', false, ['error' => $e->getMessage(), 'hint' => 'Spesso 406 nasce qui: controlla body_html/handle/options incoerenti']);
					throw new \RuntimeException("[updateProduct] " . $e->getMessage(), 0, $e);
				}
				
				try {
					$this->ensureProductLegacyRedirects($psProductId, $textsByLocale, $handle, $primaryLocale);
				} catch (\Throwable $e) {
					if ($debug) $say('[REDIR] warn: '.$e->getMessage());
				}

				// Immagini (solo nuove)
				/*
				try {
					$already = $existing['images'] ?? [];
					$pos = count($already) + 1;
					foreach ($payload['images'] as $img) {
						if (empty($img['src']) || in_array($img['src'], $already, true)) continue;
						if ($debug) { $say("  ADD IMAGE /products/{$pid}/images.json"); $logPayload('image', $img); }
						$this->addImage($pid, $img['src'], $pos++);
					}
					$addStep('addImages', true);
				} catch (\Throwable $e) {
					$addStep('addImages', false, ['error' => $e->getMessage()]);
				}
				*/

				// Varianti
				try {
					$map = $existing['variants'] ?? [];
					
					$say("XXXX MAP:" . json_encode($map));
					
					foreach ($payload['variants'] as $v) {
						$vf = [
							'price'                => $v['price'] ?? null,
							'inventory_management' => $v['inventory_management'] ?? null,
							'inventory_policy'     => $v['inventory_policy'] ?? null,
							'taxable'              => $v['taxable'] ?? null,
							'requires_shipping'    => $v['requires_shipping'] ?? null,
							'grams'                => $v['grams'] ?? null,
							'option1'              => $v['option1'] ?? null,
							'option2'              => $v['option2'] ?? null,
							'option3'              => $v['option3'] ?? null,
							'sku'                  => $v['sku'] ?? null,
							'barcode'              => $v['barcode'] ?? null,
						];
						$vf = array_filter($vf, fn($x)=> !is_null($x));
						$sku = $v['sku'] ?? null;

						if ($sku && isset($map[$sku])) {
							$vid = (int)preg_replace('~\D+~', '', $map[$sku]);
							if ($debug) { $say("  UPDATE VARIANT /variants/{$vid}.json"); $logPayload('variant', $vf); }
							$this->updateVariant($vid, $vf);
						} else {
							if ($debug) { $say("  CREATE VARIANT /products/{$pid}/variants.json"); $logPayload('variant', $vf); }
							$this->createVariant($pid, $vf);
						}
					}
					$addStep('variantsUpsert', true);
				} catch (\Throwable $e) {
					$addStep('variantsUpsert', false, ['error' => $e->getMessage(), 'hint' => '406 qui = optionN non-string o null in body']);
					//throw new \RuntimeException("[variantsUpsert] " . $e->getMessage(), 0, $e);
				}

				// Variant swatch textures (metafield file_reference) - non fatale
				/*
				try {
					$this->applyVariantTextureMetafields($pid, $payload, $debug, $say);
					$addStep('variant_textures_update', true);
				} catch (\Throwable $e) {
					$addStep('variant_textures_update', false, ['error' => $e->getMessage()]);
					if ($debug) $say('  [SWATCH] warn: ' . $e->getMessage());
				}
				*/
				
				

				if($syncInventory){
					// Inventario
					try {
						
						
						
						if ($debug) $say("  INVENTORY set / InventoryLevel POST");
						$fresh = $this->getProduct($pid);
						$skuToQty = [];
						foreach ($payload['variants'] as $idx=>$pv) $skuToQty[$pv['sku'] ?? ("idx_$idx")] = $pv['_ps_qty'] ?? null;
						foreach ($fresh['variants'] as &$fv) {
							$sku = $fv['sku'] ?: ("id_".$fv['id']);
							if (isset($skuToQty[$sku]) && $skuToQty[$sku] !== null) $fv['_ps_qty'] = (int)$skuToQty[$sku];
						} unset($fv);

						
						
						//$this->setInventory($fresh, $locationId, $defaultQtyIfInStock, $debug, $say);
						$this->setInventoryGraph($fresh, $locationId, $defaultQtyIfInStock, $debug, $say);
						$addStep('setInventory_update', true);
					} catch (\Throwable $e) {
						$addStep('setInventory_update', false, ['error' => $e->getMessage()]);
						throw new \RuntimeException("[setInventory_update] " . $e->getMessage(), 0, $e);
					}
				}
				

				// Traduzioni prodotto
				try {
					foreach ($textsByLocale as $loc => $data) {
						if ($loc === $primaryLocale) continue;
						if ($debug) { $say("  TRANSLATE {$loc} (product {$pid})"); $logPayload('translations', $data); }
						$this->registerProductTranslations($pid, $loc, $data);
					}
					$addStep('translations_update', true);
				} catch (\Throwable $e) {
					$addStep('translations_update', false, ['error' => $e->getMessage()]);
				}

				// I18N opzioni/valori
				try {
					if ($ps && $psProductId) {
						$cfg    = $ps->getConfig();
						$groups = $ps->getOptionGroupTranslations($psProductId, $cfg['lang_map'], (int)$cfg['primary_lang_id']);
						$titles = $ps->getVariantTitlesByLocaleBySku($psProductId, $cfg['lang_map']);
						if ($debug) { $say("  I18N options/values"); $logPayload('groups', $groups); $logPayload('titlesBySku', $titles); }
						$this->translateOptionsAndValuesInternal($pid, $primaryLocale, $groups, $titles, $debug, $say);
					}
					$addStep('i18n_options_values_update', true);
				} catch (\Throwable $e) {
					$addStep('i18n_options_values_update', false, ['error' => $e->getMessage()]);
				}

				// Collections
				try {
					if ($ps && $psProductId) {
						$cfg  = $ps->getConfig();
						$cats = $ps->getCategoriesForProduct($psProductId, $cfg['lang_map'], (int)$cfg['primary_lang_id']);
						if ($debug) { $say("  [COLL] sync from PS"); $logPayload('psCategories', $cats); }
						$this->syncCollectionsFromPrestashop($pid, $cats, $primaryLocale, $textsByLocale, $debug, $say);
					}
					$addStep('collections_update', true);
				} catch (\Throwable $e) {
					$addStep('collections_update', false, ['error' => $e->getMessage()]);
				}

				// Features -> metafields (e relative trad.)
				try {
					
					
					
					if ($ps && $psProductId) {
						$cfg        = $ps->getConfig();
						$featBundle = $ps->getFeaturesBundle($psProductId, $cfg['lang_map'], (int)$cfg['primary_lang_id']);
						
						if ($debug) { $say("  FEATURES bundle"); $logPayload('featuresBundle', $featBundle); }
						$this->upsertPrestashopFeaturesMetafields($pid, $featBundle, $primaryLocale, $debug, $say);
					}
					$addStep('features_metafields_update', true);
				} catch (\Throwable $e) {
					$addStep('features_metafields_update', false, ['error' => $e->getMessage()]);
				}
				
				
								// ATTACHMENTS (PDF/manuals) -> Shopify Files + product metafield list.file_reference
				try {
					if ($ps && $psProductId) {
						$this->syncAttachmentsFromPrestashop($pid, $ps, $psProductId, $primaryLocale, $debug, $say);
					}
					$addStep('attachments_update', true);
				} catch (\Throwable $e) {
					$addStep('attachments_update', false, ['error' => $e->getMessage()]);
					if ($debug) $say('  [ATT] warn: ' . $e->getMessage());
				}


				$report['result'] = ['action' => 'updated', 'productId' => $pid];
				$report['timings']['total_ms'] = $ms(microtime(true) - $tStart);
				return $report;
			}

		} catch (\Throwable $e) {
			$report['error'] = $e->getMessage();
			$report['timings']['total_ms'] = $ms(microtime(true) - $tStart);
			if ($debug) $say("!! UPSERT FAILED handle={$handle}: ".$e->getMessage() . "");
			return $report;
		}
	}
	
	public function debugFetchTranslations(int $productId, string $locale): array
	{
		$gid = "gid://shopify/Product/{$productId}";
		$q = <<<'GQL'
	query($id: ID!, $locale: String!) {
	  translations(resourceId: $id, locale: $locale) {
		key
		value
		locale
	  }
	}
	GQL;
		$res = $this->graphql($q, ['id'=>$gid, 'locale'=>$locale]);
		return $res['data']['translations'] ?? [];
	}
	
	
	
	// Traduci NOME opzione (ProductOption.name)
	private function translateOptionName(int $optionId, string $locale, string $translatedName): void
	{
		$gid = "gid://shopify/ProductOption/{$optionId}";
		$dig = $this->fetchTranslatableDigests($gid);
		if (!isset($dig['name'])) return;

		$m = <<<'GQL'
	mutation TransOpt($id: ID!, $translations: [TranslationInput!]!) {
	  translationsRegister(resourceId: $id, translations: $translations) {
		userErrors { field message code }
	  }
	}
	GQL;
		$res = $this->graphql($m, ['id'=>$gid, 'translations'=>[[
			'key'                       => 'name',
			'value'                     => (string)$translatedName,
			'locale'                    => $locale,
			'translatableContentDigest' => $dig['name'],
		]]]);

		if (!empty($res['errors'])) {
			$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (option name): ".implode(' | ', $msgs));
		}
		$ue = $res['data']['translationsRegister']['userErrors'] ?? [];
		if ($ue) {
			$msgs = array_map(fn($e)=>($e['message'] ?? 'unknown'), $ue);
			throw new \RuntimeException("option name userErrors: ".implode(' | ', $msgs));
		}
	}

	// Traduci TITOLO variante (ProductVariant.title)
	private function translateVariantTitle(int $variantId, string $locale, string $translatedTitle): void
	{
		$gid = "gid://shopify/ProductVariant/{$variantId}";
		$dig = $this->fetchTranslatableDigests($gid);
		if (!isset($dig['title'])) return;

		$m = <<<'GQL'
	mutation TransVar($id: ID!, $translations: [TranslationInput!]!) {
	  translationsRegister(resourceId: $id, translations: $translations) {
		userErrors { field message code }
	  }
	}
	GQL;
		$res = $this->graphql($m, ['id'=>$gid, 'translations'=>[[
			'key'                       => 'title',
			'value'                     => (string)$translatedTitle,
			'locale'                    => $locale,
			'translatableContentDigest' => $dig['title'],
		]]]);

		if (!empty($res['errors'])) {
			$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (variant title): ".implode(' | ', $msgs));
		}
		$ue = $res['data']['translationsRegister']['userErrors'] ?? [];
		if ($ue) {
			$msgs = array_map(fn($e)=>($e['message'] ?? 'unknown'), $ue);
			throw new \RuntimeException("variant title userErrors: ".implode(' | ', $msgs));
		}
	}

	// Orchestratore locale usato da upsertProduct (non tocca il main)
	private function translateOptionsAndValuesInternal(
		int $productId,
		string $primaryLocale,
		array $groups,      // ['primaryNames'=>[...], 'byLocale'=>[loc=>[primaryName=>translatedName]]]
		array $titlesBySku, // [loc => [sku => 'Value1 / Value2' ]]
		bool $debug,
		callable $say
	): void {
		$prod    = $this->getProduct($productId);
		$options = $prod['options']  ?? [];
		$variants= $prod['variants'] ?? [];

		// SKU -> variantId
		$skuToVid = [];
		foreach ($variants as $v) {
			$sku = (string)($v['sku'] ?? '');
			if ($sku !== '') $skuToVid[$sku] = (int)$v['id'];
		}

		foreach (($groups['byLocale'] ?? []) as $locale => $mapByPrimaryName) {
			if ($locale === $primaryLocale) continue;

			// nomi opzione
			foreach ($options as $opt) {
				$primaryName = (string)($opt['name'] ?? '');
				$optId = (int)$opt['id'];
				if ($primaryName === '') continue;

				$translated = $mapByPrimaryName[$primaryName] ?? null;
				if (!$translated) continue;

				try {
					if ($debug) $say("  [i18n:opt] {$locale} Option#{$optId} '{$primaryName}' -> '{$translated}'");
					$this->translateOptionName($optId, $locale, $translated);
				} catch (\Throwable $e) {
					if ($debug) $say("  [i18n:opt] warn: ".$e->getMessage());
				}
			}

			// titoli variante per SKU
			$skuMap = $titlesBySku[$locale] ?? [];
			foreach ($skuMap as $sku => $title) {
				$vid = $skuToVid[$sku] ?? null;
				if (!$vid) continue;
				try {
					if ($debug) $say("  [i18n:var] {$locale} Variant#{$vid} sku={$sku} -> '{$title}'");
					$this->translateVariantTitle($vid, $locale, $title);
				} catch (\Throwable $e) {
					if ($debug) $say("  [i18n:var] warn: ".$e->getMessage());
				}
			}
		}
	}
	

	// --- i18n collection title/handle ---
	private function translateCollection(int $collectionId, string $locale, array $data, bool $debug = false, ?callable $say = null): void
	{
		$say = $say ?? function() {};
		$gid = "gid://shopify/Collection/{$collectionId}";
		$dig = $this->fetchTranslatableDigests($gid);

		$want = [];
		if (!empty($data['title'])  && isset($dig['title']))  $want['title']  = (string)$data['title'];
		if (!empty($data['handle']) && isset($dig['handle'])) $want['handle'] = (string)$this->slugify((string)$data['handle']);
		if (!$want) return;

		$translations = [];
		foreach ($want as $k=>$v) {
			$translations[] = [
				'key'                       => $k,
				'value'                     => $v,
				'locale'                    => $locale,
				'translatableContentDigest' => $dig[$k],
			];
		}

		$m = <<<'GQL'
	mutation TransColl($id: ID!, $translations: [TranslationInput!]!) {
	  translationsRegister(resourceId: $id, translations: $translations) {
		userErrors { field message code }
	  }
	}
	GQL;
		$res = $this->graphql($m, ['id'=>$gid, 'translations'=>$translations]);

		if (!empty($res['errors'])) {
			$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (collection i18n): ".implode(' | ', $msgs));
		}
		$ue = $res['data']['translationsRegister']['userErrors'] ?? [];
		if ($ue) {
			$msgs = array_map(fn($e)=>($e['message'] ?? 'unknown'), $ue);
			throw new \RuntimeException("collection i18n userErrors: ".implode(' | ', $msgs));
		}
	}
	
	private function syncCollectionsFromPrestashop(
		int $productId,
		array $psCats,
		string $primaryLocale,
		array $textsByLocale,
		bool $debug,
		?callable $say = null
	): void {
		$say = $say ?? function ($m) {};
		
		

		// warmup cache una sola volta per processo
		$this->loadAllCollectionsCache();

		foreach ($psCats as $cat) {
			$primary = $cat['primary'] ?? null;
			if (!$primary) continue;

			$title  = trim((string)$primary['title']);
			$handle = trim((string)($primary['handle'] ?: self::slugify($title)));
			if ($title === '') continue;

			// 1) find-or-create collection by handle
			$existing = null;
			try {
				$existing = $this->findCustomCollectionByHandle($handle);
			} catch (\Throwable $e) { /* fallback a create */ }

			if (!$existing) {
				if ($debug) $say("  [COLL] create handle={$handle} title={$title}");
				$coll = $this->createCustomCollection($title, $handle);
				$cid  = (int)$coll['id'];
			} else {
				$cid = (int)$existing['id'];
				// eventualmente aggiorna il title se diverso
				$existingTitle = (string)($existing['title'] ?? '');
				if ($existingTitle !== $title) {
					if ($debug) $say("  [COLL] update #{$cid} title='{$existingTitle}' -> '{$title}'");
					$this->updateCustomCollection($cid, ['title'=>$title]);
				}
			}

			// 2) ensure collect
			if ($debug) $say("  [COLL] ensureCollect product={$productId} -> collection={$cid}");
			$this->ensureCollect($productId, $cid);

			// 3) translations for collection (per ciascun locale != primario)
			$byLocale = $cat['byLocale'] ?? [];
			foreach ($byLocale as $loc => $vals) {
				if ($loc === $primaryLocale) continue;
				try {
					$this->translateCollection($cid, $loc, $vals, $debug, $say);
					if ($debug) $say("  [COLL] i18n {$loc} on #{$cid} ok");
				} catch (\Throwable $e) {
					if ($debug) $say("  [COLL] i18n {$loc} warn: ".$e->getMessage());
				}
			}
		}
	}
	
	
	private function metafieldsSet(string $ownerGid, array $entries): void
	{
		$m = <<<'GQL'
	mutation MFSet($metafields: [MetafieldsSetInput!]!) {
	  metafieldsSet(metafields: $metafields) {
		metafields { id key namespace type }
		userErrors { field message }
	  }
	}
	GQL;
		$payload = [];
		foreach ($entries as $e) $payload[] = ['ownerId'=>$ownerGid] + $e;
		
		
		

		$res = $this->graphql($m, ['metafields'=>$payload]);

		if (!empty($res['errors'])) {
			$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (metafieldsSet): ".implode(' | ', $msgs));
		}
		$ue = $res['data']['metafieldsSet']['userErrors'] ?? [];
		if ($ue) {
			$msgs = array_map(function($e){
				$f = isset($e['field']) ? implode('.', (array)$e['field']) : '';
				return ($f ? "{$f}: " : '') . ($e['message'] ?? 'unknown');
			}, $ue);
			throw new \RuntimeException("metafieldsSet userErrors: ".implode(' | ', $msgs));
		}
	}
	
	
	// Key safe per metafield: slug ASCII, [a-z0-9_], 3..30 chars
	private static function metafieldKeyFromLabel(string $label, int $fallbackId): string
	{
		$s = mb_strtolower($label, 'UTF-8');
		// traslitterazione basilare; se fallisce, uso direttamente $s
		$t = @iconv('UTF-8', 'ASCII//TRANSLIT//IGNORE', $s);
		if ($t !== false && $t !== '') $s = $t;
		$s = preg_replace('~[^a-z0-9]+~', '_', $s);
		$s = trim($s, '_');
		if (strlen($s) < 3) $s = 'f_' . $fallbackId;
		if (strlen($s) > 30) $s = substr($s, 0, 30);
		return $s;
	}
	
	

	
	
	// Dentro App\ShopifyClient
	private function readProductMetafields(int $productId, string $namespace = 'prestashop'): array
	{
		$gid = "gid://shopify/Product/{$productId}";
		$q = <<<'GQL'
	query ReadMF($id: ID!, $ns: String!) {
	  node(id: $id) {
		... on Product {
		  id
		  metafields(first: 100, namespace: $ns) {
			edges {
			  node {
				id
				namespace
				key
				type
				value
			  }
			}
		  }
		}
	  }
	}
	GQL;

		$res = $this->graphql($q, ['id' => $gid, 'ns' => $namespace]);

		// Se la query ha errori, solleva eccezione con messaggio leggibile
		if (!empty($res['errors'])) {
			$msgs = array_map(fn($e) => $e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (readProductMetafields): " . implode(' | ', $msgs));
		}

		$rows = [];
		$edges = $res['data']['node']['metafields']['edges'] ?? [];
		foreach ($edges as $e) {
			// qui 'type' è già una stringa (es. "single_line_text_field", "json", "list.single_line_text_field", ...)
			$rows[] = $e['node'];
		}
		return $rows;
	}


	
	
	

	
	
	
	
	private function registerProductMetafieldTranslation(
		int $productId,
		string $namespace,
		string $key,
		string $locale,
		string $translatedValue
	): void {
		// 1) Trova il GID del metafield (per namespace+key) sul prodotto
		$metas = $this->readProductMetafields($productId, $namespace); // hai già questo helper
		
		/*
		echo "<br><br><br><br><br><br>----------------<br>";
		echo json_encode($metas);
		echo "<br>----------------<br><br><br><br><br><br>";
		*/
		
		$mfGid = null;
		foreach ($metas as $mf) {
			if (($mf['key'] ?? '') === $key) {
				$mfGid = (string)($mf['id'] ?? '');
				break;
			}
		}
		
		if (!$mfGid) {
			// niente metafield => niente traduzione (evito eccezione per non bloccare il flusso)
			return;
		}
		
		

		// 2) Ottieni il digest del campo "value" del metafield
		$q = <<<'GQL'
	query($id: ID!) {
	  translatableResource(resourceId: $id) {
		resourceId
		translatableContent {
		  key
		  digest
		}
	  }
	}
	GQL;
		
		
		
		
		
		$res = $this->graphql($q, ['id' => $mfGid]);
		if (!empty($res['errors'])) {
			$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
			throw new \RuntimeException("GraphQL errors (get mf digest): ".implode(' | ', $msgs));
		}
		$digest = null;
		foreach (($res['data']['translatableResource']['translatableContent'] ?? []) as $row) {
			if (($row['key'] ?? '') === 'value' && !empty($row['digest'])) {
				$digest = (string)$row['digest'];
				break;
			}
		}
		
		
		
		if ($digest === null) {
			// La definition non è translatable oppure il tipo non espone 'value'
			return;
		}

		// 3) Registra la traduzione sul METAFIELD (resourceId = metafield GID), key="value"
		$m = <<<'GQL'
	mutation SetMetaTranslation($id: ID!, $locale: String!, $digest: String!, $val: String!) {
	  translationsRegister(
		resourceId: $id,
		translations: [{
		  key: "value",
		  value: $val,
		  locale: $locale,
		  translatableContentDigest: $digest
		}]
	  ) {
		userErrors { field message }
	  }
	}
	GQL;

		$out = $this->graphql($m, [
			'id'      => $mfGid,
			'locale'  => $locale,
			'digest'  => $digest,
			'val'     => $translatedValue,
		]);
		if (!empty($out['errors'])) {
			$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $out['errors']);
			throw new \RuntimeException("GraphQL errors (metafield translation): ".implode(' | ', $msgs));
		}
		$ue = $out['data']['translationsRegister']['userErrors'] ?? [];
		if ($ue) {
			$msgs = array_map(fn($e)=>($e['message'] ?? 'unknown'), $ue);
			throw new \RuntimeException("metafield translation userErrors: ".implode(' | ', $msgs));
		}
	}

	
	
	private function ensurePrestashopMetafieldDefinitions(
		bool $debug = false,
		?callable $say = null,
		array $extraTextDefs = []   // ogni item: ['key'=>..., 'name'=>...]
	): void {
		static $doneOnce = false;
		$say = $say ?? function ($m) {};

		$namespace = 'prestashop';

		if ($doneOnce && !$extraTextDefs) return;

		// Definizioni "legacy" (facoltative)
		$defsWanted = [
			['key' => 'ps_features',       'name' => 'Prestashop Features',       'type' => 'json'],
			['key' => 'ps_feature_values', 'name' => 'Prestashop Feature Values', 'type' => 'json'],
			['key' => 'ps_feature_pairs',  'name' => 'Prestashop Feature Pairs',  'type' => 'json'],
			['key' => 'attachments', 'name' => 'Prestashop Attachments', 'type' => 'list.file_reference'],

		];

		// Aggiungo quelle dinamiche: testo singola riga, TRANSLATABLE
		foreach ($extraTextDefs as $d) {
			$defsWanted[] = [
				'key'          => $d['key'],
				'name'         => $d['name'] ?? ('PS '.$d['key']),
				'type'         => 'single_line_text_field',
				'translatable' => true, // <-- importante
			];
		}

		// 1) leggi cosa c'è già
		$q = <<<'GQL'
	query MFDefs($ownerType: MetafieldOwnerType!, $namespace: String!) {
	  metafieldDefinitions(ownerType: $ownerType, namespace: $namespace, first: 200) {
		edges { node { id key name namespace } }
	  }
	}
	GQL;
		$res = $this->graphql($q, ['ownerType' => 'PRODUCT', 'namespace' => $namespace]);
		$have = [];
		foreach (($res['data']['metafieldDefinitions']['edges'] ?? []) as $e) {
			$have[$e['node']['key']] = true;
		}

		// 2) crea le mancanti (uso MetafieldDefinitionInput)
		$m = <<<'GQL'
	mutation MFDefCreate($definition: MetafieldDefinitionInput!) {
	  metafieldDefinitionCreate(definition: $definition) {
		createdDefinition { id key name namespace }
		userErrors { field message code }
	  }
	}
	GQL;

		foreach ($defsWanted as $d) {
			if (isset($have[$d['key']])) {
				if ($debug) $say("  [MF-DEF] esiste già {$namespace}.{$d['key']}");
				continue;
			}
			$defInput = [
				'name'      => $d['name'],
				'namespace' => $namespace,
				'key'       => $d['key'],
				'ownerType' => 'PRODUCT',
				'type'      => $d['type'],
			];
			
			
			//if (!empty($d['translatable'])) {
			//	$defInput['translatable'] = true; // se lo schema lo supporta, bene; altrimenti Shopify ignora con userError non-bloccante
			//}

			$out = $this->graphql($m, ['definition' => $defInput]);
			if (!empty($out['errors'])) {
				$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $out['errors']);
				throw new \RuntimeException("metafieldDefinitionCreate errors: ".implode(' | ', $msgs));
			}
			$ue = $out['data']['metafieldDefinitionCreate']['userErrors'] ?? [];
			if ($ue) {
				$joined = implode(' | ', array_map(function($e){
					$f = isset($e['field']) ? implode('.', (array)$e['field']) : '';
					return ($f ? "{$f}: " : '') . ($e['message'] ?? 'unknown');
				}, $ue));
				// consento messaggi tipo "already exists"/"taken"/"Field not defined" su translatable
				if (
					stripos($joined, 'already') === false &&
					stripos($joined, 'taken')   === false &&
					stripos($joined, 'exists')  === false &&
					stripos($joined, 'Field is not defined') === false
				) {
					throw new \RuntimeException("metafieldDefinitionCreate userErrors: ".$joined);
				}
			} else {
				if ($debug) $say("  [MF-DEF] creato {$namespace}.{$d['key']}");
			}
			self::tick();
		}

		$doneOnce = true;
	}

	private function upsertPrestashopFeaturesMetafields(
		int $productId,
		array $bundle,           // output di Prestashop::getFeaturesBundle
		string $primaryLocale,
		bool $debug = false,
		?callable $say = null
	): void {
		$say = $say ?? function ($m) {};
		$ownerGid = "gid://shopify/Product/{$productId}";
		
		
		
		$featuresMap = $bundle['features'] ?? []; // [fid => ['primary'=>['name'=>...], 'byLocale'=>[...] ]]
		$valuesMap   = $bundle['values']   ?? []; // [fid => [vid => ['primary'=>['value'=>...], 'byLocale'=>[...] ]]]
		
		
		// 1) costruisco base entries (lingua primaria) e mappa per traduzioni
		$textDefs   = [];   // per definizioni dinamiche (translatable)
		$entries    = [];   // per metafieldsSet (valore primario)
		$i18nValues = [];   // key => [ locale => value ]

		foreach ($valuesMap as $fid => $vals) {
			$label = trim((string)($featuresMap[$fid]['primary']['name'] ?? ''));
			if ($label === '') continue;

			$key = self::metafieldKeyFromLabel($label, (int)$fid);
			$textDefs[] = ['key'=>$key, 'name'=>$label];

			// --- valore primario (unisco multi-valori con " / ")
			$primaryVals = [];
			foreach ($vals as $vid => $val) {
				$v = trim((string)($val['primary']['value'] ?? ''));
				if ($v !== '') $primaryVals[] = $v;
			}
			if ($primaryVals) {
				$entries[] = [
					'namespace' => 'prestashop',
					'key'       => $key,
					'type'      => 'single_line_text_field',
					'value'     => implode(' / ', array_unique($primaryVals)),
				];
			}
			
			

			// --- per ciascun locale presente, costruisco la stringa tradotta
			//     (se un valore manca in quel locale, salto quel valore)
			$localesSeen = [];
			foreach ($vals as $vid => $val) {
				foreach (($val['byLocale'] ?? []) as $loc => $payload) {
					$t = trim((string)($payload['value'] ?? ''));
					if ($t === '') continue;
					$i18nValues[$key][$loc][] = $t;
					$localesSeen[$loc] = true;
				}
			}
			// se c'è anche nome feature tradotto lo uso solo per la Definition (già fatto con 'name')
			// i18nValues resta solo per i "value"
			if (isset($featuresMap[$fid]['byLocale'])) {
				// nulla di specifico da fare qui per il value
			}
		}

		
		if ($debug) {
			$say("  [MF-DBG] textDefs=" . count($textDefs) . " entries=" . count($entries));
			$say("  [MF-DBG] sampleKeys=" . json_encode(array_slice(array_map(fn($d)=>$d['key'], $textDefs), 0, 10), JSON_UNESCAPED_SLASHES|JSON_UNESCAPED_UNICODE));
		}
		
		// 2) assicuro definizioni (translatable = true)
		try {
			$this->ensurePrestashopMetafieldDefinitions($debug, $say, $textDefs);
			$say("[MF-A] after ensure OK");
		} catch (\Throwable $e) {
			$say("[MF-A] ensure THREW: " . get_class($e) . " :: " . $e->getMessage());
			throw $e; // per non “nascondere” il problema
		}

		// 3) scrivo i valori primari
		if ($entries) {
			if ($debug) $say("  [MF] upsert ".count($entries)." metafield dinamici nel namespace 'prestashop' (lingua primaria)");
			$this->metafieldsSet($ownerGid, $entries);
		} else {
			if ($debug) $say("  [MF] nessuna feature da scrivere");
			return;
		}
		// 4) leggo i metafield appena scritti per ottenere i GID (key -> gid)
		$existing = $this->readProductMetafields($productId, 'prestashop'); // ritorna id=GID, key, value, ...	
		
		
		
		$keyToGid = [];
		foreach ($existing as $row) {
			$k = $row['key'] ?? null;
			$gid = $row['id'] ?? null; // GID completo
			if ($k && $gid) $keyToGid[$k] = $gid;
		}

		// 5) registra TRADUZIONI per ciascuna lingua (diversa dalla primaria)
		foreach ($i18nValues as $key => $map) {
			foreach ($map as $loc => $vals) {
				if ($loc === $primaryLocale) continue;   // <--- SKIP primaria

				$vals = array_values(array_unique(array_filter(array_map('trim', $vals), fn($s)=>$s!=='')));
				if (!$vals) continue;
				$valueStr = implode(' / ', $vals);
				try {
					if ($debug) $say("  [MF-i18n] {$key} -> {$loc} = '{$valueStr}'");
					$this->registerProductMetafieldTranslation($productId, 'prestashop', $key, $loc, $valueStr);
				} catch (\Throwable $e) {
					if ($debug) $say("  [MF-i18n] warn {$key} {$loc}: ".$e->getMessage());
				}
			}
		}

		if ($debug) {
			$read = $this->readProductMetafields($productId, 'prestashop');
			$say("  [MF] ora presenti ".count($read)." metafield 'prestashop' (con traduzioni registrate dove disponibili)");
		}
	}
	
	////fix velocità
	
	
	// sotto ai metodi GraphQL/REST
	private function loadAllCollectionsCache(): void
	{
		if ($this->collectionsCacheLoaded) return;

		$query = <<<'GQL'
	query($first: Int!, $after: String) {
	  collections(first: $first, after: $after) {
		edges {
		  cursor
		  node {
			id
			handle
			title
		  }
		}
		pageInfo {
		  hasNextPage
		  endCursor
		}
	  }
	}
	GQL;

		$after = null;
		$first = 250;
		$map   = [];

		do {
			$vars = ['first' => $first, 'after' => $after];
			$res  = $this->graphql($query, $vars);

			if (!empty($res['errors'])) {
				$msgs = array_map(fn($e)=>$e['message'] ?? json_encode($e, JSON_UNESCAPED_UNICODE), $res['errors']);
				throw new \RuntimeException("GraphQL collections errors: ".implode(' | ', $msgs));
			}

			$edges = $res['data']['collections']['edges'] ?? [];
			foreach ($edges as $e) {
				$n = $e['node'] ?? [];
				$id     = isset($n['id']) ? (int)preg_replace('~\D+~','',$n['id']) : null;
				$handle = (string)($n['handle'] ?? '');
				$title  = (string)($n['title']  ?? '');
				if ($id && $handle !== '') {
					$map[$handle] = ['id'=>$id, 'title'=>$title];
				}
			}

			$pi = $res['data']['collections']['pageInfo'] ?? [];
			$hasNext  = (bool)($pi['hasNextPage'] ?? false);
			$after    = $hasNext ? ($pi['endCursor'] ?? null) : null;
		} while ($after);

		$this->collectionsByHandle     = $map;
		$this->collectionsCacheLoaded  = true;
	}
	
	
	private function getCollectionFromCacheByHandle(string $handle): ?array
	{
		$this->loadAllCollectionsCache();
		return $this->collectionsByHandle[$handle] ?? null;
	}
	
	private function findCustomCollectionByHandle(string $handle): ?array
	{
		$row = $this->getCollectionFromCacheByHandle($handle);
		if (!$row) return null;
		// normalizzo al formato usato dal resto del codice
		return ['id' => $row['id'], 'handle' => $handle, 'title' => $row['title']];
	}
	
	
	private function createCustomCollection(string $title, string $handle): array
	{
		$payload = ['custom_collection' => ['title'=>$title, 'handle'=>$handle, 'published'=>true]];
		$resp = $this->rest('POST', 'custom_collections.json', $payload);
		self::tick();
		$coll = $resp['custom_collection'] ?? $resp;

		// update cache
		$id = (int)($coll['id'] ?? 0);
		if ($id) {
			$this->collectionsByHandle[$handle] = ['id'=>$id, 'title'=>$title];
			$this->collectionsCacheLoaded = true; // cache valida
		}
		return $coll;
	}
	
	
	private function updateCustomCollection(int $collectionId, array $fields): array
	{
		$payload = ['custom_collection' => ['id'=>$collectionId] + $fields];
		$resp = $this->rest('PUT', "custom_collections/{$collectionId}.json", $payload);
		self::tick();
		$coll = $resp['custom_collection'] ?? $resp;

		// update cache title se conosciamo l'handle (lo trovi passando dal mapping inverso)
		$title = (string)($fields['title'] ?? ($coll['title'] ?? ''));
		if ($title !== '') {
			// trova l'handle attuale da mappa (lineare: poche decine normalmente)
			foreach ($this->collectionsByHandle as $h => $row) {
				if (($row['id'] ?? null) === $collectionId) {
					$this->collectionsByHandle[$h]['title'] = $title;
					break;
				}
			}
		}
		return $coll;
	}

	
	private function ensureCollect(int $productId, int $collectionId): void
	{
		$key = $productId . ':' . $collectionId;
		if (isset($this->collectCache[$key])) return; // già verificato/creato in questo processo

		// controlla se esiste un collect (una sola volta per processo)
		$resp = $this->rest('GET', "collects.json?product_id={$productId}&collection_id={$collectionId}&limit=1");
		$exists = !empty($resp['collects']);
		if ($exists) {
			$this->collectCache[$key] = true;
			return;
		}

		// crea link
		try {
			$this->rest('POST', 'collects.json', [
				'collect' => [
					'product_id'    => $productId,
					'collection_id' => $collectionId,
				],
			]);
		} catch (\Throwable $e) {
			// se 422 already exists (race), ignora
			if (strpos($e->getMessage(), '422') === false) throw $e;
		}
		self::tick();
		$this->collectCache[$key] = true; // evita chiamate successive
	}
	
	
	
	// Dentro ShopifyClient
	private function ensureRedirect(string $fromPath, string $toPath): void {
		// normalizza: i redirect Shopify lavorano sul PATH assoluto
		$from = '/' . ltrim($fromPath, '/');
		$to   = '/' . ltrim($toPath,   '/');

		// (opzionale) evita duplicati banali in memoria processo
		static $seen = [];
		$key = $from.'→'.$to;
		if (isset($seen[$key])) return;

		// Shopify non ha regex via API: crea uno per ogni sorgente
		$this->rest('POST', 'redirects.json', [
			'redirect' => [
				'path'   => $from,
				'target' => $to,
			]
		]);
		$seen[$key] = true;
	}

	public function ensureProductLegacyRedirects(
		int $psProductId,
		array $textsByLocale,
		string $shopifyHandle,
		string $primaryLocale
	): void {
		// Esempi di pattern PS tipici: adatta ai tuoi URL reali
		//  - /{lang}/{id}-{slug}.html  -> /{lang}/products/{handle}
		//  - /{id}-{slug}.html         -> /products/{handle}
		// Costruiamo sia con lang prefix che senza, usando gli slug disponibili
		foreach ($textsByLocale as $loc => $t) {
			$slug = trim((string)($t['handle'] ?? $t['name'] ?? ''));
			if ($slug === '') continue;

			$legacy = sprintf('%d-%s.html', $psProductId, $slug);
			$fromNoLang = '/' . $legacy;
			$toNoLang   = '/products/' . $shopifyHandle;

			// redirect senza prefisso lingua
			$this->ensureRedirect($fromNoLang, $toNoLang);

			// se usi Shopify Markets con prefissi lingua:
			$fromLang = '/' . $loc . '/' . $legacy;
			$toLang   = '/' . $loc . '/products/' . $shopifyHandle;
			$this->ensureRedirect($fromLang, $toLang);
		}
	}
	
	
	private function publishProductToChannel(string $productGid, string $publicationGid): void
	{
		$q = <<<'GQL'
	mutation publish($id: ID!, $publicationId: ID!) {
	  publishablePublish(
		id: $id,
		input: { publicationId: $publicationId }
	  ) {
		userErrors {
		  field
		  message
		}
	  }
	}
	GQL;

		$res = $this->graphql($q, [
			'id' => $productGid,
			'publicationId' => $publicationGid,
		]);

		$ue = $res['data']['publishablePublish']['userErrors'] ?? [];
		if (!empty($ue)) {
			throw new \RuntimeException(
				'publishablePublish userErrors: ' . json_encode($ue)
			);
		}
	}
	
	
	
		/**
	 * Carica un file locale (PDF/manuale) su Shopify Files e ritorna il GID del GenericFile.
	 * Usa stagedUploadsCreate + upload multipart + fileCreate(contentType: FILE).
	 */
	private function ensureShopifyGenericFileFromLocalPath(
		string $localPath,
		string $filename,
		string $mimeType,
		string $psHashKey = '',
		bool $debug = false,
		?callable $say = null
	): string {
		$say = $say ?? function($m){};

		// cache per hash PS (se fornito)
		if ($psHashKey !== '' && isset($this->genericFileIdByPsHash[$psHashKey])) {
			return $this->genericFileIdByPsHash[$psHashKey];
		}

		if (!is_file($localPath) || !is_readable($localPath)) {
			throw new RuntimeException("Attachment file not readable: {$localPath}");
		}

		$size = filesize($localPath);
		if ($size === false || $size <= 0) {
			throw new RuntimeException("Attachment file empty: {$localPath}");
		}

		// 1) stagedUploadsCreate
		$qStage = <<<'GQL'
mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
  stagedUploadsCreate(input: $input) {
    stagedTargets {
      url
      resourceUrl
      parameters { name value }
    }
    userErrors { field message }
  }
}
GQL;

		$vars = [
			'input' => [[
				'resource'  => 'FILE',
				'filename'  => $filename,
				'mimeType'  => $mimeType,
				'fileSize'  => (string)$size,
				'httpMethod'=> 'POST',
			]],
		];

		$r = $this->graphql($qStage, $vars);
		$ue = $r['data']['stagedUploadsCreate']['userErrors'] ?? [];
		if (!empty($ue)) {
			throw new RuntimeException('stagedUploadsCreate userErrors: ' . json_encode($ue, JSON_UNESCAPED_UNICODE));
		}
		$target = $r['data']['stagedUploadsCreate']['stagedTargets'][0] ?? null;
		if (!$target || empty($target['url']) || empty($target['resourceUrl'])) {
			throw new RuntimeException('stagedUploadsCreate: risposta inattesa: ' . json_encode($r, JSON_UNESCAPED_UNICODE));
		}

		// 2) upload multipart
		$this->multipartPostFile($target['url'], $target['parameters'] ?? [], $localPath);

		// 3) fileCreate (GenericFile)
		$qCreate = <<<'GQL'
mutation fileCreate($files: [FileCreateInput!]!) {
  fileCreate(files: $files) {
    files {
      ... on GenericFile { id url }
    }
    userErrors { field message }
  }
}
GQL;

		$out = $this->graphql($qCreate, [
			'files' => [[
				'contentType'    => 'FILE',
				'originalSource' => $target['resourceUrl'],
				'alt'            => $filename,
			]],
		]);

		$ue2 = $out['data']['fileCreate']['userErrors'] ?? [];
		if (!empty($ue2)) {
			throw new RuntimeException('fileCreate userErrors: ' . json_encode($ue2, JSON_UNESCAPED_UNICODE));
		}

		$fileId = $out['data']['fileCreate']['files'][0]['id'] ?? '';
		if ($fileId === '') {
			throw new RuntimeException('fileCreate: risposta inattesa: ' . json_encode($out, JSON_UNESCAPED_UNICODE));
		}

		if ($psHashKey !== '') {
			$this->genericFileIdByPsHash[$psHashKey] = $fileId;
		}

		if ($debug) {
			$say("  [ATT] file uploaded filename={$filename} mime={$mimeType} fileId={$fileId}");
		}

		return $fileId;
	}

	/**
	 * Imposta un metafield di tipo list.file_reference sul PRODOTTO.
	 * namespace default: prestashop, key default: attachments
	 */
	private function setProductAttachmentsMetafield(int $productId, array $fileGids, string $namespace = 'prestashop', string $key = 'attachments'): void
	{
		$fileGids = array_values(array_filter(array_map('strval', $fileGids)));
		if (!$fileGids) return;

		$ownerId = "gid://shopify/Product/{$productId}";

		$q = <<<'GQL'
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key type }
    userErrors { field message }
  }
}
GQL;

		$vars = [
			'metafields' => [[
				'ownerId'   => $ownerId,
				'namespace' => $namespace,
				'key'       => $key,
				'type'      => 'list.file_reference',
				'value'     => json_encode($fileGids, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES),
			]],
		];

		$res = $this->graphql($q, $vars);
		$ue = $res['data']['metafieldsSet']['userErrors'] ?? [];
		if (!empty($ue)) {
			throw new RuntimeException('metafieldsSet userErrors: ' . json_encode($ue, JSON_UNESCAPED_UNICODE));
		}
	}

	/**
	 * Sync allegati PrestaShop -> Shopify Files + metafield list.file_reference sul prodotto.
	 * Non crea definizioni, usa solo metafieldsSet.
	 */
	private function syncAttachmentsFromPrestashop(int $productId, Prestashop $ps, int $psProductId, string $primaryLocale, bool $debug = false, ?callable $say = null): void
	{
		$say = $say ?? function($m){};

		$cfg = $ps->getConfig();
		$langMap = $cfg['lang_map'] ?? [];
		$primaryLangId = (int)($cfg['primary_lang_id'] ?? 1);

		$atts = $ps->getProductAttachments($psProductId, $langMap, $primaryLangId);
		if (!$atts) {
			if ($debug) $say("  [ATT] no attachments on PS product={$psProductId}");
			return;
		}

		$fileGids = [];
		foreach ($atts as $a) {
			$hash = (string)($a['hash'] ?? '');
			$path = (string)($a['local_path'] ?? '');
			$fn   = (string)($a['filename'] ?? ('attachment_' . ($a['id_attachment'] ?? '')));
			$mime = (string)($a['mime'] ?? 'application/octet-stream');

			$fileGids[] = $this->ensureShopifyGenericFileFromLocalPath($path, $fn, $mime, $hash, $debug, $say);

			// Se Prestashop ha scaricato in tmp (fallback), puoi pulire qui se vuoi
			// ma attenzione: se path è reale in /download non va cancellato.
			if (strpos($path, sys_get_temp_dir()) === 0) {
				@unlink($path);
			}
		}

		$this->setProductAttachmentsMetafield($productId, $fileGids, 'prestashop', 'attachments');

		if ($debug) {
			$say("  [ATT] set prestashop.attachments count=" . count($fileGids));
		}
	}

	
private function withPrestashopIdInHandle(?string $handle, $psId): ?string
{
    $handle = $handle !== null ? trim($handle) : null;
    $psId = trim((string)$psId);

    if ($handle === '' || $handle === null) return null;
    if ($psId === '') return $handle;

    // evita doppio suffisso
    if (preg_match('~-' . preg_quote($psId, '~') . '$~', $handle)) {
        return $handle;
    }

    // Shopify handle max 255: taglia base se necessario
    $suffix = '-' . $psId;
    $maxBaseLen = 255 - strlen($suffix);
    if ($maxBaseLen < 1) {
        // caso patologico: psId enorme -> taglia psId
        $psId = substr($psId, 0, 32);
        $suffix = '-' . $psId;
        $maxBaseLen = 255 - strlen($suffix);
    }
    $base = substr($handle, 0, max(1, $maxBaseLen));

    return $base . $suffix;
}



}

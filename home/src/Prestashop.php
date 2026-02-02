<?php
declare(strict_types=1);

namespace App;

use PDO;

final class Prestashop
{
    private PDO $pdo;
    private array $cfg;

    public function __construct(array $psConfig)
    {
        $this->cfg = $psConfig;
        $this->pdo = new PDO(
            $psConfig['dsn'],
            $psConfig['user'],
            $psConfig['password'],
            [
                PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
            ]
        );
    }

    // -------- Utils --------
    private static function slugify(string $s): string {
        $s = trim(mb_strtolower($s, 'UTF-8'));
        $s = preg_replace('~[^\pL0-9]+~u', '-', $s);
        $s = trim($s, '-');
        return $s === '' ? 'item' : $s;
    }
    private static function imagePath(int $idImage): string {
        $id = (string)$idImage;
        return implode('/', str_split($id)) . '/' . $id;
    }
    private function imageUrl(int $idImage): string {
        return rtrim($this->cfg['base_url'], '/') . '/img/p/' . self::imagePath($idImage) . '.jpg';
    }
	
	
	
	
	
	// Dentro class App\Prestashop

	/**
	 * Conta i prodotti (opzionale: filtra per brand/manufacturer).
	 * @param ?int $brandId  id_manufacturer oppure null per tutti
	 */
	public function countProductsFiltered(?int $brandId = null): int
	{
		$prefix = $this->cfg['db_prefix'] ?? $this->cfg['prefix'] ?? 'ps_';
		$pdo    = $this->pdo;  // assumo che tu abbia un PDO in $this->pdo
		$shopId = (int)($this->cfg['shop_id'] ?? 1);  // id shop per stock_available

		$sql = "SELECT COUNT(*) AS c
				FROM {$prefix}product p
				
				LEFT JOIN {$prefix}product_shop ps
                ON ps.id_product = p.id_product AND ps.id_shop = :shopId
				
				WHERE ps.active=1  ";
		$params = [];
		$params = [':shopId' => $shopId];

		if ($brandId !== null) {
			$sql .= " AND p.id_manufacturer = :bid";
			$params[':bid'] = $brandId;
		}

		// se vuoi contare solo attivi, decommenta:
		// $sql .= " AND p.active = 1";
		
		//echo $sql; exit();

		$stmt = $pdo->prepare($sql);
		$stmt->execute($params);
		return (int)$stmt->fetchColumn();
	}

	/**
	 * Estrae il blocco di prodotti “base” con limit/offset e filtro opzionale per brand.
	 * Ritorna array di righe con chiavi: id_product, reference, base_price, weight, brand, quantity
	 *
	 * @param int      $limit    numero massimo righe da estrarre
	 * @param int      $offset   offset di partenza (0-based)
	 * @param ?int     $brandId  id_manufacturer o null per tutti
	 */
	public function getBaseProductsFiltered(int $limit, int $offset, ?int $brandId = null): array
	{
		$prefix = $this->cfg['db_prefix'] ?? $this->cfg['prefix'] ?? 'ps_';
		$pdo    = $this->pdo;                         // PDO esistente
		$shopId = (int)($this->cfg['shop_id'] ?? 1);  // id shop per stock_available

		// NB: quantity da stock_available per il prodotto “base” (id_product_attribute = 0)
		//     se hai multi-warehouse/advanced stock, qui potresti dover sommare a livello di location.
		$sql = "SELECT
				  p.id_product,
				  p.reference,
				  p.price       AS base_price,
				  p.weight,
				  m.name        AS brand,
				  COALESCE(sa.quantity, 0) AS quantity
				FROM {$prefix}product p
				LEFT JOIN {$prefix}manufacturer m
					   ON m.id_manufacturer = p.id_manufacturer
				LEFT JOIN {$prefix}stock_available sa
					   ON sa.id_product = p.id_product
					  AND sa.id_product_attribute = 0
					  AND (sa.id_shop = :shopId OR sa.id_shop IS NULL)
				LEFT JOIN {$prefix}product_shop ps
                ON ps.id_product = p.id_product AND ps.id_shop = :shopId
				
				WHERE ps.active=1 ";
		$params = [':shopId' => $shopId];

		if ($brandId !== null) {
			$sql .= " AND p.id_manufacturer = :bid";
			$params[':bid'] = $brandId;
		}

		// se vuoi solo attivi, decommenta:
		// $sql .= " AND p.active = 1";

		$sql .= " ORDER BY p.id_product ASC
				  LIMIT :lim OFFSET :off";

		$stmt = $pdo->prepare($sql);
		// bindValue esplicito per interi su LIMIT/OFFSET
		foreach ($params as $k => $v) $stmt->bindValue($k, $v, is_int($v) ? \PDO::PARAM_INT : \PDO::PARAM_STR);
		$stmt->bindValue(':lim', $limit,  \PDO::PARAM_INT);
		$stmt->bindValue(':off', $offset, \PDO::PARAM_INT);
		$stmt->execute();

		$rows = $stmt->fetchAll(\PDO::FETCH_ASSOC) ?: [];

		// normalizzazione minima dei tipi
		foreach ($rows as &$r) {
			$r['id_product'] = (int)$r['id_product'];
			$r['reference']  = $r['reference'] ?? null;
			$r['base_price'] = (float)$r['base_price'];
			$r['weight']     = (float)$r['weight'];
			$r['brand']      = $r['brand'] ?? null;
			$r['quantity']   = (int)$r['quantity'];
		}
		unset($r);

		return $rows;
	}

	/**
	 * Estrae prodotti “base” dato un elenco di id_product (utile per sincronizzare selezioni puntuali).
	 * Ritorna righe con chiavi: id_product, reference, base_price, weight, brand, quantity
	 *
	 * @param int[] $ids
	 */
	public function getBaseProductsByIds(array $ids): array
	{
		$ids = array_values(array_unique(array_map('intval', $ids)));
		$ids = array_filter($ids, fn($v) => $v > 0);
		if (!$ids) return [];

		$prefix = $this->cfg['db_prefix'] ?? $this->cfg['prefix'] ?? 'ps_';
		$pdo    = $this->pdo;
		$shopId = (int)($this->cfg['shop_id'] ?? 1);

		$placeholders = implode(',', array_fill(0, count($ids), '?'));
		$sql = "SELECT
				  p.id_product,
				  p.reference,
				  p.price       AS base_price,
				  p.weight,
				  m.name        AS brand,
				  COALESCE(sa.quantity, 0) AS quantity
				FROM {$prefix}product p
				LEFT JOIN {$prefix}manufacturer m
				       ON m.id_manufacturer = p.id_manufacturer
				LEFT JOIN {$prefix}stock_available sa
				       ON sa.id_product = p.id_product
				      AND sa.id_product_attribute = 0
				      AND (sa.id_shop = ? OR sa.id_shop IS NULL)
				LEFT JOIN {$prefix}product_shop ps
				  ON ps.id_product = p.id_product AND ps.id_shop = ?
				WHERE ps.active = 1
				  AND p.id_product IN ($placeholders)
				ORDER BY p.id_product ASC";

		$stmt = $pdo->prepare($sql);
		$bind = array_merge([$shopId, $shopId], $ids);
		$stmt->execute($bind);
		$rows = $stmt->fetchAll(\PDO::FETCH_ASSOC) ?: [];

		foreach ($rows as &$r) {
			$r['id_product'] = (int)$r['id_product'];
			$r['reference']  = $r['reference'] ?? null;
			$r['base_price'] = (float)$r['base_price'];
			$r['weight']     = (float)$r['weight'];
			$r['brand']      = $r['brand'] ?? null;
			$r['quantity']   = (int)$r['quantity'];
		}
		unset($r);

		return $rows;
	}
	
	

    // -------- Reads --------
    public function getBaseProducts(int $limit): array
    {
        $p   = $this->cfg['prefix'];
        $idS = (int)$this->cfg['id_shop'];

        $sql = "
            SELECT
                p.id_product,
                p.reference,
                p.price AS base_price,
                p.weight,
                m.name AS brand,
                sa.quantity
            FROM {$p}product p
            LEFT JOIN {$p}manufacturer m
                ON m.id_manufacturer = p.id_manufacturer
            LEFT JOIN {$p}stock_available sa
                ON sa.id_product = p.id_product AND sa.id_product_attribute = 0
            LEFT JOIN {$p}product_shop ps
                ON ps.id_product = p.id_product AND ps.id_shop = :id_shop
            WHERE ps.active = 1
            ORDER BY p.id_product ASC
            LIMIT :lim
        ";
        $st = $this->pdo->prepare($sql);
        $st->bindValue(':id_shop', $idS, PDO::PARAM_INT);
        $st->bindValue(':lim', $limit, PDO::PARAM_INT);
        $st->execute();
        return $st->fetchAll();
    }

    /** Ritorna testi per locale: [ 'it' => ['name','description','meta_*','handle'], ... ] */
    public function getTextsByLocale(int $idProduct, array $langMap): array
    {
        if (!$langMap) return [];
        $p   = $this->cfg['prefix'];
        $ids = implode(',', array_map('intval', array_keys($langMap)));

        $sql = "
            SELECT pl.id_lang, pl.name, pl.description, pl.description_short,
                   pl.meta_title, pl.meta_description, pl.link_rewrite
            FROM {$p}product_lang pl
            WHERE pl.id_product = :idp AND pl.id_lang IN ($ids)
        ";
        $st = $this->pdo->prepare($sql);
        $st->execute(['idp' => $idProduct]);

        $out = [];
        while ($r = $st->fetch()) {
            $loc = $langMap[(int)$r['id_lang']] ?? null;
            if (!$loc) continue;
            $desc = $r['description'] ?: ($r['description_short'] ?? '');
			$desc_short = $r['description_short'];
            $out[$loc] = [
                'name'             => $r['name'] ?? '',
                'description'      => $desc,
				'description_short'      => $desc_short,
                'meta_title'       => $r['meta_title'] ?? '',
                'meta_description' => $r['meta_description'] ?? '',
                'handle'           => self::slugify($r['link_rewrite'] ?? '') . "_" . $idProduct,
            ];
        }
        return $out;
    }

    /** Ritorna URL immagini ordinate (cover prima) */
    public function getImageUrls(int $idProduct): array
    {
        $p = $this->cfg['prefix'];
        $sql = "SELECT id_image FROM {$p}image
                WHERE id_product = :idp
                ORDER BY cover DESC, position ASC, id_image ASC";
        $st = $this->pdo->prepare($sql);
        $st->execute(['idp' => $idProduct]);

        $urls = [];
        while ($r = $st->fetch()) {
            $urls[] = $this->imageUrl((int)$r['id_image']);
        }
        return $urls;
    }

    public function hasCombinations(int $idProduct): bool
    {
        $p = $this->cfg['prefix'];
        $st = $this->pdo->prepare("SELECT COUNT(*) c FROM {$p}product_attribute WHERE id_product = :idp");
        $st->execute(['idp' => $idProduct]);
        return ((int)$st->fetchColumn()) > 0;
    }

    /** Combinazioni con option_pairs: ['Gruppo' => 'Valore', ...] + quantity */
    public function getCombinations(int $idProduct, int $anyLangId): array
    {
        $p = $this->cfg['prefix'];
        $sql = "
            SELECT
                pa.id_product_attribute,
                pa.reference,
                pa.price AS price_impact,
                pa.weight AS weight_impact,
                sa.quantity,
				pa.ean13,
                GROUP_CONCAT(CONCAT(agl.name, '::', al.name, '::', a.id_attribute, '::', ag.id_attribute_group, '::', ag.is_color_group, '::', IFNULL(a.color,'')) SEPARATOR '||') AS options
            FROM {$p}product_attribute pa
            LEFT JOIN {$p}stock_available sa
                ON sa.id_product_attribute = pa.id_product_attribute
            INNER JOIN {$p}product_attribute_combination pac
                ON pac.id_product_attribute = pa.id_product_attribute
            INNER JOIN {$p}attribute a ON a.id_attribute = pac.id_attribute
            INNER JOIN {$p}attribute_lang al ON (al.id_attribute = a.id_attribute AND al.id_lang = :id_lang)
            INNER JOIN {$p}attribute_group ag ON ag.id_attribute_group = a.id_attribute_group
            INNER JOIN {$p}attribute_group_lang agl ON (agl.id_attribute_group = ag.id_attribute_group AND agl.id_lang = :id_lang)
            WHERE pa.id_product = :idp
            GROUP BY pa.id_product_attribute
            ORDER BY pa.id_product_attribute ASC
        ";
        $st = $this->pdo->prepare($sql);
        $st->execute(['id_lang' => $anyLangId, 'idp' => $idProduct]);

        $rows = [];
        while ($r = $st->fetch()) {
            $pairs = [];
            $meta  = []; // groupName => valueName => ['id_attribute'=>int,'id_attribute_group'=>int,'is_color_group'=>int,'texture_url'=>string|null]
            if (!empty($r['options'])) {
                foreach (explode('||', $r['options']) as $chunk) {
                    // chunk: groupName::valueName::id_attribute::id_group::is_color_group::color
                    $parts = array_pad(explode('::', $chunk), 6, null);
                    [$g, $v, $idAttr, $idGroup, $isColor, $colorHex] = $parts;
                    if ($g === null || $v === null) continue;
                    $pairs[$g] = $v;

                    $idAttrI  = (int)($idAttr ?? 0);
                    $idGroupI = (int)($idGroup ?? 0);
                    $isColorI = (int)($isColor ?? 0);
                    $texUrl = null;
                    if ($isColorI === 1 && $idAttrI > 0) {
                        $texUrl = rtrim($this->cfg['base_url'], '/') . '/img/co/' . $idAttrI . '.jpg';
                    }
                    $colorHexS = null;
                    if ($isColorI === 1) {
                        $c = trim((string)($colorHex ?? ''));
                        if ($c !== '') {
                            // Prestashop salva spesso senza '#'
                            if ($c[0] !== '#') {
                                $c = '#' . $c;
                            }
                            $colorHexS = $c;
                        }
                    }
                    if (!isset($meta[$g])) $meta[$g] = [];
                    $meta[$g][$v] = [
                        'id_attribute' => $idAttrI,
                        'id_attribute_group' => $idGroupI,
                        'is_color_group' => $isColorI,
                        'texture_url' => $texUrl,
                        'color' => $colorHexS,
                    ];
                }
            }
            $r['option_pairs'] = $pairs;
            $r['_option_meta'] = $meta;
            $rows[] = $r;
        }
        return $rows;
    }

    public function getConfig(): array { return $this->cfg; }
	
	
	
	
	// --- Restituisce le traduzioni dei NOMI gruppo attributo (opzioni) usati dal prodotto ---
	// Risultato:
	// [
	//   'primaryNames' => ['Taglia','Colore', ...], // nomi nella lingua primaria
	//   'byLocale'     => [
	//       'it' => ['Taglia'=>'Taglia','Colore'=>'Colore'],
	//       'en' => ['Taglia'=>'Size',  'Colore'=>'Color'],
	//       ...
	//   ],
	// ]
	public function getOptionGroupTranslations(int $idProduct, array $langMap, int $primaryLangId): array
	{
		$p = $this->cfg['prefix'];
		// 1) prendo i group_id usati dal prodotto
		$sql = "
		  SELECT DISTINCT ag.id_attribute_group
		  FROM {$p}product_attribute pa
		  JOIN {$p}product_attribute_combination pac ON pac.id_product_attribute = pa.id_product_attribute
		  JOIN {$p}attribute a ON a.id_attribute = pac.id_attribute
		  JOIN {$p}attribute_group ag ON ag.id_attribute_group = a.id_attribute_group
		  WHERE pa.id_product = :idp
		";
		$st = $this->pdo->prepare($sql);
		$st->execute(['idp'=>$idProduct]);
		$groupIds = array_map(fn($r)=>(int)$r['id_attribute_group'], $st->fetchAll());

		if (!$groupIds) return ['primaryNames'=>[], 'byLocale'=>[]];

		// 2) leggo i nomi per tutte le lingue richieste
		$ids    = implode(',', $groupIds);
		$langIds= implode(',', array_map('intval', array_keys($langMap)));
		$sql2 = "
		  SELECT agl.id_lang, agl.name, ag.id_attribute_group
		  FROM {$p}attribute_group_lang agl
		  JOIN {$p}attribute_group ag ON ag.id_attribute_group = agl.id_attribute_group
		  WHERE ag.id_attribute_group IN ($ids) AND agl.id_lang IN ($langIds)
		";
		$st2 = $this->pdo->query($sql2);

		$primaryNames = [];
		$byLocale = [];
		while ($r = $st2->fetch()) {
			$loc = $langMap[(int)$r['id_lang']] ?? null;
			if (!$loc) continue;
			$name = (string)$r['name'];

			// Nella primaria: raccolgo i nomi così come compariranno su Shopify (per mapping)
			if ((int)$r['id_lang'] === (int)$primaryLangId) {
				$primaryNames[(int)$r['id_attribute_group']] = $name;
			}
			// Per ogni locale: mappa group_id->nome_locale
			$byLocale[$loc][(int)$r['id_attribute_group']] = $name;
		}

		// Esponi anche l'array dei nomi primari nell'ordine che già usi (per semplicità: per id)
		$primaryOrdered = array_values($primaryNames);

		// Trasforma byLocale da group_id a "nome primario" => "nome locale"
		$byLocaleByPrimaryName = [];
		foreach ($byLocale as $loc => $m) {
			foreach ($primaryNames as $gid => $primaryName) {
				if (isset($m[$gid])) {
					$byLocaleByPrimaryName[$loc][$primaryName] = $m[$gid];
				}
			}
		}

		return [
			'primaryNames' => $primaryOrdered,
			'byLocale'     => $byLocaleByPrimaryName,
		];
	}


	// --- Restituisce i TITOLI VARIANTE tradotti, indicizzati per SKU ---
	// Risultato:
	// [
	//   'it' => ['SKU1' => '38 / Nero', 'SKU2' => '39 / Nero', ...],
	//   'en' => ['SKU1' => '38 / Black', ...],
	//   ...
	// ]
	public function getVariantTitlesByLocaleBySku(int $idProduct, array $langMap): array
	{
		$p = $this->cfg['prefix'];
		if (!$langMap) return [];

		// Prelevo, per ogni lingua, i ValueName di ciascun pa (variant), ordinando per gruppo (per stabilità)
		// NB: la stringa "titolo" la uniamo con " / " per uniformità a Shopify.
		$out = [];
		foreach ($langMap as $idLang => $locale) {
			$sql = "
			  SELECT
				pa.id_product_attribute,
				pa.reference AS sku,
				ag.id_attribute_group,
				agl.name      AS group_name,
				al.name       AS value_name
			  FROM {$p}product_attribute pa
			  JOIN {$p}product_attribute_combination pac ON pac.id_product_attribute = pa.id_product_attribute
			  JOIN {$p}attribute a  ON a.id_attribute = pac.id_attribute
			  JOIN {$p}attribute_lang al ON (al.id_attribute = a.id_attribute AND al.id_lang = :id_lang)
			  JOIN {$p}attribute_group ag ON ag.id_attribute_group = a.id_attribute_group
			  JOIN {$p}attribute_group_lang agl ON (agl.id_attribute_group = ag.id_attribute_group AND agl.id_lang = :id_lang)
			  WHERE pa.id_product = :idp
			  ORDER BY pa.id_product_attribute ASC, ag.id_attribute_group ASC, al.name ASC
			";
			$st = $this->pdo->prepare($sql);
			$st->execute(['id_lang'=>$idLang, 'idp'=>$idProduct]);

			$tmp = [];    // paId => [values...]
			$skuMap = []; // paId => sku
			while ($r = $st->fetch()) {
				$paId = (int)$r['id_product_attribute'];
				$sku  = $r['sku'];
				$val  = (string)$r['value_name'];
				$tmp[$paId][] = $val;
				$skuMap[$paId] = $sku;
			}

			foreach ($tmp as $paId => $values) {
				$title = implode(' / ', $values);
				$sku   = $skuMap[$paId] ?? null;
				if ($sku) $out[$locale][$sku] = $title;
			}
		}
		return $out;
	}
	
	
	public function getCategoriesForProduct(int $idProduct, array $langMap, ?int $primaryLangId = null): array
	{
		if (!$langMap) return [];
		$p   = $this->cfg['prefix'];
		$idS = (int)$this->cfg['id_shop'];
		$primaryLangId = $primaryLangId ?? (int)array_key_first($langMap);

		// 1) prendo le categorie attive associate al prodotto e allo shop
		$sql = "
			SELECT DISTINCT c.id_category
			FROM {$p}category_product cp
			INNER JOIN {$p}category c ON c.id_category = cp.id_category AND c.active = 1
			INNER JOIN {$p}category_shop cs ON (cs.id_category = c.id_category AND cs.id_shop = :id_shop)
			WHERE cp.id_product = :idp
		";
		$st = $this->pdo->prepare($sql);
		$st->execute(['id_shop'=>$idS, 'idp'=>$idProduct]);
		$catIds = array_map(fn($r)=>(int)$r['id_category'], $st->fetchAll());
		if (!$catIds) return [];

		// 2) leggo i titoli/slug per tutte le lingue richieste
		$ids    = implode(',', $catIds);
		$langIds= implode(',', array_map('intval', array_keys($langMap)));
		$sql2 = "
			SELECT cl.id_category, cl.id_lang, cl.name, cl.link_rewrite
			FROM {$p}category_lang cl
			WHERE cl.id_category IN ($ids) AND cl.id_lang IN ($langIds)
		";
		$st2 = $this->pdo->query($sql2);

		$tmp = []; // id_category => ['primary'=>..., 'byLocale'=>...]
		while ($r = $st2->fetch()) {
			$cid   = (int)$r['id_category'];
			$idLng = (int)$r['id_lang'];
			$loc   = $langMap[$idLng] ?? null;
			if (!$loc) continue;

			$title  = (string)($r['name'] ?? '');
			$handle = self::slugify((string)($r['link_rewrite'] ?? $title));

			$tmp[$cid]['byLocale'][$loc] = ['title'=>$title, 'handle'=>$handle];
			if ($idLng === $primaryLangId) {
				$tmp[$cid]['primary'] = ['title'=>$title, 'handle'=>$handle];
			}
		}

		// 3) normalizza output
		$out = [];
		foreach ($catIds as $cid) {
			$row = $tmp[$cid] ?? null;
			if (!$row) continue;
			// se manca 'primary', prendi il primo locale disponibile
			if (empty($row['primary'])) {
				$first = reset($row['byLocale']);
				$row['primary'] = $first ?: ['title'=>'Collection','handle'=>'collection'];
			}
			$out[] = ['id_category'=>$cid] + $row;
		}
		return $out;
	}
	
	
	public function getCategoriesForProductSimple(int $idProduct, array $langMap, ?int $primaryLangId = null): array
	{
		if (!$langMap) return [];
		$p   = $this->cfg['prefix'];
		$idS = (int)$this->cfg['id_shop'];
		$primaryLangId = $primaryLangId ?? (int)array_key_first($langMap);

		// 1) prendo le categorie attive associate al prodotto e allo shop
		$sql = "
			SELECT DISTINCT c.id_category
			FROM {$p}category_product cp
			INNER JOIN {$p}category c ON c.id_category = cp.id_category AND c.active = 1
			INNER JOIN {$p}category_shop cs ON (cs.id_category = c.id_category AND cs.id_shop = :id_shop)
			WHERE cp.id_product = :idp
		";
		$st = $this->pdo->prepare($sql);
		$st->execute(['id_shop'=>$idS, 'idp'=>$idProduct]);
		$catIds = array_map(fn($r)=>(int)$r['id_category'], $st->fetchAll());
		if (!$catIds) return [];

		// 2) leggo i titoli/slug per tutte le lingue richieste
		$ids    = implode(',', $catIds);
		$langIds= implode(',', array_map('intval', array_keys($langMap)));
		$sql2 = "
			SELECT cl.id_category, cl.id_lang, cl.name, cl.link_rewrite
			FROM {$p}category_lang cl
			WHERE cl.id_category IN ($ids) AND cl.id_lang IN ($primaryLangId)
		";
		$st2 = $this->pdo->query($sql2);
		
		
		
		$out = [];
		while ($r = $st2->fetch()) {
			$cid   = (int)$r['id_category'];
			$idLng = (int)$r['id_lang'];
			
			$title  = (string)($r['name'] ?? '');
			
			$out[] = $title;
			
		}

		return $out;
	}
	
	
	public function getFeaturesBundle(int $idProduct, array $langMap, int $primaryLangId): array
	{
		$p = $this->cfg['prefix'];
		if (empty($langMap)) {
			return ['features' => [], 'values' => []];
		}

		// 1) Prendo l’elenco delle feature usate dal prodotto e i rispettivi id_feature_value
		$sqlBase = "
		  SELECT
			fp.id_feature       AS fid,
			fp.id_feature_value AS fvid
		  FROM {$p}feature_product fp
		  WHERE fp.id_product = :idp
		";
		$stBase = $this->pdo->prepare($sqlBase);
		$stBase->execute(['idp' => $idProduct]);

		$featureIds = [];    // elenco distinti di id_feature
		$valueIds   = [];    // elenco distinti di id_feature_value
		while ($row = $stBase->fetch()) {
			$fid = (int)$row['fid'];
			$fvid = (int)$row['fvid'];
			if ($fid)  $featureIds[$fid] = true;
			if ($fvid) $valueIds[$fvid]   = true;
		}

		if (!$featureIds) {
			return ['features' => [], 'values' => []];
		}

		// 2) Traduzioni dei NOMI feature (feature_lang)
		$features = []; // fid => ['primary'=>['name'=>...], 'byLocale'=>[loc=>['name'=>...]]]
		$idsF     = implode(',', array_map('intval', array_keys($featureIds)));
		$langIds  = implode(',', array_map('intval', array_keys($langMap)));

		$sqlF = "
		  SELECT
			fl.id_lang,
			fl.name,
			f.id_feature AS fid
		  FROM {$p}feature f
		  JOIN {$p}feature_lang fl
			ON fl.id_feature = f.id_feature
		  WHERE f.id_feature IN ($idsF)
			AND fl.id_lang IN ($langIds)
		";
		$stF = $this->pdo->query($sqlF);
		while ($r = $stF->fetch()) {
			$fid  = (int)$r['fid'];
			$loc  = $langMap[(int)$r['id_lang']] ?? null;
			if (!$loc) continue;
			$name = (string)$r['name'];

			$features[$fid]['byLocale'][$loc] = ['name' => $name];
			if ((int)$r['id_lang'] === (int)$primaryLangId) {
				$features[$fid]['primary'] = ['name' => $name];
			}
		}

		// 3) Traduzioni dei VALORI (feature_value_lang) — FIX: id_feature preso da fv.id_feature
		$values = []; // fid => [fvid => ['primary'=>..., 'byLocale'=>...]]
		if ($valueIds) {
			$idsV   = implode(',', array_map('intval', array_keys($valueIds)));
			$sqlV = "
			  SELECT
				fvl.id_lang,
				fvl.value,
				fv.id_feature_value AS fvid,
				fv.id_feature       AS fid
			  FROM {$p}feature_value fv
			  JOIN {$p}feature_value_lang fvl
				ON fvl.id_feature_value = fv.id_feature_value
			  WHERE fv.id_feature_value IN ($idsV)
				AND fvl.id_lang IN ($langIds)
			";
			$stV = $this->pdo->query($sqlV);

			while ($r = $stV->fetch()) {
				$fid  = (int)$r['fid'];
				$fvid = (int)$r['fvid'];
				$loc  = $langMap[(int)$r['id_lang']] ?? null;
				if (!$loc) continue;
				$val  = (string)$r['value'];

				$values[$fid][$fvid]['byLocale'][$loc] = ['value' => $val];
				if ((int)$r['id_lang'] === (int)$primaryLangId) {
					$values[$fid][$fvid]['primary'] = ['value' => $val];
				}
			}
		}
		
		//print_r($values); exit();

		return [
			'features' => $features,
			'values'   => $values,
		];
	}
	
	
	
		/**
	 * Legge gli allegati (PDF/manuali) associati a un prodotto.
	 * Ritorna array di item:
	 * [
	 *   [
	 *     'id_attachment' => int,
	 *     'hash'          => string,   // campo attachment.file
	 *     'mime'          => string,
	 *     'filename'      => string,   // nome "umano" con estensione
	 *     'local_path'    => string,   // path leggibile (o tmp)
	 *     'namesByLocale' => [ 'en' => 'Manuale ...', 'it' => '...' ] (se disponibili)
	 *   ], ...
	 * ]
	 *
	 * Requisiti:
	 * - se cfg['download_dir'] è impostato, usa quello (es: /var/www/.../download)
	 * - altrimenti prova base_url/download/<hash> e scarica in tmp (fallback)
	 */
	public function getProductAttachments(int $idProduct, array $langMap, int $primaryLangId): array
	{
		$prefix = $this->cfg['db_prefix'] ?? $this->cfg['prefix'] ?? 'ps_';

		$pa = $prefix . 'product_attachment';
		$a  = $prefix . 'attachment';
		$al = $prefix . 'attachment_lang';

		// Prendo lista allegati per prodotto + metadati base
		$sql = "
			SELECT
				a.id_attachment,
				a.file AS hash_file,
				a.mime AS mime,
				COALESCE(a.file_name, '') AS file_name
			FROM {$pa} pa
			INNER JOIN {$a} a ON a.id_attachment = pa.id_attachment
			WHERE pa.id_product = :pid
			ORDER BY a.id_attachment ASC
		";

		$st = $this->pdo->prepare($sql);
		$st->execute([':pid' => $idProduct]);
		$rows = $st->fetchAll(PDO::FETCH_ASSOC) ?: [];
		if (!$rows) return [];

		// Preparo i nomi per lingua (se esistono)
		$langIds = array_keys($langMap);
		$langIds = array_values(array_unique(array_map('intval', $langIds)));
		$langIds = array_filter($langIds, fn($v)=>$v>0);

		$namesByAttachment = []; // id_attachment => [ locale => name ]
		if ($langIds) {
			$in = implode(',', array_fill(0, count($langIds), '?'));
			$sql2 = "
				SELECT id_attachment, id_lang, name
				FROM {$al}
				WHERE id_attachment IN (" . implode(',', array_map('intval', array_column($rows, 'id_attachment'))) . ")
				  AND id_lang IN ($in)
			";
			// Nota: IN (ids) sopra è già int; ok
			$st2 = $this->pdo->prepare($sql2);
			$st2->execute($langIds);
			$langRows = $st2->fetchAll(PDO::FETCH_ASSOC) ?: [];
			foreach ($langRows as $r) {
				$aid = (int)$r['id_attachment'];
				$lid = (int)$r['id_lang'];
				$loc = $langMap[$lid] ?? null;
				if (!$loc) continue;
				$nm = trim((string)($r['name'] ?? ''));
				if ($nm === '') continue;
				$namesByAttachment[$aid][$loc] = $nm;
			}
		}

		$out = [];
		foreach ($rows as $r) {
			$aid  = (int)$r['id_attachment'];
			$hash = trim((string)($r['hash_file'] ?? ''));
			if ($hash === '') continue;

			$mime = trim((string)($r['mime'] ?? 'application/octet-stream'));
			$fnameBase = trim((string)($r['file_name'] ?? ''));

			// nome base: se manca, usa name (lingua primaria) o fallback
			$names = $namesByAttachment[$aid] ?? [];
			$primaryLocale = $langMap[$primaryLangId] ?? (array_values($langMap)[0] ?? 'en');
			$primaryName = $names[$primaryLocale] ?? (reset($names) ?: '');
			if ($fnameBase === '') $fnameBase = $primaryName;
			if ($fnameBase === '') $fnameBase = "attachment_{$aid}";

			$filename = $this->normalizeFilenameWithExtension($fnameBase, $mime);

			// path locale o fallback tmp via HTTP
			$localPath = $this->resolveAttachmentLocalPathOrDownloadTmp($hash, $filename);

			$out[] = [
				'id_attachment' => $aid,
				'hash'          => $hash,
				'mime'          => $mime !== '' ? $mime : 'application/octet-stream',
				'filename'      => $filename,
				'local_path'    => $localPath,
				'namesByLocale' => $names,
			];
		}

		return $out;
	}

	private function normalizeFilenameWithExtension(string $base, string $mime): string
	{
		$base = trim($base);
		$base = preg_replace('/[^\pL\pN\.\-\_\s]+/u', '', $base) ?: 'attachment';
		$base = trim(preg_replace('/\s+/', ' ', $base));

		// se ha già estensione, ok
		if (preg_match('/\.[a-z0-9]{2,6}$/i', $base)) return $base;

		$ext = 'bin';
		if (stripos($mime, 'pdf') !== false) $ext = 'pdf';

		return $base . '.' . $ext;
	}

	/**
	 * 1) Se cfg['download_dir'] è presente, prova <download_dir>/<hash>
	 * 2) Altrimenti scarica da <base_url>/download/<hash> in tmp e ritorna il tmp path
	 */
	private function resolveAttachmentLocalPathOrDownloadTmp(string $hash, string $filename): string
	{
		// 1) filesystem diretto
		$downloadDir = trim((string)($this->cfg['download_dir'] ?? ''));
		if ($downloadDir !== '') {
			$downloadDir = rtrim($downloadDir, '/');
			$p = $downloadDir . '/' . $hash;
			if (is_file($p) && is_readable($p)) return $p;
		}

		// 2) fallback HTTP: /download/<hash>
		$base = rtrim((string)($this->cfg['base_url'] ?? ''), '/');
		if ($base === '') {
			throw new \RuntimeException("Prestashop: base_url non impostato e download_dir assente (hash={$hash})");
		}
		$url = $base . '/download/' . rawurlencode($hash);

		$tmp = tempnam(sys_get_temp_dir(), 'ps_att_');
		if ($tmp === false) {
			throw new \RuntimeException("Prestashop: impossibile creare tmp per allegato (hash={$hash})");
		}

		$bin = $this->httpGetBinary($url);
		file_put_contents($tmp, $bin);

		$size = filesize($tmp);
		if ($size === false || $size <= 0) {
			@unlink($tmp);
			throw new \RuntimeException("Prestashop: download allegato vuoto o fallito: {$url}");
		}

		// Mantengo il tmp path (non rinomino per evitare problemi permessi); filename lo gestisce Shopify
		return $tmp;
	}

	private function httpGetBinary(string $url): string
	{
		$ch = curl_init($url);
		curl_setopt_array($ch, [
			CURLOPT_RETURNTRANSFER => true,
			CURLOPT_FOLLOWLOCATION => true,
			CURLOPT_TIMEOUT        => 60,
			CURLOPT_CONNECTTIMEOUT => 15,
			CURLOPT_USERAGENT      => 'CarnovaMigrator/1.0',
		]);
		$bin = curl_exec($ch);
		$code = (int)curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
		$err = curl_error($ch);
		curl_close($ch);

		if ($bin === false || $code >= 400) {
			throw new \RuntimeException("Prestashop HTTP GET failed code={$code} err={$err} url={$url}");
		}
		return (string)$bin;
	}


	
}

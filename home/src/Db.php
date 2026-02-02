<?php
declare(strict_types=1);

namespace App;

use PDO;

class Db
{
    private PDO $pdo;
    private array $psCfg;

    public function __construct(array $psCfg)
    {
        $this->psCfg = $psCfg;
        $this->pdo = new PDO($psCfg['dsn'], $psCfg['user'], $psCfg['password'], [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        ]);
    }

    public static function slugify(string $s): string
    {
        $s = trim(mb_strtolower($s, 'UTF-8'));
        $s = preg_replace('~[^\pL0-9]+~u', '-', $s);
        $s = trim($s, '-');
        return $s === '' ? 'item' : $s;
    }

    public static function psImagePathFromId(int $idImage): string
    {
        $id = (string)$idImage;
        return implode('/', str_split($id)) . '/' . $id; // es. "1/2/3/123"
    }

    public static function psImageUrl(string $baseUrl, int $idImage): string
    {
        $path = self::psImagePathFromId($idImage);
        return rtrim($baseUrl, '/') . '/img/p/' . $path . '.jpg';
    }

    public function fetchBaseProducts(): array
    {
        $p   = $this->psCfg['prefix'];
        $idS = (int)$this->psCfg['id_shop'];

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
        ";
        $st = $this->pdo->prepare($sql);
        $st->execute(['id_shop' => $idS]);
        return $st->fetchAll();
    }

    public function fetchProductLangs(int $idProduct, array $langMap): array
    {
        $p = $this->psCfg['prefix'];
        if (!$langMap) return [];
        $ids = implode(',', array_map('intval', array_keys($langMap)));

        $sql = "
            SELECT pl.id_product, pl.id_lang, pl

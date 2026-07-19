-- =============================================================================
--  01 - ClickHouse'u Nessie/Iceberg katalogua baglama
-- =============================================================================
--  Calistirma:
--    docker compose exec clickhouse clickhouse-client --multiquery < /sql/01_catalog.sql
--  veya interaktif:
--    docker compose exec clickhouse clickhouse-client
--
--  !!! BU ORTAMDA OLCULEN DURUM (ClickHouse 25.6.13 + Nessie 0.104.1)
--
--  ONEMLI DUZELTME: Bu dosyanin onceki hali "DataLakeCatalog KESFI bile
--  supheli" izlenimi veriyordu ve CREATE DATABASE'de YANLIS bir URL
--  kullaniyordu. Sistematik test sonucu:
--
--  A) DataLakeCatalog KESFI -> CALISIYOR. Ama DOGRU BASE URL ile:
--
--        DOGRU : 'http://nessie:19120/iceberg'        (base; ref bir 'prefix')
--        YANLIS: 'http://nessie:19120/iceberg/main'   (404 verir)
--
--     Nessie, Iceberg REST 'prefix' mekanizmasini kullanir. Client once
--     /iceberg/v1/config'i cagirir, oradan prefix'i (main|warehouse)
--     ogrenir, sonra /iceberg/v1/{prefix}/namespaces'i cagirir. Ref'i
--     base URL'e gomerseniz (/iceberg/main) client yanlis yol kurar:
--        GET /iceberg/main/v1/namespaces   -> 404
--        GET /iceberg/v1/main|warehouse/namespaces -> 200 (dogru)
--     Duzeltilmis URL ile 9 tablonun tamami listelendi. DOGRULANDI.
--
--  A) DataLakeCatalog OKUMA (SELECT) -> SURUME BAGLI, 25.6'da CALISMIYOR.
--     Metadata dosyasi cozumlenirken yol hatasi olusur. Uc surumde olculdu:
--
--        25.6.13 : ...islem_<uuid>/s3://lakehouse/warehouse/.../x.json
--                  (tablo yolu + MUTLAK metadata yolu birlesiyor; s3:// duruyor)
--        25.8.28 : ...islem_<uuid>/lakehouse/warehouse/.../x.json
--                  (s3:// soyuluyor ama hala birlesme var -- kismen duzelmis)
--        26.6.1  : yol DOGRU cozumleniyor (birlesme YOK), ama S3 okumasi
--                  403/404 donuyor -- kimlik/cozumleme baglamasi henuz
--                  tam oturmamis.
--
--     Yani bu ClickHouse tarafinda AKTIF DUZELEN bir hata. 26.x'te yol
--     sorunu gecmis; tam calisir hale gelince (dogrulayin) DataLakeCatalog
--     okumasina gecin -- mimarideki en degerli iyilestirme budur (bkz.
--     tests/06: o zaman branch izolasyonu ClickHouse'ta da gecerli olur).
--
--  C) icebergS3()      -> CALISIYOR. 20.000.000 satir uzerinde dogrulandi.
--                         BU ORTAMDA OKUMA ICIN KULLANILACAK YOL BUDUR.
--                         TEK AMA ONEMLI BEDELI: katalogu atlar, dolayisiyla
--                         Nessie branch izolasyonunu GORMEZ (asagidaki nota
--                         ve tests/06_clickhouse_branch_izolasyonu.py'ye bakin).
--
--  B) Iceberg table engine -> Tek tabloyu yoluyla baglar, kalici nesne
--                         olusturur. C ile ayni yol cozumlemesini kullanir.
-- =============================================================================


-- -----------------------------------------------------------------------------
--  A) DataLakeCatalog  (birincil yol)
-- -----------------------------------------------------------------------------
-- NOT: Bu ayarin adi ClickHouse surumleri arasinda degisti ve bazi
-- surumlerde stable'a alinip kaldirildi. "Unknown setting" hatasi
-- alirsaniz satiri atlayin; 25.3+ surumlerde gerekmeyebilir.
SET allow_experimental_database_iceberg = 1;

DROP DATABASE IF EXISTS nessie_lake;

-- DIKKAT: URL '/iceberg' (base). '/iceberg/main' DEGIL -- ref bir prefix'tir
-- ve config uzerinden kesfedilir. '/iceberg/main' yazarsaniz namespace
-- listeleme 404 verir ve tablo GORUNMEZ. (Dosya basindaki A) notu.)
CREATE DATABASE nessie_lake
ENGINE = DataLakeCatalog('http://nessie:19120/iceberg', 'minioadmin', 'minioadmin123')
SETTINGS
    catalog_type     = 'rest',
    -- Nessie tarafinda NESSIE_CATALOG_DEFAULT_WAREHOUSE ile ayni olmali.
    -- Nessie prefix'i '{ref}|{warehouse}' olarak kurar -> 'main|warehouse'.
    warehouse        = 'warehouse',
    -- REST katalogu 's3://lakehouse/...' dondurur; ClickHouse'un bunu
    -- gercek bir HTTP adresine cevirebilmesi icin endpoint sarttir.
    storage_endpoint = 'http://minio:9000/lakehouse';

-- Katalog kesfi: Spark'in yazdigi tablolar BURADA GORUNUR (dogrulandi, 25.6).
-- Tablo adlari nokta icerir; sorgularken backtick sart:
--   SELECT count() FROM nessie_lake.`gold.yatirimci_pozisyon`;
-- 25.6'da bu SELECT metadata yol hatasiyla patlar (dosya basi A). Kesif
-- (SHOW TABLES / DESCRIBE) calisir; OKUMA icin icebergS3 (asagida C).
SHOW TABLES FROM nessie_lake;

-- DIKKAT - URL'deki '/main' bir BRANCH secimidir.
-- Baska bir branch'i okumak icin AYRI bir database tanimlayin:
--
--   CREATE DATABASE nessie_lake_dev
--   ENGINE = DataLakeCatalog('http://nessie:19120/iceberg/dev', ...)
--   SETTINGS catalog_type='rest', warehouse='warehouse',
--            storage_endpoint='http://minio:9000/lakehouse';
--
-- Boylece ayni ClickHouse icinden iki branch'i YAN YANA sorgulayabilir,
-- hatta JOIN'leyip farki alabilirsiniz.
--
-- ONEMLI: Bu yol CALISTIGINDA, ClickHouse gercekten KATALOGDAN okur ve
-- Nessie'nin branch izolasyonu ClickHouse icin de gecerli olur. Bugun
-- kullandigimiz icebergS3() yolu katalogu ATLADIGI icin bu garanti YOK:
-- merge edilmemis bir branch'in verisi federe gorunumlerde gorunebiliyor.
-- Olculdu ve belgelendi: tests/06_clickhouse_branch_izolasyonu.py
-- DataLakeCatalog'un duzelmesi, bu mimarideki en degerli tek iyilestirmedir.


-- -----------------------------------------------------------------------------
--  C) Fallback: icebergS3() tablo fonksiyonu
-- -----------------------------------------------------------------------------
-- A yolu surumunuzde calismazsa bu her zaman calisir. Katalog kullanmaz,
-- dogrudan tablonun S3 yolunu okur.
--
-- SORUN: Nessie tablo dizinlerine UUID ekler:
--     s3://lakehouse/warehouse/silver/islem_a3f9c1e2-.../metadata/...
-- Yolu ezberden yazamazsiniz. Gercek yolu ogrenmenin iki yolu:
--
--   1) Spark'tan:
--        docker compose exec spark-master /opt/spark/bin/spark-sql ... \
--          -e "SELECT file_path FROM nessie.silver.islem.files LIMIT 1"
--   2) MinIO konsolundan: http://localhost:9001 -> lakehouse/warehouse/
--
-- Yolu bulduktan sonra:
--
--   SELECT count() FROM icebergS3(
--       'http://minio:9000/lakehouse/warehouse/silver/islem_<UUID>',
--       'minioadmin', 'minioadmin123'
--   );
--
-- Named collection ile parolasiz (tercih edilen):
--
--   SELECT count() FROM icebergS3(
--       minio_lakehouse,
--       path = 'warehouse/silver/islem_<UUID>'
--   );


-- -----------------------------------------------------------------------------
--  Baglanti dogrulama
-- -----------------------------------------------------------------------------
SELECT 'Katalog baglantisi kuruldu' AS durum;

-- Ham S3 erisimi de calisiyor mu? (named collection testi)
SELECT count() AS parquet_dosya_sayisi
FROM s3(minio_lakehouse, path = 'warehouse/**/*.parquet', format = 'One');

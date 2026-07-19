-- =============================================================================
--  03 - Materyalizasyon: SICAK katman
-- =============================================================================
--  DIKKAT -- bu dosyanin adi once "SUB-SECOND katman"di ve girisinde
--  "Iceberg 0.3-3 sn surer, sub-second icin materyalizasyon sart" yaziyordu.
--  OLCTUK, YANLISTI: federe Iceberg okumasi da 17-229 ms, zaten sub-second.
--
--  MATERYALIZASYONUN GERCEK GEREKCESI:
--    * Nokta atisi sorgular -> Iceberg'de indeks yok, tum tabloyu tarar.
--      "WHERE yatirimci_id = X": Iceberg 204ms / 2M satir, MergeTree
--      11ms / 803k satir. 18,5x -- ve fark buyudukce buyur.
--    * Yuksek eszamanlilik  -> sparse index + mark cache cok daha iyi olcekler
--    * Uzak object storage / metadata sismesi -> makas acilir
--
--  Yani buradaki kazanc "saniyeden milisaniyeye" degil, "milisaniyeden
--  daha az milisaniyeye + olceklenebilirlik". Sunumda boyle cerceveleyin;
--  abartirsaniz ilk denemede yakalanirsiniz.
--
--  MIMARI KURAL:
--    Iceberg/MinIO -> tek gercek kaynak (system of record). Her sey burada.
--    ClickHouse MT -> tureilmis okuma kopyasi (read replica). Silinebilir,
--                     her an Iceberg'den yeniden uretilebilir.
--
--  Bu ayrim kritik: ClickHouse'taki veri OTORITE DEGILDIR. Diski patlasa,
--  container silinse, veri kaybi YOKTUR -- tek yapilacak bu scripti tekrar
--  calistirmaktir. Yoneticiye anlatilacak dayaniklilik argumani budur.
-- =============================================================================

SET allow_experimental_database_iceberg = 1;

CREATE DATABASE IF NOT EXISTS csd;


-- -----------------------------------------------------------------------------
--  1. Gunluk menkul kiymet ozeti  (dashboard'un ana besleyicisi)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS csd.gunluk_menkul_ozet;

CREATE TABLE csd.gunluk_menkul_ozet
(
    islem_tarihi     Date,
    menkul_id        UInt64,
    isin_kodu        LowCardinality(String),
    kisa_kod         LowCardinality(String),
    kiymet_tipi      LowCardinality(String),
    pazar            LowCardinality(String),
    kanal            LowCardinality(String),
    islem_adedi      UInt64,
    toplam_hacim     Decimal(24, 4),
    toplam_adet      Decimal(24, 4),
    toplam_komisyon  Decimal(20, 4),
    alis_hacmi       Decimal(24, 4),
    satis_hacmi      Decimal(24, 4),
    tekil_yatirimci  UInt64,
    agirlikli_fiyat  Decimal(24, 8),
    min_fiyat        Decimal(18, 6),
    max_fiyat        Decimal(18, 6)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(islem_tarihi)
-- ============ SUB-SECOND'IN GELDIGI YER: ORDER BY ============
-- Bu ClickHouse'un SPARSE PRIMARY INDEX'idir. Veri diskte bu siraya gore
-- fiziksel olarak dizilir ve her 8192 satirda bir index isareti konur.
--
-- WHERE islem_tarihi = '2025-03-15' AND kiymet_tipi = 'HISSE' dediginizde
-- ClickHouse index'ten dogrudan ilgili granule'lere atlar; aradaki
-- milyonlarca satiri OKUMAZ BILE. Iceberg tarafinda boyle bir yapi yok --
-- fark tam olarak buradan cikiyor.
--
-- SIRA ONEMLIDIR: en cok filtrelenen ve en dusuk kardinaliteli sutun
-- basa gelir. Yanlis sira = index ise yaramaz = sub-second gider.
ORDER BY (islem_tarihi, kiymet_tipi, menkul_id, kanal)
SETTINGS index_granularity = 8192;

-- Iceberg'den doldur
INSERT INTO csd.gunluk_menkul_ozet
SELECT
    islem_tarihi, menkul_id, isin_kodu, kisa_kod, kiymet_tipi, pazar, kanal,
    islem_adedi, toplam_hacim, toplam_adet, toplam_komisyon,
    alis_hacmi, satis_hacmi, tekil_yatirimci,
    agirlikli_fiyat, min_fiyat, max_fiyat
FROM lake.gold_gunluk_menkul_ozet;


-- -----------------------------------------------------------------------------
--  2. Yatirimci pozisyonlari
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS csd.yatirimci_pozisyon;

CREATE TABLE csd.yatirimci_pozisyon
(
    yatirimci_id     UInt64,
    yatirimci_tipi   LowCardinality(String),
    uyruk            LowCardinality(String),
    il_kodu          UInt16,
    risk_profili     LowCardinality(String),
    menkul_id        UInt64,
    isin_kodu        LowCardinality(String),
    kisa_kod         LowCardinality(String),
    kiymet_tipi      LowCardinality(String),
    net_adet         Decimal(24, 4),
    net_tutar        Decimal(24, 4),
    islem_adedi      UInt64,
    ilk_islem        DateTime,
    son_islem        DateTime,
    hesaplama_ts     DateTime
)
ENGINE = MergeTree
-- Bu tablonun ana erisim deseni "su yatirimcinin pozisyonlari" -> yatirimci_id basa.
ORDER BY (yatirimci_id, kiymet_tipi, menkul_id)
SETTINGS index_granularity = 8192;

INSERT INTO csd.yatirimci_pozisyon
SELECT
    yatirimci_id, yatirimci_tipi, uyruk, il_kodu, risk_profili,
    menkul_id, isin_kodu, kisa_kod, kiymet_tipi,
    net_adet, net_tutar, islem_adedi, ilk_islem, son_islem, hesaplama_ts
FROM lake.gold_yatirimci_pozisyon;


-- -----------------------------------------------------------------------------
--  3. Detay islem tablosu  (drill-down icin)
-- -----------------------------------------------------------------------------
-- Ozet tablolar %95 sorguyu karsilar; kalan %5 "tek yatirimcinin tek
-- islemine kadar in" der. Silver'in tamamini kopyaliyoruz.
DROP TABLE IF EXISTS csd.islem;

CREATE TABLE csd.islem
(
    islem_id         UInt64,
    yatirimci_id     UInt64,
    menkul_id        UInt64,
    islem_zamani     DateTime,
    islem_tarihi     Date,
    islem_saati      UInt8,
    islem_tipi       LowCardinality(String),
    adet             Decimal(18, 4),
    fiyat            Decimal(18, 6),
    tutar            Decimal(20, 4),
    komisyon         Decimal(12, 4),
    net_tutar        Decimal(20, 4),
    araci_kurum_kodu LowCardinality(String),
    kanal            LowCardinality(String),
    yatirimci_tipi   LowCardinality(String),
    uyruk            LowCardinality(String),
    il_kodu          UInt16,
    risk_profili     LowCardinality(String),
    isin_kodu        LowCardinality(String),
    kisa_kod         LowCardinality(String),
    kiymet_tipi      LowCardinality(String),
    pazar            LowCardinality(String),
    para_birimi      LowCardinality(String),

    -- Ikincil (skipping) indeks: ORDER BY'da olmayan ama sik filtrelenen
    -- sutunlar icin. Primary index kadar guclu degil ama granule atlatir.
    INDEX idx_araci  araci_kurum_kodu TYPE set(100)          GRANULARITY 4,
    INDEX idx_tutar  tutar            TYPE minmax            GRANULARITY 4,
    INDEX idx_yat    yatirimci_id     TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(islem_tarihi)
ORDER BY (islem_tarihi, kiymet_tipi, menkul_id, islem_id)
SETTINGS index_granularity = 8192;

INSERT INTO csd.islem
SELECT
    islem_id, yatirimci_id, menkul_id, islem_zamani, islem_tarihi, islem_saati,
    islem_tipi, adet, fiyat, tutar, komisyon, net_tutar,
    araci_kurum_kodu, kanal, yatirimci_tipi, uyruk, il_kodu, risk_profili,
    isin_kodu, kisa_kod, kiymet_tipi, pazar, para_birimi
FROM lake.silver_islem
SETTINGS max_insert_threads = 8;


-- -----------------------------------------------------------------------------
--  4. Tazelik: Refreshable Materialized View
-- -----------------------------------------------------------------------------
-- "Materyalize ettik, peki veri bayatlamayacak mi?" sorusunun cevabi.
-- ClickHouse belirlenen araliklarla Iceberg'i yeniden okuyup tabloyu
-- ATOMIK olarak degistirir. Yenileme sirasinda okuyucular eski tabloyu
-- gormeye devam eder; yarim veri gormezler.
--
-- NOT: Bu ozellik bazi surumlerde deneyseldir. "Unknown setting" hatasi
-- alirsaniz bu bolumu atlayin -- alternatif olarak Airflow'dan basit bir
-- "INSERT INTO ... SELECT" zamanlamasi ayni isi gorur.
SET allow_experimental_refreshable_materialized_view = 1;

DROP TABLE IF EXISTS csd.gunluk_menkul_ozet_mv;

-- assumeNotNull() ZORUNLU -- aciklamasi:
-- icebergS3() TUM sutunlari Nullable(...) olarak dondurur (Iceberg semasinda
-- required olsalar bile). Materialized view semasini SELECT'ten cikarsadigi
-- icin ORDER BY sutunlari da Nullable olur ve ClickHouse reddeder:
--     Sorting key contains nullable columns, but merge tree setting
--     `allow_nullable_key` is disabled. (ILLEGAL_COLUMN)
-- allow_nullable_key=1 ile de gecilebilir ama YAPMAYIN: nullable sorting key
-- indeksi belirgin sekilde yavaslatir. Dogrusu, zaten NULL olmadigini
-- bildigimiz sutunlarda assumeNotNull() kullanmaktir.
-- (Yukaridaki 1-3 numarali tablolar bu sorunu yasamiyor cunku semalari
--  ACIKCA yazildi; cikarima birakilmadi. Sebeplerinden biri de bu.)
CREATE MATERIALIZED VIEW csd.gunluk_menkul_ozet_mv
REFRESH EVERY 1 HOUR
ENGINE = MergeTree
ORDER BY (islem_tarihi, kiymet_tipi, menkul_id)
AS SELECT
    assumeNotNull(islem_tarihi)                       AS islem_tarihi,
    assumeNotNull(menkul_id)                          AS menkul_id,
    toLowCardinality(assumeNotNull(isin_kodu))        AS isin_kodu,
    toLowCardinality(assumeNotNull(kisa_kod))         AS kisa_kod,
    toLowCardinality(assumeNotNull(kiymet_tipi))      AS kiymet_tipi,
    toLowCardinality(assumeNotNull(pazar))            AS pazar,
    toLowCardinality(assumeNotNull(kanal))            AS kanal,
    assumeNotNull(islem_adedi)                        AS islem_adedi,
    assumeNotNull(toplam_hacim)                       AS toplam_hacim,
    assumeNotNull(toplam_adet)                        AS toplam_adet,
    assumeNotNull(toplam_komisyon)                    AS toplam_komisyon,
    assumeNotNull(alis_hacmi)                         AS alis_hacmi,
    assumeNotNull(satis_hacmi)                        AS satis_hacmi,
    assumeNotNull(tekil_yatirimci)                    AS tekil_yatirimci,
    assumeNotNull(agirlikli_fiyat)                    AS agirlikli_fiyat,
    assumeNotNull(min_fiyat)                          AS min_fiyat,
    assumeNotNull(max_fiyat)                          AS max_fiyat
FROM lake.gold_gunluk_menkul_ozet;

-- Yenileme durumunu izleme:
--   SELECT view, status, last_refresh_time, next_refresh_time,
--          exception, progress
--   FROM system.view_refreshes;


-- -----------------------------------------------------------------------------
--  5. Dogrulama + boyut karsilastirmasi
-- -----------------------------------------------------------------------------
SELECT
    table                                            AS tablo,
    formatReadableQuantity(sum(rows))                AS satir,
    formatReadableSize(sum(data_compressed_bytes))   AS sikistirilmis,
    formatReadableSize(sum(data_uncompressed_bytes)) AS ham,
    round(sum(data_uncompressed_bytes) / sum(data_compressed_bytes), 1) AS sikistirma_orani
FROM system.parts
WHERE database = 'csd' AND active
GROUP BY table
ORDER BY sum(data_compressed_bytes) DESC;


-- -----------------------------------------------------------------------------
--  6. SUB-SECOND KANITI
-- -----------------------------------------------------------------------------
-- Ayni is sorusu, ayni veri, iki farkli katman.

-- (a) Iceberg uzerinden (federe)
SELECT kisa_kod, sum(toplam_hacim) AS hacim
FROM lake.gold_gunluk_menkul_ozet
WHERE islem_tarihi BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY kisa_kod ORDER BY hacim DESC LIMIT 10
SETTINGS log_comment = 'kanit_iceberg';

-- (b) MergeTree uzerinden (materyalize)
SELECT kisa_kod, sum(toplam_hacim) AS hacim
FROM csd.gunluk_menkul_ozet
WHERE islem_tarihi BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY kisa_kod ORDER BY hacim DESC LIMIT 10
SETTINGS log_comment = 'kanit_mergetree';

SYSTEM FLUSH LOGS;

SELECT
    log_comment                          AS katman,
    query_duration_ms                    AS ms,
    formatReadableQuantity(read_rows)    AS okunan_satir,
    formatReadableSize(read_bytes)       AS okunan_bayt
FROM system.query_log
WHERE log_comment IN ('kanit_iceberg', 'kanit_mergetree')
  AND type = 'QueryFinish'
  AND event_time > now() - INTERVAL 10 MINUTE
ORDER BY event_time DESC
LIMIT 2;

-- DIKKAT: Burada eskiden "kanit_iceberg ~400-2000 ms" yaziyordu. Bu rakam
-- OLCULMEMIS bir tahmindi ve YANLIS cikti -- ustelik bu dosyanin kendi
-- basligindaki (satir 4-17) duzeltmeyle de celisiyordu. Olculen degerler
-- icin tests/03_clickhouse_perf.sql dosyasinin sonundaki tabloya bakin;
-- orada rakamlar system.query_log'dan, yani motorun kendi olcumunden gelir.
--
-- Beklenen BUYUKLUK MERTEBESI (kendi ortaminizda dogrulayin):
--   kanit_iceberg    -> on-toplanmis gold uzerinde onlarca ms
--   kanit_mergetree  -> tek haneli ms
--
-- Iki katman da ayni Nessie commit'inden beslendigi icin rakamlar
-- birbirini tutar -- tutarsizlik riski yoktur.
--
-- SUNUM NOTU: Aradaki farki "1-2 mertebe" diye anlatmayin; tek sorguda
-- fark bundan KUCUK. Materyalizasyonun asil gerekcesi nokta atisi erisim
-- ve eszamanlilik -- ikisi de tests/03_clickhouse_perf.sql'de olculu.

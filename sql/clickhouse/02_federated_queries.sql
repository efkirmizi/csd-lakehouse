-- =============================================================================
--  02 - Federe sorgular: ClickHouse dogrudan Iceberg/MinIO uzerinde
-- =============================================================================
--  Bu katman ESNEKLIK icindir, HIZ icin degil.
--
--  Ne zaman kullanilir:
--    * Kesif / ad-hoc analiz ("su veriye bir bakayim")
--    * Veri bilimci sorgusu, gunde birkac kez
--    * Henuz modellenmemis, yeni gelen veri
--    * Kopyalamaya degmeyecek soguk veri
--
--  Ne zaman KULLANILMAZ:
--    * Dashboard arkasi, saniyede onlarca sorgu
--    * Sub-second SLA'si olan is  -> 03_materialize_mergetree.sql'e bakin
--
--  Gerekce asagida "GERCEKCI BEKLENTI" bolumunde.
-- =============================================================================

SET allow_experimental_database_iceberg = 1;


-- -----------------------------------------------------------------------------
--  1. Kesif: tablolar ve semalar
-- -----------------------------------------------------------------------------
SHOW TABLES FROM lake;

DESCRIBE TABLE lake.silver_islem;

SELECT count() AS satir_sayisi FROM lake.silver_islem;


-- -----------------------------------------------------------------------------
--  2. En yuksek hacimli 10 menkul kiymet
-- -----------------------------------------------------------------------------
SELECT
    kisa_kod,
    kiymet_tipi,
    count()                    AS islem_adedi,
    round(sum(tutar), 2)       AS toplam_hacim,
    uniqExact(yatirimci_id)    AS tekil_yatirimci
FROM lake.silver_islem
GROUP BY kisa_kod, kiymet_tipi
ORDER BY toplam_hacim DESC
LIMIT 10;


-- -----------------------------------------------------------------------------
--  3. PARTITION PRUNING kaniti
-- -----------------------------------------------------------------------------
-- Spark tabloyu months(islem_zamani) ile partition'ladi. Zaman filtresi
-- verdigimizde ClickHouse Iceberg metadata'sindan ilgisiz partition'lari
-- ELEMELI ve onlarin Parquet dosyalarini hic acmamali.
--
-- Iki sorguyu calistirip system.query_log'daki read_rows / read_bytes
-- degerlerini karsilastirin. Fark = pruning'in ise yaradiginin kaniti.

-- (a) Filtresiz: tum partition'lar taranir
SELECT count(), round(sum(tutar),2)
FROM lake.silver_islem
SETTINGS log_comment = 'pruning_test_filtresiz';

-- (b) Tek aya filtreli: sadece 1 partition okunmali
SELECT count(), round(sum(tutar),2)
FROM lake.silver_islem
WHERE islem_zamani >= toDateTime('2025-03-01 00:00:00')
  AND islem_zamani <  toDateTime('2025-04-01 00:00:00')
SETTINGS log_comment = 'pruning_test_filtreli';

-- Olcumu okuyun:
SYSTEM FLUSH LOGS;

SELECT
    log_comment                                   AS senaryo,
    formatReadableQuantity(read_rows)             AS okunan_satir,
    formatReadableSize(read_bytes)                AS okunan_bayt,
    round(query_duration_ms / 1000, 3)            AS saniye
FROM system.query_log
WHERE log_comment LIKE 'pruning_test%'
  AND type = 'QueryFinish'
  AND event_time > now() - INTERVAL 10 MINUTE
ORDER BY event_time DESC
LIMIT 2;


-- -----------------------------------------------------------------------------
--  4. Zaman serisi: gunluk hacim trendi
-- -----------------------------------------------------------------------------
SELECT
    toStartOfMonth(islem_tarihi)  AS ay,
    kanal,
    count()                       AS islem_adedi,
    round(sum(tutar) / 1e6, 2)    AS hacim_milyon_tl
FROM lake.silver_islem
GROUP BY ay, kanal
ORDER BY ay, kanal;


-- -----------------------------------------------------------------------------
--  5. Federe JOIN: Lakehouse (Iceberg) + Canli OLTP (Postgres)
-- -----------------------------------------------------------------------------
-- ClickHouse'un guclu ama az bilinen ozelligi: tek sorguda IKI FARKLI
-- SISTEMI birlestirebilir. Iceberg'deki tarihsel islemleri, Postgres'teki
-- ANLIK yatirimci durumuyla eslestiriyoruz -- ETL beklemeden.
--
-- CSD senaryosu: "Gecen ay islem yapmis ama bugun hesabi kapali olan
-- yatirimcilar kimler?" Tarihsel veri lake'te, hesap durumu OLTP'de canli.

SELECT
    p.yatirimci_tipi,
    count()                  AS kapali_hesap_islemi,
    round(sum(i.tutar), 2)   AS hacim
FROM lake.silver_islem AS i
INNER JOIN postgresql(
    'postgres:5432', 'csd_oltp', 'yatirimci', 'csd', 'csd_pass', 'csd'
) AS p ON i.yatirimci_id = p.yatirimci_id
WHERE p.aktif_mi = false
GROUP BY p.yatirimci_tipi
ORDER BY hacim DESC;


-- =============================================================================
--  OLCULEN GERCEK  --  yoneticilere ANLATILMASI GEREKEN kisim
-- =============================================================================
--
--  Bu dosyanin ilk halinde "bu sorgular 0.3-3 saniye surer, sub-second
--  DEGILDIR" yaziyordu. OLCTUK, YANLISTI. 20M satirda gercek rakamlar:
--
--    Ceyreklik rapor (Q1, HISSE) federe        198 ms  (1,38M satir okundu)
--    Ayni sorgu MergeTree detay tablosunda      83 ms  (2,37M satir)
--    Ayni sorgu on-toplanmis gold ozette        10 ms  (115k satir)
--    Ayni sorgu bugunku PostgreSQL yolunda  15.414 ms  (2,45M satir)
--
--  Yani federe okuma da ZATEN sub-second. "ClickHouse olmadan sorgular
--  saniyeler surer" DEMEYIN -- biri cikip bu dosyadaki sorguyu calistirir,
--  198ms alir ve tum anlattiklariniz supheli hale gelir.
--  (Saniyeler suren sey PostgreSQL yolu: 15,4 saniye. Karsilastirma ORADA.)
--
--  PEKI MATERYALIZASYON NEDEN GEREKLI?
--  Sure degil, OKUNAN SATIR sayisina bakin. Nokta atisi sorgusunda Iceberg
--  2 MILYON satirin hepsini okudu. Cunku:
--
--    * Iceberg'de INDEKS YOKTUR. Partition sutunu olmayan bir alanda filtre
--      verilince tum tablo taranir. Parquet row-group min/max istatistikleri
--      vardir ama ClickHouse'un sparse primary index'i ve bloom filter'i
--      yoktur. MergeTree ayni sorguda granule'lerin %60'ini hic acmadi (11ms).
--    * Partition pruning ISE YARIYOR (17ms/80k satir). Yani Iceberg'in zaafi
--      "yavaslik" degil, INDEKS YOKLUGU -- dogru filtre verilirse hizlidir.
--
--  FARK NEREDE BUYUR? (beklenti -- burada OLCMEDIK, olctugumuz gibi sunmayin)
--    * Uzak object storage: burada MinIO ayni makinede. Uzakta her
--      metadata.json -> manifest list -> manifest -> data file adimi ag turu.
--    * Metadata sismesi: bakimsiz tabloda binlerce manifest -> sorgu
--      PLANLAMA suresi, veri okunmadan once patlar (bkz 99_maintenance.py).
--    * Eszamanlilik: 50 kullanicida OLCTUK. Federe P99 = 1.559 ms
--      (saniyeyi kiriyor), MergeTree P99 = 426 ms. Makas orada aciliyor.
--      (tests/03_clickhouse_perf.sql EK 2)
--
--  DOGRU CERCEVE (20M satir olcumu)
--    Asil headline: bugunku PostgreSQL yolu 15.414 ms, gold katmani 10 ms
--    -> 1.541x. Ustelik bu oran veri buyudukce ARTIYOR (2M'de 129x idi).
--    Lakehouse'un getirisi burada; iki ClickHouse modu arasindaki fark degil.
--
--       Iceberg/MinIO  = tek gercek kaynak, ucuz, sinirsiz, versiyonlu
--       ClickHouse MT  = sicak veri, pahali, sinirli, indeksli
--
--  Yoneticiye soylenecek cumle:
--    "Butun veri golde duruyor ve kopyalanmadan her an sorgulanabilir --
--     198 milisaniyede. Dashboard'a giren sicak kesiti ClickHouse'a
--     materyalize ediyoruz; orada yuksek eszamanlilikta P99 saniyenin
--     altinda kaliyor. Ikisi ayni katalogdan besleniyor, tutarsizlik
--     riski yok."
-- =============================================================================

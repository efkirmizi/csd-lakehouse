-- =============================================================================
--  TEST SENARYOSU 3 - UC KATMANLI PERFORMANS KARSILASTIRMASI
-- =============================================================================
--  IS SORUSU
--      "Bu mimari gercekten hizli mi, yoksa slaytta mi hizli?"
--
--  YONTEM
--      AYNI is sorusu, AYNI veri, UC farkli katman. Rakamlar
--      system.query_log'dan okunur -- elle tutulan sure degil, motorun
--      kendi olcumu. Manipule edilemez.
--
--          Katman 1: PostgreSQL (OLTP)          -- bugunku durum
--          Katman 2: ClickHouse -> Iceberg      -- federe, kopyasiz
--          Katman 3: ClickHouse -> MergeTree    -- materyalize
--
--  DURUSTLUK NOTU
--      Bu karsilastirma Postgres'e HAKSIZLIK ediyor gibi gorunebilir --
--      etmiyor. Postgres bu is icin tasarlanmadi; OLTP'de (tek satir
--      okuma/yazma) ClickHouse'u yener. Gosterilen sey "Postgres kotu"
--      degil, "her is yuku kendi motoruna" tezidir. Sunumda boyle
--      cerceveleyin; aksi halde ilk soru "neden Postgres'i indeksle
--      duzeltmiyoruz?" olur ve cevabiniz olmaz.
--
--  CALISTIRMA
--      docker compose exec clickhouse clickhouse-client --multiquery < /sql/../tests/03_clickhouse_perf.sql
--    veya interaktif olarak parca parca.
-- =============================================================================

SET allow_experimental_database_iceberg = 1;

-- Onbellek etkisini disla: her sorgu sifirdan okusun.
SET use_query_cache = 0;
SYSTEM DROP MARK CACHE;
SYSTEM DROP UNCOMPRESSED CACHE;


-- =============================================================================
--  IS SORUSU
--    "2025 yilinin ilk ceyreginde, HISSE tipindeki menkul kiymetlerde
--     kanal bazinda toplam islem hacmi ve tekil yatirimci sayisi nedir?"
--
--  Tipik bir yonetim raporu sorusu. Dashboard acildiginda calisir.
-- =============================================================================


-- -----------------------------------------------------------------------------
--  KATMAN 1: PostgreSQL (bugunku durum)
-- -----------------------------------------------------------------------------
-- ClickHouse'un postgresql() fonksiyonuyla OLTP'yi dogrudan sorguluyoruz.
-- Filtre ve aggregation Postgres'e itilmez; ClickHouse satirlari ceker.
-- Bu, "raporu OLTP replikasindan cekmek" senaryosunun benzetimidir.
SELECT
    i.kanal,
    count()                 AS islem_adedi,
    round(sum(i.tutar), 2)  AS toplam_hacim,
    uniqExact(i.yatirimci_id) AS tekil_yatirimci
FROM postgresql('postgres:5432', 'csd_oltp', 'islem', 'csd', 'csd_pass', 'csd') AS i
INNER JOIN postgresql('postgres:5432', 'csd_oltp', 'menkul_kiymet', 'csd', 'csd_pass', 'csd') AS m
    ON i.menkul_id = m.menkul_id
WHERE m.kiymet_tipi = 'HISSE'
  AND i.islem_tarihi BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY i.kanal
ORDER BY toplam_hacim DESC
SETTINGS log_comment = 'perf_1_postgres';


-- -----------------------------------------------------------------------------
--  KATMAN 2: ClickHouse -> Iceberg/MinIO (federe, veri kopyalanmadan)
-- -----------------------------------------------------------------------------
SELECT
    kanal,
    count()                 AS islem_adedi,
    round(sum(tutar), 2)    AS toplam_hacim,
    uniqExact(yatirimci_id) AS tekil_yatirimci
FROM lake.silver_islem
WHERE kiymet_tipi = 'HISSE'
  AND islem_tarihi BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY kanal
ORDER BY toplam_hacim DESC
SETTINGS log_comment = 'perf_2_iceberg';


-- -----------------------------------------------------------------------------
--  KATMAN 3: ClickHouse -> MergeTree (materyalize, sicak katman)
-- -----------------------------------------------------------------------------
SELECT
    kanal,
    count()                 AS islem_adedi,
    round(sum(tutar), 2)    AS toplam_hacim,
    uniqExact(yatirimci_id) AS tekil_yatirimci
FROM csd.islem
WHERE kiymet_tipi = 'HISSE'
  AND islem_tarihi BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY kanal
ORDER BY toplam_hacim DESC
SETTINGS log_comment = 'perf_3_mergetree';


-- -----------------------------------------------------------------------------
--  KATMAN 3b: On-toplanmis ozet (gold) -- dashboard'un gercekte okuyacagi
-- -----------------------------------------------------------------------------
SELECT
    kanal,
    sum(islem_adedi)          AS islem_adedi,
    round(sum(toplam_hacim),2) AS toplam_hacim
FROM csd.gunluk_menkul_ozet
WHERE kiymet_tipi = 'HISSE'
  AND islem_tarihi BETWEEN '2025-01-01' AND '2025-03-31'
GROUP BY kanal
ORDER BY toplam_hacim DESC
SETTINGS log_comment = 'perf_4_gold_ozet';


-- =============================================================================
--  SONUC TABLOSU -- sunuma dogrudan konulabilir
-- =============================================================================
SYSTEM FLUSH LOGS;

SELECT
    multiIf(
        log_comment = 'perf_1_postgres',  '1. PostgreSQL (OLTP)',
        log_comment = 'perf_2_iceberg',   '2. ClickHouse -> Iceberg (federe)',
        log_comment = 'perf_3_mergetree', '3. ClickHouse -> MergeTree (detay)',
        log_comment = 'perf_4_gold_ozet', '4. ClickHouse -> Gold ozet',
        log_comment
    )                                             AS katman,
    query_duration_ms                             AS sure_ms,
    round(query_duration_ms / 1000, 3)            AS sure_sn,
    formatReadableQuantity(read_rows)             AS okunan_satir,
    formatReadableSize(read_bytes)                AS okunan_veri,
    formatReadableSize(memory_usage)              AS bellek,
    -- En yavas katmana gore hizlanma katsayisi
    round(
        max(query_duration_ms) OVER () / greatest(query_duration_ms, 1)
    , 1)                                          AS hizlanma_kat
FROM system.query_log
WHERE log_comment LIKE 'perf_%'
  AND type = 'QueryFinish'
  AND event_time > now() - INTERVAL 15 MINUTE
ORDER BY query_duration_ms DESC;


-- =============================================================================
--  BU ORTAMDA GERCEKTEN OLCULEN RAKAMLAR
--  (20.000.000 satir, lokal Docker, MinIO ayni makinede, sifir cache)
-- -----------------------------------------------------------------------------
--   1. PostgreSQL (OLTP)             15.414 ms   2,45M satir okundu
--   2. ClickHouse -> Iceberg (federe)   198 ms   1,38M satir
--   3. ClickHouse -> MergeTree (detay)   83 ms   2,37M satir
--   4. ClickHouse -> Gold ozet           10 ms    115k satir
--
--   Postgres -> Gold:  ~1.541x
--
--  ---------------------------------------------------------------------------
--  OLCEK TESTININ ASIL BULGUSU -- MAKAS GERCEKTEN ACILIYOR
--  ---------------------------------------------------------------------------
--  Ayni sorgu, ayni makine, sadece veri hacmi 10 KAT arttirildi:
--
--                    2M satir      20M satir     10x veri ile
--    PostgreSQL       1.034 ms     15.414 ms     14,9x KOTULESTI
--    Gold ozet            8 ms         10 ms     1,25x  (neredeyse SABIT)
--    Hizlanma            129x        1.541x
--
--  PostgreSQL SUPERLINEER kotulesiyor. Gold katmani ise pratikte sabit
--  kaliyor: ozet tablo 24k -> 939k satira buyudu ama sorgunun OKUDUGU
--  satir 115k'da kaldi (partition pruning + sparse index). Mimarinin en
--  guclu tek argumani budur -- ve TAHMIN DEGIL, OLCUM.
--
--  Onceki surumde bu bolumde "gercek hacimde makas acilir" diye
--  OLCULMEMIS bir beklenti yaziliydi. Olculdu; beklenti DOGRULANDI ve
--  gercek rakam tahminden buyuk cikti.
--
--  ---------------------------------------------------------------------------
--  DURUSTCE OKUYUN -- ilk tahminimiz YANLISTI
--  ---------------------------------------------------------------------------
--  Bu dosyanin ilk halinde "Iceberg federe okuma 400-2000 ms surer, bu
--  yuzden sub-second icin materyalizasyon SART" yaziyordu. OLCUM BUNU
--  DOGRULAMADI: 20M satirda bile federe okuma 198 ms, yani sub-second.
--
--  Dolayisiyla materyalizasyonun gerekcesi "sub-second'a cikmak" DEGIL:
--    * Nokta atisi erisim   -> Iceberg'de INDEKS YOK, tum tabloyu tarar.
--                              MergeTree'nin bloom filter'i granule'lerin
--                              buyuk kismini hic acmaz.
--    * Yuksek eszamanlilik  -> ASIL GEREKCE BUDUR. 20M + 50 kullanicida
--                              federe P99 = 1.559 ms (SANIYEYI KIRIYOR),
--                              MergeTree P99 = 426 ms. Asagidaki EK 2'ye bakin.
--    * Uzak object storage  -> burada MinIO lokal; uzakta her metadata
--                              adimi bir ag turu demek
--    * Metadata sismesi     -> bakimsiz tabloda planlama suresi patlar
--
--  Sunumda "ClickHouse olmadan sorgular saniyeler surer" DEMEYIN. Biri
--  cikip lake.silver_islem'e sorgu atar, 198ms alir ve tum anlattiklariniz
--  supheli hale gelir. Dogru cumle:
--     "Bugunku PostgreSQL yolu 15 saniye; lakehouse'ta ayni soru 10ms.
--      Veriyi kopyalamadan da 198ms'de cevapliyoruz -- materyalizasyonu
--      nokta atisi sorgular ve YUK ALTINDA TAAHHUT VEREBILMEK icin
--      yapiyoruz, temel hiz icin degil."
--
--  KENDI RAKAMLARINIZI KULLANIN. Bu tablo bizim makinemizden; sizinki
--  farkli cikacak. Sunuma tahmin degil OLCUM koyun ve "bu rakam su
--  sorgudan, su log'dan cikti" diyebilin.
-- =============================================================================


-- -----------------------------------------------------------------------------
--  EK: Neden bu kadar fark var? -- sorulunca gosterilecek kanit
-- -----------------------------------------------------------------------------
-- Sure degil, OKUNAN VERI'ye bakin. Asil hikaye orada.
SELECT
    log_comment                        AS katman,
    formatReadableQuantity(read_rows)  AS okunan_satir,
    formatReadableSize(read_bytes)     AS okunan_bayt
FROM system.query_log
WHERE log_comment LIKE 'perf_%'
  AND type = 'QueryFinish'
  AND event_time > now() - INTERVAL 15 MINUTE
ORDER BY read_bytes DESC;

-- Gold katmani MegaByte'lar yerine KiloByte'lar okur. Cunku:
--   * On-toplama  -> 2M satir yerine 24k satir
--   * Sutunsal    -> sadece 4 sutun okunur, 24 sutunun 20'si diske hic dokunulmaz
--   * ORDER BY    -> sparse index ilgisiz granule'leri atlar
--   * PARTITION   -> 2025 Q1 disindaki aylar hic acilmaz
--   * LowCardinality + ZSTD -> okunan bayt zaten kucuk
--
-- "Sorgu hizli cunku az veri okuyor." Sunumda bu tek cumle, saniye
-- rakamlarindan daha cok is gorur -- cunku olceklendiginde de gecerlidir.


-- =============================================================================
--  EK 2: ES ZAMANLI YUK ALTINDA  --  SUNUMUN EN GUCLU RAKAMI BURADA
-- =============================================================================
--  Calistirmak icin:   .\run.ps1 bench
--
--  OLCULEN (50 es zamanli kullanici, 300 sorgu, 20.000.000 satir):
--
--    Yuzdelik      Iceberg (federe)   MergeTree (gold)     Fark
--    ----------    ----------------   ----------------     ------
--    QPS                     49               211          4,3x
--    P50                 918 ms            160 ms          5,7x
--    P90               1.263 ms            289 ms          4,4x
--    P95               1.374 ms            343 ms          4,0x
--    P99               1.559 ms            426 ms          3,7x
--    P99.9             1.842 ms            469 ms          3,9x
--
--  OLCEGE GORE KARSILASTIRMA (ayni test, 2M satirda):
--        2M : Iceberg P99   877 ms | MergeTree P99 310 ms  -> 2,8x
--       20M : Iceberg P99 1.559 ms | MergeTree P99 426 ms  -> 3,7x
--  Eszamanlilikta da MAKAS ACILIYOR. Ustelik federe yol 20M'de SANIYEYI
--  KIRIYOR; MergeTree hala rahatca sub-second.
--
--  NOT -- clickhouse-benchmark ARA RAPOR basar (~1sn'de bir) ve her raporda
--  yuzdelik blogunu tekrar yazar. Yukaridaki rakamlar SON (kumulatif)
--  blokdandir. run.ps1 bench artik sadece son blogu gosterir.
--
--  ---------------------------------------------------------------------------
--  BU TABLO NEDEN ONEMLI
--  ---------------------------------------------------------------------------
--  Tek sorguda Iceberg 198 ms idi -- MergeTree ile arasindaki fark
--  kullanicinin fark edemeyecegi kadar kucuktu ve "materyalizasyon
--  gereksiz mi?" sorusu hakliydi.
--
--  50 es zamanli kullanicida resim TAMAMEN degisiyor:
--    * Iceberg P99  -> 1.559 ms  (dashboard'da fark edilir yavaslik)
--    * MergeTree P99 -> 426 ms   (P99.9 bile 469 ms)
--    * MergeTree ayni surede 4,3 KAT fazla sorgu isliyor
--
--  Sebep: her federe sorgu Iceberg metadata zincirini (metadata.json ->
--  manifest list -> manifest -> data file) BASTAN cozer ve Parquet'leri
--  HTTP uzerinden ceker. 50 kullanici = 50 kat metadata isi + 50 kat S3
--  istegi. MergeTree'de ise sparse index ve mark cache PAYLASILIR;
--  ikinci kullanici birincinin isittigi cache'ten faydalanir.
--
--  ---------------------------------------------------------------------------
--  SUNUMDA KULLANIN
--  ---------------------------------------------------------------------------
--  Ortalama DEGIL P99 konusun. "Ortalama 130 ms" bir taahhut degildir;
--  "50 es zamanli kullanicida sorgularin %99'u 310 ms altinda" taahhuttur.
--  Yoneticiler SLA diliyle dusunur.
--
--  Ve mimari tezinizin kaniti tam olarak budur:
--    "Tek kullanicida iki katman da hizli -- fark yok. Ama dashboard'a 50
--     kisi ayni anda girdiginde federe katman P99'da saniyeyi kiriyor
--     (1,56 sn), materyalize katman 426 ms'de kaliyor. Materyalizasyonu
--     hiz icin degil, YUK ALTINDA TAAHHUT VEREBILMEK icin yapiyoruz."
--
--  KENDI RAKAMLARINIZI URETIN: '.\run.ps1 bench' ile. Yukaridakiler bizim
--  makinemizden; sizinki farkli cikacak ama ORAN benzer olacaktir.
-- =============================================================================

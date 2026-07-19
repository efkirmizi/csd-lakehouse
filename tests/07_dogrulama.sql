-- =============================================================================
--  07 - REGRESYON DOGRULAMASI  (.\run.ps1 verify-all)
-- =============================================================================
--  NEDEN BU DOSYA VAR?
--      Bu projedeki gercek hatalarin tamami MANUEL kontrolden kacti.
--      Hicbiri "hata" vermedi; hepsi SESSIZCE yanlis sonuc uretti:
--
--        * gold.yatirimci_pozisyon, net-sifir pozisyonlari kaynaktan
--          FARKLI ele aliyordu. 2M satirda tesadufen tutuyordu, 20M'de
--          2 satir ayristi. Hicbir job hata vermedi.
--        * silver.islem_karantina .append() ile yaziliyordu; ayni ETL iki
--          kez kosunca karantina satirlari IKI KEZ gorunuyordu ve
--          "bronze = silver + karantina" esitligi bozuluyordu.
--        * ClickHouse gorunumleri UUID'li yollara bagli; tablo yeniden
--          olusunca gorunum sessizce ESKI dizini okumaya devam ediyordu.
--
--      Bir insanin her degisiklikten sonra bunlari elle kontrol etmesi
--      beklenemez. Bu dosya o kontrolleri OTOMATIK ve SESLI hale getirir.
--
--  TASARIM KARARLARI
--    1) OLCEKTEN BAGIMSIZ. Sabit satir sayisi (10.384.619 gibi) YAZILMAZ --
--       OLTP_ISLEM_ROWS degisince yanlis alarm verirdi. Kontroller
--       ILISKISELDIR: "gold sayisi = bakiye sayisi", "fark = 0" gibi.
--       Bir kontrol ancak GERCEK bir regresyonda kirmizi olmali.
--
--    2) PERFORMANS ASSERT'I YOK. "Gold sorgusu < 50ms" gibi bir kontrol
--       makineye ve o anki yuke bagli olur ve ARADA BIR bosuna kirmizi
--       yanar. Guvenilmeyen bir kontrol, bir sure sonra bakilmayan bir
--       kontrole donusur -- yoklugundan kotudur. Performans olcumu icin
--       tests/03_clickhouse_perf.sql (ayri ve bilincli calistirilir).
--
--    3) TUM kontroller once CALISIR, sonra assert edilir. Ilk hatada
--       durmak, geri kalan kontrollerin sonucunu gizlerdi.
-- =============================================================================

DROP TABLE IF EXISTS csd._dogrulama;

CREATE TABLE csd._dogrulama
(
    sira      UInt8,
    kontrol   String,
    beklenen  String,
    bulunan   String,
    gecti     UInt8
) ENGINE = Memory;


-- -----------------------------------------------------------------------------
--  1. MUTABAKAT: satir sayisi  (OLTP bakiye  vs  lakehouse gold)
-- -----------------------------------------------------------------------------
INSERT INTO csd._dogrulama
SELECT 1,
       'Mutabakat: pozisyon satir sayisi',
       toString(oltp),
       toString(lake),
       oltp = lake
FROM (
    SELECT
        (SELECT count() FROM postgresql('postgres:5432','csd_oltp','bakiye','csd','csd_pass','csd')) AS oltp,
        (SELECT count() FROM lake.gold_yatirimci_pozisyon) AS lake
);


-- -----------------------------------------------------------------------------
--  2. MUTABAKAT: DEGER farki  -- asil kanit
-- -----------------------------------------------------------------------------
-- Satir sayisinin tutmasi yetmez; DEGERLER de tutmali.
-- =============================================================================
--  JOIN'LI KONTROLLER NEDEN IKI ADIMLI? (2, 4, 5)
-- -----------------------------------------------------------------------------
--  'INSERT INTO ... SELECT <join+agregasyon>' ClickHouse 25.6'da PATLIYOR:
--      Unknown expression identifier `net_adet` in scope
--      SELECT net_adet, yatirimci_id, menkul_id FROM (SELECT * FROM icebergS3(...))
--  Sebep: analizor, gorunum (icebergS3 uzerinde VIEW) + JOIN + INSERT
--  birlesiminde gerekli sutunlari asagi iterken alias'i cozemiyor.
--  AYNI sorgu tek basina calisiyor -- yani veri veya sema sorunu DEGIL.
--
--  Denendi:
--    * agregasyonu ust seviyeye almak      -> yine patliyor
--    * SETTINGS enable_analyzer = 0        -> baska bir hata veriyor
--    * CREATE TABLE ... AS SELECT          -> CALISIYOR  <-- secilen yol
--
--  Bu yuzden join'li kontroller once bir Memory tablosuna materyalize
--  ediliyor, sonra rapor tablosuna yaziliyor. Iki fazladan satir, ama
--  kontrol GUVENILIR calisiyor.
-- =============================================================================
DROP TABLE IF EXISTS csd._d_deger;
CREATE TABLE csd._d_deger ENGINE = Memory AS
SELECT countIf(abs(b.nominal_adet - g.net_adet) > 0.01) AS v
FROM postgresql('postgres:5432','csd_oltp','bakiye','csd','csd_pass','csd') AS b
INNER JOIN lake.gold_yatirimci_pozisyon AS g
    ON b.yatirimci_id = g.yatirimci_id AND b.menkul_id = g.menkul_id;

INSERT INTO csd._dogrulama
SELECT 2, 'Mutabakat: tutmayan deger sayisi', '0', toString(v), v = 0
FROM csd._d_deger;


-- -----------------------------------------------------------------------------
--  3. NET-SIFIR INVARYANTI  -- bu tam da 20M'de yakalanan hata
-- -----------------------------------------------------------------------------
-- Kaynak sistem kapanmis (net = 0) pozisyonlari pozisyon tablosunda
-- TUTMAZ. gold da tutmamali. Bu kontrol olmasaydi, 03_silver_to_gold.py
-- icindeki filtre bir gun kazara silinse kimse fark etmezdi.
-- SKALER alt sorgu kullaniliyor: 'FROM (SELECT ... FROM lake.<gorunum>)'
-- bicimi INSERT SELECT icinde ayni analizor hatasini veriyor (bkz. 2 nolu
-- kontrolun basindaki not). '(SELECT ...)' skaler bicimi calisiyor.
INSERT INTO csd._dogrulama
SELECT 3,
       'gold.yatirimci_pozisyon: net=0 satir yok',
       '0',
       toString(sifir),
       sifir = 0
FROM (SELECT (SELECT countIf(net_adet = 0) FROM lake.gold_yatirimci_pozisyon) AS sifir);


-- -----------------------------------------------------------------------------
--  4. KAYIP KAYIT: iki yonlu anti-join
-- -----------------------------------------------------------------------------
-- "Sayilar tutuyor ama kayitlar farkli" durumunu yakalar.
-- Iki adimli desen -- gerekcesi 2 numarali kontrolun basindaki nota bakin.
DROP TABLE IF EXISTS csd._d_eksik;
CREATE TABLE csd._d_eksik ENGINE = Memory AS
SELECT count() AS v
FROM postgresql('postgres:5432','csd_oltp','bakiye','csd','csd_pass','csd') AS b
LEFT ANTI JOIN lake.gold_yatirimci_pozisyon AS g
    ON b.yatirimci_id = g.yatirimci_id AND b.menkul_id = g.menkul_id;

INSERT INTO csd._dogrulama
SELECT 4, 'Mutabakat: sadece OLTP''de olan kayit', '0', toString(v), v = 0
FROM csd._d_eksik;

DROP TABLE IF EXISTS csd._d_fazla;
CREATE TABLE csd._d_fazla ENGINE = Memory AS
SELECT count() AS v
FROM lake.gold_yatirimci_pozisyon AS g
LEFT ANTI JOIN postgresql('postgres:5432','csd_oltp','bakiye','csd','csd_pass','csd') AS b
    ON g.yatirimci_id = b.yatirimci_id AND g.menkul_id = b.menkul_id;

INSERT INTO csd._dogrulama
SELECT 5, 'Mutabakat: sadece lakehouse''ta olan kayit', '0', toString(v), v = 0
FROM csd._d_fazla;


-- -----------------------------------------------------------------------------
--  6. ETL INVARYANTI:  bronze = silver + karantina
-- -----------------------------------------------------------------------------
-- 02_bronze_to_silver.py'nin kendi mutabakat kurali. Karantina .append()
-- ile yazilirsa (eski hata) ikinci calistirmadan sonra bu esitlik BOZULUR.
INSERT INTO csd._dogrulama
SELECT 6,
       'ETL: bronze = silver + karantina',
       toString(bronze),
       toString(silver + karantina),
       bronze = silver + karantina
FROM (
    SELECT
        (SELECT count() FROM lake.bronze_islem)          AS bronze,
        (SELECT count() FROM lake.silver_islem)          AS silver,
        (SELECT count() FROM lake.silver_islem_karantina) AS karantina
);


-- -----------------------------------------------------------------------------
--  7. GORUNUM TAZELIGI: MergeTree kopyasi silver ile ayni mi?
-- -----------------------------------------------------------------------------
-- Iceberg tablo yollari UUID'li. Tablo yeniden olusturulup gorunumler
-- YENIDEN URETILMEZSE, lake.* eski dizini okumaya devam eder ve
-- materyalize kopya ile ayrisir. Bu kontrol o sessiz bayatlamayi yakalar.
INSERT INTO csd._dogrulama
SELECT 7,
       'Tazelik: csd.islem (MergeTree) = lake.silver_islem',
       toString(federe),
       toString(mergetree),
       federe = mergetree
FROM (
    SELECT
        (SELECT count() FROM lake.silver_islem) AS federe,
        (SELECT count() FROM csd.islem)         AS mergetree
);


-- -----------------------------------------------------------------------------
--  8. GORUNUMLERIN TAMAMI OKUNABILIR mi?
-- -----------------------------------------------------------------------------
-- 90_generate_ch_views.py 6 gorunum uretir. Biri kirilirsa (yol degisti,
-- tablo silindi) burada gorulur.
INSERT INTO csd._dogrulama
SELECT 8,
       'lake.* gorunum sayisi',
       '6',
       toString(adet),
       adet = 6
FROM (SELECT count() AS adet FROM system.tables WHERE database = 'lake');


-- =============================================================================
--  RAPOR
-- =============================================================================
SELECT
    sira                                        AS "#",
    kontrol,
    beklenen,
    bulunan,
    if(gecti = 1, 'GECTI', 'KALDI  <<<')        AS durum
FROM csd._dogrulama
ORDER BY sira;

SELECT
    countIf(gecti = 1)                          AS gecen,
    countIf(gecti = 0)                          AS kalan,
    count()                                     AS toplam
FROM csd._dogrulama;


-- =============================================================================
--  ASSERT -- burasi cikis kodunu belirler
-- =============================================================================
--  throwIf, kosul dogruysa sorguyu HATA ile bitirir; clickhouse-client
--  sifir olmayan bir cikis kodu doner ve run.ps1 bunu yakalar.
--  Sessizce "her sey yolunda" demektense gurultulu kirmizi tercih edilir.
SELECT throwIf(
    countIf(gecti = 0) > 0,
    'REGRESYON: yukaridaki kontrollerden en az biri KALDI'
) AS sonuc
FROM csd._dogrulama;

-- Ara tablolari birak (Memory engine; container yeniden baslayinca zaten gider).
DROP TABLE IF EXISTS csd._d_deger;
DROP TABLE IF EXISTS csd._d_eksik;
DROP TABLE IF EXISTS csd._d_fazla;

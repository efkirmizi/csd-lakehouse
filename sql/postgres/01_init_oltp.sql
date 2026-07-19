-- =============================================================================
--  OLTP KAYNAK SISTEM SIMULASYONU
-- =============================================================================
--  Bu sema, bir menkul kiymet saklama kurulusunun uretim sistemlerini
--  temsil eden, TAMAMEN SENTETIK ve BASITLESTIRILMIS bir modeldir --
--  herhangi bir kurumun gercek semasi DEGILDIR. Amac: normalize, satir-bazli,
--  indeksli, OLTP icin dogru tasarlanmis -- ve tam da bu yuzden analitik
--  tarama sorgularinda yavas kalan bir kaynak uretmek. Lakehouse'un cozdugu
--  problemi gorunur kilar.
--
--  Varliklar:
--    yatirimci      -- yatirimci hesaplari (boyut / dimension)
--    menkul_kiymet  -- sermaye piyasasi araclari (boyut / dimension)
--    islem          -- alim-satim hareketleri (olgu / fact, buyuk tablo)
--    bakiye         -- gun sonu pozisyonlar (olgu / fact, snapshot)
--
-- -----------------------------------------------------------------------------
--  !! DIKKAT: BU SCRIPT YARIDA KESILIRSE KENDINI ONARMAZ !!
--
--      Postgres imaji init scriptlerini SADECE veri dizini bossa calistirir.
--      Seed sirasinda Docker cokerse / makine kapanirsa, container yeniden
--      basladiginda su satiri yazar ve init'i ATLAR:
--
--        "PostgreSQL Database directory appears to contain a database;
--         Skipping initialization"
--
--      Geriye YARIM bir veritabani kalir: tablolar ve satirlar yerinde,
--      ama indeksler / v_islem_sinirlari / ANALYZE istatistikleri eksik.
--      Container "healthy" der, satir saymak da dogru sonuc verir --
--      bozukluk sessizdir. (20M seed sirasinda birebir yasandi.)
--
--      TESPIT:  .\run.ps1 status        -> "OLTP semasi" satirina bakin
--      COZUM :  .\run.ps1 repair-oltp   -> idempotent, eksigi tamamlar
-- =============================================================================

\echo '>> CSD OLTP semasi olusturuluyor...'

CREATE SCHEMA IF NOT EXISTS csd;
SET search_path TO csd;

-- -----------------------------------------------------------------------------
-- Boyut: Yatirimci
-- -----------------------------------------------------------------------------
CREATE TABLE yatirimci (
    yatirimci_id     BIGSERIAL PRIMARY KEY,
    sicil_no         VARCHAR(20)  NOT NULL UNIQUE,
    yatirimci_tipi   VARCHAR(20)  NOT NULL,   -- BIREYSEL | KURUMSAL | YABANCI
    uyruk            CHAR(2)      NOT NULL,
    il_kodu          SMALLINT     NOT NULL,
    hesap_acilis     DATE         NOT NULL,
    risk_profili     VARCHAR(10)  NOT NULL,   -- DUSUK | ORTA | YUKSEK
    aktif_mi         BOOLEAN      NOT NULL DEFAULT TRUE,
    guncelleme_ts    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- Boyut: Menkul Kiymet
-- -----------------------------------------------------------------------------
CREATE TABLE menkul_kiymet (
    menkul_id        BIGSERIAL PRIMARY KEY,
    isin_kodu        VARCHAR(12) NOT NULL UNIQUE,
    kisa_kod         VARCHAR(10) NOT NULL,
    ihracci_adi      VARCHAR(120) NOT NULL,
    kiymet_tipi      VARCHAR(20) NOT NULL,    -- HISSE | TAHVIL | FON | VARANT
    pazar            VARCHAR(30) NOT NULL,
    para_birimi      CHAR(3)     NOT NULL DEFAULT 'TRY',
    guncelleme_ts    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- Olgu: Islem (ana buyuk tablo)
-- -----------------------------------------------------------------------------
CREATE TABLE islem (
    islem_id         BIGSERIAL PRIMARY KEY,
    yatirimci_id     BIGINT      NOT NULL REFERENCES yatirimci(yatirimci_id),
    menkul_id        BIGINT      NOT NULL REFERENCES menkul_kiymet(menkul_id),
    islem_zamani     TIMESTAMPTZ NOT NULL,
    islem_tarihi     DATE        NOT NULL,
    islem_tipi       VARCHAR(10) NOT NULL,    -- ALIS | SATIS
    adet             NUMERIC(18,4) NOT NULL,
    fiyat            NUMERIC(18,6) NOT NULL,
    tutar            NUMERIC(20,4) NOT NULL,
    komisyon         NUMERIC(12,4) NOT NULL,
    araci_kurum_kodu VARCHAR(10) NOT NULL,
    kanal            VARCHAR(15) NOT NULL,    -- SUBE | INTERNET | MOBIL | API
    -- CDC ve artimli yukleme icin kritik: her satir ne zaman degisti?
    guncelleme_ts    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- Olgu: Gun sonu bakiye
-- -----------------------------------------------------------------------------
CREATE TABLE bakiye (
    bakiye_id        BIGSERIAL PRIMARY KEY,
    yatirimci_id     BIGINT      NOT NULL REFERENCES yatirimci(yatirimci_id),
    menkul_id        BIGINT      NOT NULL REFERENCES menkul_kiymet(menkul_id),
    valor_tarihi     DATE        NOT NULL,
    nominal_adet     NUMERIC(18,4) NOT NULL,
    piyasa_degeri    NUMERIC(20,4) NOT NULL,
    guncelleme_ts    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (yatirimci_id, menkul_id, valor_tarihi)
);


-- =============================================================================
--  VERI URETIMI
-- =============================================================================
\echo '>> Boyut tablolari dolduruluyor...'

-- 50.000 yatirimci
INSERT INTO yatirimci (sicil_no, yatirimci_tipi, uyruk, il_kodu, hesap_acilis, risk_profili, aktif_mi)
SELECT
    'INV' || LPAD(g::TEXT, 12, '0'),   -- yatirimci sicil no (sentetik)
    (ARRAY['BIREYSEL','BIREYSEL','BIREYSEL','BIREYSEL','KURUMSAL','YABANCI'])[1 + (g % 6)],
    (ARRAY['TR','TR','TR','TR','TR','DE','NL','GB','US'])[1 + (g % 9)],
    1 + (g % 81),
    DATE '2005-01-01' + ((g * 7) % 7300),
    (ARRAY['DUSUK','ORTA','ORTA','YUKSEK'])[1 + (g % 4)],
    (g % 50) <> 0
FROM generate_series(1, 50000) AS g;

-- 500 menkul kiymet
INSERT INTO menkul_kiymet (isin_kodu, kisa_kod, ihracci_adi, kiymet_tipi, pazar, para_birimi)
SELECT
    'TRE' || LPAD(g::TEXT, 9, '0'),
    UPPER(SUBSTRING(MD5(g::TEXT) FROM 1 FOR 5)),
    'IHRACCI KURUM ' || g,
    (ARRAY['HISSE','HISSE','HISSE','TAHVIL','FON','VARANT'])[1 + (g % 6)],
    (ARRAY['YILDIZ PAZAR','ANA PAZAR','ALT PAZAR','BORCLANMA ARACLARI','FON PAZARI'])[1 + (g % 5)],
    (ARRAY['TRY','TRY','TRY','TRY','USD','EUR'])[1 + (g % 6)]
FROM generate_series(1, 500) AS g;

-- ---- Islem: hacim .env icindeki OLTP_ISLEM_ROWS ile kontrol edilir ----
\getenv islem_rows OLTP_ISLEM_ROWS
\echo '>> Islem tablosu dolduruluyor. Satir sayisi:' :islem_rows
\echo '>> (2M satir ~40-70sn surer. Sabir; container healthy olunca hazirdir.)'

-- FK dogrulamasi ve indeks bakimi 2M satirlik toplu insert'i yavaslatir.
-- Once veriyi yaziyoruz, indeksleri SONRA kuruyoruz - klasik bulk-load deseni.
ALTER TABLE islem DROP CONSTRAINT islem_yatirimci_id_fkey;
ALTER TABLE islem DROP CONSTRAINT islem_menkul_id_fkey;

-- ---------------------------------------------------------------------------
--  BORSA TAKVIMI: sadece is gunleri
-- ---------------------------------------------------------------------------
--  Menkul kiymet islemleri hafta sonu OLMAZ. Sentetik veride pazar gunu
--  islem gorunmesi, sunumda ilk fark edilecek ve tum veriye olan guveni
--  sarsacak seydir. Islemleri sadece Pzt-Cum gunlerine dagitiyoruz.
CREATE TEMP TABLE _is_gunleri AS
SELECT d::DATE AS gun,
       (ROW_NUMBER() OVER (ORDER BY d) - 1)::INT AS idx
FROM generate_series(DATE '2024-07-01', DATE '2026-06-30', INTERVAL '1 day') d
WHERE EXTRACT(ISODOW FROM d) < 6;

-- Uretilebilirlik: sabit tohum -> her kurulumda AYNI veri.
SELECT setseed(0.42);

INSERT INTO islem (
    yatirimci_id, menkul_id, islem_zamani, islem_tarihi, islem_tipi,
    adet, fiyat, tutar, komisyon, araci_kurum_kodu, kanal, guncelleme_ts
)
SELECT
    s.yatirimci_id,
    s.menkul_id,
    ts.islem_zamani,
    bg.gun,
    s.islem_tipi,
    s.adet,
    s.fiyat,
    ROUND(s.adet * s.fiyat, 4),
    ROUND(s.adet * s.fiyat * 0.0002, 4),
    s.araci_kurum_kodu,
    s.kanal,
    ts.islem_zamani
FROM (
    SELECT
        -- ================================================================
        --  DIKKAT: HER BOYUT BAGIMSIZ random() ILE URETILIYOR.
        --
        --  Ilk versiyonda hepsi ayni sayactan modulo ile turetiliyordu
        --  (g % 730, g % 60, g % 5). Bu boyutlari birbirine KILITLIYOR:
        --  (tarih, araci, kanal) uclusu lcm(730,60,5)=4380 periyotla
        --  tekrar ediyordu. Yani 730x60x4 = 175.200 mumkun kombinasyondan
        --  sadece 4.380'i olusuyordu. Ayni sekilde (tarih, menkul)
        --  lcm(500,730)=36.500 ile sinirliydi.
        --
        --  Sonuc: gercek disi sekilde dusuk kardinalite -> ClickHouse
        --  oldugundan iyi gorunur, gold tablolari oldugundan kucuk cikar,
        --  ve "neden her araci kurum sadece 6 gun-kanal kombinasyonu
        --  kullaniyor?" sorusu demoyu bitirir.
        --
        --  random() bagimsiz cektigi icin boyutlar gercekten dik olur.
        -- ================================================================
        1 + FLOOR(random() * 50000)::BIGINT AS yatirimci_id,

        -- Menkul kiymet dagilimi UNIFORM DEGIL: gercek piyasada islemlerin
        -- buyuk kismi az sayida likit hissede toplanir (THYAO, GARAN...).
        -- power(random(), 3) dusuk ID'lere agirlik verir -> Pareto benzeri
        -- kuyruk. Bu ayni zamanda partition pruning ve LowCardinality
        -- kazanimlarini GERCEKCI sekilde olcmemizi saglar.
        1 + FLOOR(500 * power(random(), 3))::BIGINT AS menkul_id,

        FLOOR(random() * (SELECT COUNT(*) FROM _is_gunleri))::INT AS gun_idx,

        CASE WHEN random() < 0.5 THEN 'ALIS' ELSE 'SATIS' END AS islem_tipi,

        -- Lot buyuklugu de carpik: cok sayida kucuk, az sayida buyuk emir
        ROUND((1 + 2000 * power(random(), 2.5))::NUMERIC, 4) AS adet,
        ROUND((1.5 + 850 * power(random(), 1.8))::NUMERIC, 6) AS fiyat,

        'AK' || LPAD((1 + FLOOR(60 * power(random(), 1.5))::INT)::TEXT, 3, '0')
            AS araci_kurum_kodu,

        -- Kanal dagilimi: mobil baskin, sube azaliyor (gercek egilim)
        CASE
            WHEN random() < 0.45 THEN 'MOBIL'
            WHEN random() < 0.70 THEN 'INTERNET'
            WHEN random() < 0.88 THEN 'API'
            ELSE 'SUBE'
        END AS kanal
    FROM generate_series(1, :islem_rows) AS g
) s
JOIN _is_gunleri bg ON bg.idx = s.gun_idx
CROSS JOIN LATERAL (
    SELECT (bg.gun
            + TIME '10:00:00'
            -- BIST seans saatleri: 10:00-18:00 arasi
            + (FLOOR(random() * 480) || ' minutes')::INTERVAL
            + (FLOOR(random() * 60) || ' seconds')::INTERVAL
           ) AT TIME ZONE 'Europe/Istanbul' AS islem_zamani
) ts;

DROP TABLE _is_gunleri;

\echo '>> Indeksler kuruluyor...'
ALTER TABLE islem ADD CONSTRAINT islem_yatirimci_id_fkey
    FOREIGN KEY (yatirimci_id) REFERENCES yatirimci(yatirimci_id);
ALTER TABLE islem ADD CONSTRAINT islem_menkul_id_fkey
    FOREIGN KEY (menkul_id) REFERENCES menkul_kiymet(menkul_id);

-- OLTP icin dogru indeksler: nokta atisi erisim desenleri
CREATE INDEX idx_islem_yatirimci   ON islem (yatirimci_id, islem_tarihi);
CREATE INDEX idx_islem_menkul      ON islem (menkul_id, islem_tarihi);
-- Artimli (incremental) ETL'in dayanacagi indeks -- watermark sorgusu bunu kullanir
CREATE INDEX idx_islem_guncelleme  ON islem (guncelleme_ts);

CREATE INDEX idx_bakiye_valor      ON bakiye (valor_tarihi);

-- ---------------------------------------------------------------------------
--  Gun sonu bakiye (pozisyon) tablosu
-- ---------------------------------------------------------------------------
--  Bu tablo ONCE semada tanimlanip HIC DOLDURULMUYORDU -- olu koddu.
--  Doldurmak, silmekten daha degerli: bagimsiz bir MUTABAKAT REFERANSI
--  saglar. gold.yatirimci_pozisyon tablosu islem hareketlerinden
--  TURETILEREK hesaplaniyor; bakiye ise kaynak sistemin kendi kaydi.
--  Ikisinin tutmasi, ETL'in dogrulugunun kaniti olur -- CSD gibi bir
--  kurumda "sizin rakaminiz neden bizimkinden farkli?" sorusunun cevabi
--  tam olarak bu tur capraz kontrollerdir.
--
--  Son valor tarihi itibariyle her yatirimci-menkul cifti icin net pozisyon.
\echo '>> Gun sonu bakiyeler hesaplaniyor...'

-- ---------------------------------------------------------------------------
--  DIKKAT -- BU ADIM BUYUK SEED'DE EN YAVAS YERDIR
--  ---------------------------------------------------------------------------
--  Bu INSERT, islem tablosunu (yatirimci_id, menkul_id) ile GROUP BY yapar.
--  20M satirda ~10.4M grup olusur. Varsayilan work_mem (4MB) ile hash tablosu
--  belege sigmaz ve DISKE TASAR -> tek basina 20+ dakika surer. Bu, container
--  healthcheck penceresini asip TUM stack'in "unhealthy" ile ayaga
--  kalkmamasina yol acabilir (bu tuzaga dustuk).
--
--  work_mem'i SADECE BU OTURUM icin buyutuyoruz ki agregasyon bellekte
--  (hash aggregate) kalsin. Sunucu ayarina dokunmuyoruz. Deger container'in
--  bellegine gore secildi; kisitli bir makinede dusurun.
-- ---------------------------------------------------------------------------
SET work_mem = '512MB';
SET hash_mem_multiplier = 4.0;   -- hash aggregate'e ekstra bellek marji

INSERT INTO bakiye (yatirimci_id, menkul_id, valor_tarihi, nominal_adet, piyasa_degeri)
SELECT
    i.yatirimci_id,
    i.menkul_id,
    (SELECT MAX(islem_tarihi) FROM islem) AS valor_tarihi,
    SUM(CASE WHEN i.islem_tipi = 'ALIS' THEN i.adet ELSE -i.adet END) AS nominal_adet,
    ROUND(
        SUM(CASE WHEN i.islem_tipi = 'ALIS' THEN i.adet ELSE -i.adet END)
        * AVG(i.fiyat)
    , 4) AS piyasa_degeri
FROM islem i
GROUP BY i.yatirimci_id, i.menkul_id
-- Kapanmis pozisyonlar (net 0) bakiye tablosunda tutulmaz
HAVING SUM(CASE WHEN i.islem_tipi = 'ALIS' THEN i.adet ELSE -i.adet END) <> 0;

RESET work_mem;
RESET hash_mem_multiplier;

\echo '>> Istatistikler toplaniyor (ANALYZE)...'
ANALYZE yatirimci;
ANALYZE menkul_kiymet;
ANALYZE islem;
ANALYZE bakiye;

-- -----------------------------------------------------------------------------
-- ETL'in okuyacagi yardimci gorunum: Spark JDBC partition sinirlarini
-- bu tablodan cikaracak.
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_islem_sinirlari AS
SELECT MIN(islem_id) AS min_id,
       MAX(islem_id) AS max_id,
       COUNT(*)      AS toplam_satir,
       MIN(islem_tarihi) AS min_tarih,
       MAX(islem_tarihi) AS max_tarih
FROM islem;

\echo '>> OLTP kaynak hazir.'
SELECT * FROM v_islem_sinirlari;

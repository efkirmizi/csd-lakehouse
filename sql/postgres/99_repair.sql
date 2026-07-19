-- =============================================================================
--  OLTP SEMA ONARIM / DOGRULAMA
-- =============================================================================
--  Calistirmak icin:  .\run.ps1 repair-oltp
--
--  NEDEN BU DOSYA VAR?
--      Postgres'in resmi imaji, init scriptlerini SADECE veri dizini
--      BOSSA calistirir. Log satiri sudur:
--
--        "PostgreSQL Database directory appears to contain a database;
--         Skipping initialization"
--
--      Bunun tehlikeli sonucu su: 01_init_oltp.sql yarida kesilirse
--      (Docker cokmesi, makine kapanmasi, disk dolmasi, Ctrl+C),
--      container yeniden basladiginda init BIR DAHA CALISMAZ. Veritabani
--      "healthy" gorunur, tablolar durur, satirlar yerindedir -- ama
--      indeksler, view'lar ve istatistikler EKSIKTIR.
--
--      Bu sessiz bir bozulmadir ve tam olarak bu projede yasandi:
--      20M satirlik seed sirasinda Docker Desktop coktu. islem tablosu
--      20.000.000 satirla doluydu, FK'lar yerindeydi -- ama 4 indeks,
--      v_islem_sinirlari view'i ve ANALYZE istatistikleri yoktu.
--
--      Sonucu olcum acisindan yikici olurdu: istatistiksiz bir planlayici
--      yanlis plan secer; indekssiz Postgres benchmark'ta OLDUGUNDAN YAVAS
--      gorunur. Yani lakehouse'u haksiz yere iyi gosterirdik. Bir
--      karsilastirmayi bozmanin en kolay yolu, karsi tarafi sakat
--      birakmaktir.
--
--  BU SCRIPT IDEMPOTENT'TIR
--      Istediginiz kadar calistirin. Var olani tekrar kurmaz, veri
--      degistirmez, hicbir sey silmez. Eksik olani tamamlar.
-- =============================================================================

SET search_path TO csd;

-- Indeks kurulumu 20M satirda dakikalar surer. Varsayilan 64MB yerine
-- gecici olarak daha genis bir calisma alani veriyoruz -- SADECE bu
-- oturum icin, sunucu ayarina dokunmuyoruz.
SET maintenance_work_mem = '512MB';

\echo ''
\echo '=== 1/3  Eksik indeksler kuruluyor (20M satirda birkac dakika surebilir) ==='

-- OLTP icin dogru indeksler: nokta atisi erisim desenleri
CREATE INDEX IF NOT EXISTS idx_islem_yatirimci  ON islem (yatirimci_id, islem_tarihi);
CREATE INDEX IF NOT EXISTS idx_islem_menkul     ON islem (menkul_id, islem_tarihi);
-- Artimli (incremental) ETL'in dayanacagi indeks -- watermark sorgusu bunu kullanir
CREATE INDEX IF NOT EXISTS idx_islem_guncelleme ON islem (guncelleme_ts);
CREATE INDEX IF NOT EXISTS idx_bakiye_valor     ON bakiye (valor_tarihi);

\echo ''
\echo '=== 2/3  Yardimci view yeniden olusturuluyor ==='

-- Spark JDBC partition sinirlarini bu view'dan cikarir (jobs/common/session.py
-- icindeki table_bounds fonksiyonu). Yoksa ETL sabit/yanlis sinirla calisir.
CREATE OR REPLACE VIEW v_islem_sinirlari AS
SELECT MIN(islem_id)     AS min_id,
       MAX(islem_id)     AS max_id,
       COUNT(*)          AS toplam_satir,
       MIN(islem_tarihi) AS min_tarih,
       MAX(islem_tarihi) AS max_tarih
FROM islem;

\echo ''
\echo '=== 3/3  Istatistikler toplaniyor (ANALYZE) ==='
\echo '    Bu adim atlanirsa planlayici kor kalir ve Postgres benchmark'
\echo '    sonuclari GERCEKTEN OLDUGUNDAN KOTU cikar.'

ANALYZE yatirimci;
ANALYZE menkul_kiymet;
ANALYZE islem;
ANALYZE bakiye;


-- =============================================================================
--  DOGRULAMA -- eksik bir sey kalirsa BURADA gorulur
-- =============================================================================
\echo ''
\echo '=== DOGRULAMA RAPORU ==='

WITH beklenen(nesne, tur) AS (
    VALUES ('idx_islem_yatirimci',  'indeks'),
           ('idx_islem_menkul',     'indeks'),
           ('idx_islem_guncelleme', 'indeks'),
           ('idx_bakiye_valor',     'indeks'),
           ('islem_pkey',           'indeks'),
           ('bakiye_pkey',          'indeks')
)
SELECT b.nesne,
       b.tur,
       CASE WHEN i.indexname IS NULL THEN 'EKSIK <<<' ELSE 'var' END AS durum
FROM beklenen b
LEFT JOIN pg_indexes i
       ON i.schemaname = 'csd' AND i.indexname = b.nesne
ORDER BY (i.indexname IS NULL) DESC, b.nesne;

\echo ''
SELECT CASE WHEN count(*) = 1 THEN 'v_islem_sinirlari : var'
            ELSE               'v_islem_sinirlari : EKSIK <<<' END AS view_durumu
FROM pg_views WHERE schemaname = 'csd' AND viewname = 'v_islem_sinirlari';

\echo ''
-- Istatistik tazeligi: ANALYZE hic calismadiysa bu sutunlar BOS gelir.
-- Bos gelen bir satir = planlayici o tablo hakkinda hicbir sey bilmiyor.
SELECT relname                              AS tablo,
       n_live_tup                           AS tahmini_satir,
       COALESCE(last_analyze, last_autoanalyze)::TIMESTAMP(0) AS son_analyze,
       CASE WHEN COALESCE(last_analyze, last_autoanalyze) IS NULL
            THEN 'ISTATISTIK YOK <<<' ELSE 'ok' END AS durum
FROM pg_stat_user_tables
WHERE schemaname = 'csd'
ORDER BY relname;

\echo ''
\echo '=== Satir sayilari ==='
SELECT 'yatirimci' AS tablo, count(*) FROM yatirimci
UNION ALL SELECT 'menkul_kiymet', count(*) FROM menkul_kiymet
UNION ALL SELECT 'bakiye',        count(*) FROM bakiye
UNION ALL SELECT 'islem',         count(*) FROM islem;

\echo ''
\echo '>> Onarim tamamlandi. Yukarida "EKSIK" veya "ISTATISTIK YOK" yoksa'
\echo '>> OLTP kaynak ETL icin hazirdir.'

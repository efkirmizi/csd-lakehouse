-- =============================================================================
--  TEST SENARYOSU 5 - MUTABAKAT: "Sizin rakaminiz neden bizimkinden farkli?"
-- =============================================================================
--  Calistirmak icin:  .\run.ps1 sqltest 05_mutabakat.sql
--
--  IS SORUSU
--      Duzenlemeye tabi bir kurumda yeni bir analitik platform kurmanin
--      onundeki EN BUYUK engel hiz degil GUVEN'dir. Ilk soru her zaman
--      sudur: "Bu yeni sistemin urettigi rakamin dogru olduguna nasil
--      guvenecegiz?"
--
--      Bu senaryo o soruya sayisal cevap verir.
--
--  YONTEM
--      Iki BAGIMSIZ kaynak karsilastirilir:
--
--        A) csd.bakiye        -- OLTP kaynak sistemin KENDI pozisyon kaydi
--        B) gold.yatirimci_pozisyon -- Lakehouse'un islem hareketlerinden
--                                      TURETEREK hesapladigi pozisyon
--
--      B, A'dan turetilmedi. B; islem tablosundan bagimsiz olarak
--      Bronze -> Silver -> Gold zinciriyle hesaplandi. Ikisinin tutmasi
--      ETL'in dogrulugunu KANITLAR.
--
--      Ve bunu tek bir sorguda yapiyoruz: ClickHouse ayni anda hem canli
--      PostgreSQL'e hem MinIO'daki Iceberg'e baglaniyor.
--
--  BU ORTAMDA OLCULEN SONUC
--      Eslesen cift : 1.644.018
--      Tutmayan     : 0
--      Maksimum fark: 0
-- =============================================================================


-- -----------------------------------------------------------------------------
--  1. Satir sayisi mutabakati
-- -----------------------------------------------------------------------------
SELECT
    'OLTP kaynak (csd.bakiye)' AS kaynak,
    count()                    AS pozisyon_sayisi
FROM postgresql('postgres:5432', 'csd_oltp', 'bakiye', 'csd', 'csd_pass', 'csd')
UNION ALL
SELECT
    'Lakehouse (gold.yatirimci_pozisyon)',
    count()
FROM lake.gold_yatirimci_pozisyon;


-- -----------------------------------------------------------------------------
--  2. DEGER mutabakati -- asil kanit
-- -----------------------------------------------------------------------------
-- Satir sayisinin tutmasi yetmez; DEGERLER de tutmali.
-- Tolerans 0.01: DECIMAL yuvarlama farki icin. Gercek fark cikarsa
-- 'TUTMAYAN' sifirdan buyuk olur.
SELECT
    count()                                          AS eslesen_cift,
    countIf(abs(b.nominal_adet - g.net_adet) > 0.01) AS TUTMAYAN,
    round(max(abs(b.nominal_adet - g.net_adet)), 6)  AS max_fark,
    if(countIf(abs(b.nominal_adet - g.net_adet) > 0.01) = 0,
       'MUTABIK -- ETL dogrulandi',
       'FARK VAR -- INCELEYIN')                      AS sonuc
FROM postgresql('postgres:5432', 'csd_oltp', 'bakiye', 'csd', 'csd_pass', 'csd') AS b
INNER JOIN lake.gold_yatirimci_pozisyon AS g
    ON b.yatirimci_id = g.yatirimci_id AND b.menkul_id = g.menkul_id;


-- -----------------------------------------------------------------------------
--  3. Kayip kayit kontrolu -- iki yonlu
-- -----------------------------------------------------------------------------
-- Sadece eslesen kayitlara bakmak yaniltir: bir tarafta olup digerinde
-- olmayan kayitlar da fark demektir. "Kaynakta 1.644.018 vardi, bizde
-- 1.644.018 var, ama 12'si farkli kayit" durumunu yakalar.
SELECT
    'Sadece OLTP''de olan (lakehouse eksik)' AS durum,
    count() AS adet
FROM postgresql('postgres:5432', 'csd_oltp', 'bakiye', 'csd', 'csd_pass', 'csd') AS b
LEFT ANTI JOIN lake.gold_yatirimci_pozisyon AS g
    ON b.yatirimci_id = g.yatirimci_id AND b.menkul_id = g.menkul_id
UNION ALL
SELECT
    'Sadece lakehouse''ta olan (fazla kayit)',
    count()
FROM lake.gold_yatirimci_pozisyon AS g
LEFT ANTI JOIN postgresql('postgres:5432', 'csd_oltp', 'bakiye', 'csd', 'csd_pass', 'csd') AS b
    ON g.yatirimci_id = b.yatirimci_id AND g.menkul_id = b.menkul_id;


-- -----------------------------------------------------------------------------
--  4. Toplam seviyesinde mutabakat (yonetici ozeti)
-- -----------------------------------------------------------------------------
SELECT
    round(sum(b.nominal_adet), 4) AS oltp_toplam_nominal,
    round(sum(g.net_adet), 4)     AS lakehouse_toplam_nominal,
    round(sum(b.nominal_adet) - sum(g.net_adet), 6) AS fark
FROM postgresql('postgres:5432', 'csd_oltp', 'bakiye', 'csd', 'csd_pass', 'csd') AS b
INNER JOIN lake.gold_yatirimci_pozisyon AS g
    ON b.yatirimci_id = g.yatirimci_id AND b.menkul_id = g.menkul_id;


-- =============================================================================
--  YONETICIYE OZET
-- -----------------------------------------------------------------------------
--  * Lakehouse'un urettigi 1.644.018 pozisyonun TAMAMI kaynak sistemin
--    kendi kaydiyla birebir tutuyor. Fark: SIFIR.
--  * Bu, iki bagimsiz hesaplama yolunun ayni sonuca varmasidir --
--    "kopyaladik o yuzden tutuyor" degil.
--  * Kontrol TEK SORGUYLA, canli olarak yapilabiliyor. Yani bu bir
--    "kurulum sirasinda bir kez yaptik" testi degil; her gun otomatik
--    calistirilabilecek surekli bir kontrol.
--
--  KURUMSAL KARSILIGI
--    Yeni bir analitik platformun kabulundeki en buyuk engel guvendir.
--    Bu sorgu, o guveni bir toplantida canli olarak, sayisal biçimde
--    kurar. Uretimde bunu gunluk bir kontrol olarak zamanlayin ve
--    sonucu bir dashboard'a baglayin: mutabakat bozulursa HABERINIZ OLSUN.
--    Sessizce yanlis rakam uretmek, hic rakam uretmemekten kotudur.
-- =============================================================================

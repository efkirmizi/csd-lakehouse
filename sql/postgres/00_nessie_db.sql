-- =============================================================================
--  Nessie version store veritabani
-- =============================================================================
--  Nessie'nin commit gecmisi (branch/tag/commit grafigi) buraya yazilir.
--  Nessie bu veritabanini KENDISI OLUSTURMAZ; onceden var olmalidir.
--  Semayi (tablolari) ise Nessie acilista kendisi kurar.
--
--  Dosya adi '00_' ile basliyor cunku docker-entrypoint-initdb.d
--  scriptleri ALFABETIK sirayla calisir; bunun OLTP seed'inden (01_)
--  once bitmesi gerekiyor.
--
--  MIMARI NOT
--    Demo'da OLTP verisi ve Nessie metadata'si ayni Postgres ornegini
--    paylasiyor. URETIMDE BUNU YAPMAYIN: bunlar farkli erisilebilirlik
--    ve yedekleme profillerine sahip iki ayri sistemdir. Nessie'nin
--    Postgres'i duserse TUM lakehouse okunamaz hale gelir -- OLTP
--    yukune bagli bir instance'a bu riski yuklemek yanlis olur.
-- =============================================================================

\echo '>> Nessie version store veritabani olusturuluyor...'

-- PostgreSQL'de 'CREATE DATABASE IF NOT EXISTS' YOKTUR. Duz bir
-- CREATE DATABASE, veritabani zaten varsa hata verir ve
-- ON_ERROR_STOP=1 ile calisan entrypoint TUM init'i durdurur.
--
-- Init scriptleri normalde yalnizca bos veri dizininde calisir, yani bu
-- durum nadirdir; ancak bu dosya elle de calistirilabiliyor
-- (ornegin yarim kalmis bir kurulumu onarirken). \gexec ile kosullu
-- hale getirmek, "onarim komutu kendisi patliyor" durumunu engeller.
SELECT 'CREATE DATABASE nessie'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'nessie')
\gexec

COMMENT ON DATABASE nessie IS
    'Project Nessie version store. Tablolari Nessie acilista kendisi kurar. Elle dokunmayin.';

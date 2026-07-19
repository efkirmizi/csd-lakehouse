-- =============================================================================
--  Airflow metadata veritabani
-- =============================================================================
--  Airflow'un DAG tanimlari, calistirma gecmisi ve gorev durumlari buraya
--  yazilir. Airflow bu veritabanini KENDISI OLUSTURMAZ; onceden var olmalidir.
--  Semayi (tablolari) Airflow acilista 'db migrate' ile kendisi kurar.
--
--  Dosya adi '00_' ile basliyor cunku docker-entrypoint-initdb.d scriptleri
--  ALFABETIK sirayla calisir; bunun OLTP seed'inden (01_) once bitmesi iyi olur.
--
--  MIMARI NOT -- Nessie ile ayni gerekce
--    Demo'da OLTP verisi, Nessie metadata'si ve Airflow metadata'si ayni
--    Postgres ornegini paylasiyor. URETIMDE BUNU YAPMAYIN: ucu de farkli
--    erisilebilirlik profillerine sahiptir. Airflow'un veritabani duserse
--    orkestrasyon durur; Nessie'ninki duserse TUM lakehouse okunamaz olur.
--    Bu ikisini ayni instance'a, ustelik OLTP yukunun yanina koymak
--    tek bir arizayi uc ayri kesintiye cevirir.
--
--  DIKKAT: Bu dosya yalnizca VERI DIZINI BOSKEN calisir. Var olan bir
--  kurulumda Airflow'u sonradan ekliyorsaniz veritabanini elle yaratin:
--      docker compose exec postgres psql -U csd -d csd_oltp \
--        -c "CREATE DATABASE airflow"
-- =============================================================================

\echo '>> Airflow metadata veritabani olusturuluyor...'

-- PostgreSQL'de 'CREATE DATABASE IF NOT EXISTS' YOKTUR ve duz bir
-- CREATE DATABASE, veritabani zaten varsa ON_ERROR_STOP=1 altinda TUM
-- init'i durdurur. \gexec ile kosullu hale getiriyoruz.
-- (Ayni desen: 00_nessie_db.sql)
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')
\gexec

COMMENT ON DATABASE airflow IS
    'Apache Airflow metadata store. Tablolari Airflow acilista kendisi kurar. Elle dokunmayin.';

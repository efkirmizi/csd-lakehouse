#!/usr/bin/env python3
"""
90 - ClickHouse gorunumlerini uret

PROBLEM
    Nessie, tablo dizinlerine UUID ekler:
        s3://lakehouse/warehouse/silver/islem_57738cb1-d065-474b-.../
    Bu UUID her kurulumda farklidir. Dolayisiyla ClickHouse SQL'ine
    icebergS3('...') yolunu ELLE yazmak imkansizdir; her yeni ortamda
    5 ayri UUID'i MinIO konsolundan bulup kopyalamak gerekirdi.

    Ayrica ClickHouse 25.6 + Nessie 0.104 ciftinde DataLakeCatalog ile
    VERI OKUNAMIYOR (katalog kesfi calisiyor, SELECT patliyor -- yol iki
    kez birlesiyor; ayrinti: sql/clickhouse/01_catalog.sql). O yuzden
    icebergS3() kullanmak zorundayiz ve yollara ihtiyacimiz var.

COZUM
    Bu script Nessie katalogundan her tablonun GERCEK yolunu okur ve
    ClickHouse'ta birer VIEW olusturan SQL'i uretir. Boylece downstream
    SQL'ler temiz kalir:
        SELECT ... FROM lake.silver_islem
    yerine
        SELECT ... FROM icebergS3('http://.../islem_<uuid>', ...)

    ETL semayi degistirdiginde bu scripti tekrar calistirin.

CALISTIRMA
    docker compose exec -T spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/90_generate_ch_views.py

    # Cikti: ./out/lake_views.sql  (host'ta, ./out mount'u uzerinden)
    docker compose exec -T clickhouse clickhouse-client \
        --user analytics --password analytics_pass --multiquery < out/lake_views.sql

NOT: Cikti STDOUT'a DEGIL dogrudan dosyaya yaziliyor. Sebep: 'docker compose
exec' stdout ve stderr'i ayni akisa cogulluyor; '> out/x.sql' ile yonlendirme
yapinca Spark'in ve job'in tani mesajlari SQL dosyasina karisiyor ve
ClickHouse "Syntax error: failed at position 1 ([)" veriyor. Dosyaya
dogrudan yazmak bu sinifi tamamen ortadan kaldiriyor.

===============================================================================
 !!! MIMARI UYARI -- icebergS3() NESSIE BRANCH IZOLASYONUNU GORMEZ !!!
===============================================================================
 OLCULDU ve DOGRULANDI (tests/06_clickhouse_branch_izolasyonu.py):

   1. gold.araci_kurum_gunluk main'de 125.277 satirdi.
   2. Bir Nessie branch'i acilip branch'e 1.000 satir eklendi.
   3. Spark dogru davrandi:  main 125.277 | branch 126.277  -> IZOLE.
   4. ClickHouse ise 126.277 satir gordu -- YANI BRANCH VERISINI OKUDU.
   5. Branch DROP edildi; ClickHouse HALA 126.277 goruyordu.
   6. main'e yeni bir commit atilinca ClickHouse 125.277'ye dondu.

 SEBEP
   icebergS3() KATALOGU ATLAR. Nessie'ye "main'in su anki metadata'si
   hangisi?" diye SORMAZ; tablo dizinindeki metadata dosyalarina bakip
   EN YENISINI secer. Nessie branch'leri veri dosyalarini paylastigi icin
   AYNI dizine yazar. Dolayisiyla "en yeni metadata" bir branch'in
   commit'i olabilir.

 SONUCU
   Write-Audit-Publish'in "dogrulanmamis veri uretime ulasmaz" garantisi
   SPARK icin gecerlidir, bu kurulumda CLICKHOUSE icin DEGILDIR.
   ETL branch'i acikken federe gorunumleri okuyan bir dashboard,
   HENUZ DOGRULANMAMIS veriyi gosterir.

 TEHLIKE PENCERESI TAM OLARAK NEREDE?
   Normal ve BASARILI bir ETL'de sorun kendini toplar: job branch'i main'e
   MERGE eder, main'e yeni bir commit duser ve "en yeni metadata" yeniden
   main'inki olur. ClickHouse dogru veriyi gorur.

   ASIL RISK, WAP'in KORUMASI GEREKEN DURUMDA ortaya cikar:
     * Kalite kontrolu PATLADI  -> branch main'e merge EDILMEDI
     * --no-merge ile branch inceleme icin BIRAKILDI
   Bu iki halde branch'in metadata'si en yeni dosyadir ve ClickHouse
   REDDEDILMIS veriyi gosterir. Yani tam da "bozuk veri uretime ulasmasin"
   dedigimiz senaryoda, federe gorunumler uzerinden ULASIR.

   Ustelik 'DROP BRANCH' bunu DUZELTMEZ (olculdu): referans silinir ama
   metadata.json dosyasi S3'te kalir ve hala en yenidir. Duzeltmenin yolu
   main'e yeni bir commit atmaktir (veya Nessie GC ile artigi temizlemek).

 NE YAPMALI?
   a) Uretim dashboard'larini MergeTree tablolarindan (csd.*) besleyin.
      Materyalizasyon acik/reddedilmis bir branch yokken calistirilmalidir.
   b) Federe gorunumleri (lake.*) KESIF amacli kullanin; SLA'li is icin degil.
   c) Tek bir sorguyu gercekten sabitlemek gerekiyorsa, ayar SORGU
      SEVIYESINDE calisir:
          SELECT count() FROM icebergS3('...', '...', '...')
          SETTINGS iceberg_snapshot_id = <main_snapshot_id>;
      Olculdu: sabitsiz 126.277, sabitli 125.277 -> ayar ISE YARIYOR.
   d) Kalici cozum: DataLakeCatalog (gercek REST katalog baglantisi).
      Bu surum ciftinde veri okumuyor -- bkz. sql/clickhouse/01_catalog.sql.

 DENENDI VE VAZGECILDI -- GORUNUM SEVIYESINDE SABITLEME CALISMIYOR
   'CREATE VIEW ... AS SELECT ... SETTINGS iceberg_snapshot_id = N'
   sozdizimi KABUL EDILIR, 'SHOW CREATE VIEW' ciktisinda AYAR GORUNUR,
   ama gorunum sorgulandiginda UYGULANMAZ: ClickHouse 25.6'da gorunumun
   ic SELECT'indeki SETTINGS, gorunum genisletilirken tasinmiyor.
   Olculdu: sabitlenmis gorunum yine 126.277 (branch verisi) dondu.
   Bu yuzden buraya bir --pin-snapshot secenegi EKLENMEDI: calismayan
   ama calisiyor GORUNEN bir guvenlik anahtari, hic olmamasindan
   daha tehlikelidir.
===============================================================================
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, "/opt/spark/jobs")

from common.session import CATALOG, build_spark

TABLES = [
    "bronze.islem",
    "silver.islem",
    "silver.islem_karantina",
    "gold.gunluk_menkul_ozet",
    "gold.yatirimci_pozisyon",
    "gold.araci_kurum_gunluk",
]

CH_DB = "lake"
OUT_FILE = "/opt/spark/out/lake_views.sql"   # host: ./out/lake_views.sql


def table_location(spark, fqn: str) -> str | None:
    """
    Tablonun S3 kok dizinini metadata'dan cikar.

    .files sistem tablosundaki file_path su bicimdedir:
        s3://lakehouse/warehouse/silver/islem_<uuid>/data/<partition>/<dosya>.parquet
    Bize '/data/' oncesi lazim. Bos tablolarda .files bostur; o durumda
    .metadata_log_entries uzerinden metadata dosyasinin yolunu kullaniyoruz.
    """
    try:
        row = spark.sql(f"SELECT file_path FROM {CATALOG}.{fqn}.files LIMIT 1").first()
        if row and "/data/" in row["file_path"]:
            return row["file_path"].split("/data/")[0]
    except Exception:
        pass

    try:
        row = spark.sql(
            f"SELECT file FROM {CATALOG}.{fqn}.metadata_log_entries "
            f"ORDER BY timestamp DESC LIMIT 1"
        ).first()
        if row and "/metadata/" in row["file"]:
            return row["file"].split("/metadata/")[0]
    except Exception:
        pass

    return None


def current_snapshot(spark, fqn: str) -> int | None:
    """
    main uzerindeki GUNCEL snapshot id'si -- uretilen SQL'e YORUM olarak
    yazilir. Sabitleme icin DEGIL (bkz. dosya basindaki not: gorunum
    seviyesinde sabitleme ClickHouse 25.6'da calismiyor), fakat bir sorun
    aninda "ClickHouse hangi commit'i gormeliydi?" sorusunu cevaplamak icin
    elde referans bulunsun diye.

    '.history WHERE is_current_ancestor = true' kullaniyoruz; '.snapshots'
    DEGIL. Sebep: '.snapshots' rollback sonrasi artik gecerli olmayan
    snapshot'lari da listeler ve bunlarin zaman damgasi daha yeni olabilir
    (ayrintili aciklama: tests/01_time_travel.py).
    """
    try:
        row = spark.sql(f"""
            SELECT snapshot_id FROM {CATALOG}.{fqn}.history
            WHERE is_current_ancestor = true
            ORDER BY made_current_at DESC
            LIMIT 1
        """).first()
        return int(row["snapshot_id"]) if row else None
    except Exception:
        return None


def main() -> int:
    spark = build_spark("generate_ch_views", branch="main")

    endpoint = os.environ.get("S3_ENDPOINT", "http://minio:9000")
    ak = os.environ["AWS_ACCESS_KEY_ID"]
    sk = os.environ["AWS_SECRET_ACCESS_KEY"]

    out: list[str] = [
        "-- ==========================================================",
        "--  OTOMATIK URETILDI -- jobs/90_generate_ch_views.py",
        "--  Elle duzenlemeyin. Sema degisince scripti tekrar calistirin.",
        "-- ==========================================================",
        "--",
        "--  UYARI -- BU GORUNUMLER NESSIE BRANCH IZOLASYONUNU GORMEZ.",
        "--  icebergS3() katalogu atlar ve tablo dizinindeki EN YENI metadata'yi",
        "--  okur. Merge EDILMEMIS bir ETL branch'i varsa (kalite kontrolu",
        "--  patladiginda veya --no-merge kullanildiginda) bu gorunumler",
        "--  REDDEDILMIS veriyi gosterir. 'DROP BRANCH' bunu duzeltmez;",
        "--  main'e yeni bir commit gerekir.",
        "--  Olculdu: tests/06_clickhouse_branch_izolasyonu.py",
        "--  SLA'li is / dashboard icin csd.* (MergeTree) tablolarini kullanin.",
        "-- ==========================================================",
        "",
        f"CREATE DATABASE IF NOT EXISTS {CH_DB};",
        "",
    ]

    found = 0
    for fqn in TABLES:
        loc = table_location(spark, fqn)
        view = fqn.replace(".", "_")

        if not loc:
            out.append(f"-- ATLANDI: {fqn} (yol cozulemedi -- tablo bos veya yok)")
            print(f"[uyari] {fqn}: yol cozulemedi")
            continue

        # s3://lakehouse/warehouse/... -> http://minio:9000/lakehouse/warehouse/...
        http_loc = loc.replace("s3://", endpoint.rstrip("/") + "/", 1)

        out.append(f"-- {fqn}")
        out.append(f"--   Iceberg: {loc}")
        out.append(f"DROP VIEW IF EXISTS {CH_DB}.{view};")
        out.append(f"CREATE VIEW {CH_DB}.{view} AS")

        # Snapshot id'yi YORUM olarak yaziyoruz. Sabitleme icin degil --
        # gorunum seviyesinde sabitleme calismiyor (bkz. dosya basi).
        # Amac: bir tutarsizlik suphesinde "ClickHouse hangi commit'i
        # gormeliydi?" sorusunu cevaplayabilmek.
        snap = current_snapshot(spark, fqn)
        if snap is not None:
            out.append(f"--   uretim anindaki main snapshot: {snap}")
            out.append(f"--   (tek sorguyu sabitlemek icin sorguya ekleyin:")
            out.append(f"--    SETTINGS iceberg_snapshot_id = {snap})")

        out.append(f"    SELECT * FROM icebergS3('{http_loc}', '{ak}', '{sk}');")
        out.append("")
        found += 1
        ek = f"  [main snapshot {snap}]" if snap is not None else ""
        print(f"[ok] {fqn} -> {CH_DB}.{view}{ek}")

    with open(OUT_FILE, "w", encoding="utf-8") as fh:
        fh.write("\n".join(out) + "\n")

    print(f"[ozet] {found}/{len(TABLES)} tablo icin gorunum uretildi.")
    print(f"[ozet] Yazildi: {OUT_FILE}  (host: ./out/lake_views.sql)")

    spark.stop()
    return 0 if found else 1


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
===============================================================================
 TEST SENARYOSU 6 - ClickHouse BRANCH IZOLASYONUNU GORUYOR MU?
===============================================================================

 NEDEN BU TEST VAR?
     Test senaryosu 2 (Nessie branching), "bozuk veri branch'te izole kalir,
     uretim main'i gormeye devam eder" diyor. Bu iddia SPARK tarafinda
     dogrulandi -- Spark, Nessie katalogundan okur ve hangi referansta
     oldugunu bilir.

     Ama ClickHouse BU ORTAMDA Nessie katalogunu KULLANMIYOR. DataLakeCatalog
     bu surum ciftinde veri okuyamadigi icin (bkz. sql/clickhouse/01_catalog.sql)
     'lake.*' gorunumleri icebergS3() ile DOGRUDAN S3 YOLUNU okuyor:

         CREATE VIEW lake.gold_araci_kurum_gunluk AS
             SELECT * FROM icebergS3('http://minio:9000/lakehouse/warehouse/
                                      gold/araci_kurum_gunluk_<uuid>', ...);

     icebergS3() katalogu ATLAR. Tablo dizinindeki metadata dosyalarina
     bakar. Nessie branch'leri ise AYNI tablo dizinine yeni metadata.json
     yazar -- cunku branch veri dosyalarini paylasir.

     BURADAN CIKAN SORU SUDUR:
         Bir branch'e yazdigimizda, ClickHouse o veriyi GORUR MU?

     Eger gorurse, "bozuk veri uretime ulasmaz" iddiasi ClickHouse
     tarafinda GECERSIZDIR ve sunumda soylenmemelidir. Bu, tahmin edilerek
     degil OLCULEREK cevaplanmasi gereken bir sorudur.

 CALISTIRMA
     docker compose exec spark-master /opt/spark/bin/spark-submit \
         --master spark://spark-master:7077 \
         /opt/spark/tests/06_clickhouse_branch_izolasyonu.py

     NOT: Bu test Spark tarafini kurar ve ClickHouse'un ne gordugunu
     SIZIN kontrol etmeniz icin komutu basar. Spark container'indan
     ClickHouse'a baglanti yok; iki adimli calisir.
===============================================================================
"""

from __future__ import annotations

import sys
from datetime import datetime

sys.path.insert(0, "/opt/spark/jobs")

from common.session import CATALOG, build_spark

NS = "gold"
TBL = "araci_kurum_gunluk"
TABLE = f"{NS}.{TBL}"


def banner(n: int, title: str) -> None:
    print("\n" + "=" * 76)
    print(f"  ADIM {n}: {title}")
    print("=" * 76)


def at_ref(ref: str) -> str:
    # Backtick SADECE tablo adini sarar -- bkz. tests/02_nessie_branching.py
    return f"{CATALOG}.{NS}.`{TBL}@{ref}`"


def main() -> int:
    stamp = datetime.now().strftime("%H%M%S")
    branch = f"izolasyon_testi_{stamp}"

    spark = build_spark("test_06_ch_branch_izolasyonu", branch="main")

    # ---------------------------------------------------------------- 1
    banner(1, "main uzerindeki baslangic durumu")
    main_0 = spark.sql(f"SELECT COUNT(*) c FROM {at_ref('main')}").first()["c"]
    print(f"    main.{TABLE}: {main_0:,} satir")
    print("\n    ClickHouse'ta SIMDI su sorguyu calistirin ve sayiyi not edin:")
    print(f"      SELECT count() FROM lake.{NS}_{TBL};")
    print(f"    Beklenen: {main_0:,}")

    # ---------------------------------------------------------------- 2
    banner(2, f"Branch aciliyor ve branch'e YENI SATIRLAR yaziliyor: {branch}")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN nessie FROM main")
    spark.sql(f"USE REFERENCE {branch} IN nessie")

    # Ayirt edilebilir satirlar: araci_kurum_kodu = 'BRANCH_TESTI'
    spark.sql(f"""
        INSERT INTO {CATALOG}.{TABLE}
        SELECT islem_tarihi, 'BRANCH_TESTI' AS araci_kurum_kodu, kanal,
               islem_adedi, toplam_hacim, toplam_komisyon,
               tekil_yatirimci, tekil_menkul
        FROM {CATALOG}.{TABLE}
        LIMIT 1000
    """)

    branch_1 = spark.sql(f"SELECT COUNT(*) c FROM {at_ref(branch)}").first()["c"]
    main_1 = spark.sql(f"SELECT COUNT(*) c FROM {at_ref('main')}").first()["c"]

    print(f"\n    branch : {branch_1:,} satir  (+{branch_1 - main_0:,})")
    print(f"    main   : {main_1:,} satir")

    spark_izole = (main_1 == main_0)
    print(f"\n    SPARK tarafinda izolasyon: "
          f"{'CALISIYOR -- main degismedi' if spark_izole else 'BOZUK'}")

    # ---------------------------------------------------------------- 3
    banner(3, "ASIL SORU -- ClickHouse ne goruyor?")
    print(f"""
    Branch'te {branch_1:,} satir var, main'de {main_1:,}.

    SIMDI ClickHouse'ta AYNI sorguyu tekrar calistirin:

      SELECT count() FROM lake.{NS}_{TBL};
      SELECT count() FROM lake.{NS}_{TBL} WHERE araci_kurum_kodu = 'BRANCH_TESTI';

    YORUMLAMA:
      * Sonuc {main_1:,} ve BRANCH_TESTI sayisi 0 ise
            -> ClickHouse SADECE main'i goruyor. Izolasyon iddiasi
               ClickHouse tarafinda da GECERLIDIR.

      * Sonuc {branch_1:,} veya BRANCH_TESTI sayisi 1000 ise
            -> ClickHouse BRANCH VERISINI GORUYOR. Bu durumda
               "bozuk veri uretime ulasmaz" iddiasi ClickHouse icin
               GECERSIZDIR ve sunumda BOYLE SOYLENMEMELIDIR.
               Sebep: icebergS3() katalogu atlar, tablo dizinindeki en
               guncel metadata.json'i okur; branch commit'i de ayni
               dizine yazar.

    Test bitince temizlemek icin bu script'i tekrar calistirmayin;
    asagidaki komut branch'i ve eklenen satirlari geri alir.
    """)

    print(f"    Temizlik komutu (Spark SQL):")
    print(f"      DROP BRANCH {branch} IN nessie;")

    spark.sql("USE REFERENCE main IN nessie")
    print(f"\n[bilgi] Oturum main'e geri alindi. Branch '{branch}' DURUYOR")
    print(f"[bilgi] -- ClickHouse kontrolunu yaptiktan sonra silin.")

    spark.stop()
    return 0 if spark_izole else 1


if __name__ == "__main__":
    raise SystemExit(main())

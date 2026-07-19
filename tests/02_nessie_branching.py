#!/usr/bin/env python3
"""
===============================================================================
 TEST SENARYOSU 2 - NESSIE ile GIT BENZERI BRANCHING (Write-Audit-Publish)
===============================================================================

 IS SORUSU
     "Bozuk veri uretim raporlarina DUSMEDEN once nasil yakalanir?"
     Klasik ETL'de veri once uretime yazilir, sonra kontrol edilir.
     Yani hata bulundugunda is isten gecmistir -- rapor cekilmistir.

 TEKNIK IDDIA
     Nessie, tum KATALOG icin Git semantigi verir. Bir branch actiginizda
     onlarca tabloyu birden izole edersiniz. Tek tablo degil -- TUM
     VERITABANI'nin bir dali. Merge tek atomik commit'tir.

     Iceberg'in kendi branch'lerinden farki: Iceberg branch'i TEK TABLO
     kapsamindadir. 5 tabloyu tutarli sekilde birlikte yayinlamak
     gerekiyorsa Iceberg branch'leri yetmez; Nessie yeter.

 GOSTERILEN
     1. main'in baslangic durumu
     2. ETL branch'i acilir -- main'den izole
     3. Branch'e KIRLI veri yazilir
     4. main'in etkilenmedigi KANITLANIR  (es zamanli okuma)
     5. Kalite kontrolu branch'te patlar -> merge YAPILMAZ
     6. Veri duzeltilir, kontrol gecer -> merge YAPILIR
     7. Commit gecmisi = denetim kaydi

 CALISTIRMA
     docker compose exec spark-master /opt/spark/bin/spark-submit \
         --master spark://spark-master:7077 \
         /opt/spark/tests/02_nessie_branching.py
===============================================================================
"""

from __future__ import annotations

import sys
from datetime import datetime

sys.path.insert(0, "/opt/spark/jobs")

from pyspark.sql import functions as F

from common.session import CATALOG, build_spark

# Kendi tablosunda calisir -- silver.islem'e DOKUNMAZ.
# Senaryo sonunda kirli-sonra-duzeltilmis 50.000 satir merge ediliyor;
# bunu uretim tablosuna yapmak testi idempotent olmaktan cikarir (her
# calistirmada 50.000 satir daha eklenir). Katalog seviyesindeki
# izolasyon iddiasi kopya tabloyla da birebir gosteriliyor.
KAYNAK = "silver.islem"
NS = "silver"
TBL = "islem_wap_demo"
TABLE = f"{NS}.{TBL}"


def banner(n: int, title: str) -> None:
    print("\n" + "=" * 76)
    print(f"  ADIM {n}: {title}")
    print("=" * 76)


def at_ref(ref: str) -> str:
    """
    Belirli bir Nessie referansindaki tabloya isaret eden tam ad.

    BACKTICK YERI KRITIK:
        DOGRU  : nessie.silver.`islem@main`
        YANLIS : nessie.`silver.islem@main`
    Yanlis formda Spark, backtick icindeki her seyi TEK BIR tablo adi
    sayar ("silver.islem@main" adinda bir tablo arar) ve su hatayi verir:
        [TABLE_OR_VIEW_NOT_FOUND] The table or view
        `nessie`.`silver.islem@main` cannot be found
    Backtick sadece TABLO adini sarmali; namespace disarida kalmali.
    """
    return f"{CATALOG}.{NS}.`{TBL}@{ref}`"


def sayim(spark, ref: str) -> tuple[int, float]:
    """Belirli bir Nessie referansindaki satir sayisi ve toplam."""
    row = spark.sql(f"""
        SELECT COUNT(*) AS c, ROUND(SUM(tutar), 2) AS t
        FROM {at_ref(ref)}
    """).first()
    return row["c"], float(row["t"] or 0)


def main() -> int:
    stamp = datetime.now().strftime("%H%M%S")
    branch = f"test_etl_{stamp}"

    spark = build_spark("test_02_nessie_branching", branch="main")

    # ---------------------------------------------------------------- 0
    print(f"[hazirlik] {KAYNAK} -> {TABLE} taze kopya (uretim tablosu degismez)")
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{TABLE}")
    spark.sql(f"""
        CREATE TABLE {CATALOG}.{TABLE}
        USING iceberg
        PARTITIONED BY (months(islem_zamani))
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet',
            'write.delete.mode'='copy-on-write',
            'write.update.mode'='copy-on-write'
        )
        AS SELECT * FROM {CATALOG}.{KAYNAK}
    """)

    # ---------------------------------------------------------------- 1
    banner(1, "main branch -- uretim durumu")
    main_satir_0, main_toplam_0 = sayim(spark, "main")
    print(f"    main: {main_satir_0:,} satir | toplam {main_toplam_0:,.2f} TL")

    print("\n    Mevcut referanslar:")
    spark.sql("LIST REFERENCES IN nessie").show(truncate=False)

    # ---------------------------------------------------------------- 2
    banner(2, f"ETL branch'i aciliyor: {branch}")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN nessie FROM main")
    print(f"""
    'CREATE BRANCH {branch} FROM main' calisti.

    ONEMLI: Bu komut VERI KOPYALAMADI. Nessie sadece yeni bir isaretci
    olusturdu; iki branch de ayni veri dosyalarini gosteriyor. 2 milyar
    satirlik bir katalogda da bu islem milisaniyeler surer ve 0 byte
    ek disk tuketir. Git'in branch acmasiyla ayni mantik.
    """)
    spark.sql("LIST REFERENCES IN nessie").show(truncate=False)

    # ---------------------------------------------------------------- 3
    banner(3, "Branch'e KIRLI veri yaziliyor")
    spark.sql(f"USE REFERENCE {branch} IN nessie")

    print("""
    Senaryo: Yeni bir araci kurum entegrasyonu devreye aliniyor.
    Gonderilen dosyada birim hatasi var -- fiyatlar kurus yerine lira
    olarak gelmis, tutar alani ise hic hesaplanmamis (negatif degerler).
    """)

    kirli = spark.sql(f"""
        SELECT
            islem_id + 900000000        AS islem_id,
            yatirimci_id,
            menkul_id,
            islem_zamani,
            islem_tarihi,
            islem_saati,
            islem_tipi,
            adet,
            isaretli_adet,
            fiyat,
            CAST(-1 * ABS(tutar) AS DECIMAL(20,4))  AS tutar,   -- BOZUK
            komisyon,
            net_tutar,
            'YENI_KURUM'                AS araci_kurum_kodu,
            kanal, yatirimci_tipi, uyruk, il_kodu, risk_profili,
            isin_kodu, kisa_kod, kiymet_tipi, pazar, para_birimi,
            guncelleme_ts
        FROM {at_ref(branch)}
        LIMIT 50000
    """)

    kirli.writeTo(f"{CATALOG}.{TABLE}").append()
    print("    50.000 bozuk satir branch'e yazildi.")

    # ---------------------------------------------------------------- 4
    banner(4, "IZOLASYON KANITI -- main etkilendi mi?")
    branch_satir, branch_toplam = sayim(spark, branch)
    main_satir_1, main_toplam_1 = sayim(spark, "main")

    print(f"""
    Referans        Satir            Toplam Tutar
    --------------  ---------------  --------------------
    {branch:14s}  {branch_satir:>15,}  {branch_toplam:>20,.2f}
    main            {main_satir_1:>15,}  {main_toplam_1:>20,.2f}
    """)

    izole = (main_satir_1 == main_satir_0 and abs(main_toplam_1 - main_toplam_0) < 1)
    print(f"    main degisti mi? {'HAYIR -- izolasyon calisiyor' if izole else 'EVET -- IZOLASYON BOZUK!'}")
    print(f"""
    Bu adim senaryonun kalbi. Su anda:
      * Branch'te 50.000 bozuk satir VAR
      * ETL bu veriyi zaten YAZDI (yani is yapildi, bosa gitmedi)
      * KATALOGDAN OKUYAN her tuketici (Spark) main'i goruyor ve bozuk
        veriyi GORMUYOR
      * Kimse beklemedi, kimse kilitlenmedi, tablo kapanmadi

    ------------------------------------------------------------------
     DIKKAT -- BU GARANTI CLICKHOUSE ICIN AYNEN GECERLI DEGIL
    ------------------------------------------------------------------
    Izolasyon garantisi, tuketicinin KATALOGDAN okumasina baglidir.
    Spark Nessie katalogunu kullanir, dolayisiyla korunur.

    Bu kurulumda ClickHouse ise Nessie katalogunu KULLANAMIYOR
    (DataLakeCatalog bu surum ciftinde veri okumuyor -- bkz.
    sql/clickhouse/01_catalog.sql). Onun yerine 'lake.*' gorunumleri
    icebergS3() ile DOGRUDAN S3 yolunu okuyor. icebergS3() katalogu
    ATLAR ve tablo dizinindeki EN YENI metadata'yi secer -- bu bir
    branch'in commit'i olabilir.

    OLCULDU (tests/06_clickhouse_branch_izolasyonu.py):
        Spark      : main 125.277 | branch 126.277   -> IZOLE
        ClickHouse : 126.277                          -> BRANCH'I GORDU
        DROP BRANCH sonrasi ClickHouse hala 126.277 (metadata S3'te kalir)
        main'e yeni commit atilinca 125.277'ye dondu

    PRATIK SONUC:
      * BASARILI ETL'de sorun kendini toplar: merge main'e yeni commit
        atar, en yeni metadata yine main'inki olur.
      * ASIL RISK, kalite kontrolu PATLADIGINDA -- yani tam da bu
        senaryoda: merge edilmemis bozuk branch, federe gorunumlerde
        GORUNUR halde kalir.
      * Bu yuzden uretim dashboard'lari csd.* (MergeTree) tablolarindan
        beslenmelidir; federe gorunumler kesif icindir.

    Sunumda bu ayrimi ACIKCA soyleyin. "Bozuk veri hicbir yere ulasmaz"
    demek, biri lake.silver_islem'e bakip bozuk satirlari gordugunde
    tum anlatiyi cokertir. Dogru cumle: "Katalogdan okuyan tuketiciler
    korunur; federe gorunumler bu surumde katalogu atladigi icin
    korunmaz, o yuzden SLA'li isi materyalize katmandan veriyoruz."
    """)

    # ---------------------------------------------------------------- 5
    banner(5, "Kalite kontrolu branch uzerinde calisiyor")
    kontroller = []

    neg = spark.sql(f"""
        SELECT COUNT(*) c FROM {at_ref(branch)} WHERE tutar <= 0
    """).first()["c"]
    if neg:
        kontroller.append(f"Pozitif olmayan tutar iceren {neg:,} satir")

    tutarsiz = spark.sql(f"""
        SELECT COUNT(*) c FROM {at_ref(branch)}
        WHERE ABS(tutar - (adet * fiyat)) > 0.01
    """).first()["c"]
    if tutarsiz:
        kontroller.append(f"tutar != adet*fiyat olan {tutarsiz:,} satir")

    for k in kontroller:
        print(f"    x {k}")

    print(f"""
    KARAR: {len(kontroller)} bulgu -> MERGE ENGELLENDI.
    Uretim main'i temiz kaldi. Bu, CI/CD'deki "testler kirmizi, deploy
    yok" kuralinin veri tarafindaki karsiligidir.
    """)

    # ---------------------------------------------------------------- 6
    banner(6, "Veri duzeltiliyor -- ayni branch uzerinde")
    print("    Kaynak ekibi birim hatasini duzeltti. Branch'te onariyoruz:")
    spark.sql(f"""
        UPDATE {CATALOG}.{TABLE}
        SET tutar = CAST(ABS(adet * fiyat) AS DECIMAL(20,4))
        WHERE araci_kurum_kodu = 'YENI_KURUM'
    """)

    neg2 = spark.sql(f"SELECT COUNT(*) c FROM {at_ref(branch)} WHERE tutar <= 0").first()["c"]
    tutarsiz2 = spark.sql(f"""
        SELECT COUNT(*) c FROM {at_ref(branch)}
        WHERE ABS(tutar - (adet * fiyat)) > 0.01
    """).first()["c"]

    print(f"    Yeniden kontrol: negatif={neg2}  tutarsiz={tutarsiz2}")

    if neg2 == 0 and tutarsiz2 == 0:
        print("\n    Kontroller GECTI -> main'e merge ediliyor")
        spark.sql(f"MERGE BRANCH {branch} INTO main IN nessie")

        main_satir_2, main_toplam_2 = sayim(spark, "main")
        print(f"""
    Merge sonrasi main: {main_satir_2:,} satir | {main_toplam_2:,.2f} TL
    Onceki main       : {main_satir_0:,} satir | {main_toplam_0:,.2f} TL
    Eklenen           : {main_satir_2 - main_satir_0:,} satir

    50.000 satirin tamami TEK ATOMIK COMMIT ile gorunur oldu. Okuyucular
    ya hepsini gordu ya hicbirini -- arada bir an bile "yarim veri"
    durumu olusmadi.
        """)
        basarili = True
    else:
        print("\n    Kontroller hala basarisiz -- merge yok.")
        basarili = False

    # ---------------------------------------------------------------- 7
    banner(7, "Denetim kaydi -- Nessie commit gecmisi")
    spark.sql("USE REFERENCE main IN nessie")
    print("    main uzerindeki son commit'ler:\n")
    spark.sql("SHOW LOG IN nessie").show(10, truncate=False)

    print("""
    Her commit'te: ne zaman, hangi islem, hangi tablolar, kim.
    Bu, "veri ambarindaki su rakam nereden geldi?" sorusunun
    kayit altindaki cevabidir. Ayri bir lineage araci kurmaya
    gerek kalmadan katalogun kendisinden gelir.
    """)

    # Temizlik
    try:
        spark.sql(f"DROP BRANCH {branch} IN nessie")
        print(f"    [temizlik] {branch} silindi.")
    except Exception as e:
        print(f"    [temizlik] atlandi: {e}")

    print("""
    ---------------------------------------------------------------------
     YONETICIYE OZET
     ---------------------------------------------------------------------
     * Bozuk veri uretime HIC ULASMADI -- sonradan temizlenmedi, engellendi
     * ETL branch'i acmak 0 byte kopyalar, milisaniye surer
     * Izolasyon TABLO degil KATALOG seviyesinde: 5 tabloyu birlikte
       tutarli yayinlayabilirsiniz (mali tablo + islem + pozisyon...)
     * Yayin tek atomik commit -- "yarim yuklenmis rapor" durumu yok
     * Commit gecmisi = hazir denetim izi
     * Ayni mantikla: test ortami icin `CREATE BRANCH uat FROM main`
       -> 2 milyar satirlik uretim verisinin kopyasi, 0 maliyetle

     KURUMSAL KARSILIGI
       Klasik bir ETL akisinda bir hata uretim raporuna dustugunde: tespit,
       kok neden, veri duzeltme, rapor yenileme, bildirim... Bu akista
       o zincir hic baslamiyor.
    ---------------------------------------------------------------------
    """)

    spark.stop()
    return 0 if (izole and basarili) else 1


if __name__ == "__main__":
    raise SystemExit(main())

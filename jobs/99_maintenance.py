#!/usr/bin/env python3
"""
99 - TABLO BAKIMI  (uretimde zamanlanmis is olarak calisir)

Lakehouse projelerinin 6. ayinda cokme sebebi neredeyse her zaman aynidir:
BAKIM YAPILMAMASIDIR. Iceberg her yazimda yeni dosya + yeni manifest +
yeni snapshot uretir. Bakimsiz bir tabloda:

  * Kucuk dosya patlamasi  -> her sorgu binlerce S3 GET yapar, gecikme artar
  * Manifest sisme         -> planlama (query planning) saniyelere cikar
  * Snapshot birikimi      -> silinen veri diskten hic gitmez, maliyet buyur
  * Yetim dosyalar         -> basarisiz job'larin artiklari, kimse temizlemez

Bu script dortunu de ele alir. Uretimde: gunluk compaction, haftalik
snapshot expiry, aylik orphan temizligi.

DIKKAT - expire_snapshots TIME TRAVEL PENCERESINI KISALTIR.
Suresi dolan snapshot'a geri donemezsiniz. Duzenleyici saklama suresi
(CSD icin ilgili SPK mevzuati) ile bu deger UYUMLU olmalidir. Varsayilan
7 gun yalnizca demo icindir.

CALISTIRMA
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/99_maintenance.py --table silver.islem
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/spark/jobs")

from common.session import CATALOG, build_spark

DEFAULT_TABLES = [
    "bronze.islem",
    "silver.islem",
    "gold.gunluk_menkul_ozet",
    "gold.yatirimci_pozisyon",
    "gold.araci_kurum_gunluk",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Iceberg tablo bakimi")
    p.add_argument("--table", action="append", dest="tables",
                   help="Bakim yapilacak tablo (tekrarlanabilir). Bos = hepsi.")
    p.add_argument("--retain-days", type=int, default=7,
                   help="Snapshot saklama suresi (gun). Time travel penceresi budur.")
    p.add_argument("--skip-expire", action="store_true",
                   help="Snapshot expiry'yi atla (time travel demosunu bozmamak icin)")
    p.add_argument("--orphans", action="store_true",
                   help="Yetim dosya temizligini de calistir (yavas, aylik yeter)")
    p.add_argument("--min-input-files", type=int, default=2,
                   help="Bir partition'da compaction icin gereken en az dosya sayisi "
                        "(varsayilan 2). Aciklama icin koddaki nota bakin.")
    p.add_argument("--target-mb", type=int, default=128,
                   help="Hedef dosya boyutu (MB, varsayilan 128)")
    return p.parse_args()


def show(spark, title: str, df) -> None:
    print(f"\n--- {title} ---")
    df.show(truncate=False)


def file_stats(spark, table: str) -> tuple[int, float]:
    """Tablonun kac veri dosyasi var ve ortalama boyutu ne? Compaction oncesi/sonrasi olcum."""
    row = spark.sql(f"""
        SELECT COUNT(*) AS dosya_sayisi,
               ROUND(AVG(file_size_in_bytes) / 1048576.0, 2) AS ort_mb,
               ROUND(SUM(file_size_in_bytes) / 1048576.0, 2) AS toplam_mb
        FROM {CATALOG}.{table}.files
    """).first()
    print(f"    dosya={row['dosya_sayisi']}  ort={row['ort_mb']}MB  toplam={row['toplam_mb']}MB")
    return row["dosya_sayisi"], row["ort_mb"] or 0.0


def compaction_teshisi(spark, table: str, min_input_files: int, target_mb: int) -> None:
    """
    Compaction'in NEDEN is yapip yapmayacagini ONCEDEN soyler.

    NEDEN VAR: rewrite_data_files hicbir sey yapmadiginda sessizce
    '0 dosya yeniden yazildi' der ve basariyla biter. Kullanici "bakim
    calisti" saniyor ama aslinda hicbir sey olmamis olabilir -- ya da
    gercekten yapacak is yoktur. Ikisini ayirt etmek gerekiyor.

    Compaction PARTITION BAZINDA calisir ve bir partition'daki dosya grubu
    ancak su kosullarda yeniden yazilir:
        group.size() > 1 VE group.size() >= min-input-files
    Yani her partition'da TEK dosya varsa yapacak is YOKTUR -- bu bir
    hata degil, dogru davranistir.
    """
    # PARTITION'SIZ TABLO TUZAGI: '.files' metadata tablosunda 'partition'
    # sutunu YALNIZCA partition'li tablolarda bulunur. Partition'siz bir
    # tabloda bu sorgu su hatayi verir:
    #     UNRESOLVED_COLUMN.WITH_SUGGESTION: `partition` cannot be resolved
    # (Bu tuzaga dustuk: teshis fonksiyonu compaction'i hic calistirmadan
    #  job'i dusurdu.) Semayi kontrol edip iki durumu ayri ele aliyoruz.
    kolonlar = [f.name for f in spark.table(f"{CATALOG}.{table}.files").schema.fields]
    partitionli = "partition" in kolonlar

    if partitionli:
        gruplar = f"(SELECT partition, COUNT(*) AS c FROM {CATALOG}.{table}.files GROUP BY partition)"
    else:
        # Partition yoksa tum tablo TEK gruptur.
        gruplar = f"(SELECT 1 AS partition, COUNT(*) AS c FROM {CATALOG}.{table}.files)"

    d = spark.sql(f"""
        SELECT
            COUNT(*)                                   AS partition_sayisi,
            SUM(CASE WHEN c > 1 THEN 1 ELSE 0 END)     AS cok_dosyali_partition,
            SUM(CASE WHEN c >= {min_input_files} AND c > 1 THEN 1 ELSE 0 END) AS aday_partition,
            ROUND(AVG(c), 1)                           AS ort_dosya_per_partition
        FROM {gruplar}
    """).first()

    if not partitionli:
        print("    (tablo partition'siz -- tum dosyalar tek grup)")

    print(f"    partition={d['partition_sayisi']}  "
          f"ort dosya/partition={d['ort_dosya_per_partition']}  "
          f"compaction adayi={d['aday_partition']}")

    if d["aday_partition"] == 0:
        print("    -> YAPACAK IS YOK. Her partition'da tek dosya var; birlestirilecek")
        print("       grup olusmuyor. '0% azalma' burada DOGRU cevaptir, hata degil.")

        # Asil sorun bu olabilir: partition'lama veri hacmine gore fazla ince
        avg = spark.sql(f"""
            SELECT ROUND(AVG(file_size_in_bytes)/1048576.0, 2) AS m
            FROM {CATALOG}.{table}.files
        """).first()["m"] or 0
        if avg < target_mb * 0.1:
            print(f"\n    !! DIKKAT: ortalama dosya {avg}MB, hedef {target_mb}MB.")
            print("       Dosyalar cok kucuk ama compaction cozemiyor -- cunku sorun")
            print("       kucuk dosyalar DEGIL, FAZLA INCE PARTITION'LAMA.")
            print("       Compaction partition'lari BIRLESTIREMEZ; sadece bir")
            print("       partition ICINDEKI dosyalari birlestirir.")
            print("       Cozum: partition semasini kabalastirin. Iceberg'de bu")
            print("       eski veriyi yeniden yazmadan yapilabilir:")
            print(f"           ALTER TABLE {table} ADD PARTITION FIELD years(...);")
            print(f"           ALTER TABLE {table} DROP PARTITION FIELD months(...);")
            print("       (bkz. tests/04_schema_evolution.py -- partition evrimi)")


def maintain(spark, table: str, retain_days: int, skip_expire: bool, orphans: bool,
             min_input_files: int = 2, target_mb: int = 128) -> None:
    print("\n" + "=" * 72)
    print(f"  BAKIM: {table}")
    print("=" * 72)

    print("\n[1/4] Baslangic dosya profili:")
    before_files, _ = file_stats(spark, table)
    compaction_teshisi(spark, table, min_input_files, target_mb)

    # ---- 1. Veri dosyasi birlestirme (compaction) ----
    # Kucuk dosyalari hedef boyuta dogru birlestirir. sort-order tanimliysa
    # Iceberg birlestirirken siralamayi da korur -> min/max istatistikleri iyilesir.
    #
    # ---------------------------------------------------------------------
    #  min-input-files HAKKINDA -- ilk denemede bu yuzden HICBIR SEY OLMADI
    # ---------------------------------------------------------------------
    #  Compaction PARTITION BAZINDA calisir. Esik de partition basina
    #  degerlendirilir: bir partition'da min-input-files'tan AZ dosya varsa
    #  Iceberg o partition'a hic dokunmaz.
    #
    #  Yasadigimiz durum: silver.islem'de 96 dosya vardi ama tablo 24 aya
    #  partition'li -> partition basina 4 dosya. Varsayilan esik 5 oldugu
    #  icin compaction "yapacak is yok" deyip cikti: 96 -> 96, %0 azalma.
    #  Job basariyla bitti, hicbir uyari vermedi. Sinsi.
    #
    #  Bu yuzden varsayilani 2 yaptik. URETIMDE bu degeri dusunerek secin:
    #    * Cok dusuk  -> her gun gereksiz yere veri yeniden yazilir (pahali,
    #                    yeni snapshot'lar uretir, S3 maliyeti)
    #    * Cok yuksek -> kucuk dosyalar birikir, sorgular yavaslar
    #  Pratik kural: partition basina beklediginiz yazim sayisinin biraz
    #  altinda tutun. Saatlik yazim yapan bir tabloda 5-10 mantiklidir.
    # ---------------------------------------------------------------------
    target_bytes = target_mb * 1024 * 1024
    print(f"\n[2/4] rewrite_data_files (compaction)  "
          f"[min-input-files={min_input_files}, hedef={target_mb}MB]...")
    res = spark.sql(f"""
        CALL {CATALOG}.system.rewrite_data_files(
            table => '{table}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '{target_bytes}',
                'min-input-files', '{min_input_files}',
                -- Kismi ilerleme: gruplar halinde commit et.
                -- Job yarida cokerse yapilan is korunur, bastan baslanmaz.
                'partial-progress.enabled', 'true',
                'partial-progress.max-commits', '10',
                'max-concurrent-file-group-rewrites', '4'
            )
        )
    """)
    show(spark, "Compaction sonucu", res)

    print("\n    Compaction sonrasi profil:")
    after_files, _ = file_stats(spark, table)
    if before_files:
        print(f"    -> dosya sayisi {before_files} -> {after_files} "
              f"({100 * (1 - after_files / before_files):.0f}% azalma)")

    # ---- 2. Manifest birlestirme ----
    # Manifest'ler tablo metadata'sinin indeksidir. Cok sayida kucuk manifest,
    # sorgu PLANLAMA suresini uzatir (veri okumadan once!).
    print("\n[3/4] rewrite_manifests...")
    try:
        res = spark.sql(f"CALL {CATALOG}.system.rewrite_manifests(table => '{table}')")
        show(spark, "Manifest sonucu", res)
    except Exception as e:
        print(f"    (atlandi: {e})")

    # ---- 3. Snapshot expiry ----
    #
    # =====================================================================
    #  NESSIE ILE expire_snapshots CALISMAZ -- VE CALISMAMASI DOGRUDUR
    # =====================================================================
    #  Denerseniz su hatayi alirsiniz (aldik):
    #      ValidationException: Cannot expire snapshots: GC is disabled
    #      (deleting files may corrupt other tables)
    #
    #  Sebep: Nessie tum tablolara 'gc.enabled=false' koyar. Cunku Nessie'de
    #  AYNI veri dosyalari BIRDEN FAZLA branch/tag tarafindan referans
    #  edilebilir. Iceberg'in expire_snapshots'i yalnizca TEK bir tablonun
    #  kendi snapshot zincirine bakar; baska branch'lerden haberi yoktur.
    #  Calistirilsaydi, main'de "artik kullanilmiyor" sanip sildigi dosya
    #  bir baska branch'in tek veri kaynagi olabilirdi -> SESSIZ VERI KAYBI.
    #
    #  DOGRU ARAC: Nessie GC. Tum commit grafigini (butun branch ve tag'leri)
    #  birlikte degerlendirir, gercekten hicbir referansi kalmamis dosyalari
    #  belirler. AYRI bir jar'dir, sunucu imajinda YOKTUR:
    #
    #    curl -sLO https://github.com/projectnessie/nessie/releases/download/\
    #  nessie-0.104.1/nessie-gc-0.104.1.jar
    #
    #    java -jar nessie-gc-0.104.1.jar gc \
    #        --uri http://nessie:19120/api/v2 \
    #        --inmemory \
    #        --default-cutoff PT168H \
    #        --iceberg s3.endpoint=http://minio:9000 \
    #        --iceberg s3.path-style-access=true \
    #        --iceberg s3.access-key-id=... \
    #        --iceberg s3.secret-access-key=...
    #
    #  Bu komut BU ORTAMDA CALISTIRILDI ve dogrulandi: EXPIRY_SUCCESS,
    #  15 yetim dosya silindi, tablolarin hicbiri bozulmadi.
    #
    #  TUZAKLAR (hepsine dustuk):
    #    * Komut adi 'gc'. 'mark-and-sweep' diye bir komut YOK.
    #    * Jar adi surumlu: 'nessie-gc.jar' degil 'nessie-gc-0.104.1.jar'.
    #    * '--dry-run' YOK. '--defer-deletes' var ama --inmemory ile
    #      kullanilamaz. Yani --inmemory ile calistirirsaniz SILER.
    #    * S3 ayarlari '--s3-endpoint' ile DEGIL, '--iceberg <prop>=<deger>'
    #      ile verilir.
    #    * '--default-cutoff NONE' "tum commit'ler canli" demektir ama yine
    #      de gercek yetim dosyalari SILER. "Hicbir sey silinmez" sanmayin.
    #
    #  URETIMDE: once --write-live-set-id-to ile mark asamasini calistirip
    #  sonucu inceleyin, sonra sweep edin. JDBC storage kullanin (--inmemory
    #  degil) ki iki asama ayrilabilsin.
    #
    #  YANLIS COZUM: 'ALTER TABLE ... SET TBLPROPERTIES (gc.enabled=true)'
    #  yazip expire'i zorlamak. Hata kaybolur, veri kaybi baslar. Bu bir
    #  guvenlik kilididir, engel degil.
    #
    #  DOKUMANTASYON NOTU: Bu, cogu Iceberg egitiminde gecmeyen bir
    #  ayrintidir; klasik katalogda (Hive/Glue) expire_snapshots dogru
    #  aractir. Nessie ekleyince o alisanlik yanlisa donusuyor.
    # =====================================================================
    if skip_expire:
        print("\n[4/4] expire_snapshots ATLANDI (--skip-expire)")
    else:
        gc_acik = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{table}").where(
            "key = 'gc.enabled'"
        ).collect()
        gc_kapali = bool(gc_acik) and gc_acik[0]["value"].lower() == "false"

        if gc_kapali:
            print("\n[4/4] expire_snapshots ATLANIYOR -- gc.enabled=false")
            print("      Bu bir hata DEGIL: Nessie kataloglarinda beklenen durum.")
            print("      Ayni veri dosyalari baska branch'lerden referans ediliyor")
            print("      olabilir; tek tablonun gecmisine bakarak silmek veri kaybi")
            print("      riskidir. Nessie bu yuzden Iceberg GC'sini kilitler.")
            print("\n      DOGRU ARAC -- Nessie GC (tum commit grafigini gorur).")
            print("      Ayri bir jar; sunucu imajinda yok. Kurulum + calistirma:")
            print("        .\\run.ps1 gc            # sarmalayici (tavsiye edilen)")
            print("      veya elle:")
            print("        java -jar nessie-gc-0.104.1.jar gc \\")
            print("             --uri http://nessie:19120/api/v2 --inmemory \\")
            print(f"             --default-cutoff PT{retain_days * 24}H \\")
            print("             --iceberg s3.endpoint=http://minio:9000 \\")
            print("             --iceberg s3.path-style-access=true \\")
            print("             --iceberg s3.access-key-id=... \\")
            print("             --iceberg s3.secret-access-key=...")
            print("\n      DIKKAT: --dry-run YOK. --inmemory ile calistirirsaniz")
            print("      dosyalari GERCEKTEN SILER. Ayrinti: bu dosyadaki [4/4]")
            print("      bolumunun basindaki not.")
        else:
            cutoff = (datetime.now() - timedelta(days=retain_days)).strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[4/4] expire_snapshots (kesim: {cutoff}, {retain_days} gun saklanir)")
            print("      UYARI: Bu tarihten eski snapshot'lara TIME TRAVEL ARTIK MUMKUN DEGIL.")
            res = spark.sql(f"""
                CALL {CATALOG}.system.expire_snapshots(
                    table => '{table}',
                    older_than => TIMESTAMP '{cutoff}',
                    retain_last => 5,
                    max_concurrent_deletes => 4
                )
            """)
            show(spark, "Expiry sonucu", res)

    # ---- 4. Yetim dosya temizligi ----
    #
    # remove_orphan_files DA Nessie ile CALISMAZ -- ayni gerekce, ayni kilit:
    #     ValidationException: Cannot delete orphan files: GC is disabled
    #
    # Ve mantikli: "yetim" tanimi, dosyayi HANGI referanslarin gosterdigine
    # baglidir. Iceberg tek tablonun metadata'sina bakar; bir dosyayi baska
    # bir branch kullaniyor olabilecegini bilemez. Nessie'de "yetim" ancak
    # TUM commit grafigi tarandiktan sonra soylenebilir.
    #
    # Yani Nessie ile DOSYA SILEN her iki islem de (expire + orphan) kilitli
    # ve ikisinin de yerine gecen tek arac Nessie GC'dir. Bu tutarli bir
    # tasarim: Iceberg'in "tek tablo" dunya gorusu, Nessie'nin "coklu branch"
    # gercekligiyle guvenli sekilde birlestirilemez, o yuzden devri aliniyor.
    if orphans:
        gc_row = spark.sql(f"SHOW TBLPROPERTIES {CATALOG}.{table}").where(
            "key = 'gc.enabled'"
        ).collect()
        if gc_row and gc_row[0]["value"].lower() == "false":
            print("\n[+] remove_orphan_files ATLANIYOR -- gc.enabled=false")
            print("    expire_snapshots ile ayni sebep: Nessie'de bir dosyanin")
            print("    'yetim' oldugunu soyleyebilmek icin TUM branch'leri gormek")
            print("    gerekir. Iceberg tek tabloya bakar, bilemez.")
            print("    Yetim dosyalari da Nessie GC temizler (yukaridaki komut).")
        else:
            # Klasik katalogda (Hive/Glue) dogru arac budur.
            # Cok dikkatli olun: tablo metadata'sinin bilmedigi dosyalari SILER.
            # Halen calisan bir yazma job'i varken calistirmayin; onun yazdigi
            # ama henuz commit etmedigi dosyalari yetim sanar. older_than ile
            # guvenli bir pencere birakiyoruz.
            cutoff = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n[+] remove_orphan_files (3 gunden eski)")
            res = spark.sql(f"""
                CALL {CATALOG}.system.remove_orphan_files(
                    table => '{table}',
                    older_than => TIMESTAMP '{cutoff}'
                )
            """)
            show(spark, "Orphan sonucu", res)

    # ---- Ozet ----
    print(f"\n    Kalan snapshot sayisi:")
    spark.sql(f"""
        SELECT COUNT(*) AS snapshot_sayisi,
               MIN(committed_at) AS en_eski,
               MAX(committed_at) AS en_yeni
        FROM {CATALOG}.{table}.snapshots
    """).show(truncate=False)


def main() -> int:
    args = parse_args()
    tables = args.tables or DEFAULT_TABLES

    spark = build_spark("iceberg_maintenance", branch="main")

    # Tek tablonun patlamasi digerlerini engellememeli -- ama BASARISIZLIK
    # YUTULMAMALI da. Onceki hali her hatayi yakalayip yine de 0 donuyordu:
    # zamanlanmis bir is olarak (cron/Airflow) TUM tablolarda patlasa bile
    # "basarili" gorunurdu ve kimse haberdar olmazdi. Bakim isinin sessizce
    # calismamasi, bu dosyanin basinda anlatilan tam da o 6-ay-sonra-cokme
    # senaryosunun sebebidir.
    hatalar: list[str] = []
    for t in tables:
        try:
            maintain(spark, t, args.retain_days, args.skip_expire, args.orphans,
                     args.min_input_files, args.target_mb)
        except Exception as e:
            print(f"\n[hata] {t} bakimi basarisiz: {e}")
            hatalar.append(f"{t}: {e}")

    print("\n" + "=" * 72)
    if hatalar:
        print(f"  Bakim BASARISIZ -- {len(hatalar)}/{len(tables)} tabloda hata:")
        for h in hatalar:
            print(f"    x {h}")
    else:
        print(f"  Bakim tamamlandi -- {len(tables)} tablo, hata yok.")
    print("=" * 72)
    print("""
  URETIM ZAMANLAMASI ONERISI (Airflow/cron):

     Gunluk   : rewrite_data_files      (sicak partition'lar)
     Haftalik : rewrite_manifests
     Haftalik : NESSIE GC  <-- expire_snapshots DEGIL!
                .\\run.ps1 gc 168
                (elle: java -jar nessie-gc-<SURUM>.jar gc \\
                       --uri http://nessie:19120/api/v2 --inmemory \\
                       --default-cutoff PT168H --iceberg s3.endpoint=...)
     Aylik    : remove_orphan_files --orphans   <-- Nessie'de BU DA KILITLI,
                yerine yine Nessie GC calisir (yukaridaki komut ikisini de yapar)

  DIKKAT -- KLASIK ICEBERG ALISKANLIGI BURADA YANLIS:
     Hive/Glue katalogunda snapshot temizligi 'expire_snapshots' ile
     yapilir. NESSIE ILE YAPILAMAZ: Nessie tablolara gc.enabled=false
     koyar, cunku ayni veri dosyalari birden fazla branch tarafindan
     referans edilebilir. Tek tablonun gecmisine bakip silmek, baska
     bir branch'in verisini yok edebilir.

     Nessie GC tum commit grafigini (butun branch/tag'ler) birlikte
     degerlendirir. Saklama suresini CSD'nin tabi oldugu mevzuata gore
     secin -- bu deger ayni zamanda TIME TRAVEL PENCERENIZDIR.
    """)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
01 - OLTP  ->  BRONZE  (Write-Audit-Publish deseni)

AKIS
    1. Nessie'de izole bir ETL branch'i acilir      (ornek: etl_bronze_20260717_1430)
    2. Kaynak Postgres PARALEL olarak okunur
    3. Iceberg tablosuna o branch uzerinde YAZILIR   <- main hala temiz
    4. Kalite kontrolleri branch uzerinde CALISTIRILIR
    5. Gecerse main'e MERGE edilir; gecmezse branch birakilir/silinir

NEDEN BOYLE?
    Klasik lakehouse'ta ETL dogrudan uretim tablosuna yazar. Job yarida
    coker veya veri bozuksa, uretim tablosu yarim/bozuk veriyle kalir ve
    okuyucular bunu gorur. WAP'ta okuyucular main'i gorur; main ancak
    dogrulama gectikten sonra, TEK bir atomik commit ile ilerler.
    Kismi gorunurluk diye bir sey olusmaz.

CALISTIRMA
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/01_oltp_to_bronze.py --full

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/01_oltp_to_bronze.py --since 2026-07-01
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

sys.path.insert(0, "/opt/spark/jobs")

from pyspark.sql import functions as F

from common.session import build_spark, oltp_bounds, read_oltp

NAMESPACE = "bronze"
TABLE = "islem"
FQN = f"{NAMESPACE}.{TABLE}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="OLTP -> Bronze Iceberg yukleyici")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--full", action="store_true",
                   help="Tam yukleme: kaynaktaki TUM satirlari okur ve "
                        "dokundugu partition'lari bastan yazar "
                        "(dinamik partition overwrite -- ayrintili not koda bakin)")
    g.add_argument("--since", metavar="YYYY-MM-DD",
                   help="Artimli: bu tarihten sonra guncellenen satirlar")
    p.add_argument("--partitions", type=int, default=16,
                   help="JDBC paralel okuma parca sayisi (varsayilan 16)")
    p.add_argument("--no-merge", action="store_true",
                   help="Branch'i main'e merge etme, incelemek icin birak")
    return p.parse_args()


def ensure_namespace(spark) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")


def create_table_if_absent(spark) -> None:
    """
    Iceberg tablosunu ACIKCA olusturuyoruz (df.writeTo(...).create() yerine),
    cunku partition semasi ve tablo ozellikleri bilincli kararlar -- Spark'in
    cikarimina birakilmamali.
    """
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQN} (
            islem_id          BIGINT      COMMENT 'Kaynak PK',
            yatirimci_id      BIGINT,
            menkul_id         BIGINT,
            islem_zamani      TIMESTAMP,
            islem_tarihi      DATE,
            islem_tipi        STRING      COMMENT 'ALIS | SATIS',
            adet              DECIMAL(18,4),
            fiyat             DECIMAL(18,6),
            tutar             DECIMAL(20,4),
            komisyon          DECIMAL(12,4),
            araci_kurum_kodu  STRING,
            kanal             STRING      COMMENT 'SUBE | INTERNET | MOBIL | API',
            guncelleme_ts     TIMESTAMP   COMMENT 'Kaynak degisiklik damgasi (CDC watermark)',
            _ingest_ts        TIMESTAMP   COMMENT 'Lakehouse yazim ani (kaynak degil, teknik alan)',
            _ingest_job       STRING      COMMENT 'Hangi job/branch yazdi - soy agaci (lineage)'
        )
        USING iceberg
        -- PARTITION KARARI --------------------------------------------------
        -- Hedef: partition basina ~128MB-1GB veri.
        --   2M satir  / 24 ay ~=  83k satir/ay ~=  5-10MB  (kucuk ama demo icin yeterli)
        --  20M satir  / 24 ay ~= 833k satir/ay ~= 50-100MB (hedef araliga yaklasti)
        -- CSD gercek hacminde (gunluk milyonlarca islem) dogru secim
        -- days(islem_zamani) + bucket(N, yatirimci_id) olurdu.
        -- Iceberg'de partition semasi SONRADAN degistirilebilir (hidden
        -- partitioning), eski veriyi yeniden yazmadan. Bu yuzden bugunun
        -- hacmine gore secmek dogru; gelecege gore asiri tasarim yapmak degil.
        PARTITIONED BY (months(islem_zamani))
        TBLPROPERTIES (
            'format-version'                = '2',
            'write.format.default'          = 'parquet',
            'write.parquet.compression-codec' = 'zstd',
            'write.target-file-size-bytes'  = '134217728',

            -- COPY-ON-WRITE SECIMI ------------------------------------------
            -- Iceberg v2 varsayilani merge-on-read'dir: silme/guncelleme
            -- islemleri ayri "delete file"lar yazar, okuyucu bunlari runtime'da
            -- birlestirir. Spark bunu sorunsuz okur; ANCAK ClickHouse'un
            -- Iceberg okuyucusu positional/equality delete destegi konusunda
            -- surum surum degisken. Bu tabloyu ClickHouse okuyacagi icin
            -- copy-on-write sectik: yazim biraz pahali, okuma her motorda
            -- dogru. Yalnizca Spark'in okudugu tablolarda MOR birakilabilir.
            'write.delete.mode'             = 'copy-on-write',
            'write.update.mode'             = 'copy-on-write',
            'write.merge.mode'              = 'copy-on-write',

            'history.expire.max-snapshot-age-ms' = '604800000'  -- 7 gun
        )
    """)


def dq_checks(spark) -> list[str]:
    """
    Branch uzerinde kalite kontrolleri. Bos liste = temiz.

    Bu kontroller main'e merge ETMEDEN once calisir. Bir tanesi bile
    patlarsa kirli veri uretime hic ulasmaz.
    """
    problems: list[str] = []
    t = f"{FQN}"

    total = spark.sql(f"SELECT COUNT(*) c FROM {t}").first()["c"]
    if total == 0:
        problems.append("Tablo bos - yukleme hic satir yazmamis.")
        return problems

    # 1) Birincil anahtar tekilligi
    dup = spark.sql(f"""
        SELECT COUNT(*) c FROM (
            SELECT islem_id FROM {t} GROUP BY islem_id HAVING COUNT(*) > 1
        )
    """).first()["c"]
    if dup > 0:
        problems.append(f"islem_id tekil degil: {dup} adet tekrarli anahtar.")

    # 2) Zorunlu alanlarda NULL
    nulls = spark.sql(f"""
        SELECT
          SUM(CASE WHEN yatirimci_id IS NULL THEN 1 ELSE 0 END) AS n_yatirimci,
          SUM(CASE WHEN menkul_id    IS NULL THEN 1 ELSE 0 END) AS n_menkul,
          SUM(CASE WHEN islem_zamani IS NULL THEN 1 ELSE 0 END) AS n_zaman,
          SUM(CASE WHEN tutar        IS NULL THEN 1 ELSE 0 END) AS n_tutar
        FROM {t}
    """).first()
    for col, val in nulls.asDict().items():
        if val and val > 0:
            problems.append(f"Zorunlu alanda NULL: {col} = {val}")

    # 3) Is kurali: tutar ~ adet * fiyat  (yuvarlama toleransi ile)
    bad_amount = spark.sql(f"""
        SELECT COUNT(*) c FROM {t}
        WHERE ABS(tutar - (adet * fiyat)) > 0.01
    """).first()["c"]
    if bad_amount > 0:
        problems.append(f"tutar != adet*fiyat olan {bad_amount} satir var.")

    # 4) Is kurali: negatif/sifir tutar olamaz
    non_positive = spark.sql(f"SELECT COUNT(*) c FROM {t} WHERE tutar <= 0 OR adet <= 0").first()["c"]
    if non_positive > 0:
        problems.append(f"Pozitif olmayan adet/tutar iceren {non_positive} satir var.")

    # 5) Sozluk kontrolu
    bad_enum = spark.sql(f"""
        SELECT COUNT(*) c FROM {t} WHERE islem_tipi NOT IN ('ALIS','SATIS')
    """).first()["c"]
    if bad_enum > 0:
        problems.append(f"Beklenmeyen islem_tipi degeri iceren {bad_enum} satir var.")

    print(f"[dq] {total:,} satir kontrol edildi. Bulgu: {len(problems)}")
    return problems


def main() -> int:
    args = parse_args()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    branch = f"etl_bronze_{stamp}"

    # Once main'e baglaniyoruz: branch'i buradan cataliyoruz.
    spark = build_spark(f"oltp_to_bronze[{stamp}]", branch="main")

    # ---- BOOTSTRAP: main'in en az BIR gercek commit'i olmali ----
    # Bos bir Nessie deposunda main, "noAncestorHash" adli sentinel bir
    # degerde durur -- bu gercek bir commit DEGILDIR. Bu haldeyken branch
    # acip commit yapip main'e merge etmeye kalkarsaniz Nessie su hatayi
    # verir (yasandi):
    #     NessieReferenceNotFoundException:
    #     No common ancestor in parents of <noAncestorHash> and <branch head>
    # Cunku merge, iki referans arasinda ortak ata arar; sentinel'in atasi
    # yoktur. Namespace'i ONCE main'de olusturarak main'e gercek bir commit
    # atiyoruz. Sonraki calistirmalarda IF NOT EXISTS sayesinde no-op olur.
    ensure_namespace(spark)

    print(f"\n[wap] ETL branch'i aciliyor: {branch}  (main'den)")
    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN nessie FROM main")

    # Oturumu branch'e cevir. Bundan sonraki her yazim SADECE branch'te.
    spark.sql(f"USE REFERENCE {branch} IN nessie")

    ensure_namespace(spark)
    create_table_if_absent(spark)

    # ---------------- Kaynak okuma ----------------
    if args.since:
        # Artimli: guncelleme_ts watermark'i. Pushdown olsun diye filtreyi
        # dbtable icine gomuyoruz -- boylece Postgres tarafinda idx_islem_guncelleme
        # kullanilir, tum tablo Spark'a cekilip orada filtrelenmez.
        src_expr = f"""(
            SELECT * FROM csd.islem
            WHERE guncelleme_ts >= TIMESTAMP '{args.since}'
        ) AS src"""
        print(f"[read] Artimli yukleme: guncelleme_ts >= {args.since}")
    else:
        src_expr = "csd.islem"
        print("[read] Tam yukleme")

    lo, hi = oltp_bounds(spark)
    print(f"[read] JDBC partition araligi islem_id: {lo:,} .. {hi:,} ({args.partitions} parca)")

    src = read_oltp(spark, src_expr, "islem_id", lo, hi, args.partitions)

    staged = (
        src.select(
            F.col("islem_id").cast("bigint"),
            F.col("yatirimci_id").cast("bigint"),
            F.col("menkul_id").cast("bigint"),
            F.col("islem_zamani").cast("timestamp"),
            F.col("islem_tarihi").cast("date"),
            F.col("islem_tipi").cast("string"),
            F.col("adet").cast("decimal(18,4)"),
            F.col("fiyat").cast("decimal(18,6)"),
            F.col("tutar").cast("decimal(20,4)"),
            F.col("komisyon").cast("decimal(12,4)"),
            F.col("araci_kurum_kodu").cast("string"),
            F.col("kanal").cast("string"),
            F.col("guncelleme_ts").cast("timestamp"),
        )
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_ingest_job", F.lit(branch))
    )

    # ---------------- Yazma ----------------
    if args.full:
        # --------------------------------------------------------------
        #  overwritePartitions() = DINAMIK PARTITION OVERWRITE
        #
        #  Yalnizca GELEN VERIDE BULUNAN partition'lari degistirir.
        #  Gelen veride hic satiri olmayan bir partition tabloda OLDUGU
        #  GIBI KALIR -- silinmez.
        #
        #  Normal kullanimda dogru davranis budur: kaynak araligi ayni
        #  kaldigi surece sonuc tam yenilemeyle ozdestir ve dokunulmayan
        #  partition'lar bosuna yeniden yazilmaz.
        #
        #  DIKKAT EDILECEK TEK DURUM: kaynagin araligi DARALIRSA
        #  (ornegin .env'de tarih araligi kisaltilip yeniden seed
        #  edilirse) eski partition'lar tabloda KALIR ve toplam satir
        #  sayisi kaynaktan fazla cikar. Bu sessiz bir tutarsizliktir.
        #  Boyle bir durumda tabloyu bilerek sifirlayin:
        #      DROP TABLE nessie.bronze.islem;   (sonra bu job'i kosun)
        #  Job sonunda basilan satir sayisini kaynakla karsilastirmak
        #  bu durumu yakalamanin en hizli yoludur.
        # --------------------------------------------------------------
        print(f"[write] {FQN} <- TAM yukleme (dinamik partition overwrite)")
        (
            staged.sortWithinPartitions("islem_zamani")   # dosya ici min/max daralt
            .writeTo(FQN)
            .overwritePartitions()
        )
    else:
        # MERGE INTO: upsert. Kaynakta guncellenen satir varsa uzerine yazar,
        # yenilerse ekler. Iceberg'in ACID garantisi burada devrede.
        print(f"[write] {FQN} <- MERGE (upsert)")
        staged.createOrReplaceTempView("staged_islem")
        spark.sql(f"""
            MERGE INTO {FQN} t
            USING (SELECT * FROM staged_islem) s
            ON t.islem_id = s.islem_id
            WHEN MATCHED AND s.guncelleme_ts > t.guncelleme_ts THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    # ---------------- Denetim ----------------
    print("\n[dq] Kalite kontrolleri calisiyor (branch uzerinde)...")
    problems = dq_checks(spark)

    if problems:
        print("\n" + "=" * 70)
        print("  KALITE KONTROLU BASARISIZ - main'e MERGE EDILMEYECEK")
        print("=" * 70)
        for p in problems:
            print(f"   x {p}")
        print(f"\n  Kirli veri '{branch}' branch'inde izole. main etkilenmedi.")
        print(f"  Incelemek icin:  USE REFERENCE {branch} IN nessie; SELECT ...")
        print(f"  Silmek icin:     DROP BRANCH {branch} IN nessie;")
        spark.stop()
        return 1

    print("[dq] Tum kontroller gecti.")

    # ---------------- Yayin ----------------
    if args.no_merge:
        print(f"[wap] --no-merge verildi. Branch '{branch}' inceleme icin birakildi.")
        spark.stop()
        return 0

    print(f"[wap] main <- {branch} MERGE ediliyor (tek atomik commit)")
    spark.sql(f"MERGE BRANCH {branch} INTO main IN nessie")
    spark.sql(f"DROP BRANCH {branch} IN nessie")
    print(f"[wap] Yayinlandi. Okuyucular artik yeni veriyi goruyor.")

    spark.sql("USE REFERENCE main IN nessie")
    cnt = spark.sql(f"SELECT COUNT(*) c FROM {FQN}").first()["c"]
    print(f"\n[ok] main.{FQN} toplam satir: {cnt:,}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

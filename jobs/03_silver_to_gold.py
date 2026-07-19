#!/usr/bin/env python3
"""
03 - SILVER -> GOLD

Gold = is sorusuna gore modellenmis, onceden toplanmis servis tablolari.
ClickHouse'un native MergeTree'ye materyalize edecegi tablolar bunlar.

Uretilen tablolar:
  gold.gunluk_menkul_ozet   -- menkul x gun x kanal islem ozeti
  gold.yatirimci_pozisyon   -- yatirimci x menkul net pozisyon
  gold.araci_kurum_gunluk   -- araci kurum performans ozeti

NEDEN ON-TOPLAMA?
  Silver 2M satir. "Gunluk hacim" sorusu her sorulusunda 2M satir taramak
  yerine 24k satirlik ozet okunur. Sub-second hedefine giden yolun buyuk
  kismi burada kazanilir; ClickHouse'un hizli olmasi ikinci adim.
  Yanlis modellenmis veriyi hizli motor kurtarmaz.

CALISTIRMA
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/03_silver_to_gold.py
"""

from __future__ import annotations

import sys
from datetime import datetime

sys.path.insert(0, "/opt/spark/jobs")

from pyspark.sql import functions as F

from common.session import build_spark


def main() -> int:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    branch = f"etl_gold_{stamp}"

    spark = build_spark(f"silver_to_gold[{stamp}]", branch="main")

    # BOOTSTRAP: main'de gercek commit garantisi -- bkz 01_oltp_to_bronze.py
    # icindeki ayrintili aciklama (noAncestorHash / merge ortak ata sorunu).
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")

    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN nessie FROM main")
    spark.sql(f"USE REFERENCE {branch} IN nessie")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS gold")

    silver = spark.table("silver.islem")

    # ---------------------------------------------------------------- 1
    print("[gold] gunluk_menkul_ozet")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.gunluk_menkul_ozet (
            islem_tarihi     DATE,
            menkul_id        BIGINT,
            isin_kodu        STRING,
            kisa_kod         STRING,
            kiymet_tipi      STRING,
            pazar            STRING,
            kanal            STRING,
            islem_adedi      BIGINT,
            toplam_hacim     DECIMAL(24,4),
            toplam_adet      DECIMAL(24,4),
            toplam_komisyon  DECIMAL(20,4),
            alis_hacmi       DECIMAL(24,4),
            satis_hacmi      DECIMAL(24,4),
            tekil_yatirimci  BIGINT,
            agirlikli_fiyat  DECIMAL(24,8),
            min_fiyat        DECIMAL(18,6),
            max_fiyat        DECIMAL(18,6)
        )
        USING iceberg
        PARTITIONED BY (months(islem_tarihi))
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd',
            'write.delete.mode'='copy-on-write',
            'write.update.mode'='copy-on-write',
            'write.merge.mode'='copy-on-write'
        )
    """)

    gunluk = (
        silver.groupBy(
            "islem_tarihi", "menkul_id", "isin_kodu", "kisa_kod",
            "kiymet_tipi", "pazar", "kanal",
        )
        .agg(
            F.count("*").cast("bigint").alias("islem_adedi"),
            F.sum("tutar").cast("decimal(24,4)").alias("toplam_hacim"),
            F.sum("adet").cast("decimal(24,4)").alias("toplam_adet"),
            F.sum("komisyon").cast("decimal(20,4)").alias("toplam_komisyon"),
            F.sum(F.when(F.col("islem_tipi") == "ALIS", F.col("tutar")).otherwise(0))
                .cast("decimal(24,4)").alias("alis_hacmi"),
            F.sum(F.when(F.col("islem_tipi") == "SATIS", F.col("tutar")).otherwise(0))
                .cast("decimal(24,4)").alias("satis_hacmi"),
            F.countDistinct("yatirimci_id").cast("bigint").alias("tekil_yatirimci"),
            # VWAP: hacim agirlikli ortalama fiyat
            (F.sum(F.col("fiyat") * F.col("adet")) / F.sum("adet"))
                .cast("decimal(24,8)").alias("agirlikli_fiyat"),
            F.min("fiyat").alias("min_fiyat"),
            F.max("fiyat").alias("max_fiyat"),
        )
    )
    gunluk.writeTo("gold.gunluk_menkul_ozet").overwritePartitions()

    # ---------------------------------------------------------------- 2
    print("[gold] yatirimci_pozisyon")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.yatirimci_pozisyon (
            yatirimci_id     BIGINT,
            yatirimci_tipi   STRING,
            uyruk            STRING,
            il_kodu          SMALLINT,
            risk_profili     STRING,
            menkul_id        BIGINT,
            isin_kodu        STRING,
            kisa_kod         STRING,
            kiymet_tipi      STRING,
            net_adet         DECIMAL(24,4),
            net_tutar        DECIMAL(24,4),
            islem_adedi      BIGINT,
            ilk_islem        TIMESTAMP,
            son_islem        TIMESTAMP,
            hesaplama_ts     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (kiymet_tipi, bucket(16, yatirimci_id))
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd',
            'write.delete.mode'='copy-on-write',
            'write.update.mode'='copy-on-write',
            'write.merge.mode'='copy-on-write'
        )
    """)

    pozisyon = (
        silver.groupBy(
            "yatirimci_id", "yatirimci_tipi", "uyruk", "il_kodu",
            "risk_profili", "menkul_id", "isin_kodu", "kisa_kod", "kiymet_tipi",
        )
        .agg(
            F.sum("isaretli_adet").cast("decimal(24,4)").alias("net_adet"),
            F.sum(
                F.when(F.col("islem_tipi") == "ALIS", -F.col("net_tutar"))
                 .otherwise(F.col("net_tutar"))
            ).cast("decimal(24,4)").alias("net_tutar"),
            F.count("*").cast("bigint").alias("islem_adedi"),
            F.min("islem_zamani").alias("ilk_islem"),
            F.max("islem_zamani").alias("son_islem"),
        )
        # ------------------------------------------------------------------
        #  KAPANMIS POZISYONLAR (net = 0) DISLANIR
        #
        #  Kaynak sistem (csd.bakiye) bunu zaten yapiyor:
        #        HAVING SUM(CASE WHEN ... ) <> 0
        #  Bir yatirimci bir kiymeti alip tamamen sattiysa ELINDE HICBIR SEY
        #  YOKTUR. CSD gibi bir saklama kurulusunda "pozisyon" duzenleyici
        #  bir kavramdir; duz (flat) pozisyon bir varlik degildir ve
        #  pozisyon tablosunda satiri olmamalidir.
        #
        #  Islem YAPILDIGI bilgisi kaybolmuyor: onu gold.araci_kurum_gunluk
        #  ve gold.gunluk_menkul_ozet zaten tasiyor. Bu tablonun tanesi
        #  "elde ne var", digerlerininki "ne islem oldu".
        #
        #  ---------------------------------------------------------------
        #  BU SATIR OLCEK TESTINDE BULUNDU -- 2M'de GORUNMUYORDU
        #  ---------------------------------------------------------------
        #    2.000.000 satirda hicbir yatirimci-menkul cifti tam olarak
        #    sifira kapanmamisti. gold ile bakiye BIREBIR tutuyordu
        #    (1.644.018 = 1.644.018) ve bu eksik filtre GORUNMEZDI.
        #
        #    20.000.000 satirda 2 cift tam kapandi:
        #        gold.yatirimci_pozisyon : 10.384.621   <- 2 adet net=0 dahil
        #        csd.bakiye              : 10.384.619
        #    Mutabakat testi "lakehouse'ta 2 fazla kayit" diyecekti ve
        #    yoneticiye guven veren "SIFIR fark" tablosu bozulacakti.
        #
        #  DERS: iki kaynagin ayni SAYIYI vermesi, ayni TANIMI kullandiklari
        #  anlamina gelmez. Kucuk veride ortusen tanim farklari buyuk veride
        #  ayrisir. Mutabakati sadece sayiya degil TANIMA da kurun --
        #  ve tanim farkini kodda gorunur yerde belgeleyin.
        # ------------------------------------------------------------------
        .filter(F.col("net_adet") != 0)
        .withColumn("hesaplama_ts", F.current_timestamp())
    )
    pozisyon.writeTo("gold.yatirimci_pozisyon").overwritePartitions()

    # ---------------------------------------------------------------- 3
    print("[gold] araci_kurum_gunluk")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.araci_kurum_gunluk (
            islem_tarihi     DATE,
            araci_kurum_kodu STRING,
            kanal            STRING,
            islem_adedi      BIGINT,
            toplam_hacim     DECIMAL(24,4),
            toplam_komisyon  DECIMAL(20,4),
            tekil_yatirimci  BIGINT,
            tekil_menkul     BIGINT
        )
        USING iceberg
        PARTITIONED BY (months(islem_tarihi))
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet',
            'write.delete.mode'='copy-on-write',
            'write.update.mode'='copy-on-write',
            'write.merge.mode'='copy-on-write'
        )
    """)

    araci = (
        silver.groupBy("islem_tarihi", "araci_kurum_kodu", "kanal")
        .agg(
            F.count("*").cast("bigint").alias("islem_adedi"),
            F.sum("tutar").cast("decimal(24,4)").alias("toplam_hacim"),
            F.sum("komisyon").cast("decimal(20,4)").alias("toplam_komisyon"),
            F.countDistinct("yatirimci_id").cast("bigint").alias("tekil_yatirimci"),
            F.countDistinct("menkul_id").cast("bigint").alias("tekil_menkul"),
        )
    )
    araci.writeTo("gold.araci_kurum_gunluk").overwritePartitions()

    spark.sql(f"MERGE BRANCH {branch} INTO main IN nessie")
    spark.sql(f"DROP BRANCH {branch} IN nessie")

    spark.sql("USE REFERENCE main IN nessie")
    for t in ["gold.gunluk_menkul_ozet", "gold.yatirimci_pozisyon", "gold.araci_kurum_gunluk"]:
        c = spark.sql(f"SELECT COUNT(*) c FROM {t}").first()["c"]
        print(f"[ok] {t}: {c:,} satir")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

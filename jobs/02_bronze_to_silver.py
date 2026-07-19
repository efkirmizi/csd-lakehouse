#!/usr/bin/env python3
"""
02 - BRONZE -> SILVER

Bronze = kaynagin sadik kopyasi (ham, dokunulmamis, yeniden uretilebilir).
Silver = temizlenmis, zenginlestirilmis, is kurallari uygulanmis, sorgulanabilir.

Bu katmanda yapilanlar:
  * Boyut tablolariyla birlestirme (yatirimci, menkul_kiymet)
  * Is anlaminda turetilmis alanlar (net_tutar, isaretli_adet)
  * Tekrarli kayitlarin ayiklanmasi (kaynak tekrar gonderirse)
  * Gecersiz satirlarin karantinaya alinmasi (dusurmek DEGIL - karantina)

KARANTINA NEDEN?
  Gecersiz satiri sessizce dusurmek, veriyi kaybetmek demektir ve
  mutabakatta "kaynakta 20.000.000 vardi, silver'da 19.999.987 var, 13'u
  nerede?" sorusuna cevap birakmaz. Duzenlemeye tabi bir kurumda bu
  kabul edilemez. Ayri bir tabloya sebebiyle birlikte yaziyoruz.

CALISTIRMA
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        /opt/spark/jobs/02_bronze_to_silver.py
"""

from __future__ import annotations

import sys
from datetime import datetime

sys.path.insert(0, "/opt/spark/jobs")

from pyspark.sql import Window
from pyspark.sql import functions as F

from common.session import build_spark, read_oltp, table_bounds


def main() -> int:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    branch = f"etl_silver_{stamp}"

    spark = build_spark(f"bronze_to_silver[{stamp}]", branch="main")

    # BOOTSTRAP: main'de gercek commit garantisi -- bkz 01_oltp_to_bronze.py
    # icindeki ayrintili aciklama (noAncestorHash / merge ortak ata sorunu).
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

    spark.sql(f"CREATE BRANCH IF NOT EXISTS {branch} IN nessie FROM main")
    spark.sql(f"USE REFERENCE {branch} IN nessie")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")

    # -------- Boyutlar: kucuk tablolar, dogrudan OLTP'den, broadcast join --------
    # Iceberg'e ayrica yazmaya deger degil; her calistirmada tazesini almak
    # daha basit ve daha dogru.
    #
    # SINIRLAR KAYNAKTAN OKUNUR, SABIT YAZILMAZ.
    # Onceki hali '1, 50000' ve '1, 500' diye sabitti. Bu sessiz veri kaybi
    # demekti: .env'de OLTP hacmi buyutuldugunde (veya kaynak sistem yeni
    # yatirimci ekledigi anda) sinirin disinda kalan satirlar HIC OKUNMAZ
    # ve hata da verilmez -- sadece eksik boyut, dolayisiyla karantinaya
    # dusen olgular. Bulmasi cok zor bir hata sinifi.
    y_lo, y_hi = table_bounds(spark, "csd.yatirimci", "yatirimci_id")
    m_lo, m_hi = table_bounds(spark, "csd.menkul_kiymet", "menkul_id")

    yatirimci = read_oltp(spark, "csd.yatirimci", "yatirimci_id", y_lo, y_hi, 4).select(
        "yatirimci_id", "yatirimci_tipi", "uyruk", "il_kodu", "risk_profili"
    )
    menkul = read_oltp(spark, "csd.menkul_kiymet", "menkul_id", m_lo, m_hi, 1).select(
        "menkul_id", "isin_kodu", "kisa_kod", "kiymet_tipi", "pazar", "para_birimi"
    )

    bronze = spark.table("bronze.islem")

    # -------- Tekrar ayiklama --------
    # Ayni islem_id birden fazla kez geldiyse en guncel guncelleme_ts kazanir.
    w = Window.partitionBy("islem_id").orderBy(F.col("guncelleme_ts").desc())
    dedup = (
        bronze.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    enriched = (
        dedup.join(F.broadcast(yatirimci), "yatirimci_id", "left")
        .join(F.broadcast(menkul), "menkul_id", "left")
        .withColumn(
            # Alis pozitif, satis negatif -> net pozisyon dogrudan SUM ile cikar
            "isaretli_adet",
            F.when(F.col("islem_tipi") == "ALIS", F.col("adet")).otherwise(-F.col("adet")),
        )
        .withColumn("net_tutar", F.col("tutar") - F.col("komisyon"))
        .withColumn("islem_saati", F.hour("islem_zamani"))
    )

    # ----------------------------------------------------------------------
    #  ONBELLEKLEME DENENDI ve GERI ALINDI -- OLCUM SONUCU
    #
    #  "Bu DAG dort kez tuketiliyor (2 count + 2 write), persist edelim"
    #  diye enriched.persist(MEMORY_AND_DISK) eklendi. 20M satirda OLCULDU:
    #
    #       Orijinal (once say, sonra yaz)      : 308 saniye
    #       + persist(MEMORY_AND_DISK)          : 778 saniye  <-- 2,5x YAVAS
    #       Once yaz, sonra tablodan say (son)  : 170 saniye  <-- 1,8x HIZLI
    #
    #  NEDEN? Iki sebep:
    #    1. Spark, ayni lineage icindeki aksiyonlar arasinda SHUFFLE
    #       CIKTISINI ZATEN yeniden kullanir. Pencere fonksiyonunun
    #       shuffle'i bir kez hesaplanir; "dort kez bastan hesaplaniyor"
    #       varsayimi YANLISTI.
    #    2. Executor 3 GB; 20M zenginlestirilmis satir bellege sigmaz ve
    #       MEMORY_AND_DISK ile buyuk kismi DISKE serilestirilir. Eklenen
    #       serilestirme + disk I/O, kazanilan hesaptan pahaliya geldi.
    #
    #  DERS: "onbellek her zaman hizlandirir" bir sezgidir, olcum degil.
    #  Bu repodaki performans iddialarinin hepsi olcume dayaniyor; bu not
    #  da olcumle YANLISLANMIS bir iyilestirmenin kaydi olarak duruyor.
    #  Tekrar denemek isterseniz once executor bellegini buyutun
    #  (conf/spark/spark-defaults.conf -> spark.executor.memory).
    #
    #  Bunun yerine GERCEKTEN ise yarayan optimizasyon asagida: onceden
    #  count() almak yerine ONCE YAZIP sonra tablodan saymak. Iceberg
    #  satir sayisini metadata'dan cevaplayabildigi icin iki tam gecis
    #  ortadan kalkiyor.
    # ----------------------------------------------------------------------

    # -------- Gecerlilik ayrimi --------
    gecerlilik = (
        F.col("yatirimci_tipi").isNotNull()
        & F.col("isin_kodu").isNotNull()
        & (F.col("tutar") > 0)
        & (F.col("adet") > 0)
        & F.col("islem_tipi").isin("ALIS", "SATIS")
    )

    valid = enriched.filter(gecerlilik)
    invalid = enriched.filter(~gecerlilik).withColumn(
        "_karantina_sebebi",
        F.concat_ws(
            "; ",
            F.when(F.col("yatirimci_tipi").isNull(), F.lit("yatirimci boyutunda eslesmedi")),
            F.when(F.col("isin_kodu").isNull(), F.lit("menkul kiymet boyutunda eslesmedi")),
            F.when(F.col("tutar") <= 0, F.lit("tutar pozitif degil")),
            F.when(F.col("adet") <= 0, F.lit("adet pozitif degil")),
            F.when(~F.col("islem_tipi").isin("ALIS", "SATIS"), F.lit("gecersiz islem_tipi")),
        ),
    ).withColumn("_karantina_ts", F.current_timestamp())

    # -------- Silver tablosu --------
    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.islem (
            islem_id         BIGINT,
            yatirimci_id     BIGINT,
            menkul_id        BIGINT,
            islem_zamani     TIMESTAMP,
            islem_tarihi     DATE,
            islem_saati      INT,
            islem_tipi       STRING,
            adet             DECIMAL(18,4),
            isaretli_adet    DECIMAL(18,4),
            fiyat            DECIMAL(18,6),
            tutar            DECIMAL(20,4),
            komisyon         DECIMAL(12,4),
            net_tutar        DECIMAL(20,4),
            araci_kurum_kodu STRING,
            kanal            STRING,
            yatirimci_tipi   STRING,
            uyruk            STRING,
            il_kodu          SMALLINT,
            risk_profili     STRING,
            isin_kodu        STRING,
            kisa_kod         STRING,
            kiymet_tipi      STRING,
            pazar            STRING,
            para_birimi      STRING,
            guncelleme_ts    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (months(islem_zamani), kiymet_tipi)
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd',
            'write.target-file-size-bytes'='134217728',
            'write.delete.mode'='copy-on-write',
            'write.update.mode'='copy-on-write',
            'write.merge.mode'='copy-on-write',
            -- Bu tabloyu ClickHouse tarayacak. Sik filtrelenen sutunlari
            -- dosya ici siralamaya sokmak, Parquet row-group min/max
            -- istatistiklerini daraltir -> okuyucu bloklari atlayabilir.
            'sort-order'='islem_zamani ASC NULLS LAST, menkul_id ASC NULLS LAST'
        )
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS silver.islem_karantina (
            islem_id          BIGINT,
            yatirimci_id      BIGINT,
            menkul_id         BIGINT,
            islem_zamani      TIMESTAMP,
            tutar             DECIMAL(20,4),
            adet              DECIMAL(18,4),
            islem_tipi        STRING,
            _karantina_sebebi STRING,
            _karantina_ts     TIMESTAMP
        )
        USING iceberg
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
    """)

    # ONCE YAZ, SONRA SAY. Onceki hali 'valid.count()' ile yazimdan ONCE
    # tam bir gecis yapiyordu; ayni veri hemen ardindan yazim icin tekrar
    # okunuyordu. Yazilmis Iceberg tablosundan saymak cok daha ucuz --
    # sayim buyuk olcude manifest metadata'sindan cevaplanir.
    target_cols = [f.name for f in spark.table("silver.islem").schema.fields]
    print("[write] silver.islem yaziliyor...")
    (
        valid.select(*target_cols)
        .sortWithinPartitions("islem_zamani", "menkul_id")
        .writeTo("silver.islem")
        .overwritePartitions()
    )
    tgt_cnt = spark.table("silver.islem").count()
    print(f"[write] silver.islem <- {tgt_cnt:,} gecerli satir")

    q_cols = [f.name for f in spark.table("silver.islem_karantina").schema.fields]
    # ----------------------------------------------------------------------
    #  KARANTINA DA TAM YENILEME -- .append() DEGIL
    #
    #  Onceki hali .append() idi ve bu bir IDEMPOTENSIZLIK hatasiydi:
    #  silver.islem her calistirmada overwritePartitions() ile bastan
    #  yaziliyor, karantina ise UZERINE EKLENIYORDU. Ayni ETL'i iki kez
    #  kosturunca ayni bozuk satirlar karantinada IKI KEZ gorunuyordu.
    #
    #  Bu sadece "cirkin" degil, asagidaki mutabakat kontrolunu de BOZAR:
    #        bronze == silver + karantina
    #  Ikinci calistirmadan sonra bu esitlik tutmaz ve job, gercek bir
    #  sorun yokken "fark var" diye uyarir. Yanlis alarm ureten bir kontrol,
    #  bir sure sonra bakilmayan bir kontrol olur.
    #
    #  overwrite(lit(True)) = tum satirlari degistir. silver.islem ile ayni
    #  "tam yenileme" semantigi.
    #
    #  URETIMDE ARTIMLI YUKLEME yapiyorsaniz bunun yerine karantina
    #  tablosunu days(_karantina_ts) ile partition'layip yalnizca o gunun
    #  partition'ini overwrite edin: hem gecmis korunur hem tekrar
    #  calistirma guvenli olur.
    # ----------------------------------------------------------------------
    #  Bulgu olsa da olmasa da AYNI yazim yapiliyor: bos sonuc yazmak
    #  onceki calistirmanin kalintisini temizler. Aksi halde duzelmis veri
    #  icin eski karantina raporu gosterilmeye devam ederdi.
    #  Sayimi yazimdan SONRA aliyoruz (bkz. yukaridaki 'once yaz sonra say').
    invalid.select(*q_cols).writeTo("silver.islem_karantina").overwrite(F.lit(True))
    q_count = spark.table("silver.islem_karantina").count()
    if q_count:
        print(f"[write] silver.islem_karantina <- {q_count:,} gecersiz satir")
    else:
        print("[write] Karantinaya dusen satir yok (tablo temizlendi).")

    # -------- Mutabakat: kaynak = silver + karantina olmali --------
    # tgt_cnt yukarida yazimdan sonra zaten alindi; tekrar saymiyoruz.
    src_cnt = bronze.count()
    print(f"\n[mutabakat] bronze={src_cnt:,}  silver={tgt_cnt:,}  karantina={q_count:,}")
    if src_cnt != tgt_cnt + q_count:
        # dedup farki olabilir; bilgi amacli, bloke etmiyoruz
        print(f"[mutabakat] NOT: fark {src_cnt - tgt_cnt - q_count:,} satir "
              f"(tekrar ayiklamadan kaynaklanabilir)")

    spark.sql(f"MERGE BRANCH {branch} INTO main IN nessie")
    spark.sql(f"DROP BRANCH {branch} IN nessie")
    print("[wap] silver yayinlandi.")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

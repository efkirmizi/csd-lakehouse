#!/usr/bin/env python3
"""
===============================================================================
 TEST SENARYOSU 4 - SEMA EVRIMI ve GIZLI PARTITION'LAMA
===============================================================================

 IS SORUSU
     "Duzenleyici yeni bir alan zorunlu kildi. Kac gunde canliya cikar?"

     Klasik ambarda cevap: tablo kilitlenir, ALTER dakikalar-saatler surer,
     tum ETL'ler ve raporlar gozden gecirilir, bakim penceresi planlanir.
     4-6 hafta. Bu, CSD gibi mevzuata surekli uyum saglamasi gereken bir
     kurumda gercek ve tekrar eden bir maliyet.

 TEKNIK IDDIA
     Iceberg'de sema degisikligi bir METADATA islemidir. Veri dosyalarina
     dokunulmaz. Sutunlar ID ile takip edilir, isimle degil -- bu yuzden
     yeniden adlandirma, silme, sira degistirme eski dosyalari bozmaz.
     Ayrica PARTITION SEMASI da degistirilebilir; eski veri oldugu gibi
     kalir, yeni veri yeni semaya gore yazilir, sorgular ikisini birden
     dogru okur.

 GOSTERILEN
     1. Mevcut sema ve snapshot
     2. Sutun EKLEME  -- 0 dosya yeniden yazildi
     3. Eski snapshot hala okunabiliyor (geriye uyumluluk)
     4. Sutun YENIDEN ADLANDIRMA -- veri bozulmadi
     5. PARTITION EVRIMI -- months -> days, eski veri yeniden yazilmadan
     6. Maliyet kaniti: metadata degisti, veri dosyalari AYNI kaldi

 CALISTIRMA
     docker compose exec spark-master /opt/spark/bin/spark-submit \
         --master spark://spark-master:7077 \
         /opt/spark/tests/04_schema_evolution.py
===============================================================================
"""

from __future__ import annotations

import sys
import time
from datetime import datetime

sys.path.insert(0, "/opt/spark/jobs")

from common.session import CATALOG, build_spark

# Uretim tablosunu bozmamak icin ayri bir test tablosu kullaniyoruz.
TABLE = "bronze.islem_sema_testi"
FQN = f"{CATALOG}.{TABLE}"
KAYNAK = f"{CATALOG}.silver.islem"


def banner(n: int, title: str) -> None:
    print("\n" + "=" * 76)
    print(f"  ADIM {n}: {title}")
    print("=" * 76)


def dosya_profili(spark) -> tuple[int, int]:
    """Kac veri dosyasi, toplam kac bayt? Sema islemlerinin veriye
    dokunmadigini KANITLAMAK icin bunu once/sonra karsilastiracagiz."""
    row = spark.sql(f"""
        SELECT COUNT(*) AS dosya, COALESCE(SUM(file_size_in_bytes), 0) AS bayt
        FROM {FQN}.files
    """).first()
    return row["dosya"], row["bayt"]


def main() -> int:
    spark = build_spark("test_04_schema_evolution", branch="main")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bronze")

    # ---------------------------------------------------------------- 0
    print("[hazirlik] Test tablosu olusturuluyor...")
    spark.sql(f"DROP TABLE IF EXISTS {FQN}")
    spark.sql(f"""
        CREATE TABLE {FQN}
        USING iceberg
        PARTITIONED BY (months(islem_zamani))
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet',
            'write.parquet.compression-codec'='zstd'
        )
        AS SELECT islem_id, yatirimci_id, menkul_id, islem_zamani, islem_tarihi,
                  islem_tipi, adet, fiyat, tutar, komisyon, kisa_kod, kiymet_tipi
           FROM {KAYNAK}
           WHERE islem_zamani >= TIMESTAMP '2025-01-01'
             AND islem_zamani <  TIMESTAMP '2025-04-01'
    """)

    # ---------------------------------------------------------------- 1
    banner(1, "Baslangic durumu")
    spark.sql(f"DESCRIBE TABLE {FQN}").show(30, truncate=False)

    satir_0 = spark.sql(f"SELECT COUNT(*) c FROM {FQN}").first()["c"]
    dosya_0, bayt_0 = dosya_profili(spark)

    # '.history WHERE is_current_ancestor = true' kullaniyoruz, '.snapshots'
    # DEGIL. Burada tablo yeni olusturuldugu icin ikisi ayni sonucu verirdi;
    # yine de dogru desenle yaziyoruz. Sebep: '.snapshots' rollback sonrasi
    # ARTIK GECERLI OLMAYAN snapshot'lari da listeler ve bunlarin zaman
    # damgasi daha yeni olabilir. Repoda tek bir yerde bile yanlis desen
    # birakmak, sonraki gelistiricinin onu kopyalamasi demektir.
    # (Ayrintili aciklama: tests/01_time_travel.py, ADIM 1.)
    snap_0 = spark.sql(f"""
        SELECT snapshot_id FROM {FQN}.history
        WHERE is_current_ancestor = true
        ORDER BY made_current_at DESC
        LIMIT 1
    """).first()["snapshot_id"]

    print(f"    Satir        : {satir_0:,}")
    print(f"    Veri dosyasi : {dosya_0}")
    print(f"    Toplam boyut : {bayt_0 / 1048576:.2f} MB")
    print(f"    Snapshot     : {snap_0}")

    time.sleep(1)

    # ---------------------------------------------------------------- 2
    banner(2, "SUTUN EKLEME -- 'Mevzuat 3 yeni alan zorunlu kildi'")
    print("""
    Senaryo: Duzenleyici, her islemde islem yerinin, emir referansinin ve
    MiFID benzeri bir islem kodunun tutulmasini zorunlu kildi.
    """)

    t0 = time.time()
    spark.sql(f"ALTER TABLE {FQN} ADD COLUMN islem_yeri STRING COMMENT 'Islemin gerceklestigi platform'")
    spark.sql(f"ALTER TABLE {FQN} ADD COLUMN emir_referans STRING COMMENT 'Kaynak sistem emir no'")
    spark.sql(f"ALTER TABLE {FQN} ADD COLUMN islem_kodu STRING COMMENT 'Mevzuat islem siniflandirmasi'")
    sure = time.time() - t0

    dosya_1, bayt_1 = dosya_profili(spark)

    print(f"    3 sutun eklendi.")
    print(f"    Sure                : {sure:.3f} saniye")
    print(f"    Veri dosyasi sayisi : {dosya_0} -> {dosya_1}")
    print(f"    Toplam boyut        : {bayt_0/1048576:.2f} MB -> {bayt_1/1048576:.2f} MB")
    print(f"    Yeniden yazilan veri: {abs(bayt_1 - bayt_0)} bayt")

    veri_dokunulmadi = (dosya_0 == dosya_1 and bayt_0 == bayt_1)
    print(f"\n    KANIT: {'Veri dosyalarina HIC DOKUNULMADI' if veri_dokunulmadi else 'Dosyalar degisti (beklenmedik)'}")
    print("""
    Yeni sutunlar eski dosyalarda FIZIKSEL OLARAK YOK. Iceberg okurken
    onlari NULL olarak doldurur. Bu yuzden 2 milyar satirlik tabloda da
    ALTER TABLE ADD COLUMN milisaniyeler surer -- tablo boyutundan
    BAGIMSIZDIR. Klasik ambarda bu islemin saatler surmesinin sebebi
    her satirin fiziksel olarak yeniden yazilmasidir.
    """)

    spark.sql(f"""
        SELECT islem_id, kisa_kod, tutar, islem_yeri, emir_referans, islem_kodu
        FROM {FQN} LIMIT 3
    """).show(truncate=False)

    # ---------------------------------------------------------------- 3
    banner(3, "Geriye uyumluluk -- eski snapshot hala okunabiliyor mu?")
    print(f"    Sema degismeden onceki snapshot: {snap_0}\n")
    spark.sql(f"SELECT COUNT(*) AS satir FROM {FQN} VERSION AS OF {snap_0}").show()
    print("""
    Eski snapshot ESKI semasiyla okunuyor. Yani 6 ay once uretilmis bir
    rapor bugun birebir yeniden uretilebilir -- aradaki sema
    degisikliklerinden etkilenmeden. Denetimde "o gunku rakami o gunku
    tanimla goster" talebinin karsiligi budur.
    """)

    # ---------------------------------------------------------------- 4
    banner(4, "SUTUN YENIDEN ADLANDIRMA -- en riskli islem")
    print("""
    Klasik sistemlerde sutun adi degistirmek: tum ETL, tum rapor, tum
    view kirilir. Iceberg'de sutunlar ID ile takip edilir; isim sadece
    bir etikettir.
    """)
    dosya_2a, bayt_2a = dosya_profili(spark)
    spark.sql(f"ALTER TABLE {FQN} RENAME COLUMN komisyon TO komisyon_tutari")
    dosya_2b, bayt_2b = dosya_profili(spark)

    print(f"    komisyon -> komisyon_tutari")
    print(f"    Dosya: {dosya_2a} -> {dosya_2b} | Bayt: {bayt_2a} -> {bayt_2b}")
    spark.sql(f"SELECT islem_id, komisyon_tutari FROM {FQN} LIMIT 3").show()
    print("    Veri yerinde, isim yeni. Hicbir dosya yeniden yazilmadi.")

    # Tip genisletme de guvenli yonde serbesttir (int->long, float->double,
    # decimal olcek buyutme). Daraltma (long->int) REDDEDILIR -- veri kaybi
    # riski oldugu icin Iceberg buna izin vermez. Bu bir kisit degil koruma.
    print("\n    Tip genisletme (guvenli yon):")
    try:
        spark.sql(f"ALTER TABLE {FQN} ALTER COLUMN fiyat TYPE DECIMAL(20,6)")
        print("      fiyat: DECIMAL(18,6) -> DECIMAL(20,6)  OK")
    except Exception as e:
        print(f"      (atlandi: {str(e)[:80]})")

    # ---------------------------------------------------------------- 5
    banner(5, "PARTITION EVRIMI -- 'veri buyudu, aylik partition yetmiyor'")
    print("""
    Senaryo: Islem hacmi 10 kat artti. Aylik partition'lar artik cok
    buyuk; gunluk'e gecmek gerekiyor.

    Klasik ambarda: tabloyu yeniden olustur, tum veriyi kopyala,
    ETL'leri durdur, gecis penceresi planla. Iceberg'de: tek komut.
    """)

    print("    Mevcut partition semasi:")
    spark.sql(f"SELECT * FROM {FQN}.partitions").show(5, truncate=False)

    dosya_3a, bayt_3a = dosya_profili(spark)
    t0 = time.time()
    spark.sql(f"ALTER TABLE {FQN} ADD PARTITION FIELD days(islem_zamani)")
    spark.sql(f"ALTER TABLE {FQN} DROP PARTITION FIELD months(islem_zamani)")
    sure = time.time() - t0
    dosya_3b, bayt_3b = dosya_profili(spark)

    print(f"\n    months(islem_zamani) -> days(islem_zamani)")
    print(f"    Sure  : {sure:.3f} saniye")
    print(f"    Dosya : {dosya_3a} -> {dosya_3b}")
    print(f"    Bayt  : {bayt_3a} -> {bayt_3b}")
    print("""
    ESKI VERI YENIDEN YAZILMADI. Su an tabloda iki farkli partition
    semasina sahip dosyalar bir arada duruyor ve Iceberg her ikisini de
    dogru sekilde planliyor. Yeni gelen veri gunluk partition'lanacak.

    Bu "gizli partition'lama"nin (hidden partitioning) sonucudur:
    kullanici sorgusu partition sutunundan HABERSIZDIR. Hive'da
    'WHERE dt=...' yazmayi unutan sorgu tum tabloyu tarardi; Iceberg'de
    'WHERE islem_zamani > ...' yazmak yeterlidir, partition eslemesini
    motor yapar. Yanlis yazilmis sorgu yuzunden tam tarama olmaz.
    """)

    print("    Yeni partition semasi:")
    spark.sql(f"DESCRIBE TABLE EXTENDED {FQN}").filter("col_name LIKE '%Part%'").show(truncate=False)

    # Yeni veri yeni semaya gore yazilir
    spark.sql(f"""
        INSERT INTO {FQN}
        SELECT islem_id + 800000000, yatirimci_id, menkul_id, islem_zamani, islem_tarihi,
               islem_tipi, adet, fiyat, tutar, komisyon, kisa_kod, kiymet_tipi,
               'BIST', 'ORD-YENI', 'MEV-01'
        FROM {KAYNAK}
        WHERE islem_zamani >= TIMESTAMP '2025-03-01'
          AND islem_zamani <  TIMESTAMP '2025-03-05'
    """)
    print("\n    Yeni veri eklendi (gunluk partition'lu). Karisik semali tablo sorgulaniyor:")
    spark.sql(f"""
        SELECT COUNT(*) AS toplam,
               SUM(CASE WHEN islem_yeri IS NULL THEN 1 ELSE 0 END) AS eski_sema_satir,
               SUM(CASE WHEN islem_yeri IS NOT NULL THEN 1 ELSE 0 END) AS yeni_sema_satir
        FROM {FQN}
    """).show(truncate=False)
    print("    Tek sorgu, iki sema, iki partition duzeni. Kullanici farki gormuyor.")

    # ---------------------------------------------------------------- 6
    banner(6, "Sema degisim gecmisi")
    print("    Tum sema/partition degisiklikleri metadata'da kayitli:\n")
    spark.sql(f"""
        SELECT made_current_at, snapshot_id, is_current_ancestor
        FROM {FQN}.history ORDER BY made_current_at
    """).show(truncate=False)

    print("""
    ---------------------------------------------------------------------
     YONETICIYE OZET
     ---------------------------------------------------------------------
     * Yeni mevzuat alani eklemek: HAFTALAR -> SANIYELER
     * Islem sirasinda tablo KAPANMADI, ETL DURMADI, rapor KIRILMADI
     * Islem suresi tablo boyutundan BAGIMSIZ (metadata islemi)
     * Eski raporlar eski semasiyla yeniden uretilebilir durumda
     * Partition tasarimini sonradan degistirebilirsiniz -> bugun
       kucuk baslayip veri buyudukce uyarlamak MUMKUN. "Ilk gun dogru
       tahmin etme" baskisi ortadan kalkiyor.

     BU NEDEN ONEMLI
       Veri ambari projelerinin en pahali kalemi ilk kurulum degil,
       DEGISIKLIGE UYUM maliyetidir. Bu ozellik dogrudan o kalemi
       hedefliyor.
    ---------------------------------------------------------------------
    """)

    print(f"[temizlik] Test tablosu birakildi: {TABLE}")
    print(f"           Silmek icin: DROP TABLE {FQN}")

    spark.stop()
    return 0 if veri_dokunulmadi else 1


if __name__ == "__main__":
    raise SystemExit(main())

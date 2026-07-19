#!/usr/bin/env python3
"""
===============================================================================
 TEST SENARYOSU 1 - ZAMANDA YOLCULUK ve OPERATOR HATASINDAN DONUS
===============================================================================

 IS SORUSU
     "31 Mart kapanisi itibariyle bu yatirimcinin pozisyonu tam olarak neydi?"
     Duzenleyici denetimde sorulan sorunun birebir kendisi. Klasik veri
     ambarinda cevap: "Bilmiyoruz, tablo o gunden beri guncellendi."

 TEKNIK IDDIA
     Iceberg her yazimi bir SNAPSHOT olarak saklar. Gecmis, uzerine
     yazilmaz -- eklenir. Herhangi bir ana donmek TEK SATIR SQL'dir ve
     veri kopyalamaz, yedekten donmez, dakikalar surmez.

 GOSTERILEN
     1. Tablonun anlik hali
     2. KAZA: yanlis bir UPDATE tum tabloyu bozuyor  (gercek bir olay)
     3. Hasar tespiti
     4. Zamanda geri gidip eski hali OKUMA (tablo hala bozuk)
     5. ROLLBACK ile tabloyu geri alma  -- saniyeler icinde
     6. Kanit: bozuk snapshot hala gecmiste duruyor (denetim izi silinmiyor)

 CALISTIRMA
     docker compose exec spark-master /opt/spark/bin/spark-submit \
         --master spark://spark-master:7077 \
         /opt/spark/tests/01_time_travel.py
===============================================================================
"""

from __future__ import annotations

import sys
import time

sys.path.insert(0, "/opt/spark/jobs")

from common.session import CATALOG, build_spark

# ---------------------------------------------------------------------------
#  KENDI TABLOSUNDA CALISIR -- silver.islem'e DOKUNMAZ.
#
#  Bu senaryo tabloyu BILEREK bozuyor. Ilk versiyonu dogrudan silver.islem
#  uzerinde calisiyordu ve su hatayi yasadik: script rollback adimindan
#  ONCE coktu, silver.islem x100 bozuk kaldi. Sonraki calistirma bozuk
#  hali "saglam referans" sanip x100 daha bozdu -- her calistirmada
#  degerler kaydi ve kimse fark etmedi (test yine "BASARILI" diyordu).
#
#  Ders: yikici bir demo, uretim tablosunda calismamali ve idempotent
#  olmali. Her calistirmada kaynaktan taze bir kopya uretiyoruz.
# ---------------------------------------------------------------------------
KAYNAK = "silver.islem"
TABLE = "silver.islem_timetravel_demo"
FQN = f"{CATALOG}.{TABLE}"


def banner(n: int, title: str) -> None:
    print("\n" + "=" * 76)
    print(f"  ADIM {n}: {title}")
    print("=" * 76)


def snapshots(spark):
    return spark.sql(f"""
        SELECT snapshot_id, committed_at, operation,
               summary['added-records']   AS eklenen,
               summary['deleted-records'] AS silinen
        FROM {FQN}.snapshots
        ORDER BY committed_at
    """)


def main() -> int:
    spark = build_spark("test_01_time_travel", branch="main")

    # ---------------------------------------------------------------- 0
    print(f"[hazirlik] {KAYNAK} -> {TABLE} taze kopya olusturuluyor...")
    print(f"[hazirlik] (uretim tablosu {KAYNAK} bu senaryoda DEGISMEZ)")
    spark.sql(f"DROP TABLE IF EXISTS {FQN}")
    spark.sql(f"""
        CREATE TABLE {FQN}
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
    banner(1, "Baslangic durumu")
    spark.sql(f"""
        SELECT COUNT(*) AS satir,
               ROUND(SUM(tutar), 2) AS toplam_hacim,
               ROUND(AVG(fiyat), 4) AS ort_fiyat
        FROM {FQN}
    """).show(truncate=False)

    print("Mevcut snapshot gecmisi:")
    snapshots(spark).show(truncate=False)

    # ---- TABLONUN GUNCEL SNAPSHOT'INI DOGRU SEKILDE BULMAK ----
    # TUZAK: '.snapshots' tablosundan 'ORDER BY committed_at DESC LIMIT 1'
    # cekmek YANLISTIR. '.snapshots' tablodaki TUM snapshot'lari listeler --
    # bir rollback yapildiysa, artik gecerli olmayan (ata zincirinde
    # bulunmayan) snapshot'lar da listede kalir ve bunlarin zaman damgasi
    # daha YENI olabilir. O snapshot'a rollback denemesi su hatayi verir:
    #     ValidationException: Cannot roll back to snapshot,
    #     not an ancestor of the current state
    #
    # DOGRUSU: '.history' tablosunda is_current_ancestor = true olanlar,
    # yani tablonun su anki halini olusturan zincir.
    saglam_snapshot = spark.sql(f"""
        SELECT snapshot_id FROM {FQN}.history
        WHERE is_current_ancestor = true
        ORDER BY made_current_at DESC
        LIMIT 1
    """).first()["snapshot_id"]

    saglam_toplam = spark.sql(f"SELECT ROUND(SUM(tutar),2) t FROM {FQN}").first()["t"]
    saglam_satir = spark.sql(f"SELECT COUNT(*) c FROM {FQN}").first()["c"]

    print(f"\n>> Saglam referans noktasi kaydedildi:")
    print(f"   snapshot_id = {saglam_snapshot}")
    print(f"   satir       = {saglam_satir:,}")
    print(f"   toplam      = {saglam_toplam:,}")

    time.sleep(1)  # snapshot zaman damgalari ayrissin

    # ---------------------------------------------------------------- 2
    banner(2, "KAZA -- hatali bir duzeltme betigi uretimde calisiyor")
    print("""
    Gercek hayat senaryosu: Bir analist, komisyon hesabinda duzeltme
    yapmak icin script yaziyor. WHERE kosulunu yazmayi UNUTUYOR.
    Sonuc: tablonun TAMAMI bozuluyor.

    Calisan komut:
        UPDATE silver.islem SET tutar = tutar * 100
    """)
    spark.sql(f"UPDATE {FQN} SET tutar = tutar * 100")
    print("    ... komut calisti. Hasar olustu.")

    # ---------------------------------------------------------------- 3
    banner(3, "Hasar tespiti")
    bozuk = spark.sql(f"""
        SELECT COUNT(*) AS satir, ROUND(SUM(tutar), 2) AS toplam_hacim
        FROM {FQN}
    """).first()
    # NOT: Spark DECIMAL sutunlari Python'a decimal.Decimal olarak gelir.
    # Decimal / float bir TypeError'dir -- ikisini de acikca float'a cevirin.
    bozuk_toplam = float(bozuk["toplam_hacim"])
    print(f"    Bozuk toplam : {bozuk_toplam:,.2f}")
    print(f"    Saglam toplam: {float(saglam_toplam):,.2f}")
    print(f"    Sapma        : {bozuk_toplam / float(saglam_toplam):.0f} kat")
    print("\n    Klasik ambar dunyasinda bu noktada yapilacaklar:")
    print("      - Geceki yedegi bul, restore talebi ac       (saatler)")
    print("      - Restore suresince tabloyu kapat            (kesinti)")
    print("      - Yedekten bu yana gelen veriyi tekrar yukle (risk)")

    # ---------------------------------------------------------------- 4
    banner(4, "Zamanda yolculuk -- ESKI HALI OKUMA (tablo hala bozuk)")
    print("""
    Onemli ayrim: bu adimda HICBIR SEY DEGISTIRMIYORUZ. Tablo hala bozuk.
    Sadece gecmisteki bir snapshot'i okuyoruz. Denetci "31 Mart'ta ne
    vardi?" diye sordugunda tabloyu geri almadan cevap verebilirsiniz.
    """)
    print(f"    SELECT ... FROM {TABLE} VERSION AS OF {saglam_snapshot}")
    spark.sql(f"""
        SELECT COUNT(*) AS satir, ROUND(SUM(tutar), 2) AS toplam_hacim
        FROM {FQN} VERSION AS OF {saglam_snapshot}
    """).show(truncate=False)

    print("    Ayni sey zaman damgasiyla da yapilabilir:")
    print(f"      SELECT * FROM {TABLE} TIMESTAMP AS OF '2026-03-31 18:00:00'")

    print("\n    Yan yana karsilastirma (bozuk vs saglam):")
    spark.sql(f"""
        SELECT 'SIMDIKI (bozuk)' AS durum, ROUND(SUM(tutar),2) AS toplam FROM {FQN}
        UNION ALL
        SELECT 'SNAPSHOT (saglam)', ROUND(SUM(tutar),2) FROM {FQN} VERSION AS OF {saglam_snapshot}
    """).show(truncate=False)

    # ---------------------------------------------------------------- 5
    banner(5, "ROLLBACK -- tabloyu geri alma")
    t0 = time.time()
    spark.sql(f"""
        CALL {CATALOG}.system.rollback_to_snapshot('{TABLE}', {saglam_snapshot})
    """).show(truncate=False)
    sure = time.time() - t0

    duzeltilmis = spark.sql(f"""
        SELECT COUNT(*) AS satir, ROUND(SUM(tutar), 2) AS toplam_hacim FROM {FQN}
    """).first()

    print(f"\n    Rollback suresi : {sure:.2f} saniye")
    print(f"    Satir           : {duzeltilmis['satir']:,}  (beklenen {saglam_satir:,})")
    print(f"    Toplam          : {float(duzeltilmis['toplam_hacim']):,.2f}"
          f"  (beklenen {float(saglam_toplam):,.2f})")

    ok = (duzeltilmis["satir"] == saglam_satir
          and abs(float(duzeltilmis["toplam_hacim"]) - float(saglam_toplam)) < 1)
    print(f"\n    SONUC: {'BASARILI - veri birebir geri geldi' if ok else 'BASARISIZ'}")

    print("""
    NEDEN BU KADAR HIZLI?
      Rollback veri KOPYALAMAZ. Iceberg sadece tablonun "su anki
      snapshot" isaretcisini eski snapshot'a geri cevirir. Eski veri
      dosyalari zaten diskte duruyordu -- UPDATE onlari silmemis, yeni
      dosyalar yazmisti. Yapilan is bir metadata commit'idir.

      DIKKAT -- "SURE TABLO BOYUTUNDAN BAGIMSIZDIR" DEMEYIN.
      Bu dosyada eskiden "2M satirda da 2 milyar satirda da sure aynidir"
      yaziyordu. OLCTUK, TAM DOGRU DEGIL:

            2M satir  ->  0,32 saniye
           20M satir  ->  5,49 saniye

      Veri kopyalanmiyor, ama metadata isi (manifest listeleri, partition
      sayisi) tabloyla birlikte buyuyor. Yani rollback O(veri) degil ama
      O(1) de degil -- O(metadata).

      DOGRU CERCEVE: karsilastirma "0,3 sn mi 5 sn mi" degil.
      Karsilastirma sudur:
          yedekten geri yukleme  -> SAATLER + kesinti + veri kaybi riski
          Iceberg rollback       -> SANIYELER, kesintisiz, birebir
      Buyuklukler arasi fark burada; saniyeler mertebesindeki degisim
      bu argumani zayiflatmaz. Ama olcmediginiz bir seyi "sabit" diye
      sunarsaniz, biri buyuk tabloda deneyip farki gorur ve tum
      anlattiklariniz supheli hale gelir.
    """)

    # ---------------------------------------------------------------- 6
    banner(6, "Denetim izi -- bozuk snapshot SILINMEDI")
    print("""
    Kritik nokta: rollback, hatayi gecmisten SILMEZ. Bozuk snapshot
    hala gecmiste duruyor ve okunabilir. Duzenlemeye tabi bir kurumda
    istenen tam olarak budur: "hata yapildi, su an tespit edildi, su an
    duzeltildi" zincirinin tamami kanitlanabilir olmali. Gecmisi silmek
    denetim acisindan bir ozellik degil, kusurdur.
    """)
    snapshots(spark).show(truncate=False)

    print("""
    ---------------------------------------------------------------------
     YONETICIYE OZET
     ---------------------------------------------------------------------
     * Operator hatasindan donus: SAATLER -> SANIYELER
     * Geri donus sirasinda sistem KAPANMADI
     * Yedekten restore GEREKMEDI (yedek zaten tablonun kendisinde)
     * "X tarihinde ne vardi?" sorusu tek SQL ile cevaplanabilir
     * Hatanin kendisi denetim izinde KORUNDU
     * Maliyet: ek altyapi yok -- Iceberg'in dogal davranisi

     TEK KISIT: time travel penceresi expire_snapshots ile sinirlidir
     (jobs/99_maintenance.py). Bu pencere, CSD'nin tabi oldugu saklama
     mevzuatiyla uyumlu secilmelidir.
    ---------------------------------------------------------------------
    """)

    spark.stop()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

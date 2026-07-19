"""
===============================================================================
 CSD Lakehouse — uçtan uca ETL DAG'i
===============================================================================

 NEDEN ORKESTRASYON?
     Bu repodaki job'lar tek tek doğru çalışıyor. Üretimde eksik olan şey
     onları BİRBİRİNE BAĞLAYAN katman:

       * Bağımlılık zorlaması -- 02, 01 bitmeden başlamamalı. Bugün
         'run.ps1 pipeline' bunu sırayla yapıyor ama bir insan orada
         durup bakmak zorunda.
       * Yeniden deneme      -- geçici bir S3/ağ hatası tüm gecelik yükü
         düşürmemeli.
       * Sesli başarısızlık  -- iş başarısız olduğunda BİLİNMELİ. Sessizce
         çalışmayan bir bakım/ETL işi, bu projenin docs/01-mimari.md bölüm 6'da
         anlattığı "6. ayda çöken lakehouse" senaryosunun ta kendisidir.
       * Veri kalitesi kapısı -- pipeline "yeşil" bitmesi yetmez; ürettiği
         rakam KAYNAKLA TUTMALI. Bu DAG mutabakatla biter (aşağıya bakın).

 TASARIM KARARI — Spark job'ları NEREDE çalışıyor?
     DAG, Spark'ı kendi içinde çalıştırmıyor. Mevcut Spark cluster'ına
     'docker exec' ile iş gönderiyor:

         docker exec lh-spark-master spark-submit ...

     Gerekçe: Airflow'u bir Spark client'ına dönüştürmek (jar'lar, java,
     eşleşen sürümler) hem imajı şişirir hem SÜRÜM İKİLİĞİ yaratır --
     Airflow'daki Spark ile cluster'daki Spark birbirinden kayabilir.
     İş, zaten doğru yapılandırılmış olan cluster'da çalışsın; Airflow
     sadece NE ZAMAN ve HANGİ SIRAYLA sorusunu cevaplasın.

     Bedeli: Airflow container'ının docker soketine erişmesi gerekiyor.
     Bu bir GÜVENLİK TAVİZİDİR (soket erişimi = host'ta root'a yakın yetki)
     ve yalnızca bu lokal referans kurulumu için kabul edilebilir.
     Üretimde: KubernetesPodOperator veya Spark on K8s / YARN kullanın,
     docker soketi paylaşmayın.

 CALISTIRMA
     docker compose --profile orchestration up -d --build
     -> http://localhost:8090   (kullanıcı: admin, parola: aşağıdaki nota bakın)
===============================================================================
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ---------------------------------------------------------------------------
#  Kimlik bilgileri ORTAM DEĞİŞKENİNDEN okunur, DAG'a gömülmez.
#  compose bunları airflow servisine .env üzerinden geçiriyor.
#  Üretimde: Airflow Connections / Variables veya bir secret backend.
# ---------------------------------------------------------------------------
CH_USER = os.environ.get("CH_USER", "analytics")
CH_PASS = os.environ.get("CH_PASSWORD", "analytics_pass")
PG_USER = os.environ.get("PG_USER", "csd")
PG_PASS = os.environ.get("PG_PASSWORD", "csd_pass")
PG_DB = os.environ.get("PG_DB", "csd_oltp")

SPARK = "docker exec lh-spark-master /opt/spark/bin/spark-submit " \
        "--master spark://spark-master:7077 --deploy-mode client"
CH = f"docker exec lh-clickhouse clickhouse-client --user {CH_USER} --password {CH_PASS}"

default_args = {
    "owner": "veri-muhendisligi",
    # Geçici hatalar (S3 zaman aşımı, container yeniden başlatma) tüm yükü
    # düşürmemeli. Ama SONSUZ deneme de yanlış: bozuk veri veya kod hatası
    # tekrar denemekle düzelmez, sadece keşfi geciktirir.
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "depends_on_past": False,
    # Bir iş asılı kalırsa DAG'ı süresiz meşgul etmesin.
    "execution_timeout": timedelta(minutes=60),
}

with DAG(
    dag_id="csd_lakehouse_pipeline",
    description="OLTP -> Bronze -> Silver -> Gold -> ClickHouse (mutabakat kapılı)",
    default_args=default_args,
    # Zamanlanmış çalıştırma BİLEREK kapalı: bu bir referans kurulumu ve
    # 20M satırlık tam yükleme ~20 dk sürüyor. Üretimde '0 2 * * *' gibi
    # bir cron ve artımlı (--since) yükleme kullanın.
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,          # aynı anda iki tam yükleme = çakışan yazım
    tags=["csd", "lakehouse", "etl"],
    doc_md=__doc__,
) as dag:

    # -- 1) OLTP -> Bronze (WAP: branch -> doğrula -> merge) ----------------
    bronze = BashOperator(
        task_id="01_oltp_to_bronze",
        bash_command=f"{SPARK} /opt/spark/jobs/01_oltp_to_bronze.py --full",
        doc_md="Kaynak Postgres'ten JDBC paralel okuma. İzole Nessie branch'ine "
               "yazar, kalite kontrolleri branch üzerinde çalışır, geçerse "
               "main'e tek atomik commit ile merge edilir.",
    )

    # -- 2) Bronze -> Silver (temizleme + karantina) ------------------------
    silver = BashOperator(
        task_id="02_bronze_to_silver",
        bash_command=f"{SPARK} /opt/spark/jobs/02_bronze_to_silver.py",
        doc_md="Boyut birleştirme, tekrar ayıklama, türetilmiş alanlar. "
               "Geçersiz satırlar düşürülmez, karantinaya alınır.",
    )

    # -- 3) Silver -> Gold (ön-toplama) -------------------------------------
    gold = BashOperator(
        task_id="03_silver_to_gold",
        bash_command=f"{SPARK} /opt/spark/jobs/03_silver_to_gold.py",
        doc_md="İş sorusuna göre ön-toplanmış servis tabloları. "
               "Sub-second hedefine giden yolun büyük kısmı burada kazanılır.",
    )

    # -- 4) ClickHouse federe görünümlerini yenile --------------------------
    # Iceberg tablo yolları UUID'li; tablo yeniden oluşursa UUID DEĞİŞİR ve
    # eski görünüm sessizce ESKİ dizini okumaya devam eder. Bu yüzden
    # görünüm üretimi pipeline'ın PARÇASI olmalı, ayrı bir hatırlanacak adım değil.
    refresh_views = BashOperator(
        task_id="04_refresh_ch_views",
        bash_command=(
            f"{SPARK} /opt/spark/jobs/90_generate_ch_views.py && "
            f"docker exec lh-spark-master cat /opt/spark/out/lake_views.sql | "
            f"docker exec -i lh-clickhouse clickhouse-client "
            f"--user {CH_USER} --password {CH_PASS} --multiquery"
        ),
        doc_md="Iceberg yollarını katalogdan yeniden çözer ve lake.* "
               "görünümlerini ClickHouse'ta yeniden oluşturur.",
    )

    # -- 5) Sıcak katmanı materyalize et ------------------------------------
    materialize = BashOperator(
        task_id="05_materialize_mergetree",
        bash_command=f"{CH} --multiquery --queries-file /sql/03_materialize_mergetree.sql",
        doc_md="Gold ve silver'ı ClickHouse MergeTree'ye kopyalar. Dashboard'lar "
               "buradan beslenir (federe görünümler branch izolasyonunu görmez -- "
               "bkz. tests/06_clickhouse_branch_izolasyonu.py).",
    )

    # -- 6) MUTABAKAT KAPISI -- DAG burada başarısız OLABİLMELİ -------------
    #
    #  Bu görevin varlık sebebi: bir pipeline'ın "yeşil" bitmesi, ÜRETTİĞİ
    #  RAKAMIN DOĞRU olduğu anlamına gelmez. Bu proje boyunca tam da bu
    #  sınıftan bir hata yaşandı: gold tablosu net-sıfır pozisyonları
    #  kaynaktan farklı ele alıyordu; 2M satırda tesadüfen tutuyor, 20M'de
    #  ayrışıyordu. Hiçbir job hata vermemişti.
    #
    #  throwIf() mutabakat bozulursa sorguyu HATA ile bitirir -> görev
    #  kırmızı olur -> haber alınır. "Sessizce yanlış rakam üretmek,
    #  hiç rakam üretmemekten kötüdür."
    reconcile_gate = BashOperator(
        task_id="06_mutabakat_kapisi",
        bash_command=(
            f'{CH} --query "'
            "SELECT throwIf("
            "  countIf(abs(b.nominal_adet - g.net_adet) > 0.01) > 0,"
            "  'MUTABAKAT BOZUK: gold.yatirimci_pozisyon kaynak bakiye ile tutmuyor'"
            ") AS kapi "
            # postgresql() imza sirasi: (host:port, veritabani, tablo,
            # KULLANICI, PAROLA, SEMA). Sema'yi kullanici konumuna yazmak
            # tum argumanlari kaydirir ve "password authentication failed"
            # verir -- parola olarak kullanici adi gonderilmis olur.
            # (Bu tuzaga dustuk; testte yakalandi.)
            f"FROM postgresql('postgres:5432','{PG_DB}','bakiye','{PG_USER}','{PG_PASS}','csd') AS b "
            "INNER JOIN lake.gold_yatirimci_pozisyon AS g "
            "  ON b.yatirimci_id = g.yatirimci_id AND b.menkul_id = g.menkul_id"
            '"'
        ),
        # Mutabakat hatası GEÇİCİ DEĞİLDİR -- tekrar denemek düzeltmez,
        # sadece gerçeği geciktirir. Bu görevde retry YOK.
        retries=0,
        doc_md="Lakehouse'un ürettiği pozisyonları kaynak sistemin kendi "
               "kaydıyla karşılaştırır. Fark varsa DAG KIRMIZI olur.",
    )

    bronze >> silver >> gold >> refresh_views >> materialize >> reconcile_gate

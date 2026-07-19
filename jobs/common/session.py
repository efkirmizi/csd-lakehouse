"""
Nessie + Iceberg + MinIO uclusuyle konusan SparkSession fabrikasi.

Tum job'lar SparkSession'i buradan alir. Konfigurasyonun tek bir yerde
toplanmasinin sebebi sadece tekrari onlemek degil: katalog ayarlarinda
tek bir yazim hatasi (ornegin io-impl unutulmasi) Spark'i sessizce
LOKAL DISKE yazmaya dusurur. Job basina kopyalanan config bunu er ya da
gec yasatir; tek kaynak yasatmaz.
"""

from __future__ import annotations

import os
import sys
from typing import Mapping

from pyspark.sql import SparkSession


# Nessie SQL eklentileri CREATE BRANCH / MERGE BRANCH gramerini acar.
# Iceberg eklentileri MERGE INTO, CALL ..., time-travel sozdizimini acar.
_EXTENSIONS = ",".join(
    [
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    ]
)

CATALOG = "nessie"


def _env(key: str, default: str | None = None) -> str:
    val = os.environ.get(key, default)
    if val is None:
        raise RuntimeError(
            f"Zorunlu ortam degiskeni tanimli degil: {key}. "
            f"Job'u 'docker compose exec spark-master ...' ile calistirdiginizdan "
            f"emin olun; degiskenler compose tarafindan enjekte ediliyor."
        )
    return val


def build_spark(
    app_name: str,
    branch: str = "main",
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    """
    Args:
        app_name: Spark UI'da gorunecek isim.
        branch:   Baglanilacak Nessie referansi (branch veya tag).
                  ETL job'lari main'e degil, izole bir branch'e yazmali.
        extra_conf: Job'a ozel ek ayarlar.
    """
    nessie_uri = _env("NESSIE_URI", "http://nessie:19120/api/v2")
    warehouse = _env("WAREHOUSE", "s3://lakehouse/warehouse")
    s3_endpoint = _env("S3_ENDPOINT", "http://minio:9000")
    access_key = _env("AWS_ACCESS_KEY_ID")
    secret_key = _env("AWS_SECRET_ACCESS_KEY")
    region = _env("AWS_REGION", "us-east-1")

    conf: dict[str, str] = {
        # ---- SQL eklentileri ----
        "spark.sql.extensions": _EXTENSIONS,

        # ---- Nessie katalogu ----
        # SparkCatalog + catalog-impl=NessieCatalog: Iceberg'e "tablo
        # metadata pointer'ini Nessie'den sor" demenin yolu.
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.catalog-impl": "org.apache.iceberg.nessie.NessieCatalog",
        f"spark.sql.catalog.{CATALOG}.uri": nessie_uri,
        f"spark.sql.catalog.{CATALOG}.ref": branch,
        f"spark.sql.catalog.{CATALOG}.authentication.type": "NONE",
        f"spark.sql.catalog.{CATALOG}.warehouse": warehouse,

        # ZORUNLU. Atlanirsa job su hatayla duser:
        #   NessieApiCompatibilityException: API version mismatch,
        #   check URI prefix (expected: 1, actual: 2)
        # Nessie client'i VARSAYILAN OLARAK v1 konusur; URI'de /api/v2
        # yazmak client'i degistirmez, sadece sunucu ucunu degistirir.
        # Ikisi birlikte ayarlanmali.
        f"spark.sql.catalog.{CATALOG}.client-api-version": "2",

        # ---- Dosya IO: S3FileIO (Hadoop S3A DEGIL) ----
        # S3FileIO dogrudan AWS SDK v2 kullanir; S3A'nin dizin/rename
        # taklidi katmanini atlar. Iceberg zaten rename'e ihtiyac duymaz.
        # Olcumlerde metadata agirlikli islerde belirgin fark yaratir.
        f"spark.sql.catalog.{CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        f"spark.sql.catalog.{CATALOG}.s3.endpoint": s3_endpoint,
        f"spark.sql.catalog.{CATALOG}.s3.path-style-access": "true",
        f"spark.sql.catalog.{CATALOG}.s3.access-key-id": access_key,
        f"spark.sql.catalog.{CATALOG}.s3.secret-access-key": secret_key,
        f"spark.sql.catalog.{CATALOG}.client.region": region,

        # nessie katalogunu varsayilan yap -> "SELECT * FROM bronze.islem"
        # yazabilmek icin (nessie.bronze.islem demek zorunda kalmadan)
        "spark.sql.defaultCatalog": CATALOG,

        # ---- KATALOG METADATA CACHE'I KAPALI ----
        # Iceberg varsayilan olarak tablo metadata'sini (snapshot isaretcisini)
        # oturum icinde CACHE'LER. Bu, "oku -> yaz -> tekrar oku" yapan her
        # kodda BAYAT VERI okunmasina yol acar: ilk okuma metadata'yi
        # cache'ler, araya giren yazim cache'i gecersiz kilmaz, ikinci okuma
        # yazimdan ONCEKI snapshot'i dondurur.
        #
        # Bu tuzaga dustuk: WAP testi branch'e 50.000 bozuk satir yazdi,
        # sonra saydi ve 2.000.000 gordu (gercekte 2.050.000'di). Kalite
        # kontrolleri "0 bulgu" dedi, bozuk veri merge EDILDI ve test yine
        # "BASARILI" raporladi. Sessiz ve tehlikeli bir yanlislik.
        #
        # Kapatmanin bedeli: her tablo erisiminde ekstra bir katalog turu.
        # Bu bedel, veri dogrulugunun yanina bile yaklasmaz -- ozellikle
        # kalite kontrolu YAPAN bir pipeline'da. Yuksek frekansli, salt
        # okunur is yuklerinde tekrar acmayi degerlendirebilirsiniz.
        f"spark.sql.catalog.{CATALOG}.cache-enabled": "false",

        # ---- S3A yedek yolu (bazi araclar hala s3a:// bekler) ----
        "spark.hadoop.fs.s3a.endpoint": s3_endpoint,
        "spark.hadoop.fs.s3a.access.key": access_key,
        "spark.hadoop.fs.s3a.secret.key": secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    }

    if extra_conf:
        conf.update(extra_conf)

    builder = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        builder = builder.config(k, v)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Tani mesajlari STDERR'e. Stdout'u kirletmemek onemli: bazi job'lar
    # (ornegin 90_generate_ch_views.py) stdout'a MAKINE TARAFINDAN OKUNAN
    # cikti uretir ve '> out/x.sql' ile dosyaya yonlendirilir. Buradan
    # stdout'a yazilan tek bir satir o dosyayi bozar; ClickHouse
    # "Syntax error: failed at position 1 ([)" der. Tam olarak yasandi.
    print(f"[session] Spark {spark.version} | katalog={CATALOG} | ref={branch}", file=sys.stderr)
    print(f"[session] warehouse={warehouse}  s3={s3_endpoint}", file=sys.stderr)
    return spark


def read_oltp(spark: SparkSession, table: str, partition_column: str,
              lower: int, upper: int, num_partitions: int = 16):
    """
    OLTP'den PARALEL okuma.

    KRITIK: partitionColumn/lowerBound/upperBound/numPartitions verilmezse
    Spark tum tabloyu TEK executor ile, TEK JDBC baglantisindan ceker.
    2M satirda bu dakikalar demektir ve kaynak sistemde tek uzun sorgu
    olusturur. Bu dortlu verildiginde Spark isi N adet aralik sorgusuna
    boler; hem paralellesir hem her sorgu kisa surer -- OLTP sisteminin
    uzerindeki baskiyi da azaltir (canli bir uretim OLTP'sine dokunurken
    pazarlik konusu olan sey tam olarak budur).
    """
    return (
        spark.read.format("jdbc")
        .option("url", _env("PG_URL"))
        .option("dbtable", table)
        .option("user", _env("PG_USER"))
        .option("password", _env("PG_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", partition_column)
        .option("lowerBound", str(lower))
        .option("upperBound", str(upper))
        .option("numPartitions", str(num_partitions))
        # Surucu tarafinda satir tamponlama; varsayilan 0 = tumunu bellege al
        .option("fetchsize", "10000")
        .load()
    )


def oltp_bounds(spark: SparkSession, view: str = "csd.v_islem_sinirlari") -> tuple[int, int]:
    """JDBC partition sinirlarini kaynaktan tek sorguyla ogren."""
    row = (
        spark.read.format("jdbc")
        .option("url", _env("PG_URL"))
        .option("dbtable", view)
        .option("user", _env("PG_USER"))
        .option("password", _env("PG_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()
    )
    return int(row["min_id"]), int(row["max_id"])


def table_bounds(spark: SparkSession, table: str, key: str) -> tuple[int, int]:
    """
    Herhangi bir OLTP tablosunun anahtar araligini kaynaktan OGREN.

    NEDEN VAR: JDBC paralel okumasinda lowerBound/upperBound'u SABIT yazmak
    sessiz veri kaybina yol acar. Ornegin '1..50000' yazilmis bir kod,
    yatirimci sayisi 80.000'e cikinca 30.000 satiri OKUMAZ -- hata da
    vermez, sadece eksik sonuc uretir. (Bu tuzak 02_bronze_to_silver.py
    icinde vardi: .env'deki OLTP_ISLEM_ROWS degistirilince bozuluyordu.)

    Sinirlari her calistirmada kaynaktan sormak, tek bir hafif sorgu
    maliyetiyle bu sinifi tamamen ortadan kaldirir.
    """
    q = f"(SELECT MIN({key}) AS lo, MAX({key}) AS hi, COUNT(*) AS n FROM {table}) AS b"
    row = (
        spark.read.format("jdbc")
        .option("url", _env("PG_URL"))
        .option("dbtable", q)
        .option("user", _env("PG_USER"))
        .option("password", _env("PG_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()
    )
    if row is None or row["lo"] is None:
        raise RuntimeError(f"{table} bos veya {key} okunamadi.")
    print(f"[bounds] {table}.{key}: {row['lo']:,} .. {row['hi']:,} ({row['n']:,} satir)",
          file=sys.stderr)
    return int(row["lo"]), int(row["hi"])

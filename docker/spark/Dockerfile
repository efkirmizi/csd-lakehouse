# Spark + Iceberg + Nessie + Postgres JDBC calisma imaji
#
# TASARIM KARARI: JAR'lar imaj build'inde Maven Central'dan indirilip
# $SPARK_HOME/jars icine gomulur. spark-submit --packages KULLANILMAZ.
# Gerekce:
#   1. Determinizm  - her calistirmada ayni bytecode.
#   2. Hiz          - job baslangicinda Ivy cozumleme turu yok (~30-60sn tasarruf).
#   3. Offline      - kurumsal air-gapped ortamda --packages calismaz, bu calisir.
#      CSD gibi disari cikisi kisitli aglarda tek yapmaniz gereken bu imaji
#      bir kez build edip internal registry'ye push etmek.

ARG SPARK_VERSION=3.5.3
FROM apache/spark:${SPARK_VERSION}-scala2.12-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.9.1
ARG NESSIE_VERSION=0.104.1
ARG PG_JDBC_VERSION=42.7.4

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/spark/jars

RUN set -eux; \
    M2="https://repo1.maven.org/maven2"; \
    # Iceberg <-> Spark 3.5 kopru katmani (NessieCatalog impl'i de icinde bundle'li)
    curl -fSL -O "${M2}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    # S3FileIO + AWS SDK v2 (MinIO ile konusan katman)
    curl -fSL -O "${M2}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    # Nessie SQL eklentileri: CREATE BRANCH / MERGE BRANCH / USE REFERENCE gramerini acar
    curl -fSL -O "${M2}/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/${NESSIE_VERSION}/nessie-spark-extensions-3.5_2.12-${NESSIE_VERSION}.jar"; \
    # OLTP kaynagi okumak icin
    curl -fSL -O "${M2}/org/postgresql/postgresql/${PG_JDBC_VERSION}/postgresql-${PG_JDBC_VERSION}.jar"; \
    chmod 644 *.jar

WORKDIR /opt/spark/work-dir
USER spark

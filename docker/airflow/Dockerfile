# =============================================================================
#  Airflow + docker CLI
# =============================================================================
#  Neden ozel imaj? Resmi apache/airflow imajinda docker CLI YOKTUR.
#  DAG'lar Spark job'larini mevcut cluster'a 'docker exec lh-spark-master ...'
#  ile gonderiyor (gerekce: dags/csd_lakehouse_pipeline.py basligi).
#
#  Neden statik binary, 'apt install docker.io' degil?
#  docker.io paketi containerd + daemon dahil ~300MB getirir; bize sadece
#  ISTEMCI lazim. Statik binary'den yalnizca 'docker' cikariyoruz (~50MB).
#
#  GUVENLIK NOTU: Bu container docker soketine baglanacak (compose'da).
#  Soket erisimi pratikte host'ta root yetkisine yakindir. Bu, YALNIZCA
#  lokal referans kurulumu icin kabul edilebilir bir tavizdir.
#  Uretimde: KubernetesPodOperator / Spark on K8s kullanin, soket paylasmayin.
# =============================================================================

ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}

ARG DOCKER_CLI_VERSION=27.3.1

USER root

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates; \
    curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
      | tar -xzC /usr/local/bin --strip-components=1 docker/docker; \
    chmod 0755 /usr/local/bin/docker; \
    docker --version; \
    apt-get purge -y curl; \
    apt-get autoremove -y; \
    rm -rf /var/lib/apt/lists/*

# Imajin varsayilan kullanicisina geri donuyoruz. compose bu servisi
# soket erisimi icin root olarak calistiriyor; oradaki not'a bakin.
USER airflow

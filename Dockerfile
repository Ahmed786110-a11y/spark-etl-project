# Airflow image for the Spark project:
# official Apache Airflow 3.3.2 on Python 3.11 + Java 17 + PySpark 3.5.9,
# so Airflow tasks can run spark-submit against the Spark cluster.
FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && apt-get autoremove -yqq --purge \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
# Pin apache-airflow to the image's version so pip never up/downgrades it
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements.txt

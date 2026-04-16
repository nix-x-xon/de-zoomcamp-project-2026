FROM apache/spark-py:v3.5.0

USER root

WORKDIR /app

# GCS + BQ connectors for Spark
ARG GCS_CONNECTOR_VERSION=hadoop3-2.2.19
ARG BQ_CONNECTOR_VERSION=0.38.0
ARG SPARK_VERSION=3.5

RUN curl -L -o /opt/spark/jars/gcs-connector.jar \
        "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar" \
 && curl -L -o /opt/spark/jars/spark-bigquery.jar \
        "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-${SPARK_VERSION}-bigquery/${BQ_CONNECTOR_VERSION}/spark-${SPARK_VERSION}-bigquery-${BQ_CONNECTOR_VERSION}.jar"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY conf/ /app/conf/
COPY jobs/ /app/jobs/

ENV PYTHONPATH=/app
ENV SPARK_CONF_DIR=/app/conf
ENV TZ=UTC

ENTRYPOINT ["python", "-m"]

FROM apache/spark:3.5.6-python3

USER root
ARG ICEBERG_VERSION=1.9.2

# Tải Iceberg runtime + AWS bundle (S3FileIO) vào thư mục jars của Spark
RUN set -eux; \
    JARS=/opt/spark/jars; \
    BASE=https://repo1.maven.org/maven2/org/apache/iceberg; \
    curl -fSL -o "$JARS/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        "$BASE/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "$JARS/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "$BASE/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

WORKDIR /opt/spark

# Spark + Iceberg runtime — engine chạy tầng ingestion (CSV -> bronze Iceberg).
# Mọi phiên bản đến từ .env qua build args; không hardcode version trong image.
ARG SPARK_BASE_IMAGE=apache/spark:3.5.6-python3
FROM ${SPARK_BASE_IMAGE}

USER root
ARG ICEBERG_VERSION

# Iceberg runtime + AWS bundle (S3FileIO) để Spark đọc/ghi được Iceberg trên MinIO
RUN set -eux; \
    test -n "${ICEBERG_VERSION}"; \
    JARS=/opt/spark/jars; \
    BASE=https://repo1.maven.org/maven2/org/apache/iceberg; \
    curl -fSL -o "$JARS/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        "$BASE/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "$JARS/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "$BASE/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# Entrypoint nội suy spark-defaults.conf.tmpl bằng env rồi mới chạy lệnh.
# Template KHÔNG copy vào image mà được mount lúc chạy, để sửa config không phải build lại.
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /opt/spark
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

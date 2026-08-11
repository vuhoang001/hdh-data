# Spark + Iceberg runtime — engine chạy tầng ingestion (CSV -> bronze Iceberg).
# Mọi phiên bản đến từ .env qua build args; không hardcode version trong image.
ARG SPARK_BASE_IMAGE=apache/spark:3.5.6-python3
FROM ${SPARK_BASE_IMAGE}

USER root
ARG ICEBERG_VERSION
ARG PYYAML_VERSION
ARG HADOOP_AWS_VERSION
ARG AWS_SDK_BUNDLE_VERSION

# pyyaml: ingestion/common/spec.py đọc ingestion/config/sources.yml lúc chạy.
# Image Spark gốc không có sẵn. Pin chính xác (==) như mọi version khác trong repo.
RUN set -eux; \
    test -n "${PYYAML_VERSION}"; \
    pip install --no-cache-dir "pyyaml==${PYYAML_VERSION}"

# Iceberg runtime + AWS bundle (S3FileIO) để Spark đọc/ghi được Iceberg trên MinIO
RUN set -eux; \
    test -n "${ICEBERG_VERSION}"; \
    JARS=/opt/spark/jars; \
    BASE=https://repo1.maven.org/maven2/org/apache/iceberg; \
    curl -fSL -o "$JARS/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
        "$BASE/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fSL -o "$JARS/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
        "$BASE/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"

# hadoop-aws + AWS SDK: cần cho S3A, tức để spark.read.parquet("s3a://...") đọc được
# LANDING ZONE. Iceberg dùng S3FileIO (nằm trong iceberg-aws-bundle ở trên) cho các bảng
# của nó, nhưng đường đó KHÔNG phục vụ việc đọc file thô — đó là hai hệ thống IO khác
# nhau, và thiếu jar này Spark báo "An error occurred while calling o41.parquet".
# Version PHẢI khớp hadoop-client mà image Spark đóng gói, nếu không sẽ lỗi NoSuchMethod.
RUN set -eux; \
    test -n "${HADOOP_AWS_VERSION}"; \
    JARS=/opt/spark/jars; \
    curl -fSL -o "$JARS/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar"; \
    curl -fSL -o "$JARS/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar"

# Entrypoint nội suy spark-defaults.conf.tmpl bằng env rồi mới chạy lệnh.
# Template KHÔNG copy vào image mà được mount lúc chạy, để sửa config không phải build lại.
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

WORKDIR /opt/spark
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

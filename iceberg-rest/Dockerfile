# Image iceberg-rest-fixture gốc chỉ đóng gói driver SQLite, nên muốn dùng Postgres
# làm metastore thì phải tự thêm driver vào và chạy bằng -cp (java -jar bỏ qua classpath).
ARG ICEBERG_VERSION=1.9.2
ARG POSTGRES_JDBC_VERSION=42.7.4

FROM curlimages/curl:latest AS driver
ARG POSTGRES_JDBC_VERSION
RUN curl -fsSL -o /tmp/postgresql.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar"

FROM apache/iceberg-rest-fixture:${ICEBERG_VERSION}
COPY --from=driver /tmp/postgresql.jar /usr/lib/iceberg-rest/postgresql.jar
ENTRYPOINT ["java", "-cp", \
    "/usr/lib/iceberg-rest/iceberg-rest-adapter.jar:/usr/lib/iceberg-rest/postgresql.jar", \
    "org.apache.iceberg.rest.RESTCatalogServer"]

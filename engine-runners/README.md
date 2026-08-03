# engine-runners/

Image và cấu hình của từng **engine** chạy pipeline. Mỗi thư mục sở hữu trọn vẹn một engine:
Dockerfile + file cấu hình của nó nằm cùng chỗ.

```text
engine-runners/
├── spark-runner/     ingest CSV -> bronze Iceberg
├── dbt-runner/       transform silver/gold (chạy được cả DuckDB lẫn Trino)
└── trino-runner/     cấu hình catalog cho Trino (dùng image stock, không build)
```

Nguyên tắc chung: **file cấu hình khai báo cấu trúc, `.env` cung cấp giá trị.**
Không thư mục nào ở đây được hardcode endpoint, bucket, hay version.

## spark-runner/

| File | Vai trò |
| --- | --- |
| `Dockerfile` | Spark base image + Iceberg runtime jars + AWS bundle (S3FileIO) |
| `entrypoint.sh` | Nội suy `spark-defaults.conf.tmpl` bằng env rồi mới chạy lệnh |
| `spark-defaults.conf.tmpl` | Toàn bộ cấu hình Spark, giá trị viết dạng `${VAR}` |

**Vì sao phải nội suy ở entrypoint thay vì viết `${env:VAR}` thẳng vào file:** Iceberg đọc
các key `spark.sql.catalog.*` qua `getAllConfs` của Spark, đường này không chạy qua bộ nội
suy biến của `SQLConf`, nên `${env:...}` sẽ tới Iceberg nguyên văn và catalog trỏ vào một
URI vô nghĩa. Nội suy trước là cách chắc chắn đúng.

Entrypoint **dừng container** nếu render xong còn `${...}` ở dòng cấu hình — thiếu biến
trong `.env` phải lộ ra ngay, chứ không để Spark nuốt giá trị sai rồi báo lỗi ở chỗ khó lần.

Template được **mount** lúc chạy chứ không COPY vào image, nên sửa cấu hình không cần build lại.

## dbt-runner/

Một image dùng cho **cả hai môi trường**: cài sẵn `dbt-duckdb` và `dbt-trino`, chọn engine
bằng `dbt build --target duckdb|trino`. Cùng project, cùng model, cùng test.

Project dbt (`transforms/`) và `profiles.yml` được mount vào lúc chạy — image chỉ chứa
runtime, không chứa code.

## trino-runner/

Không build image (dùng `trinodb/trino:latest`), chỉ có `catalog/iceberg.properties` được
mount vào `/etc/trino/catalog`. Trino nội suy sẵn cú pháp `${ENV:VAR}` nên file này đọc
thẳng từ biến môi trường, không cần bước render.

> **TÊN FILE quyết định tên catalog trong Trino.** `iceberg.properties` → catalog `iceberg`.
> Đổi `ICEBERG_CATALOG_NAME` trong `.env` thì phải đổi tên file này cho khớp.

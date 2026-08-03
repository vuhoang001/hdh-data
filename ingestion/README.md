# ingestion/

Đưa dữ liệu thô vào **bronze layer** dưới dạng bảng Iceberg. Chỉ dùng ở môi trường lakehouse
(`make lake-*`); ở môi trường DuckDB, bronze do dbt làm — xem `transforms/models/bronze/`.

```text
ingestion/
├── config/sources.yml    # ĐĂNG KÝ NGUỒN — nơi duy nhất liệt kê 13 bảng bronze
├── common/               # hạ tầng dùng chung, không biết gì về bảng cụ thể
└── connectors/           # 13 file, mỗi file là business logic của một bảng
```

## Quy tắc phân chia

`common/` chứa thứ **đúng với mọi bảng**. `connectors/` chứa thứ **chỉ đúng với một bảng**.
Ranh giới này là lý do 13 file connector không lặp lại nhau.

| Module | Vai trò |
| --- | --- |
| `common/config.py` | Nơi duy nhất biết tên catalog, namespace bronze, thư mục dữ liệu — đọc từ `.env` |
| `common/job.py` | `BronzeJob` + `run_job()` — luồng đọc → transform → cột audit → ghi Iceberg → log |
| `common/session.py` | SparkSession + logger |
| `common/io.py` | Đọc CSV với schema tường minh |
| `common/iceberg.py` | Cột audit `_source_file`/`_ingested_at` + ghi bảng Iceberg |

Không file connector nào tự ghép chuỗi `"iceberg.bronze.orders"` hay
`"/opt/spark/data/orders.csv"`. Chúng khai báo `table="orders"`, `source_csv="orders.csv"`,
còn `config.py` dựng đường dẫn đầy đủ từ `.env`.

## Một connector trông như thế nào

Chỉ ba phần: `SCHEMA`, `transform()`, và khai báo `BronzeJob`.

```python
from common import BronzeJob, run_job

SCHEMA = StructType([...])                    # schema tường minh của CSV nguồn

def transform(df):                            # chuẩn hoá text + gắn cờ chất lượng
    ...                                       # KHÔNG lọc bỏ dòng

JOB = BronzeJob(
    table="orders",
    source_csv="orders.csv",
    schema=SCHEMA,
    transform=transform,
    partition_by=partition_columns,           # bỏ đi nếu để nguyên một file
)

if __name__ == "__main__":
    run_job(JOB)
```

## Hợp đồng của bronze layer

1. **Giữ nguyên số dòng nguồn.** `transform()` không được lọc bỏ dòng. Dòng hỏng được
   *gắn cờ* `_is_valid = false` kèm `_invalid_reason`, việc loại bỏ là chuyện của silver.
2. **Schema tường minh, áp theo thứ tự cột.** Spark bỏ qua tên trong header, nên đổi tên
   cột ngay ở `SCHEMA` được (`Date` → `sale_date`). Cái giá: nếu nguồn đổi thứ tự cột,
   dữ liệu vào nhầm cột mà không có lỗi nào báo.
3. **Mọi bảng có cột audit** `_source_file` và `_ingested_at`, do `common/` gắn tự động.

## Khi nguồn không còn là CSV (local → production)

Ở đây nguồn là file CSV trong `data/`. Production thường lấy từ Postgres, một API, hay
Parquet trên S3. **Chỗ đổi chỉ có một**, vì `BronzeJob` đã tách sẵn ba thứ:

```python
JOB = BronzeJob(
    source_csv="orders.csv",   # NGUỒN  ← thứ duy nhất đổi
    transform=transform,       # RULE làm sạch — không đổi
    table="orders",            # ĐÍCH bronze — không đổi
)
```

Chuyển `orders` sang đọc từ Postgres cần đúng ba thay đổi:

```python
# 1. ingestion/common/io.py — viết reader
def read_jdbc(spark, source, schema):
    return spark.read.format("jdbc").option("url", source).option("dbtable", ...).load()

# 2. đăng ký nó
SOURCE_READERS = {"csv": read_csv, "postgres": read_jdbc}
```

```yaml
# 3. ingestion/config/sources.yml
  - table: orders
    file: jdbc:postgresql://.../orders
    type: postgres          # ← đổi ở đây
```

Rồi thêm `source_type="postgres"` vào `BronzeJob` của connector đó. `transform()`, mọi model
silver/gold, và mọi test dbt **không sửa dòng nào** — chúng chỉ biết bảng bronze, không biết
dữ liệu từ đâu tới. Đó là toàn bộ giá trị của việc có một lớp bronze.

`type:` không phải chú thích trang trí: `tests/test_sources_registry.py` đối chiếu nó với
`SOURCE_READERS`, nên khai một loại chưa có reader là fail ngay, không đợi tới lúc chạy Spark.

## Chỗ CHƯA sẵn sàng cho production: chế độ ghi

Đọc nguồn thì đã có seam. **Ghi thì chưa.** `common/iceberg.py` ghi bằng `createOrReplace()`
— tức ghi đè toàn bộ bảng mỗi lần chạy:

```python
writer.tableProperty("format-version", "2").createOrReplace()
```

Điều này đúng ở đây vì nguồn là file CSV tĩnh và bảng lớn nhất chỉ 714k dòng — nạp lại toàn
bộ mất vài giây và cho tính **idempotent** miễn phí (chạy 1 lần hay 10 lần đều ra kết quả y
hệt, không nhân đôi dữ liệu).

Nó **không dùng được ở production** với bảng 500 triệu dòng nạp hàng ngày. Lúc đó cần ba
thứ mà hiện tại chưa có:

| Cần gì | Vì sao | Đụng vào đâu |
| --- | --- | --- |
| **Watermark / incremental** | Chỉ đọc dòng đổi từ lần chạy trước, không quét cả bảng nguồn | `io.py` + lưu trạng thái lần chạy |
| **MERGE INTO thay vì replace** | Ghi đè cả bảng 500M dòng mỗi ngày là bất khả thi | `iceberg.py` |
| **Backfill theo khoảng ngày** | Chạy lại một tháng cũ mà không đụng dữ liệu khác | tham số cho `run_job()` |

Đây là **giới hạn có thật, không phải thiếu sót do quên**. Xây incremental cho một file CSV
tĩnh là viết code không ai chạy được. Nhưng khi nguồn thật xuất hiện, đây là việc lớn nhất —
lớn hơn nhiều so với đổi reader.

`transform()`, silver, gold và test dbt vẫn không phải sửa: chúng chỉ đọc bảng bronze, không
quan tâm bảng đó được ghi bằng cách nào.

## Bronze được cài đặt HAI LẦN — và điều đó ràng buộc bạn

Cùng 13 bảng, cùng rule, hai engine:

| Môi trường | Ai ghi bronze | File |
| --- | --- | --- |
| lakehouse | Spark | `connectors/ingest_<bảng>.py` |
| DuckDB | dbt | `../transforms/models/bronze/bronze_<bảng>.sql` |

Đây là **lựa chọn có ý thức**: đổi lại được dev loop và CI chạy trong vài giây thay vì phải
dựng MinIO+Spark+Trino. Cái giá là mỗi lần đổi rule chất lượng phải sửa **cả hai** — quên một
bên thì hai môi trường cho hai kết quả khác nhau, im lặng, không lỗi nào báo.

`tests/test_bronze_parity.py` bịt đúng chỗ đó: nó so tập nhãn `_invalid_reason` của từng cặp
file và fail nếu lệch. Chạy `pytest tests -q` sau mỗi lần sửa rule.

> Giới hạn: test so **tên nhãn**, không so logic bên trong. Một bên `< 0` còn bên kia `<= 0`
> thì nó không thấy. Muốn chặt tới mức đó phải chạy cả hai engine trên cùng dữ liệu rồi đối
> chiếu — tức là quay lại đúng chi phí mà trùng lặp này né.

## Thêm một bảng mới

1. Thêm một mục vào `config/sources.yml`
2. Tạo `connectors/ingest_<bảng>.py` theo mẫu trên
3. Tạo `../transforms/models/bronze/bronze_<bảng>.sql` với **cùng tập nhãn lỗi**
4. `pytest tests -q` — bắt ngay nếu ba bước trên lệch nhau
5. `make lake-ingest-<bảng>`

Không phải sửa Makefile: danh sách target sinh ra từ `sources.yml`.
Chi tiết đầy đủ tới tận gold: [`docs/them-bang-moi.md`](../docs/them-bang-moi.md).

## Vì sao chọn partition như vậy

`sources.yml` ghi chiến lược partition của từng bảng cùng số dòng, để nhìn thấy toàn cảnh
mà không phải mở 13 file. Nguyên tắc: **partition quá mịn còn hại hơn không partition** —
650k dòng trải 2012-2023 mà partition theo ngày sẽ ra ~3800 file vài chục KB và làm writer OOM.

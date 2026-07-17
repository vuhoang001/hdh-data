# Thêm một bảng mới vào pipeline

Hướng dẫn thêm một bảng đi hết chặng **CSV → bronze → silver → gold**, dùng `order_items`
làm ví dụ xuyên suốt. Tài liệu giải thích từng thành phần dùng làm gì và **tại sao phải có
nó**, chứ không chỉ đưa code để copy.

## Mục lục

- [Hiểu kiến trúc trước khi gõ code](#hiểu-kiến-trúc-trước-khi-gõ-code)
- [Checklist](#checklist)
- [Bước 0 — Xem trước dữ liệu nguồn](#bước-0--xem-trước-dữ-liệu-nguồn)
- [Bước 1 — Spark job (bronze)](#bước-1--spark-job-bronze)
- [Bước 2 — Makefile](#bước-2--makefile)
- [Bước 3 — Khai báo source cho dbt](#bước-3--khai-báo-source-cho-dbt)
- [Bước 4 — Model silver](#bước-4--model-silver)
- [Bước 5 — Model gold](#bước-5--model-gold)
- [Chạy](#chạy)
- [Chạy lại job — ghi đè, snapshot, time travel](#chạy-lại-job--ghi-đè-snapshot-time-travel)
- [Truy vấn](#truy-vấn)
- [Lỗi hay gặp](#lỗi-hay-gặp)

---

## Hiểu kiến trúc trước khi gõ code

### Tại sao chia 3 layer bronze / silver / gold?

Đây là kiến trúc **medallion**. Ý tưởng cốt lõi: mỗi layer chỉ chịu trách nhiệm một việc,
nên khi có lỗi bạn biết ngay phải sửa ở đâu.

| Layer | Trả lời câu hỏi | Ai ghi | Vật liệu hoá |
|---|---|---|---|
| **bronze** | "Dữ liệu nguồn nói gì?" | Spark | table (Iceberg) |
| **silver** | "Dữ liệu nào dùng được?" | dbt | view |
| **gold** | "Câu trả lời cho business là gì?" | dbt | table (Iceberg) |

**Tại sao không ingest thẳng CSV vào một bảng sạch cho nhanh?** Vì khi số liệu ra sai, bạn
không có cách nào biết là do file nguồn sai hay do code làm sạch sai — dữ liệu gốc đã mất
rồi. Bronze giữ nguyên hiện trạng nguồn để bạn luôn truy ngược được.

**Tại sao silver là `view` còn gold là `table`?**

- Silver chỉ lọc và đổi tên cột — rất rẻ. Làm `view` thì không tốn dung lượng lưu trữ, và
  mỗi lần query luôn phản ánh bronze mới nhất, không cần build lại.
- Gold có `group by`, `join`, `sum` trên hàng trăm nghìn dòng — đắt. Làm `table` để tính một
  lần rồi mọi người query lại kết quả đã tính sẵn, thay vì tính lại từ đầu mỗi lần.

Quy tắc này khai báo ở `dbt/hdh_dbt/dbt_project.yml` theo **thư mục**, nên file mới bạn thêm
vào `models/silver/` tự động là view, không cần khai báo gì thêm:

```yaml
models:
  hdh_dbt:
    silver:
      +materialized: view
    gold:
      +materialized: table
```

### Tại sao Spark lo bronze còn dbt lo silver/gold?

Mỗi công cụ làm việc nó giỏi nhất:

- **Spark** đọc được file thô (CSV, JSON, Parquet) và xử lý dữ liệu bẩn — kiểu dữ liệu sai,
  dòng hỏng, file khổng lồ. SQL thuần không đọc được CSV nằm trên disk.
- **dbt** làm SQL trên dữ liệu **đã có cấu trúc**, và cho bạn thứ Spark không có: đồ thị phụ
  thuộc giữa các model, test dữ liệu khai báo bằng YAML, tài liệu tự sinh.

Nói ngắn gọn: **Spark đưa dữ liệu vào được thế giới SQL, dbt làm mọi thứ còn lại bằng SQL.**

### Tại sao có thư mục `common/` và không được sửa nó?

`spark/jobs/common/` chỉ chứa **hạ tầng** — thứ đúng với mọi bảng, không phụ thuộc bảng nào:
tạo SparkSession, đọc CSV, ghi Iceberg, logging.

Business logic — schema của bảng, rule làm sạch — nằm trong chính file job của bảng đó.

**Tại sao phải tách như vậy?** Vì nếu nhét logic của `orders` vào `common/`, thì khi thêm
`order_items` bạn buộc phải sửa `common/`, và mỗi lần sửa lại có nguy cơ làm hỏng job đang
chạy tốt. Tách ra thì **thêm bảng mới = thêm 1 file, không đụng file cũ** — thêm bảng thứ 10
cũng an toàn như thêm bảng thứ 2.

---

## Checklist

| Bước | File | Tạo mới hay sửa |
|---|---|---|
| 1 | `spark/jobs/bronze/ingest_<bảng>.py` | tạo mới |
| 2 | `Makefile` | sửa (thêm target) |
| 3 | `dbt/hdh_dbt/models/silver/_sources.yml` | sửa (thêm mục `tables`) |
| 4 | `dbt/hdh_dbt/models/silver/silver_<bảng>.sql` + `.yml` | tạo mới |
| 5 | `dbt/hdh_dbt/models/gold/gold_<chủ đề>.sql` + `.yml` | tạo mới (khi cần tổng hợp) |

Không phải sửa `dbt_project.yml`: dbt tự quét thư mục `models/`, và rule materialization áp
theo thư mục nên file mới tự thừa hưởng.

---

## Bước 0 — Xem trước dữ liệu nguồn

```bash
head -3 data/order_items.csv
```

```
order_id,product_id,quantity,unit_price,discount_amount,promo_id,promo_id_2
1,2400,7,1138.22,0.0,,
```

**Tại sao phải xem trước?** Vì ba quyết định quan trọng nhất của cả job đều đến từ đây, và
sửa sau khi đã ghi bảng thì tốn công hơn nhiều:

1. **Schema là gì?** — map từng cột CSV sang kiểu Spark.
2. **Dòng thế nào là hỏng?** — sẽ viết thành các nhánh `F.when(...)`.
3. **Partition theo cột nào?** — xem [Chọn partition](#chọn-partition).

Đã thấy ngay ở dòng dữ liệu mẫu: `promo_id` và `promo_id_2` để trống. CSV không phân biệt
được "chuỗi rỗng" với "không có giá trị", nên đây là thứ phải xử lý trong `transform()`.

---

## Bước 1 — Spark job (bronze)

File `spark/jobs/bronze/ingest_order_items.py`. Cấu trúc luôn gồm 3 phần, và **chỉ phần giữa
thay đổi giữa các bảng** — hai phần còn lại copy y nguyên từ job có sẵn.

### Phần 1: Hằng số

```python
APP_NAME = "hdh-bronze-order-items"
NAMESPACE = "iceberg.bronze"
TABLE = "iceberg.bronze.order_items"
SOURCE_CSV = "/opt/spark/data/order_items.csv"
```

Từng cái dùng làm gì:

- **`APP_NAME`** — tên hiển thị trong Spark UI và log. Khi có nhiều job chạy cùng lúc, đây là
  thứ giúp bạn biết job nào đang ngốn tài nguyên hay job nào treo.
- **`NAMESPACE`** — namespace (schema) trong Iceberg. Phải tạo trước khi ghi bảng, giống như
  phải có thư mục trước khi tạo file trong đó.
- **`TABLE`** — tên đầy đủ 3 cấp `catalog.namespace.table`. `iceberg` là catalog khai báo ở
  `spark/conf/spark-defaults.conf`; thiếu một cấp là Spark ghi nhầm chỗ.
- **`SOURCE_CSV`** — đường dẫn **bên trong container**, không phải trên máy bạn. `docker-compose.yml`
  mount `./data` vào `/opt/spark/data`, nên trên máy là `data/order_items.csv` còn với Spark
  là `/opt/spark/data/order_items.csv`.

Tách thành hằng số ở đầu file thay vì viết thẳng vào code để người đọc biết ngay job này đọc
gì và ghi đi đâu, không phải dò xuống dưới.

### Phần 2: Business logic — phần duy nhất phải nghĩ

#### Schema — tại sao phải khai báo tay?

```python
SCHEMA = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("promo_id", StringType(), True),
    StructField("promo_id_2", StringType(), True),
])
```

Spark có `inferSchema=true` tự đoán kiểu, nhưng **repo này không dùng**, vì ba lý do:

1. **Đoán sai lặng lẽ.** Cột `zip` toàn số sẽ bị đoán thành `integer`, và `01234` biến thành
   `1234` — mất số 0 đầu mà không có lỗi nào báo.
2. **Đắt.** Muốn đoán, Spark phải đọc quét file một lượt rồi đọc lại lần nữa để lấy dữ liệu.
   Khai báo tay thì chỉ đọc một lượt.
3. **Không phát hiện được nguồn đổi.** Khai báo tay thì nguồn thêm/đổi cột là thấy ngay; đoán
   tự động thì schema âm thầm đổi theo và bảng bronze đổi hình dạng mà bạn không biết.

**Tham số thứ ba là `nullable`.** `False` nghĩa là "cột này không được rỗng". `order_id` để
`False` vì một dòng hàng không thuộc đơn nào thì vô nghĩa; các cột còn lại `True` vì thiếu
thì vẫn ghi vào bronze được và ta gắn cờ ở bước sau.

> **Lưu ý:** `nullable=False` là *khai báo ý định*, Spark không ép buộc khi đọc CSV. Nó không
> thay thế được rule chất lượng ở `transform()`.

Kiểu dữ liệu chọn theo nội dung, không theo hình thức: `quantity` là `IntegerType` (không ai
mua 2.5 cái áo), `unit_price` là `DoubleType`, còn `promo_id` là `StringType` — **mã định
danh luôn để string dù nhìn giống số**, vì bạn không bao giờ cộng trừ hai mã khuyến mãi với
nhau, và để số thì mã `007` sẽ mất số 0.

#### `transform()` — chuẩn hoá và gắn cờ

```python
def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        .withColumn("promo_id", F.nullif(F.trim(F.col("promo_id")), F.lit("")))
        .withColumn("promo_id_2", F.nullif(F.trim(F.col("promo_id_2")), F.lit("")))
    )
```

`F.trim()` bỏ khoảng trắng thừa, `F.nullif(x, "")` biến chuỗi rỗng thành `NULL`.

**Tại sao cần bước này?** Vì CSV không phân biệt được "trống" và "không có". Nếu để nguyên,
`promo_id = ""` và `promo_id = NULL` là hai giá trị khác nhau với SQL, nên `count(promo_id)`
ra số sai và `where promo_id is null` bỏ sót dòng. Quy hết về `NULL` để chỉ có **một** cách
biểu diễn "không có giá trị".

```python
    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("product_id").isNull(), F.lit("product_id_missing")),
        F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity_invalid")),
        F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("unit_price_invalid")),
        F.when(F.col("discount_amount") < 0, F.lit("discount_negative")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )
```

**Cơ chế hoạt động:** `F.when(điều_kiện, giá_trị)` không có `.otherwise()` sẽ trả `NULL` khi
điều kiện sai. `concat_ws` bỏ qua mọi `NULL` khi nối chuỗi. Kết hợp lại:

| Tình huống | `concat_ws` cho ra | `_invalid_reason` | `_is_valid` |
|---|---|---|---|
| Dòng sạch (mọi `when` đều `NULL`) | `""` | `NULL` | `true` |
| Hỏng 1 lỗi | `"quantity_invalid"` | `"quantity_invalid"` | `false` |
| Hỏng 2 lỗi | `"product_id_missing, quantity_invalid"` | (như trên) | `false` |

Dòng `F.when(invalid_reason == "", None).otherwise(...)` quy chuỗi rỗng về `NULL`, rồi
`_is_valid` chỉ là phép kiểm tra `NULL` đó.

**Tại sao gắn cờ mà không lọc bỏ luôn dòng hỏng cho gọn?** Đây là điểm quan trọng nhất của
layer bronze:

- **Đếm được thiệt hại.** Lọc ở bronze thì dòng hỏng biến mất không dấu vết — bạn không bao
  giờ biết nguồn có bao nhiêu dòng lỗi, cũng không biết hôm nay lỗi tăng hay giảm.
- **Ghi lại lý do.** `_invalid_reason` cho bạn biết hỏng *vì sao*. Một câu `group by
  _invalid_reason` là thấy ngay lỗi nào phổ biến nhất, đáng sửa trước.
- **Đảo ngược được quyết định.** Hôm nay bạn coi `quantity = 0` là lỗi; tháng sau business
  bảo đó là hàng tặng, hợp lệ. Nếu chỉ gắn cờ thì sửa một dòng SQL ở silver là xong; nếu đã
  lọc ở bronze thì dữ liệu mất rồi, phải ingest lại toàn bộ.

Nguyên tắc rút gọn: **bronze mô tả, silver mới quyết định.**

**Tại sao tên cột có dấu `_` đầu?** Quy ước của repo: `_` đánh dấu **cột kỹ thuật**, không
phải dữ liệu business. Nhờ đó silver biết chắc cột nào cần bỏ đi.

#### Rule tốt: ràng buộc chéo cột

Rule mạnh nhất không phải "cột này không được rỗng" mà là **quan hệ giữa các cột không được
phá vỡ**, vì đây là thứ bắt được lỗi mà kiểm tra từng cột riêng lẻ không thấy:

| Job | Rule | Tại sao chắc chắn là lỗi |
|---|---|---|
| `shipments` | `delivery_date < ship_date` | Giao trước khi gửi là bất khả thi về vật lý |
| `web_traffic` | `unique_visitors > sessions` | Một người vào nhiều phiên được; một phiên không thể nhiều người |
| `web_traffic` | `page_views < sessions` | Mỗi phiên xem ít nhất 1 trang |
| `inventory` | `month != month(snapshot_date)` | `month` là cột dẫn xuất; lệch nghĩa là nguồn tính sai |
| `promotions` | `end_date < start_date` | Khoảng thời gian rỗng, khuyến mãi không bao giờ chạy |
| `promotions` | `promo_type='percentage' and discount_value > 100` | Giảm quá 100% = trả tiền cho khách để họ mua |

Rule `inventory.month` đáng chú ý: nếu nguồn tính sai cột `month`, mọi report nhóm theo
`year`/`month` sẽ ra số sai **mà không có gì báo lỗi**. Cột dẫn xuất sẵn trong nguồn luôn
đáng nghi — hãy kiểm tra lại nó bằng chính cột gốc.

#### Lỗi dữ liệu hay sự thật kinh doanh?

Không phải cái gì bất thường cũng là lỗi. `sales.csv` có **382/3833 ngày (10%) bán lỗ**
(`cogs > revenue`). Gắn cờ `_is_valid = false` cho chúng là sai, vì hai lý do:

- **10% là quá nhiều để gọi là lỗi.** Lỗi dữ liệu thật thường hiếm; một "lỗi" chiếm 10% số
  dòng gần như luôn là hiện tượng thật mà ta chưa hiểu.
- **Silver sẽ vứt chúng đi**, và report doanh thu mất 10% số ngày — sai lệch lớn hơn nhiều
  so với việc giữ lại.

Cách xử lý trong `ingest_sales_daily.py`: cho ra **một cột riêng**, không đụng tới `_is_valid`.

```python
.withColumn("_margin_negative", F.col("cogs") > F.col("revenue"))
```

Giờ thông tin đó phân tích được (`where _margin_negative`) thay vì bị vứt. So sánh với
`products`, cũng cùng điều kiện `cogs > price` nhưng ở đó **0 dòng vi phạm** — hiếm nên gắn
cờ lỗi là hợp lý.

**Cùng một điều kiện, hai cách xử lý khác nhau, quyết định bởi dữ liệu thật.** Đây là lý do
[bước 0](#bước-0--xem-trước-dữ-liệu-nguồn) phải xem dữ liệu trước khi viết rule.

#### `partition_columns()` — chia file để query nhanh

```python
def partition_columns():
    """order_items không có cột ngày để partition. Bucket theo order_id giữ file cân đối
    và giúp join với orders. Gọi trong hàm vì F.bucket() cần SparkContext đã khởi tạo."""
    return [F.bucket(16, "order_id")]
```

**Partition là gì và tại sao cần?** Iceberg chia bảng thành nhiều nhóm file theo giá trị cột.
Khi query có `where order_date = '2022-12-31'`, engine chỉ đọc đúng nhóm file của ngày đó và
bỏ qua phần còn lại — gọi là *partition pruning*. Không partition thì mỗi query phải quét
toàn bộ bảng.

**Tại sao phải là hàm, không phải hằng số?** Vì `F.months()` và `F.bucket()` cần SparkContext
đã khởi tạo. Viết `PARTITION = [F.bucket(16, "order_id")]` ở cấp module thì Python chạy nó
lúc import — trước khi `build_spark_session()` được gọi — và job chết ngay.

**Cách chọn:**

**Cách chọn — mục tiêu là file cỡ vài MB, không phải vài chục KB:**

| Tình huống | Cách làm | Ví dụ trong repo |
|---|---|---|
| Bảng lớn (>500k dòng), có cột ngày | `[F.months("cột_ngày")]` | `orders`, `shipments` |
| Bảng vừa (40k–150k dòng), có cột ngày | `[F.years("cột_ngày")]` | `customers`, `reviews`, `returns`, `inventory` |
| Không có cột ngày | `[F.bucket(N, "khoá_join")]` | `order_items`, `payments` |
| Bảng dimension nhỏ (dưới ~40k dòng) | `None` | `products`, `promotions`, `geography`, `sales_daily`, `web_traffic` |

Số dòng chỉ là chỉ dấu; thứ thực sự quyết định là **kích thước mỗi partition**. Lấy dung
lượng file chia cho số partition dự kiến: dưới ~1MB một partition thì chọn hạt thô hơn
(`months` → `years`, hoặc bỏ partition hẳn).

**Tại sao `orders` partition theo tháng chứ không theo ngày?** ~650k dòng trải 2012–2023.
Theo ngày sẽ tạo ~3800 partition, mỗi file vài chục KB. File quá nhỏ là phản tác dụng: engine
tốn nhiều thời gian mở file hơn là đọc dữ liệu, và writer phải giữ 3800 file handle cùng lúc
nên OOM. Theo tháng thì còn ~140 partition, mỗi file vài MB — vừa đẹp.

**Tại sao `order_items` dùng `bucket(16, "order_id")`?** Bảng này không có cột ngày nào để
chia. `bucket` băm `order_id` vào 16 nhóm đều nhau, được hai thứ: file có kích thước cân đối,
và các dòng cùng `order_id` nằm chung một nhóm nên join với `orders` đỡ phải xáo dữ liệu qua
lại giữa các máy (*shuffle*).

**Tại sao bảng nhỏ thì `None`?** Partition một bảng 2000 dòng chỉ tạo ra hàng loạt file tí
hon, chậm hơn là để nguyên một file.

**Tại sao `customers` dùng `years` mà `orders` dùng `months`?** Cả hai đều trải 2012–2023,
nhưng `orders` có 647k dòng (44MB) còn `customers` chỉ 122k dòng (6.8MB). Chia `customers`
theo tháng ra ~140 partition × 50KB — quá vụn. Theo năm còn ~11 partition × 600KB, hợp lý
hơn. **Cùng khoảng thời gian nhưng khác kích thước thì chọn khác nhau.**

### Đổi tên cột: khi nào được phép

Bronze giữ nguyên nguồn, nhưng **tên cột là ngoại lệ có chủ ý**. Hai job trong repo đổi tên:

| Nguồn | Bronze | Lý do |
|---|---|---|
| `sales.csv`: `Date, Revenue, COGS` | `sale_date, revenue, cogs` | `Date` trùng từ khoá SQL; repo dùng snake_case |
| `web_traffic.csv`: `date` | `traffic_date` | `date` vừa là từ khoá vừa là tên kiểu dữ liệu |

Cột tên `date` hay `Date` thì mọi câu query đều phải quote (`"date"`), rất dễ quên và gây lỗi
khó hiểu. Đổi một lần ở bronze rẻ hơn là chịu đựng nó ở mọi model phía sau.

**Cách đổi:** chỉ cần đặt tên mới trong `SCHEMA`. Spark áp schema **theo thứ tự cột** và bỏ
qua tên trong header (`enforceSchema` mặc định `true`).

> **Cái giá phải trả:** vì khớp theo vị trí chứ không theo tên, nếu nguồn đổi thứ tự cột thì
> dữ liệu vào nhầm cột **mà không có lỗi nào báo** — `revenue` nhận giá trị `cogs`. Với file
> tĩnh 3 cột thì rủi ro chấp nhận được; với nguồn hay thay đổi thì nên đọc bằng tên gốc rồi
> `withColumnRenamed()` sau, để nguồn đổi cột là job fail ngay.

Sau khi ingest, **luôn đối chiếu vài dòng đầu với file gốc** để chắc dữ liệu vào đúng cột:

```bash
head -2 data/sales.csv
docker compose exec trino trino --catalog iceberg --execute \
  "SELECT sale_date, revenue, cogs FROM bronze.sales_daily ORDER BY sale_date LIMIT 1;"
```

### Phần 3: Orchestration — copy y nguyên

```python
def run(spark: SparkSession, source_csv: str, table: str, logger) -> None:
    logger.info("Đọc %s", source_csv)
    df = read_csv(spark, source_csv, SCHEMA)

    bronze_df = add_audit_columns(transform(df), source_csv)

    create_namespace(spark, NAMESPACE)
    logger.info("Ghi bảng %s", table)
    write_iceberg_table(bronze_df, table, partition_columns())

    total = count_table_rows(spark, table)
    invalid = spark.table(table).filter("not _is_valid").count()
    logger.info("%s: %s dòng (hợp lệ=%s, lỗi=%s)", table, total, total - invalid, invalid)
```

Từng hàm gọi từ `common/` làm gì:

- **`read_csv(spark, path, SCHEMA)`** — đọc CSV với schema tường minh. Nó set sẵn `header=true`
  và `dateFormat`, nên mọi job đọc CSV giống hệt nhau, không mỗi job một kiểu.
- **`add_audit_columns(df, source_csv)`** — thêm `_source_file` và `_ingested_at`. **Tại sao
  cần?** Ba tháng sau, khi một con số trông sai, đây là thứ trả lời "dòng này từ file nào,
  nạp lúc nào". Không có nó thì bảng bronze là hộp đen. Đặt trong `common/` vì đúng với mọi
  bảng, không phụ thuộc nội dung.
- **`create_namespace(spark, NAMESPACE)`** — `CREATE NAMESPACE IF NOT EXISTS`. Phải có vì ghi
  vào namespace chưa tồn tại sẽ lỗi. Có `IF NOT EXISTS` nên chạy lại nhiều lần vô hại.
- **`write_iceberg_table(df, table, partition_columns())`** — ghi bằng `createOrReplace()`,
  tức **ghi đè toàn bộ bảng**. Nghĩa là job **idempotent**: chạy 1 lần hay 10 lần đều ra kết
  quả y hệt, không nhân đôi dữ liệu. Đây là lý do bạn có thể vô tư `make ingest` lại khi nghi
  ngờ — xem [Chạy lại job](#chạy-lại-job--ghi-đè-snapshot-time-travel) để hiểu chuyện gì thực
  sự xảy ra bên dưới.

**Tại sao ở cuối lại log số dòng?** Để biết job có thực sự làm gì không. `total = 714669,
invalid = 0` nói lên nhiều hơn dòng chữ "thành công": nếu hôm nào đó `total` tụt còn 300k,
bạn biết ngay nguồn có vấn đề, không phải chờ tới lúc report ra số lạ.

```python
def main():
    args = parse_args()
    logger = get_logger("bronze.order_items")
    spark = build_spark_session(APP_NAME)
    try:
        run(spark, args.source_csv, args.table, logger)
    finally:
        spark.stop()
```

- **`parse_args()`** — cho phép override đường dẫn/tên bảng qua dòng lệnh. **Tại sao cần khi
  đã có hằng số?** Để test job trên file nhỏ (`--source-csv .../sample.csv`) hoặc ghi ra bảng
  tạm (`--table iceberg.bronze.order_items_test`) mà không phải sửa code.
- **`try/finally: spark.stop()`** — `finally` đảm bảo giải phóng tài nguyên **kể cả khi job
  lỗi**. Không có nó, job chết giữa chừng sẽ để lại session treo giữ RAM của cluster.
- **`if __name__ == "__main__":`** — chỉ chạy `main()` khi file được `spark-submit` trực tiếp,
  không chạy khi bị import. Nhờ đó bạn có thể `from ingest_order_items import transform` để
  viết unit test cho riêng hàm `transform` mà không khởi động cả job.

---

## Bước 2 — Makefile

```makefile
.PHONY: up down ... ingest ingest-orders ingest-order-items ...

ingest: ingest-orders ingest-order-items   ## Ingest toàn bộ bảng bronze

ingest-orders: ## Chỉ ingest bảng orders
	docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_orders.py

ingest-order-items: ## Chỉ ingest bảng order_items
	docker compose exec spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze/ingest_order_items.py
```

**Tại sao mỗi bảng một target riêng, lại còn thêm target gộp?** Hai nhu cầu khác nhau: sửa
rule của `order_items` thì chỉ cần chạy lại nó (`make ingest-order-items`), tiết kiệm vài
phút; còn dựng lại từ đầu sau `make clean` thì cần tất cả (`make ingest`).

**`ingest: ingest-orders ingest-order-items` nghĩa là gì?** Trong Make, những tên đứng sau
dấu `:` là **prerequisites** — Make chạy chúng trước, lần lượt. Target `ingest` tự nó không
có lệnh nào, chỉ gom hai target kia.

**`.PHONY` để làm gì?** Nó báo Make rằng đây là *tên lệnh*, không phải *tên file* cần tạo ra.
Không khai báo thì nếu thư mục lỡ có file tên `ingest`, Make sẽ thấy "file đã tồn tại, không
cần làm gì" và **im lặng không chạy lệnh**. Đây là lỗi rất khó đoán, nên cứ thêm mọi target
vào `.PHONY`.

> **Nhớ dùng Tab, không phải space** để thụt dòng lệnh trong Makefile — Make bắt buộc Tab và
> báo `missing separator` nếu bạn dùng space.

Kiểm tra target đúng chưa mà không chạy thật:

```bash
make -n ingest    # in ra các lệnh sẽ chạy, không thực thi
```

---

## Bước 3 — Khai báo source cho dbt

`dbt/hdh_dbt/models/silver/_sources.yml`:

```yaml
version: 2

sources:
  - name: bronze
    database: iceberg      # catalog Trino
    schema: bronze         # namespace Spark ghi Bronze layer vào
    tables:
      - name: order_items
        description: "Dòng hàng Bronze: giữ nguyên số dòng nguồn, có cờ chất lượng"
        columns:
          - name: order_id
            tests: [not_null]
```

**Source là gì?** Là bảng do thứ khác tạo ra — ở đây là Spark. dbt không quản lý chúng, chỉ
đọc.

**Tại sao phải khai báo, dbt không tự thấy bảng à?** dbt *thấy* được, nhưng khai báo cho bạn
ba thứ:

1. **Đồ thị phụ thuộc.** dbt biết `silver_order_items` phụ thuộc bronze, vẽ được lineage đầy
   đủ từ nguồn tới gold.
2. **Đổi chỗ không phải sửa model.** Nếu namespace đổi từ `bronze` sang `raw`, bạn sửa **một
   dòng** `schema:` ở đây thay vì sửa mọi file SQL có nhắc tới bảng đó.
3. **Test ngay tại nguồn.** Đặt `not_null` ở đây là dbt kiểm tra bronze *trước khi* build
   silver — bắt lỗi từ gốc thay vì để nó lan xuống.

Ba trường quan trọng:

- **`name: bronze`** — tên gọi trong code, chính là `'bronze'` trong `{{ source('bronze', 'order_items') }}`.
- **`database: iceberg`** — catalog của Trino. dbt nối qua Trino nên dùng thuật ngữ Trino:
  `database` = catalog.
- **`schema: bronze`** — namespace mà Spark ghi vào. Phải **khớp chính xác** hằng số `NAMESPACE`
  trong Spark job, nếu không dbt tìm không ra bảng.

**Tại sao tên file có `_` đầu?** Quy ước dbt: `_` đưa file config lên đầu danh sách khi sort,
và báo cho người đọc biết đây không phải model.

---

## Bước 4 — Model silver

`silver_order_items.sql`:

```sql
-- Silver: chỉ giữ dòng hàng đạt kiểm tra chất lượng ở bronze, thêm thành tiền
with source as (
    select * from {{ source('bronze', 'order_items') }}
)

select
    order_id,
    product_id,
    quantity,
    unit_price,
    coalesce(discount_amount, 0)                            as discount_amount,
    quantity * unit_price - coalesce(discount_amount, 0)    as line_amount,
    promo_id
from source
where _is_valid
```

Giải thích từng phần:

- **`{{ source('bronze', 'order_items') }}`** — cú pháp Jinja, dbt thay bằng tên bảng thật
  `iceberg.bronze.order_items` lúc compile. **Tại sao không viết thẳng tên bảng?** Vì viết
  thẳng thì dbt không biết model này phụ thuộc bronze, và `dbt build` có thể chạy sai thứ tự.
- **`with source as (...)`** — CTE. Với model ngắn thế này nó không đổi kết quả, nhưng là quy
  ước của repo: mọi model mở đầu bằng CTE khai báo nguồn, nên người đọc luôn biết dữ liệu từ
  đâu ra ngay 3 dòng đầu.
- **`where _is_valid`** — **đây là chỗ dòng hỏng bị loại**. Bronze gắn cờ, silver mới thực sự
  quyết định. Muốn đổi định nghĩa "hợp lệ" thì sửa ở đây, dữ liệu bronze vẫn nguyên vẹn.
- **Không `select *`** — liệt kê tay từng cột để cột kỹ thuật (`_is_valid`, `_source_file`,
  `_ingested_at`, `_invalid_reason`) không lọt xuống silver. Người dùng silver không cần biết
  chuyện nội bộ của quá trình ingest.
- **`coalesce(discount_amount, 0)`** — biến `NULL` thành `0`. **Tại sao bắt buộc?** Vì trong
  SQL, `NULL` lây lan qua phép tính: `100 - NULL = NULL`. Không có `coalesce`, mọi dòng không
  khuyến mãi sẽ có `line_amount = NULL` và `sum()` ở gold bỏ qua chúng — **doanh thu thiếu
  hụt mà không có lỗi nào báo**. Đây là loại bug nguy hiểm nhất: sai âm thầm.
- **`line_amount`** — cột dẫn xuất. **Tại sao đặt ở silver mà không ở bronze?** Bronze chỉ
  mô tả nguồn; công thức tính tiền là quyết định business, có thể đổi (thuế? phí ship?). Đặt
  ở silver thì đổi công thức chỉ cần `dbt build`, không phải ingest lại 714k dòng.
  **Tại sao không đặt ở gold?** Vì nhiều model gold sẽ cùng cần nó — định nghĩa một lần ở
  silver để mọi report tính tiền giống nhau. Đó chính là ý nghĩa của silver: *một sự thật
  chung, đã sạch, ai cũng dùng được*.

`silver_order_items.yml` — file test đi kèm:

```yaml
version: 2

models:
  - name: silver_order_items
    description: "Dòng hàng đã làm sạch (silver layer)"
    columns:
      - name: order_id
        description: "Không unique — một đơn có nhiều dòng hàng"
        tests:
          - not_null
          - relationships:
              to: ref('silver_orders')
              field: order_id
      - name: line_amount
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
```

**Test dbt hoạt động thế nào?** Mỗi test compile thành một câu SQL đếm dòng vi phạm. Trả về
0 dòng = PASS. Không có gì huyền bí — bạn xem được SQL thật trong `dbt/hdh_dbt/target/compiled/`.

Từng test và lý do có nó:

- **`not_null` trên `order_id`** — dòng hàng không thuộc đơn nào là dữ liệu rác, join sẽ mất.
- **`relationships`** — kiểm tra mọi `order_id` ở đây đều tồn tại trong `silver_orders`. **Đây
  là test giá trị nhất cho bảng fact**, vì nó bắt được thứ mà rule ở bronze *về nguyên tắc
  không thể thấy*: job bronze xử lý từng bảng độc lập, nó không biết bảng `orders` có gì.
  Chỉ ở silver, khi cả hai bảng đã cùng trong SQL, mới kiểm tra chéo được. Test này fail
  nghĩa là có dòng hàng mồ côi → `join` ở gold sẽ âm thầm nuốt mất doanh thu.
- **`accepted_range` trên `line_amount`** — tiền âm là dấu hiệu công thức sai (ví dụ chiết
  khấu lớn hơn giá trị đơn). `min_value: 0` cho phép bằng 0, chặn số âm.

**Cẩn thận với `unique`.** Bảng **fact** như `order_items` có `order_id` lặp lại — một đơn
nhiều dòng hàng — nên test `unique` ở đây **sẽ fail**. Chỉ đặt `unique` khi cột thật sự là
khoá chính, như `order_id` của bảng `orders`.

**Tại sao `dbt_utils.accepted_range` mà không phải `accepted_range`?** Đây là test từ package
`dbt_utils` khai báo ở `packages.yml`, phải gọi kèm tên package. Nếu chưa chạy `make dbt-deps`
thì dbt báo lỗi không tìm thấy macro.

---

## Bước 5 — Model gold

`gold_revenue_daily.sql`:

```sql
-- Gold: doanh thu theo ngày.
-- order_items không có cột ngày nên phải join với orders để lấy order_date.
with orders as (
    select * from {{ ref('silver_orders') }}
),

items as (
    select * from {{ ref('silver_order_items') }}
)

select
    o.order_date,
    count(distinct o.order_id)      as num_orders,
    count(*)                        as num_lines,
    sum(i.quantity)                 as num_units,
    sum(i.line_amount)              as revenue,
    sum(i.discount_amount)          as total_discount,
    sum(i.line_amount) / count(distinct o.order_id) as avg_order_value
from items i
join orders o on i.order_id = o.order_id
group by o.order_date
```

**`ref()` khác `source()` chỗ nào?** `source()` trỏ tới bảng do thứ khác tạo (Spark);
`ref()` trỏ tới model do chính dbt tạo. `ref` là thứ dựng nên đồ thị phụ thuộc — nhờ nó
`dbt build` biết phải chạy `silver_order_items` xong mới tới `gold_revenue_daily`, mà bạn
không cần khai báo thứ tự ở đâu cả.

**Tại sao phải join?** `order_items` không có cột ngày. Ngày nằm ở `orders`. Muốn nhóm doanh
thu theo ngày thì bắt buộc phải kéo `order_date` từ bảng kia sang — đây chính là lý do repo
tách hai bảng và cũng là lý do test `relationships` ở bước 4 quan trọng.

Ba điểm dễ sai khi viết model gold:

- **Phân biệt `num_orders` với `num_lines`.** Sau khi join, mỗi dòng là một *dòng hàng*, không
  phải một *đơn*. `count(*)` cho ra số dòng hàng; muốn đếm đơn phải `count(distinct order_id)`.
  Đây là lỗi kinh điển: đơn có 5 sản phẩm sẽ bị đếm thành 5 đơn, và mọi chỉ số bình quân sai
  theo. Repo này để cả hai cột cạnh nhau, đặt tên rõ ràng, để không ai nhầm.
- **`join` (inner) loại đơn không có dòng hàng nào.** Muốn giữ cả đơn rỗng thì `left join` từ
  `orders` sang `items`, nhưng khi đó `revenue` ra `NULL` và phải `coalesce`. Chọn inner join
  ở đây là *quyết định có chủ ý*: bảng này nói về doanh thu, đơn không có hàng thì không đóng
  góp doanh thu.
- **Không cần `order by`.** Model materialized thành `table` ghi kết quả xuống file Iceberg,
  và thứ tự sắp xếp **không được bảo toàn** khi đọc lại — `order by` ở đây chỉ tốn công sort
  vô ích. Sắp xếp ở câu query cuối cùng lúc bạn đọc bảng.

`gold_revenue_daily.yml`:

```yaml
version: 2

models:
  - name: gold_revenue_daily
    description: "Doanh thu theo ngày (gold layer), tính từ dòng hàng join với đơn"
    columns:
      - name: order_date
        tests:
          - not_null
          - unique          # group by order_date -> mỗi ngày đúng 1 dòng
      - name: num_orders
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: false   # ngày đã lên bảng thì phải có ít nhất 1 đơn
      - name: revenue
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0       # cho phép 0, không cho phép âm
```

**Tại sao `unique` trên `order_date` ở gold nhưng không ở silver?** Vì `group by order_date`
đảm bảo mỗi ngày đúng một dòng. Test này trông thừa — nhưng nó là **lưới an toàn cho tương
lai**: hôm nào đó ai đó thêm `product_id` vào `group by`, hạt mịn của bảng đổi mà tên bảng
vẫn là "daily", test này fail ngay và chặn lỗi trước khi report sai.

**Khác biệt giữa hai mức `inclusive`** — đọc kỹ chỗ này vì nó thể hiện cách nghĩ khi viết test:

- `num_orders` dùng `inclusive: false` (tức `> 0`): một ngày **có mặt trong bảng** thì bắt
  buộc phải có ít nhất một đơn, vì `group by` không thể sinh ra nhóm rỗng. Ra 0 nghĩa là logic
  hỏng ở đâu đó.
- `revenue` cho phép `0` nhưng chặn số âm: doanh thu bằng 0 là *có thể* (chiết khấu bằng đúng
  giá trị đơn), còn âm thì chắc chắn công thức tính tiền sai.

Nguyên tắc: **test phải chặn đúng cái không được phép xảy ra, không chặn cái hiếm gặp.** Test
quá chặt sẽ fail vì dữ liệu hợp lệ nhưng bất thường, và người ta sẽ quen tay bỏ qua nó.

### Đối chiếu chéo giữa các model gold

Khi có hai model gold cùng nói về một thứ, hãy đối chiếu chúng — cách bắt lỗi này mạnh hơn
test đơn lẻ, vì nó kiểm tra **hai đường tính độc lập có gặp nhau không**. `gold_orders_daily`
đếm đơn từ `silver_orders`; `gold_revenue_daily` đếm đơn qua `silver_order_items`. Hai con số
phải khớp:

```sql
SELECT d.order_date, d.num_orders AS from_orders, r.num_orders AS from_items
FROM iceberg.analytics.gold_orders_daily d
LEFT JOIN iceberg.analytics.gold_revenue_daily r ON d.order_date = r.order_date
WHERE d.num_orders <> coalesce(r.num_orders, -1);
```

Không trả về dòng nào = khớp. Nếu có dòng lệch nghĩa là tồn tại đơn không có dòng hàng nào
(hoặc ngược lại) — **cần điều tra chứ đừng vội sửa query cho hết lệch**. Con số lệch đang nói
cho bạn biết một sự thật về dữ liệu.

---

## Chạy

```bash
make ingest-order-items    # chỉ chạy job mới (make ingest chạy lại toàn bộ bảng)
make dbt                   # = dbt build: chạy model + test
```

**Tại sao `dbt build` mà không phải `dbt run`?** `dbt run` chỉ tạo model; `dbt build` tạo model
**và chạy test ngay sau mỗi model**, theo đúng thứ tự phụ thuộc. Nghĩa là nếu silver fail test,
gold **không được build** từ dữ liệu hỏng đó. Đó là lý do `make dbt` dùng `build`.

Kết quả mong đợi:

```
OK created sql view model analytics.silver_order_items
OK created sql table model analytics.gold_revenue_daily .... [CREATE TABLE (3_833 rows)]
Done. PASS=30 WARN=0 ERROR=0 SKIP=0 TOTAL=30
```

**Đọc dòng `Done.` ở cuối, đừng chỉ nhìn model đã tạo hay chưa.** `ERROR` nghĩa là model không
tạo được; test fail thì model vẫn tạo nhưng `dbt build` trả exit code khác 0. `SKIP` nghĩa là
model bị bỏ qua vì thứ nó phụ thuộc đã fail — thường lỗi thật nằm ở model phía trên.

Kiểm tra bảng đã vào catalog:

```bash
docker compose exec trino trino --catalog iceberg --execute "SHOW TABLES FROM bronze;"
docker compose exec trino trino --catalog iceberg --execute "SHOW TABLES FROM analytics;"
```

---

## Chạy lại job — ghi đè, snapshot, time travel

**Chạy `make ingest` hai lần có nhân đôi dữ liệu không? Không.** Job ghi đè toàn bộ bảng, nên
chạy bao nhiêu lần cũng ra kết quả y hệt. Bạn có thể vô tư ingest lại khi nghi ngờ dữ liệu.

Thí nghiệm thật trên `bronze.promotions` (50 dòng), chạy `ingest-promotions` hai lần:

| | Số dòng | `_ingested_at` | Số snapshot |
|---|---:|---|---:|
| Sau lần 1 | 50 | 07:41:22 | 1 |
| Sau lần 2 | **50** (không phải 100) | **08:00:38** | **2** |

Số dòng giữ nguyên → ghi đè, không cộng thêm. `_ingested_at` đổi → dữ liệu thật sự được viết
lại. Nhưng **số snapshot tăng lên 2** — đó là phần đáng biết.

### Vì sao idempotent: createOrReplace

Nằm ở `spark/jobs/common/iceberg.py`:

```python
writer.tableProperty("format-version", "2").createOrReplace()
```

`createOrReplace` = "tạo mới nếu chưa có, **thay thế toàn bộ** nếu đã có". Không có bước kiểm
tra dòng nào đã tồn tại — cả bảng bị viết lại từ đầu. Đơn giản, và luôn đúng.

### Dữ liệu cũ không biến mất: time travel

Iceberg không xoá phiên bản cũ khi ghi đè; nó tạo một **snapshot** mới và giữ lại cái cũ. Xem
lịch sử:

```sql
SELECT snapshot_id, committed_at, operation, summary['total-records'] AS so_dong
FROM bronze."promotions$snapshots" ORDER BY committed_at;
```

```
9057656408815972620 | 2026-07-17 07:41:24 | overwrite | 50
3885891985372690357 | 2026-07-17 08:00:41 | overwrite | 50
```

Và đọc lại được phiên bản cũ bằng `FOR VERSION AS OF`:

```sql
SELECT count(*), max(_ingested_at) FROM bronze.promotions
FOR VERSION AS OF 9057656408815972620;
-- 50 dòng, _ingested_at = 07:41:22  <- trạng thái TRƯỚC khi ghi đè
```

**Tại sao điều này hữu ích?** Lỡ chạy job với rule sai và ghi đè mất bảng tốt: bạn so sánh
được ngay trước/sau, hoặc rollback. Không có time travel thì lần ghi đè hỏng là mất dữ liệu
vĩnh viễn — và đây chính là lý do ghi đè toàn bộ mà vẫn an toàn.

### Cái giá: file cũ vẫn nằm trên MinIO

Snapshot cũ còn đọc được nghĩa là **file dữ liệu cũ vẫn chiếm chỗ**, không tự xoá.

| Bảng | Kích thước | Chạy lại 10 lần tốn |
|---|---:|---:|
| `promotions` | 5 KB | ~50 KB — không đáng kể |
| `orders` | 44 MB | **~440 MB** |

Bảng chỉ hiển thị một phiên bản, nhưng đĩa thì giữ tất cả. Với dataset học tập, cách dọn đơn
giản nhất là `make clean` (xoá sạch volume MinIO) rồi ingest lại. Iceberg cũng có thủ tục
`expire_snapshots` để xoá snapshot cũ hơn một mốc thời gian, nhưng ở quy mô này thì chưa cần.

### Cảnh báo: điều này KHÔNG đúng với append

Nếu sau này bạn đổi sang **incremental** — nạp thêm dữ liệu mới mà giữ dữ liệu cũ — thì tính
idempotent biến mất:

| Cách ghi | Chạy lại lần 2 | An toàn? |
|---|---|---|
| `createOrReplace()` (hiện tại) | Thay toàn bộ → vẫn 50 dòng | ✅ |
| `append()` | Cộng thêm → **100 dòng** | ❌ nhân đôi |
| `MERGE INTO` theo khoá | Cập nhật dòng trùng, thêm dòng mới | ✅ |

Ghi đè toàn bộ chỉ khả thi khi dữ liệu đủ nhỏ để viết lại hết trong vài giây — đúng với repo
này (bảng lớn nhất 714k dòng, ~6 giây). Khi bảng lên hàng trăm triệu dòng thì không còn khả
thi, lúc đó phải chuyển sang `MERGE INTO` và tự lo chuyện idempotent.

---

## Truy vấn

**Tên bảng luôn có 3 cấp: `catalog.schema.table`.**

- **catalog** (`iceberg`) — kết nối tới kho dữ liệu, khai báo ở `trino/etc/catalog/iceberg.properties`.
- **schema** — bronze nằm ở `bronze`; **cả silver lẫn gold đều ở `analytics`**, vì dbt build
  mọi model vào schema khai báo trong `dbt/profiles.yml`. Tên `silver_`/`gold_` là *tiền tố
  quy ước*, không phải schema riêng.

### Cách 1 — Trino CLI (dùng nhiều nhất)

```bash
docker compose exec trino trino --catalog iceberg --schema analytics
```
```sql
SELECT * FROM silver_order_items LIMIT 20;
```

Đặt sẵn `--catalog`/`--schema` thì khỏi gõ tên đầy đủ mỗi câu. Không đặt thì phải viết
`iceberg.analytics.silver_order_items`.

### Cách 2 — Chạy một câu rồi thoát

```bash
docker compose exec trino trino --catalog iceberg --execute \
  "SELECT count(*) FROM bronze.order_items;"
```

Hợp khi kiểm tra nhanh hoặc viết vào script. Đây chính là cách target `query` trong `Makefile`
hoạt động.

### Cách 3 — Spark SQL

```bash
make spark-sql
```

**Khi nào dùng Spark thay vì Trino?** Gần như không, cho việc query khám phá — Trino nhanh hơn
nhiều vì nó sinh ra để phục vụ query tương tác, còn Spark tối ưu cho xử lý theo lô. Chỉ dùng
Spark SQL khi cần kiểm tra ngay trong môi trường Spark lúc debug job ingest.

### Query kiểm tra sau khi ingest

Đếm dòng và phân bố lỗi — **đây chính là lý do gắn cờ `_is_valid` ở bronze**:

```sql
SELECT count(*) AS total, count_if(_is_valid) AS valid, count_if(NOT _is_valid) AS invalid
FROM iceberg.bronze.order_items;

SELECT _invalid_reason, count(*)
FROM iceberg.bronze.order_items
WHERE NOT _is_valid
GROUP BY _invalid_reason;
```

Câu thứ hai cho bạn thứ tự ưu tiên sửa lỗi: lỗi nào nhiều dòng nhất thì đáng xử lý trước.

Kiểm tra toàn vẹn tham chiếu giữa hai bảng:

```sql
SELECT count(*)
FROM iceberg.analytics.silver_order_items i
LEFT JOIN iceberg.analytics.silver_orders o ON i.order_id = o.order_id
WHERE o.order_id IS NULL;
```

> **Nếu `invalid` ra 0 trên toàn bộ dữ liệu, đừng vội mừng.** Có thể dữ liệu vốn sạch, nhưng
> cũng có thể rule trong `transform()` chưa đủ chặt — một bộ rule không bắt được gì thì không
> phân biệt được với việc không có rule nào. Kiểm chứng bằng query ràng buộc mà rule chưa
> cover, như query toàn vẹn tham chiếu ở trên (bronze không thể tự kiểm tra điều này vì nó
> xử lý từng bảng độc lập).

---

## Lỗi hay gặp

| Triệu chứng | Nguyên nhân | Cách sửa |
|---|---|---|
| `Table 'bronze.x' does not exist` khi chạy dbt | Chưa chạy job Spark, hoặc `schema:` ở `_sources.yml` không khớp `NAMESPACE` | Chạy `make ingest-<bảng>`; đối chiếu hai chỗ khai báo |
| `Compilation Error: source 'bronze.x' not found` | Thiếu mục trong `_sources.yml` | Thêm vào bước 3 |
| `Compilation Error: macro accepted_range not found` | Chưa cài package | `make dbt-deps` |
| Test `unique` fail trên bảng fact | Cột không phải khoá chính (một đơn nhiều dòng hàng) | Bỏ test `unique`, giữ `not_null` |
| Job Spark OOM lúc ghi | Partition quá mịn → hàng nghìn file tí hon | Đổi `F.days()` sang `F.months()`, xem [Chọn partition](#chọn-partition) |
| `SparkContext should only be created...` | Đặt `F.months()`/`F.bucket()` ở cấp module | Bọc trong hàm `partition_columns()` |
| `make ingest` không chạy gì, báo "up to date" | Target thiếu trong `.PHONY`, Make tưởng là tên file | Thêm target vào `.PHONY` |
| `missing separator` trong Makefile | Thụt dòng bằng space | Thay bằng Tab |
| Model dbt không được build | File `.sql` sai thư mục | Đặt trong `models/silver/` hoặc `models/gold/` |
| MinIO phình to dù số dòng không đổi | Mỗi lần ingest lại tạo snapshot mới, file cũ không tự xoá | `make clean` rồi ingest lại — xem [Chạy lại job](#chạy-lại-job--ghi-đè-snapshot-time-travel) |
| Ingest lại xong số dòng nhân đôi | Đã đổi sang `append()` thay vì `createOrReplace()` | Dùng `MERGE INTO` hoặc quay lại ghi đè |
| `revenue` ra `NULL` | `NULL` lây qua phép tính | `coalesce(cột, 0)` trước khi tính |
| Số đơn ở gold lớn bất thường | Dùng `count(*)` sau join thay vì `count(distinct order_id)` | Xem [Bước 5](#bước-5--model-gold) |

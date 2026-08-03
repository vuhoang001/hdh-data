"""
Đọc dữ liệu nguồn.

ĐÂY LÀ SEAM GIỮA LOCAL VÀ PRODUCTION. Hiện tại nguồn là file CSV trong data/; ở production
nguồn thường là bảng Postgres, một API, hay file Parquet trên S3. Chỗ duy nhất phải đổi là
module này — `transform()` của từng bảng, silver, gold và test đều không biết dữ liệu đến
từ đâu, nên không sửa dòng nào.

Thêm một loại nguồn = viết một hàm `read_*` rồi đăng ký vào SOURCE_READERS. Sau đó khai
`type: <loại>` cho bảng đó trong ingestion/config/sources.yml.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def read_csv(
    spark: SparkSession,
    source: str,
    schema: StructType,
    header: bool = True,
    date_format: str = "yyyy-MM-dd",
    timestamp_format: str = "yyyy-MM-dd'T'HH:mm:ss",
) -> DataFrame:
    """Đọc CSV với schema tường minh. Job gọi hàm này tự khai báo schema của mình.

    `source` là đường dẫn, có thể local (/opt/spark/data/x.csv) hoặc S3 (s3a://...).

    KHÔNG dùng inferSchema: nó đoán sai lặng lẽ (cột zip toàn số thành integer, `01234`
    mất số 0 đầu), đắt (phải quét file hai lượt), và che mất việc nguồn đổi cột.
    """
    return (
        spark.read
        .option("header", header)
        .option("dateFormat", date_format)
        .option("timestampFormat", timestamp_format)
        .schema(schema)
        .csv(source)
    )


# Đăng ký các loại nguồn đọc được. Test kiểm tra sources.yml không khai loại nào ngoài đây.
#
# Khi production đọc từ Postgres, thêm vào đây:
#
#     def read_jdbc(spark, source, schema, **opts):
#         return spark.read.format("jdbc").option("url", source).option(...).load()
#
#     SOURCE_READERS = {"csv": read_csv, "postgres": read_jdbc}
#
# rồi đổi `type: csv` thành `type: postgres` trong sources.yml cho bảng tương ứng.
# Không có file nào khác phải sửa.
SOURCE_READERS = {
    "csv": read_csv,
}


def read_source(
    spark: SparkSession, source_type: str, source: str, schema: StructType
) -> DataFrame:
    """Đọc nguồn theo loại đã khai. Loại lạ thì báo lỗi ngay, không đoán."""
    reader = SOURCE_READERS.get(source_type)
    if reader is None:
        raise ValueError(
            f"Không đọc được nguồn loại '{source_type}'. "
            f"Các loại đã đăng ký: {sorted(SOURCE_READERS)}. "
            f"Thêm reader mới vào ingestion/common/io.py."
        )
    return reader(spark, source, schema)

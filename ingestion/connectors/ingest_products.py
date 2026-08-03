"""
Bronze: data/products.csv -> bronze.products

Chạy:
    make lake-ingest-products
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from common import BronzeJob, run_job

SCHEMA = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("size", StringType(), True),
    StructField("color", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("cogs", DoubleType(), True),
])

VALID_CATEGORIES = ["casual", "genz", "outdoor", "streetwear"]
VALID_SEGMENTS = [
    "activewear", "all-weather", "balanced", "everyday",
    "performance", "premium", "standard", "trendy",
]
VALID_SIZES = ["s", "m", "l", "xl"]


def transform(df: DataFrame) -> DataFrame:
    """Chuẩn hoá text + gắn cờ chất lượng. Không lọc bỏ dòng — bronze giữ nguyên số dòng nguồn."""
    normalized = (
        df
        # product_name giữ nguyên hoa/thường: tên thương mại, không phải mã phân loại
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("segment", F.lower(F.trim(F.col("segment"))))
        .withColumn("size", F.lower(F.trim(F.col("size"))))
        .withColumn("color", F.lower(F.trim(F.col("color"))))
    )

    invalid_reason = F.concat_ws(
        ", ",
        F.when(F.col("product_name").isNull(), F.lit("product_name_missing")),
        F.when(F.col("price").isNull() | (F.col("price") <= 0), F.lit("price_invalid")),
        F.when(F.col("cogs").isNull() | (F.col("cogs") < 0), F.lit("cogs_invalid")),
        # Giá vốn cao hơn giá bán = bán lỗ. Có thể là thật (xả hàng) nhưng thường là lỗi nhập liệu,
        # nên gắn cờ để người dùng silver tự quyết định.
        F.when(F.col("cogs") > F.col("price"), F.lit("cogs_above_price")),
        F.when(~F.col("category").isin(*VALID_CATEGORIES), F.lit("category_unknown")),
        F.when(~F.col("segment").isin(*VALID_SEGMENTS), F.lit("segment_unknown")),
        F.when(~F.col("size").isin(*VALID_SIZES), F.lit("size_unknown")),
    )

    return (
        normalized
        .withColumn("_invalid_reason", F.when(invalid_reason == "", None).otherwise(invalid_reason))
        .withColumn("_is_valid", F.col("_invalid_reason").isNull())
    )


# Bảng dimension rất nhỏ (~2.4k dòng, 192KB), không có cột ngày -> để nguyên một file.
JOB = BronzeJob(
    table="products",
    source_csv="products.csv",
    schema=SCHEMA,
    transform=transform,
)

if __name__ == "__main__":
    run_job(JOB)

"""SparkSession + logger. Cấu hình Iceberg/MinIO nằm ở spark/conf/spark-defaults.conf."""
import logging

from pyspark.sql import SparkSession

DEFAULT_LOG_LEVEL = "WARN"


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def build_spark_session(app_name: str, log_level: str = DEFAULT_LOG_LEVEL) -> SparkSession:
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark

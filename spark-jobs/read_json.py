from __future__ import annotations

import logging
from datetime import datetime
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.functions import col, floor, to_timestamp, unix_timestamp
from pyspark.sql.types import DecimalType

LAST_VALUE_FPATH = Path("/opt/spark/data/publish_last_value.txt")
POSTGRES_JDBC_URL = "jdbc:postgresql://192.168.28.3:5431/postgres"
BIGQUERY_TABLE = "olist_dataset.olist_orders"
GCS_BUCKET = "olist_data_buckets"
PARQUET_PATH = f"gs://{GCS_BUCKET}/orders/orders.parquet"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
LOGGER = logging.getLogger(__name__)


def read_last_value(path: Path) -> datetime:
    if not path.exists():
        default = "2000-01-01 00:00:00.000"
        path.write_text(default)
        return datetime.strptime(default, "%Y-%m-%d %H:%M:%S.%f")

    raw = path.read_text().strip()
    return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f")


def write_last_value(path: Path, value: datetime) -> None:
    """최신 처리 시각 저장."""
    path.write_text(value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("pyspark-gcs-connection")
        .master("local[*]")
        .config("spark.jars", "/opt/spark/data/gcs-connector-hadoop3-2.2.9-shaded.jar")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/opt/spark/data/key.json")
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.33.0")
        .config("temporaryGcsBucket", "olist_archive")
        .getOrCreate()
    )


def load_incremental(spark: SparkSession, since: datetime) -> DataFrame:
    """PostgreSQL에서 *since* 이후 데이터만 읽어 옴."""
    query = f"""
        SELECT *
        FROM pubsub.olist_pubsub
        WHERE timestamp > TO_TIMESTAMP('{since}', 'YYYY-MM-DD HH24:MI:SS.MS')
        ORDER BY publish_time DESC
    """
    return (
        spark.read.format("jdbc")
        .option("url", POSTGRES_JDBC_URL)
        .option("driver", "org.postgresql.Driver")
        .option("query", query)
        .option("user", "postgres")
        .option("password", "postgres")
        .load()
    )


def clean(df: DataFrame) -> DataFrame:
    if df.rdd.isEmpty():
        return df
    for c in ("price", "freight_value", "payment_value"):
        df = df.withColumn(c, F.round(col(c), 2))
    df = (
        df.withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date")))
          .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp")))
          .withColumn(
              "time_diff_seconds",
              unix_timestamp(col("order_delivered_customer_date")) - unix_timestamp(col("order_purchase_timestamp")),
          )
          .withColumn("delivery_time", floor(col("time_diff_seconds") / 3600))
    )
    df = df.drop(
        "id",
        "order_estimated_delivery_date",
        "order_approved_at",
        "order_delivered_carrier_date",
        "delivery_attempt",
        "ordering_key",
        "time_diff_seconds",
    )
    dec_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, DecimalType)]
    for c in dec_cols:
        df = df.withColumn(c, col(c).cast("double"))

    return df.na.drop()


def write_outputs(df: DataFrame) -> None:
    df.write.format("bigquery") \
        .option("parentProject", "olist-data-engineering") \
        .option("table", BIGQUERY_TABLE) \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()

    df.write.mode("append").parquet(PARQUET_PATH)
    LOGGER.info("BigQuery & GCS 저장 완료")


# ──────────────────────────────
# 엔트리 포인트
# ──────────────────────────────
def main() -> None:
    last_ts = read_last_value(LAST_VALUE_FPATH)
    spark = build_spark()

    df = load_incremental(spark, last_ts)
    if df.rdd.isEmpty():
        LOGGER.info("새로운 데이터가 없습니다.")
        return

    # 최신 타임스탬프 저장
    write_last_value(LAST_VALUE_FPATH, df.first()["timestamp"])

    processed = clean(df)
    write_outputs(processed)
    LOGGER.info("파이프라인 완료")


if __name__ == "__main__":
    main()

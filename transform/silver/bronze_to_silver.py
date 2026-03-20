"""
Silver layer: Bronze → Silver Delta table.

Applies:
  - Type casting and null handling
  - Deduplication (keep latest record per order_id)
  - Great Expectations data quality checks (fail-fast)
  - SCD Type 2 pattern (_is_current, _valid_from, _valid_to)
  - PII masking (customer_id hashed with SHA-256)
"""
from __future__ import annotations

import argparse
import hashlib
import logging
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

from quality.expectations import run_order_expectations
from utils.delta_utils import upsert_to_delta
from utils.schema_registry import (
    VALID_CURRENCIES,
    VALID_ORDER_STATUSES,
)
from utils.spark_session import get_spark

logger = logging.getLogger(__name__)

BRONZE_PATH = "s3a://{bucket}/bronze/orders"
SILVER_PATH = "s3a://{bucket}/silver/orders"


def run(env: str, date: str, bucket: str) -> None:
    """
    Transform Bronze orders into Silver for a given date.

    Args:
        env:    Environment tag (staging | prod).
        date:   Partition date in YYYY-MM-DD format.
        bucket: S3 bucket name.
    """
    spark = get_spark(app_name=f"silver_orders_{env}_{date}")

    bronze_path = BRONZE_PATH.format(bucket=bucket)
    silver_path = SILVER_PATH.format(bucket=bucket)

    # Read only today's Bronze partition — avoids full table scan
    bronze_df = (
        spark.read.format("delta")
        .load(bronze_path)
        .filter(F.col("_ingest_timestamp").cast("date") == date)
    )

    logger.info("Bronze rows for date=%s: %d", date, bronze_df.count())

    # Run transformation steps in order
    cleaned_df   = _clean(bronze_df)
    validated_df = _validate(spark, cleaned_df)
    typed_df     = _cast_types(validated_df)
    deduped_df   = _deduplicate(typed_df)
    masked_df    = _mask_pii(deduped_df)
    silver_df    = _add_scd2_columns(masked_df)

    upsert_to_delta(
        spark=spark,
        source_df=silver_df,
        target_path=silver_path,
        merge_keys=["order_id", "_valid_from"],
    )

    logger.info("Silver write complete — %s", silver_path)


# ── Transformation steps ──────────────────────────────────────────────────────

def _clean(df: DataFrame) -> DataFrame:
    """
    Drop rows missing business-critical fields.
    Filter out invalid enum values for status and currency.
    Trim whitespace from all string columns.
    """
    # Drop rows where key fields are null
    df = df.dropna(subset=["order_id", "customer_id", "order_amount"])

    # Remove rows with unknown status or currency values
    df = df.filter(F.col("status").isin(list(VALID_ORDER_STATUSES)))
    df = df.filter(F.col("currency").isin(list(VALID_CURRENCIES)))

    # Trim leading/trailing whitespace from string columns
    for col in ["order_id", "customer_id", "status", "currency"]:
        df = df.withColumn(col, F.trim(F.col(col)))

    return df


def _validate(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Run Great Expectations suite against the cleaned DataFrame.
    Raises DataQualityError if any expectation fails — job stops here,
    nothing bad gets written to Silver.
    """
    run_order_expectations(spark, df, layer="silver")
    return df


def _cast_types(df: DataFrame) -> DataFrame:
    """
    Cast raw string columns to their correct types.
    Bronze stores everything as strings to avoid parse errors at ingest.
    """
    return (
        df.withColumn("order_amount", F.col("order_amount").cast("decimal(18,2)"))
          .withColumn("created_at",   F.to_timestamp("created_at"))
          .withColumn("updated_at",   F.to_timestamp("updated_at"))
    )


def _deduplicate(df: DataFrame) -> DataFrame:
    """
    Keep only the latest record per order_id.
    Uses a window function ordered by updated_at descending.
    """
    window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


def _mask_pii(df: DataFrame) -> DataFrame:
    """
    Replace customer_id with a one-way SHA-256 hash.
    The hash is deterministic — same input always gives same output —
    so joins between tables still work, but the real ID is never exposed.
    """
    mask_udf = F.udf(
        lambda val: hashlib.sha256(val.encode()).hexdigest() if val else None
    )
    return df.withColumn("customer_id", mask_udf(F.col("customer_id")))


def _add_scd2_columns(df: DataFrame) -> DataFrame:
    """
    Attach SCD Type 2 tracking columns.
    _is_current = 1 means this is the active version of the row.
    _valid_to = NULL means the row is still active (no end date yet).
    """
    now = datetime.now(timezone.utc).isoformat()
    return (
        df.withColumn("_is_current", F.lit(1))
          .withColumn("_valid_from",  F.lit(now).cast("timestamp"))
          .withColumn("_valid_to",    F.lit(None).cast("timestamp"))
    )


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(description="Silver transformation job")
    parser.add_argument("--env",    choices=["staging", "prod"], required=True)
    parser.add_argument("--date",   required=True, help="YYYY-MM-DD")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()

    run(env=args.env, date=args.date, bucket=args.bucket)
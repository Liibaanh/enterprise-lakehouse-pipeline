"""
Gold layer: Silver → Gold daily revenue aggregates.

Computes business-level KPIs consumed directly by BI tools and analysts:
  - Daily gross revenue per currency
  - Total order volume
  - Average order value

Partitioned by report_date for fast time-range queries.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F

from utils.delta_utils import optimize_table, upsert_to_delta
from utils.spark_session import get_spark

logger = logging.getLogger(__name__)

SILVER_PATH       = "s3a://{bucket}/silver/orders"
GOLD_REVENUE_PATH = "s3a://{bucket}/gold/daily_revenue"


def run(env: str, date: str, bucket: str) -> None:
    """
    Aggregate Silver orders into Gold daily revenue for a given date.

    Args:
        env:    Environment tag (staging | prod).
        date:   Partition date in YYYY-MM-DD format.
        bucket: S3 bucket name.
    """
    spark = get_spark(app_name=f"gold_daily_revenue_{env}_{date}")

    silver_path = SILVER_PATH.format(bucket=bucket)
    gold_path   = GOLD_REVENUE_PATH.format(bucket=bucket)

    # Read only current Silver rows — ignore historical SCD2 versions
    silver_df = (
        spark.read.format("delta")
        .load(silver_path)
        .filter(F.col("_is_current") == 1)
        # Only count orders that actually generated revenue
        .filter(F.col("status").isin(["confirmed", "shipped", "delivered"]))
    )

    # Aggregate to daily revenue per currency
    daily_revenue = (
        silver_df
        .withColumn("report_date", F.col("created_at").cast("date").cast("string"))
        .groupBy("report_date", "currency")
        .agg(
            F.count("order_id")
             .alias("total_orders"),
            F.sum("order_amount").cast("decimal(18,2)")
             .alias("gross_revenue"),
            F.avg("order_amount").cast("decimal(18,2)")
             .alias("avg_order_value"),
        )
        # Attach pipeline metadata
        .withColumn(
            "_updated_at",
            F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp")
        )
        # Only process today's data
        .filter(F.col("report_date") == date)
    )

    logger.info("Gold rows for date=%s: %d", date, daily_revenue.count())

    upsert_to_delta(
        spark=spark,
        source_df=daily_revenue,
        target_path=gold_path,
        merge_keys=["report_date", "currency"],
    )

    # Compact small files and sort by most common query pattern
    optimize_table(
        spark,
        gold_path,
        z_order_cols=["report_date", "currency"]
    )

    logger.info("Gold write complete — %s", gold_path)


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(description="Gold aggregation job")
    parser.add_argument("--env",    choices=["staging", "prod"], required=True)
    parser.add_argument("--date",   required=True, help="YYYY-MM-DD")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()

    run(env=args.env, date=args.date, bucket=args.bucket)
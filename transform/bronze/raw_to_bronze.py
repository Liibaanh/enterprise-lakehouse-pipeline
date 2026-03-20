"""
Bronze layer: Raw S3 → Bronze Delta table.

Reads raw JSON files from the S3 landing zone, attaches pipeline
metadata, and writes to the Bronze Delta table using an idempotent
MERGE. No business logic here — Bronze is a faithful copy of the source.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from pyspark.sql import functions as F

from utils.delta_utils import upsert_to_delta
from utils.schema_registry import BRONZE_ORDER_SCHEMA
from utils.spark_session import get_spark

logger = logging.getLogger(__name__)

# ── Paths ─────────────────────────────────────────────────────────────────────

S3_RAW_BASE  = "s3a://{bucket}/raw/orders"
BRONZE_PATH  = "s3a://{bucket}/bronze/orders"


def run(env: str, date: str, bucket: str) -> None:
    """
    Ingest raw order files for a given date into the Bronze Delta table.

    Args:
        env:    Environment tag (staging | prod).
        date:   Partition date in YYYY-MM-DD format.
        bucket: S3 bucket name.
    """
    spark = get_spark(app_name=f"bronze_orders_{env}_{date}")

    raw_path    = S3_RAW_BASE.format(bucket=bucket) + f"/date={date}/"
    bronze_path = BRONZE_PATH.format(bucket=bucket)

    logger.info("Reading raw files from %s", raw_path)

    # Read raw JSON — schema enforced, evolution allowed
    raw_df = (
        spark.read.format("json")
        .option("multiLine", "true")
        .schema(BRONZE_ORDER_SCHEMA)
        .load(raw_path)
    )

    # Attach pipeline metadata before writing
    ingested_df = raw_df.withColumns(
        {
            "_ingest_timestamp": F.lit(
                datetime.now(timezone.utc).isoformat()
            ).cast("timestamp"),
            "_source_file": F.input_file_name(),
        }
    )

    row_count = ingested_df.count()
    logger.info("Loaded %d rows from raw zone.", row_count)

    # Skip write if source is empty — avoids creating empty Delta commits
    if row_count == 0:
        logger.warning("No data found for date=%s — skipping write.", date)
        return

    # Idempotent upsert — safe to re-run without creating duplicates
    upsert_to_delta(
        spark=spark,
        source_df=ingested_df,
        target_path=bronze_path,
        merge_keys=["order_id", "_source_file"],
    )

    logger.info("Bronze write complete — %s", bronze_path)


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    parser = argparse.ArgumentParser(description="Bronze ingestion job")
    parser.add_argument("--env",    choices=["staging", "prod"], required=True)
    parser.add_argument("--date",   required=True, help="YYYY-MM-DD")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    args = parser.parse_args()

    run(env=args.env, date=args.date, bucket=args.bucket)
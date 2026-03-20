"""
Delta Lake helper utilities.
Wraps common Delta operations: upsert (MERGE), optimize, vacuum, and time-travel.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def upsert_to_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_path: str,
    merge_keys: list[str],
    update_columns: Optional[list[str]] = None,
) -> None:
    """
    Idempotent MERGE (upsert) into a Delta table.
    Creates the table on first run if it does not exist.

    Args:
        spark:          Active SparkSession.
        source_df:      Incoming DataFrame to merge.
        target_path:    S3 path or Unity Catalog table name.
        merge_keys:     Columns used to match source vs target rows.
        update_columns: Columns to update on match (default: all columns).
    """
    if not DeltaTable.isDeltaTable(spark, target_path):
        logger.info("Target %s does not exist — creating on first write.", target_path)
        source_df.write.format("delta").mode("overwrite").save(target_path)
        return

    delta_table = DeltaTable.forPath(spark, target_path)

    # Build the JOIN condition from merge keys e.g. "target.order_id = source.order_id"
    merge_condition = " AND ".join(
        f"target.{key} = source.{key}" for key in merge_keys
    )

    # If no specific columns given, update all columns
    if update_columns:
        update_map = {col: f"source.{col}" for col in update_columns}
    else:
        update_map = {col: f"source.{col}" for col in source_df.columns}

    insert_map = {col: f"source.{col}" for col in source_df.columns}

    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
    logger.info("Upsert complete into %s (%d rows).", target_path, source_df.count())


def optimize_table(
    spark: SparkSession,
    table_path: str,
    z_order_cols: Optional[list[str]] = None,
) -> None:
    """
    Run OPTIMIZE on a Delta table to compact small files.
    Optionally apply Z-ORDER for faster multi-column filtering.

    Small files are a common performance killer in data lakes —
    OPTIMIZE merges them into larger, more efficient files.
    """
    sql = f"OPTIMIZE delta.`{table_path}`"
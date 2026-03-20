"""
Central schema registry.
All Bronze / Silver / Gold schemas are defined here to enforce
consistency across ingestion, transformation, and quality layers.
"""
from __future__ import annotations

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Bronze ────────────────────────────────────────────────────────────────────
# Raw schema — as close to source as possible, all fields nullable.
# We never reject data at this layer, even if it looks wrong.

BRONZE_ORDER_SCHEMA = StructType(
    [
        StructField("order_id",           StringType(), nullable=True),
        StructField("customer_id",        StringType(), nullable=True),
        StructField("order_amount",       StringType(), nullable=True),  # Raw string, cast later
        StructField("currency",           StringType(), nullable=True),
        StructField("status",             StringType(), nullable=True),
        StructField("created_at",         StringType(), nullable=True),  # Raw string, cast later
        StructField("updated_at",         StringType(), nullable=True),
        StructField("_ingest_timestamp",  TimestampType(), nullable=False),
        StructField("_source_file",       StringType(), nullable=True),
    ]
)

# ── Silver ────────────────────────────────────────────────────────────────────
# Cleaned and typed schema. Business keys are non-nullable.
# This is the layer analysts and models read from.

SILVER_ORDER_SCHEMA = StructType(
    [
        StructField("order_id",           StringType(),       nullable=False),
        StructField("customer_id",        StringType(),       nullable=False),
        StructField("order_amount",       DecimalType(18, 2), nullable=False),
        StructField("currency",           StringType(),       nullable=False),
        StructField("status",             StringType(),       nullable=False),
        StructField("created_at",         TimestampType(),    nullable=False),
        StructField("updated_at",         TimestampType(),    nullable=True),
        StructField("_ingest_timestamp",  TimestampType(),    nullable=False),
        StructField("_is_current",        IntegerType(),      nullable=False),  # SCD2: 1 = active row
        StructField("_valid_from",        TimestampType(),    nullable=False),
        StructField("_valid_to",          TimestampType(),    nullable=True),   # NULL means still active
    ]
)

# ── Gold ──────────────────────────────────────────────────────────────────────
# Business aggregates. Optimised for fast reads by BI tools.

GOLD_DAILY_REVENUE_SCHEMA = StructType(
    [
        StructField("report_date",      StringType(),       nullable=False),
        StructField("currency",         StringType(),       nullable=False),
        StructField("total_orders",     IntegerType(),      nullable=False),
        StructField("gross_revenue",    DecimalType(18, 2), nullable=False),
        StructField("avg_order_value",  DecimalType(18, 2), nullable=False),
        StructField("_updated_at",      TimestampType(),    nullable=False),
    ]
)

# ── Allowed values ────────────────────────────────────────────────────────────
# Used by both the Silver transform and the Great Expectations quality suite.

VALID_ORDER_STATUSES = {"pending", "confirmed", "shipped", "delivered", "cancelled"}
VALID_CURRENCIES     = {"NOK", "USD", "EUR", "GBP", "SEK", "DKK"}
"""
Unit tests for Silver transformation logic.
Uses a local SparkSession — no AWS or Databricks connection required.
"""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from transform.silver.bronze_to_silver import (
    _clean,
    _deduplicate,
    _mask_pii,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Single SparkSession shared across all tests in this file.
    scope="session" means it is created once and reused — much faster.
    """
    return (
        SparkSession.builder.appName("test_silver")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0")
        .config("spark.driver.extraJavaOptions", "-Dio.netty.tryReflectionSetAccessible=true")
        .getOrCreate()
    )


@pytest.fixture
def raw_orders(spark: SparkSession):
    """
    Sample Bronze rows covering common edge cases:
      - ord_001: clean valid row
      - ord_002: appears twice (duplicate — keep latest updated_at)
      - ord_003: null customer_id (should be dropped)
      - ord_004: invalid status (should be dropped)
      - ord_005: invalid currency (should be dropped)
    """
    return spark.createDataFrame(
        [
            # order_id   customer_id   amount    currency  status       created_at            updated_at
            ("ord_001",  "cust_a",    "99.99",  "NOK",   "confirmed", "2024-01-10 10:00:00", "2024-01-10 10:00:00"),
            ("ord_002",  "cust_b",    "250.00", "USD",   "delivered", "2024-01-10 11:00:00", "2024-01-10 11:00:00"),
            ("ord_002",  "cust_b",    "250.00", "USD",   "shipped",   "2024-01-10 11:00:00", "2024-01-10 13:00:00"),  # newer
            ("ord_003",  None,        "10.00",  "EUR",   "pending",   "2024-01-10 12:00:00", None),                   # null customer
            ("ord_004",  "cust_c",    "50.00",  "NOK",   "INVALID",   "2024-01-10 13:00:00", None),                   # bad status
            ("ord_005",  "cust_d",    "75.00",  "XYZ",   "confirmed", "2024-01-10 14:00:00", None),                   # bad currency
        ],
        schema=StructType(
            [
                StructField("order_id",      StringType(), True),
                StructField("customer_id",   StringType(), True),
                StructField("order_amount",  StringType(), True),
                StructField("currency",      StringType(), True),
                StructField("status",        StringType(), True),
                StructField("created_at",    StringType(), True),
                StructField("updated_at",    StringType(), True),
            ]
        ),
    )


# ── Tests: _clean ─────────────────────────────────────────────────────────────

class TestClean:

    def test_drops_null_customer_id(self, raw_orders):
        """Rows with null customer_id must be removed."""
        result = _clean(raw_orders)
        assert result.filter(F.col("customer_id").isNull()).count() == 0

    def test_drops_invalid_status(self, raw_orders):
        """Rows with unknown status values must be removed."""
        result = _clean(raw_orders)
        assert result.filter(F.col("status") == "INVALID").count() == 0

    def test_drops_invalid_currency(self, raw_orders):
        """Rows with unknown currency codes must be removed."""
        result = _clean(raw_orders)
        assert result.filter(F.col("currency") == "XYZ").count() == 0

    def test_correct_row_count_after_clean(self, raw_orders):
        """
        Started with 6 rows.
        Dropped: ord_003 (null customer), ord_004 (bad status), ord_005 (bad currency).
        Expected: 3 rows remaining (ord_001 + two ord_002 duplicates).
        """
        result = _clean(raw_orders)
        assert result.count() == 3

    def test_trims_whitespace(self, spark):
        """Leading and trailing whitespace must be removed from string columns."""
        def test_trims_whitespace(self, spark):
            df = spark.createDataFrame(
                [("  ord_x  ", " cust_x ", "10.00", " NOK ", "confirmed", "2024-01-01", None)],
                schema=StructType([
                StructField("order_id",     StringType(), True),
                StructField("customer_id",  StringType(), True),
                StructField("order_amount", StringType(), True),
                StructField("currency",     StringType(), True),
                StructField("status",       StringType(), True),
                StructField("created_at",   StringType(), True),
                StructField("updated_at",   StringType(), True),
        ])
    )


# ── Tests: _deduplicate ───────────────────────────────────────────────────────

class TestDeduplicate:

    def test_keeps_latest_updated_at(self, raw_orders, spark):
        """
        ord_002 appears twice with different updated_at values.
        Only the row with the later updated_at should survive.
        """
        df = _clean(raw_orders).withColumn(
            "updated_at", F.to_timestamp("updated_at")
        )
        result    = _deduplicate(df)
        ord_002   = result.filter(F.col("order_id") == "ord_002")

        assert ord_002.count() == 1
        assert ord_002.first()["status"] == "shipped"  # newer row

    def test_no_duplicate_order_ids(self, raw_orders):
        """After deduplication every order_id must be unique."""
        df = _clean(raw_orders).withColumn(
            "updated_at", F.to_timestamp("updated_at")
        )
        result   = _deduplicate(df)
        total    = result.count()
        distinct = result.select("order_id").distinct().count()

        assert total == distinct


# ── Tests: _mask_pii ──────────────────────────────────────────────────────────

class TestMaskPii:

    def test_original_ids_not_present(self, raw_orders):
        """No original customer_id values should appear after masking."""
        cleaned = _clean(raw_orders)
        original_ids = {
            r.customer_id
            for r in raw_orders.filter(F.col("customer_id").isNotNull()).collect()
        }
        masked_ids = {r.customer_id for r in _mask_pii(cleaned).collect()}

        assert original_ids.isdisjoint(masked_ids)

    def test_hash_is_deterministic(self, raw_orders):
        """
        Hashing the same input twice must produce the same output.
        This ensures joins between tables still work after masking.
        """
        cleaned = _clean(raw_orders)
        ids_run1 = {r.customer_id for r in _mask_pii(cleaned).collect()}
        ids_run2 = {r.customer_id for r in _mask_pii(cleaned).collect()}

        assert ids_run1 == ids_run2

    def test_null_customer_id_stays_null(self, spark):
        """Null customer_id must remain null after masking — not crash."""
        def test_null_customer_id_stays_null(self, spark):
            df = spark.createDataFrame(
                [(None, "99.00", "NOK", "confirmed")],
                schema=StructType([
                StructField("customer_id",  StringType(), True),
                StructField("order_amount", StringType(), True),
                StructField("currency",     StringType(), True),
                StructField("status",       StringType(), True),
        ])
    )
            result = _mask_pii(df)
            assert result.first()["customer_id"] is None
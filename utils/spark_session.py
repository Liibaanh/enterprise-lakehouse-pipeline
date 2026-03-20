"""
Spark session factory.
Returns a configured SparkSession for local dev or Databricks runtime.
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "LakehousePipeline") -> SparkSession:
    """
    Return a SparkSession.
    - On Databricks: reuses the running cluster session.
    - Locally (Docker / CI): creates a local session with Delta support.
    """
    if _running_on_databricks():
        return SparkSession.builder.getOrCreate()

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Enable Delta Lake support
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Low shuffle partitions for local dev (default 200 is too high)
        .config("spark.sql.shuffle.partitions", "8")
        # Allow schema changes without breaking existing tables
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

def _running_on_databricks() -> bool:
    """Check if code is running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ
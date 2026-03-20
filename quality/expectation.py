"""
Data quality expectations using Great Expectations.
Called from the Silver transformation job before any data is written.
Raises DataQualityError on failure — bad data never reaches Silver.
"""
from __future__ import annotations

import logging

from great_expectations.core import ExpectationSuite
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when one or more expectations fail validation."""


def run_order_expectations(
    spark: SparkSession,
    df: DataFrame,
    layer: str,
) -> None:
    """
    Validate an order DataFrame against the expectation suite.

    Args:
        spark: Active SparkSession.
        df:    DataFrame to validate.
        layer: Pipeline layer name — used in log messages.

    Raises:
        DataQualityError: If any expectation fails.
    """
    ge_df  = SparkDFDataset(df)
    suite  = _build_order_suite()
    results = ge_df.validate(expectation_suite=suite, catch_exceptions=False)

    # Collect all failures before raising — log every problem at once
    failed = [r for r in results.results if not r.success]

    if failed:
        for f in failed:
            logger.error(
                "[%s] DQ FAILURE — %s | kwargs=%s",
                layer,
                f.expectation_config.expectation_type,
                f.expectation_config.kwargs,
            )
        raise DataQualityError(
            f"{len(failed)} expectation(s) failed in layer '{layer}'. "
            "Aborting write to prevent bad data propagation."
        )

    logger.info("[%s] All %d expectations passed.", layer, len(results.results))


def _build_order_suite() -> ExpectationSuite:
    """
    Define all expectations for the orders entity.
    Grouped by category for readability.
    """
    suite = ExpectationSuite(expectation_suite_name="orders_silver")

    # ── Completeness ──────────────────────────────────────────────────────────
    # These fields must always have a value — no nulls allowed

    for col in ["order_id", "customer_id", "order_amount"]:
        suite.add_expectation_configuration(
            {
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": col},
            }
        )

    # ── Uniqueness ────────────────────────────────────────────────────────────
    # Each order_id must appear exactly once after deduplication

    suite.add_expectation_configuration(
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "order_id"},
        }
    )

    # ── Value ranges ──────────────────────────────────────────────────────────
    # Orders must have a positive amount — zero or negative is a data error

    suite.add_expectation_configuration(
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column":    "order_amount",
                "min_value": 0.01,
                "max_value": 1_000_000,
            },
        }
    )

    # ── Referential integrity ─────────────────────────────────────────────────
    # Only known statuses and currencies are allowed

    suite.add_expectation_configuration(
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column":    "status",
                "value_set": [
                    "pending",
                    "confirmed",
                    "shipped",
                    "delivered",
                    "cancelled",
                ],
            },
        }
    )

    suite.add_expectation_configuration(
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column":    "currency",
                "value_set": ["NOK", "USD", "EUR", "GBP", "SEK", "DKK"],
            },
        }
    )

    # ── Volume guard ──────────────────────────────────────────────────────────
    # Catch upstream outages — if we get 0 rows something is wrong upstream

    suite.add_expectation_configuration(
        {
            "expectation_type": "expect_table_row_count_to_be_between",
            "kwargs": {"min_value": 1, "max_value": 10_000_000},
        }
    )

    return suite
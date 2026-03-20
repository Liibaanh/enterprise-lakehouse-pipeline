-- =============================================================================
-- Gold layer analytical queries
-- Target: Databricks SQL endpoint / Unity Catalog
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Rolling 7-day revenue per currency
--    Shows revenue trend smoothed over a week — removes daily noise
-- -----------------------------------------------------------------------------
SELECT
    report_date,
    currency,
    gross_revenue,
    SUM(gross_revenue) OVER (
        PARTITION BY currency
        ORDER BY report_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                               AS revenue_7d_rolling,
    AVG(gross_revenue) OVER (
        PARTITION BY currency
        ORDER BY report_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                               AS avg_revenue_7d
FROM gold.daily_revenue
ORDER BY report_date DESC, currency;


-- -----------------------------------------------------------------------------
-- 2. Month-over-month revenue growth
--    Uses LAG to compare each month against the previous month
-- -----------------------------------------------------------------------------
WITH monthly AS (
    SELECT
        DATE_TRUNC('month', report_date)    AS month,
        currency,
        SUM(gross_revenue)                  AS monthly_revenue,
        SUM(total_orders)                   AS monthly_orders
    FROM gold.daily_revenue
    GROUP BY 1, 2
),
with_prev AS (
    SELECT
        month,
        currency,
        monthly_revenue,
        monthly_orders,
        LAG(monthly_revenue) OVER (
            PARTITION BY currency
            ORDER BY month
        )                                   AS prev_month_revenue
    FROM monthly
)
SELECT
    month,
    currency,
    monthly_revenue,
    monthly_orders,
    ROUND(
        (monthly_revenue - prev_month_revenue)
        / NULLIF(prev_month_revenue, 0) * 100,
        2
    )                                       AS mom_growth_pct
FROM with_prev
ORDER BY month DESC, currency;


-- -----------------------------------------------------------------------------
-- 3. Customer cohort retention analysis
--    Groups customers by first-order month and tracks how many return
--    Runs against silver.orders for customer-level granularity
-- -----------------------------------------------------------------------------
WITH first_order AS (
    -- Find the month each customer placed their very first order
    SELECT
        customer_id,
        DATE_TRUNC('month', MIN(created_at)) AS cohort_month
    FROM silver.orders
    WHERE _is_current = 1
      AND status IN ('confirmed', 'shipped', 'delivered')
    GROUP BY customer_id
),
monthly_activity AS (
    -- For each order, calculate how many months after cohort it was placed
    SELECT
        o.customer_id,
        f.cohort_month,
        DATE_TRUNC('month', o.created_at)           AS activity_month,
        DATEDIFF(
            DATE_TRUNC('month', o.created_at),
            f.cohort_month
        ) / 30                                       AS months_since_first
    FROM silver.orders o
    JOIN first_order f ON o.customer_id = f.customer_id
    WHERE o._is_current = 1
)
SELECT
    cohort_month,
    months_since_first,
    COUNT(DISTINCT customer_id)                      AS active_customers,
    ROUND(
        COUNT(DISTINCT customer_id) * 100.0
        / FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month
            ORDER BY months_since_first
        ),
        2
    )                                                AS retention_pct
FROM monthly_activity
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first;


-- -----------------------------------------------------------------------------
-- 4. Top 10 revenue days per currency (all time)
--    Uses RANK and QUALIFY — Databricks SQL syntax
-- -----------------------------------------------------------------------------
SELECT
    report_date,
    currency,
    gross_revenue,
    total_orders,
    avg_order_value,
    RANK() OVER (
        PARTITION BY currency
        ORDER BY gross_revenue DESC
    )                                               AS revenue_rank
FROM gold.daily_revenue
QUALIFY revenue_rank <= 10
ORDER BY currency, revenue_rank;
# enterprise-lakehouse-pipeline

A production-grade data pipeline built on **Databricks + Delta Lake + AWS**,
implementing the Medallion architecture (Bronze → Silver → Gold) with full
CI/CD, schema evolution, data quality checks, and idempotent processing.

![CI](https://github.com/liibaanh/enterprise-lakehouse-pipeline/actions/workflows/ci.yml/badge.svg)

---

## Architecture
```
Raw Sources                Databricks (AWS)                        Consumers
────────────               ──────────────────────────────────      ──────────
S3 Raw Zone   ──────────►  Bronze  (raw ingest, no transforms)     BI Tools
REST APIs     ──────────►  Silver  (cleaned, typed, validated) ──► Data Science
Kafka Topics  ──────────►  Gold    (business aggregates)           SQL Analysts
                           │
                           ▼
                    Delta Lake (ACID transactions)
                    Great Expectations (data quality)
                    Unity Catalog (RBAC)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Compute | Databricks (AWS) |
| Storage | AWS S3 + Delta Lake |
| Transformation | PySpark, dbt |
| Orchestration | Databricks Workflows |
| Data Quality | Great Expectations |
| CI/CD | GitHub Actions |
| Infrastructure | Terraform |
| Containerization | Docker + LocalStack |

---

## Project Structure
```
enterprise-lakehouse-pipeline/
├── .github/workflows/       # CI and deploy pipelines
├── transform/
│   ├── bronze/              # Raw S3 → Bronze (faithful copy of source)
│   ├── silver/              # Bronze → Silver (clean, typed, validated)
│   └── gold/                # Silver → Gold (business aggregates)
├── utils/
│   ├── spark_session.py     # SparkSession factory (local + Databricks)
│   ├── schema_registry.py   # Central schema definitions for all layers
│   └── delta_utils.py       # MERGE, OPTIMIZE, VACUUM, time-travel helpers
├── quality/
│   └── expectations.py      # Great Expectations data quality suite
├── sql/queries/             # Analytical SQL — window functions, cohorts
├── tests/unit/              # Pytest unit tests (10/10 passing)
├── docker/                  # Dockerfile + docker-compose with LocalStack
└── terraform/               # AWS S3 + Databricks infrastructure as code
```

---

## Medallion Architecture

### Bronze — Raw Ingestion
- Data lands exactly as received from source — no transformations
- Schema-on-read with schema evolution enabled
- Idempotent: re-running never creates duplicates (uses Delta MERGE)

### Silver — Cleaned and Validated
- Type casting, null handling, deduplication
- PII masking: `customer_id` replaced with SHA-256 hash
- Great Expectations quality checks — job fails fast on violations
- SCD Type 2: full row history preserved on updates

### Gold — Business Aggregates
- Pre-computed KPIs: daily revenue, order volume, average order value
- Optimised with Delta OPTIMIZE + Z-ORDER for fast BI queries
- Exposed via Databricks SQL endpoints

---

## Getting Started

### Prerequisites
- Python 3.11+
- Docker + Docker Compose
- Java 21 (required by PySpark)
- AWS CLI configured
- Databricks CLI configured

### Run locally with Docker
```bash
git clone https://github.com/liibaanh/enterprise-lakehouse-pipeline
cd enterprise-lakehouse-pipeline

# Copy and fill in your credentials
cp .env.example .env

# Build Docker image
docker compose -f docker/docker-compose.yml build

# Run unit tests inside container
docker compose -f docker/docker-compose.yml run --rm app pytest tests/unit/ -v
```

### Run pipeline locally
```bash
# Activate virtual environment
source .venv/bin/activate

# Run Bronze ingestion for a specific date
python -m transform.bronze.raw_to_bronze \
    --env staging \
    --date 2024-01-15 \
    --bucket your-bucket-name
```

---

## CI/CD Pipeline
```
Push to main
    │
    ├── Lint (ruff)
    ├── Type check (mypy)
    ├── Unit tests (pytest + coverage)
    └── Docker build check
            │
            ▼
    Deploy to staging (Databricks Asset Bundles)
            │
            ▼
    Integration tests
            │
            ▼
    Deploy to prod (requires manual approval)
```

---

## Data Quality

All Silver jobs run a Great Expectations validation suite before writing.
Jobs fail fast if any expectation is violated — bad data never reaches Silver.

| Check | Expectation |
|---|---|
| Completeness | `order_id`, `customer_id`, `order_amount` never null |
| Uniqueness | `order_id` unique after deduplication |
| Value range | `order_amount` between 0.01 and 1,000,000 |
| Referential integrity | `status` and `currency` within allowed sets |
| Volume guard | Row count between 1 and 10,000,000 |

---

## Key Design Decisions

**Why idempotent pipelines?**
Jobs can fail and be re-run safely. Delta MERGE ensures re-running the same
job twice produces the same result — no duplicates, no data loss.

**Why SCD Type 2 in Silver?**
Overwriting rows loses history. SCD2 preserves every version of a record,
enabling point-in-time analysis and audit trails.

**Why SHA-256 for PII masking?**
One-way hash — original IDs cannot be recovered. Deterministic — the same
`customer_id` always produces the same hash, so joins between tables still work.

**Why fail-fast on data quality?**
It is better to stop the pipeline than to write bad data that analysts and
models rely on. One bad day of data can corrupt weeks of reporting.

---

## Notes

> Data is **synthetically generated** using the `faker` library to simulate
> realistic e-commerce transactions. No real customer data is used.
> The architecture mirrors production patterns used at scale.
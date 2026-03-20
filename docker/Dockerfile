# =============================================================================
# Multi-stage build
# Stage 1 (builder): installs all dependencies
# Stage 2 (runtime): lean image with only what is needed to run
# Multi-stage keeps the final image small — no build tools in production
# =============================================================================

# ── Stage 1: builder ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Java is required by PySpark — install before Python deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy only dependency file first — Docker caches this layer
# if pyproject.toml has not changed, pip install is skipped on rebuild
COPY pyproject.toml .

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[dev]"

# ── Stage 2: runtime ──────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install Java in runtime stage as well (PySpark needs it at runtime)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy installed packages from builder — not pip itself
COPY --from=builder /usr/local/lib/python3.11/site-packages \
                    /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copy source code last — changes here don't invalidate the pip cache layer
COPY . .

# Default command: run unit tests
# Override in docker-compose for other commands
CMD ["pytest", "tests/unit/", "-v"]
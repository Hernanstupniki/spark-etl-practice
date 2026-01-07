#!/bin/bash
set -euo pipefail

echo "========================================"
echo "Starting BRONZE job: film"
echo "========================================"

# Environment
export ENV=${ENV:-dev}

if [[ -z "${POSTGRES_JAR:-}" ]]; then
  echo "POSTGRES_JAR not set"
  exit 1
fi

if [[ -z "${JDBC_URL:-}" ]]; then
  echo "JDBC_URL not set"
  exit 1
fi

if [[ -z "${DB_USER:-}" ]]; then
  echo "DB_USER not set"
  exit 1
fi

if [[ -z "${DB_PASSWORD:-}" ]]; then
  echo "DB_PASSWORD not set"
  exit 1
fi

echo "ENV: $ENV"
echo "Postgres JAR: $POSTGRES_JAR"

# -------------------------------
# Run Spark job
# -------------------------------
spark-submit \
  --master local[*] \
  --jars "$POSTGRES_JAR" \
  src/bronze/bronze.py

echo "BRONZE job finished successfully"

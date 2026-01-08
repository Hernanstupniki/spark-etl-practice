#!/bin/bash
set -euo pipefail

echo "Starting full ETL pipeline"

export ENV=${ENV:-dev}

echo "Environment: $ENV"

./scripts/bronze/run_bronze.sh
./scripts/silver/run_silver.sh
./scripts/gold/run_gold.sh

echo "ETL pipeline finished successfully"

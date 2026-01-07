#!/bin/bash
set -euo pipefail

echo "Starting SILVER job: film"

export ENV=${ENV:-dev}

if [[ -z "${ENV}" ]]; then
  echo "ENV not set"
  exit 1
fi

echo "Environment: $ENV"

spark-submit \
  --master local[*] \
  src/silver/silver.py

echo "SILVER job finished successfully"

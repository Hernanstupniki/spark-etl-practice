#!/bin/bash
set -e

echo "Starting Gold pipeline"

python3 src/gold/analytics/metrics/film.py
python3 src/gold/analytics/aggregations/film.py
python3 src/gold/analytics/coverage/film.py
python3 src/gold/analytics/rankings/film.py
python3 src/gold/analytics/quality/film.py
python3 src/gold/analytics/marts/film_overview.py

echo "Gold pipeline finished successfully"

#!/bin/bash
set -e

echo "Starting GOLD layer pipeline"

echo "Running GOLD dimensions"
python3 src/gold/star/dim_film.py
python3 src/gold/star/dim_rating.py
python3 src/gold/star/dim_length_segment.py

echo "Running GOLD fact table"
python3 src/gold/star/fact_film.py

echo "Running GOLD marts"
python3 src/gold/analytics/marts/film_overview.py

echo "GOLD pipeline finished successfully"

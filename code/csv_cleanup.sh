#!/bin/bash
set -e

echo "Removing intermediate CSV files..."

rm -f /mnt/data/use_cases/clean_weather.csv
rm -f /mnt/data/use_cases/expanded_weather.csv

echo "Cleanup done!"

#!/bin/bash

CORES=$(nproc)
echo "Detected $CORES cores. Starting parallel sort..."

mkdir -p /mnt/data/use_cases/tmp_sort

head -n 1 /mnt/data/use_cases/expanded_weather.csv > /mnt/data/use_cases/sorted_weather.csv
tail -n +2 /mnt/data/use_cases/expanded_weather.csv | \
LC_ALL=C sort --parallel=$CORES -T /mnt/data/use_cases/tmp_sort -S 50% -t, -k1,1 >> /mnt/data/use_cases/sorted_weather.csv

rm -rf /mnt/data/use_cases/tmp_sort

echo "Sort complete!"

#!/bin/bash

# Directory containing the date-named folders
BASE_DIR="/home/md_recorder_py/data/gateio"

# Get today's date in YYYYMMDD format
TODAY=$(date +%Y%m%d)

cd "$BASE_DIR" || { echo "Directory $BASE_DIR not found!"; exit 1; }

# Loop through all directories matching the YYYYMMDD pattern
for dir in [0-9][0-9][0-9][0-9][0-1][0-9][0-3][0-9]; do
    # Check if it's a directory and matches the date format
    if [[ -d "$dir" && "$dir" =~ ^[0-9]{8}$ ]]; then
        # Compare with today's date
        if [[ "$dir" < "$TODAY" ]]; then
            echo "Packing $dir..."
            tar -czf "${dir}.tar.gz" "$dir" && rm -rf "$dir"
        fi
    fi
done

echo "Packing complete."
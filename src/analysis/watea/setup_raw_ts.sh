#!/bin/bash

# Directory containing the subfolders
SOURCE_DIR="./data_cache/bib_export"

# Destination directory where the .parquet files will be moved
DEST_DIR="./data_cache/raw_time_series/"

# Create the destination directory if it doesn't exist
mkdir -p "$DEST_DIR"

# Loop through each folder matching the pattern "vehicle_id=XX"
for folder in "$SOURCE_DIR"/vehicle_id=*; do
    # Extract the vehicle ID (XX)
    vehicle_id=$(basename "$folder" | cut -d'=' -f2)
    
    # Find the .parquet file in the current folder
    parquet_file=$(find "$folder" -maxdepth 1 -name "*.parquet")

    if [ -n "$parquet_file" ]; then
        # Construct the new file name with the vehicle ID
        new_filename="${vehicle_id}.snappy.parquet"
        echo $new_filename
        # Move and rename the .parquet file to the destination directory
        cp "$parquet_file" "$DEST_DIR/$new_filename"
    fi
done


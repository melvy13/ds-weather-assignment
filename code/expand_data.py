import pandas as pd
import numpy as np
import os
import sys
import time

def expand_data(input_file, output_file, target_size_gb, chunk_size):
    """
    Duplicate dataset with slight variations to simulate multiple sensors

    Concept: Each duplicate represents a different sensor at the same location
    taking slightly different readings due to sensor calibration differences
    """

    start_time = time.time()
    print("Starting data expansion...\n")

    print(f"Reading input file: {input_file}...")
    df = pd.read_csv(input_file)
    
    # Calculate how many duplicates we need
    original_size_gb = os.path.getsize(input_file) / (1024**3)
    num_duplicates = int(target_size_gb / original_size_gb)
    
    print(f"\n{'='*60}")
    print(f"SENSOR DUPLICATION SETUP")
    print(f"{'='*60}")
    print(f"Original size: {original_size_gb:.2f} GB")
    print(f"Original rows: {len(df):,}")
    print(f"Target size: {target_size_gb} GB")
    print(f"Number of sensor copies: {num_duplicates}")
    print(f"Final rows: ~{len(df) * num_duplicates:,}")
    print(f"Expected size: ~{original_size_gb * num_duplicates:.2f} GB")
    print(f"\nInterpretation: {num_duplicates} sensors at each location")
    print(f"{'='*60}\n")
    
    # Identify numeric columns
    numeric_cols = ['temperature', 'pressure', 'dew_point', 'humidity', 'wind_speed', 'wind_chill']
    
    # Write header
    print(f"Writing to {output_file}...")
    df.head(0).to_csv(output_file, index=False, mode='w')
    
    total_chunks = int(np.ceil(len(df) / chunk_size))

    total_rows_written = 0

    # Create each sensor copy
    for sensor_id in range(num_duplicates):
        sensor_start = time.time()
        print(f"\nProcessing Sensor {sensor_id + 1}/{num_duplicates}...")
        
        # Process in chunks to manage memory
        for chunk_num in range(total_chunks):
            start_idx = chunk_num * chunk_size
            end_idx = min((chunk_num + 1) * chunk_size, len(df))
            
            df_chunk = df.iloc[start_idx:end_idx].copy()
            
            # Add sensor-specific noise to numeric columns
            # Each sensor has slightly different calibration
            for col in numeric_cols:
                mask = df_chunk[col].notna()
                if mask.any():
                    # Different noise levels for different measurements
                    if col == 'temperature' or col == 'dew_point' or col == 'wind_chill':
                        noise_scale = 0.02
                    elif col == 'pressure':
                        noise_scale = 0.005
                    elif col == 'humidity':
                        noise_scale = 0.02
                    elif col == 'wind_speed':
                        noise_scale = 0.05
                    
                    # Generate noise for this sensor
                    noise = np.random.normal(0, noise_scale, size=len(df_chunk))
                    df_chunk.loc[mask, col] = (
                        df_chunk.loc[mask, col] * (1 + noise[mask])
                    ).round(2)

            rows_written = len(df_chunk)
            total_rows_written += rows_written

            # Write chunk to file
            df_chunk.to_csv(output_file, index=False, mode='a', header=False)
            
            # Progress update
            if (chunk_num + 1) % 10 == 0 or chunk_num == total_chunks - 1:
                progress = ((sensor_id * total_chunks + chunk_num + 1) / (num_duplicates * total_chunks)) * 100

                elapsed = time.time() - start_time
                rate_mb_s = (os.path.getsize(output_file) / 1e6) / elapsed

                print(
                    f"  Progress: {progress:.1f}% | "
                    f"Rows written: {total_rows_written:,} | "
                    f"Elapsed: {elapsed:.1f}s | "
                    f"Rate: {rate_mb_s:.1f} MB/s"
                )

        sensor_elapsed = time.time() - sensor_start
        print(
            f"Finished Sensor {sensor_id + 1} "
            f"in {sensor_elapsed:.1f}s "
            f"({sensor_elapsed / 60:.2f} min)"
        )
    
    # Final statistics
    output_size_gb = os.path.getsize(output_file) / (1024**3)
    total_elapsed = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"Duplication Complete!")
    print(f"{'='*60}")
    print(f"Total rows written: {total_rows_written:,}")
    print(f"Final size: {output_size_gb:.2f} GB")
    print(f"Total time: {total_elapsed:.1f}s ({total_elapsed / 60:.2f} min)")
    print(f"Average write rate: {(output_size_gb * 1024) / (total_elapsed):.1f} MB/s")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    TARGET_SIZE_GB = 10
    CHUNK_SIZE = 500000

    if len(sys.argv) <= 2:
        print(
            "Usage: python3 expand_data.py "
            "<input_csv> <output_csv> "
            "<target_size_gb [default 10]> <chunk_size [default 500000]"
        )
        sys.exit(1)
        
    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]

    if len(sys.argv) > 3:
        TARGET_SIZE_GB = float(sys.argv[3])
    if len(sys.argv) > 4:
        CHUNK_SIZE = int(sys.argv[4])

    expand_data(INPUT_FILE, OUTPUT_FILE, TARGET_SIZE_GB, CHUNK_SIZE)

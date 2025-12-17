import pandas as pd
import time
import sys

def initial_clean(input_file, output_file, chunksize):
    """
    Clean raw weather CSV by:
    - Keeping selected columns
    - Dropping rows where BOTH humidity and temperature are missing
    - Downcasting float64 to float32

    This helps prevent the expansion script from duplicating too many missing values
    """

    # Columns to keep (Drop mostly empty ones - determined from dataset website)
    KEEP_COLUMNS = [
        'datetime',
        'place',
        'city',
        'state',
        'temperature',
        'pressure',
        'dew_point',
        'humidity',
        'wind_speed',
        'wind_chill'
    ]

    first_chunk = True
    total_rows_read = 0
    total_rows_written = 0
    chunk_idx = 0

    start_time = time.time()
    print("Starting initial CSV cleaning...\n")

    for chunk in pd.read_csv(input_file, usecols=KEEP_COLUMNS,  chunksize=chunksize, low_memory=True):
        chunk_idx += 1
        rows_before = len(chunk)
        total_rows_read += rows_before
        print(f"Chunk {chunk_idx}: read {rows_before:,} rows")

        # Drop rows if both humidity & temperature are missing
        chunk.dropna(
            axis=0,
            subset=['humidity', 'temperature'],
            how='all',
            inplace=True
        )

        rows_after = len(chunk)
        removed = rows_before - rows_after
        total_rows_written += rows_after
        print(f"  ├─ removed {removed:,} invalid rows")
        print(f"  ├─ remaining {rows_after:,} rows")

        # Downcast floats to reduce memory
        float_cols = chunk.select_dtypes(include='float64').columns
        if len(float_cols) > 0:
            print(f"  ├─ downcasting {len(float_cols)} float columns")
            for col in float_cols:
                chunk[col] = chunk[col].astype('float32')

        chunk.to_csv(
            output_file,
            mode='w' if first_chunk else 'a',
            index=False,
            header=first_chunk
        )
        first_chunk = False

        elapsed = time.time() - start_time
        print(f"  └─ total written so far: {total_rows_written:,} rows")
        print(f"     elapsed time: {elapsed:.1f}s\n")

    print(f"{'='*60}")
    print("Done cleaning!")
    print(f"{'='*60}")
    print(f"Total rows read: {total_rows_read:,}")
    print(f"Total rows written: {total_rows_written:,}")
    print(f"Total time: {time.time() - start_time:.1f}s")
    print(f"{'='*60}")

if __name__ == "__main__":
    CHUNK_SIZE = 500000

    if len(sys.argv) <= 2:
        print(
            "Usage: python3 initial_clean.py "
            "<input_csv> <output_csv> "
            "<chunk_size [default 500000]>"
        )
        sys.exit(1)

    INPUT_FILE = sys.argv[1]
    OUTPUT_FILE = sys.argv[2]

    if len(sys.argv) > 3:
        CHUNK_SIZE = int(sys.argv[3])

    initial_clean(INPUT_FILE, OUTPUT_FILE, CHUNK_SIZE)

import pandas as pd
import sys
import os
import time
import re

def clean_weather_data(input_csv, output_dir):
    # Extract filename without path
    base = os.path.basename(input_csv)

    year = None
    part_idx = None

    # Regex to match both forms
    m = re.match(r"weather_(\d{4})(?:_part_(\d+))?", base)
    if m:
        year = int(m.group(1))
        if m.group(2):
            part_idx = int(m.group(2))
    
    # Construct output filename
    out_name = f"clean_{year}" if year is not None else "clean"
    if part_idx is not None:
        out_name += f"_part_{part_idx:02d}"
    output_file = os.path.join(output_dir, out_name + ".csv")

    start_time = time.time()
    print(f"Starting job for: {input_csv}")

    # Read data
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        print(f"ERROR: Cannot read {input_csv}: {e}")
        sys.exit(1)

    original_rows = len(df)

    # Drop columns with more than 50% rows empty
    limit = len(df) * 0.5
    original_cols = df.columns.tolist()
    df = df.dropna(axis=1, thresh=limit)

    dropped_cols = list(set(original_cols) - set(df.columns))
    if dropped_cols:
        print(f"  -> Dropped {len(dropped_cols)} empty/sparse columns: {dropped_cols}")

    # Impute missing data
    # 1. Forward Fill
    missing_before = df.isna().sum().sum()
    print(f"  -> Filling gaps forward (ffill)... (Missing values: {missing_before})")
    df = df.ffill()

    # 2. Backward Fill
    missing_middle = df.isna().sum().sum()
    print(f"  -> Filling start gaps backward (bfill)... (Remaining missing: {missing_middle})")
    df = df.bfill()

    # 3. Fill NaNs with 0
    missing_last = df.isna().sum().sum()
    if missing_last > 0:
        print(f"  -> Safety net: Filling {missing_last} remaining NaNs with 0...")
        df = df.fillna(0)
    else:
        print("  -> No remaining NaNs to fill with 0.")

    # Logic: Sanity checks (Physics enforcement)
    sanity_limits = {
        'humidity': (0, 100),         
        'temperature': (-60, 60),     
        'wind_speed': (0, 150),       
        'pressure': (800, 1100),      
        'precipitation_rate': (0, 500), 
        'precipitation_total': (0, 500), 
        'uv_index': (0, 15),          
        'visibility': (0, 100)       
    }

    for col, (min_val, max_val) in sanity_limits.items():
        # Case-insensitive matching (e.g. 'Humidity' matches 'humidity')
        matching_cols = [c for c in df.columns if c.lower() == col.lower()]
        
        for found_col in matching_cols:
            # Check for bad values
            bad_mask = (df[found_col] < min_val) | (df[found_col] > max_val)
            bad_count = bad_mask.sum()
            
            if bad_count > 0:
                print(f"  -> Fixing {bad_count} abnormal values in '{found_col}' (Capping to {min_val}-{max_val})")
                # Clip forces values into the specific range
                df[found_col] = df[found_col].clip(lower=min_val, upper=max_val)

    base_name = "clean"
    if year is not None:
        base_name += f"_{year}"
    if part_idx is not None:
        base_name += f"_part_{part_idx:02d}"

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{base_name}.csv")

    df.to_csv(output_file, index=False)

    duration = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"Processing Complete!")
    print(f"{'='*60}")
    print(f"File: {input_csv}")
    print(f"Rows: {original_rows}")
    print(f"Time Taken: {duration:.4f} seconds")
    print(f"Saved to: {output_file}")
    print(f"{'='*60}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 clean_data.py <input_csv> <output_dir>")
        sys.exit(1)

    input_csv = sys.argv[1]
    output_dir = sys.argv[2]
    
    clean_weather_data(input_csv, output_dir)

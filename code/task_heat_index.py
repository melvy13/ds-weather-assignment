import sys
import pandas as pd
import numpy as np
import os
import time

def calculate_heat_index(temp_c, humidity):
    """
    Calculate heat index using the Rothfusz regression (NWS formula).
    
    Args:
        temp_c: Temperature in Celsius
        humidity: Relative humidity (0-100)
    
    Returns:
        Heat index in Celsius
    """
    # Convert Celsius to Fahrenheit for the formula
    temp_f = (temp_c * 9/5) + 32
    
    # Simple formula for lower temperatures
    hi_f = 0.5 * (temp_f + 61.0 + ((temp_f - 68.0) * 1.2) + (humidity * 0.094))
    
    # If heat index is >= 80°F, use the full Rothfusz regression
    if hi_f >= 80:
        hi_f = (-42.379 + 
                2.04901523 * temp_f + 
                10.14333127 * humidity - 
                0.22475541 * temp_f * humidity - 
                0.00683783 * temp_f * temp_f - 
                0.05481717 * humidity * humidity + 
                0.00122874 * temp_f * temp_f * humidity + 
                0.00085282 * temp_f * humidity * humidity - 
                0.00000199 * temp_f * temp_f * humidity * humidity)
        
        # Adjustments for specific conditions
        if humidity < 13 and 80 <= temp_f <= 112:
            adjustment = ((13 - humidity) / 4) * np.sqrt((17 - abs(temp_f - 95)) / 17)
            hi_f -= adjustment
        elif humidity > 85 and 80 <= temp_f <= 87:
            adjustment = ((humidity - 85) / 10) * ((87 - temp_f) / 5)
            hi_f += adjustment
    
    # Convert back to Celsius
    hi_c = (hi_f - 32) * 5/9
    return hi_c

def calculate_monthly_heat_index(input_file, output_file):
    """
    Calculate monthly heat index statistics from weather data.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
    """
    t_start = time.time()

    # Read CSV
    df = pd.read_csv(input_file)
    t_read = time.time()
    print(f"Disk I/O (Read) Time: {t_read - t_start:.4f} seconds")
    print(f"Rows after reading CSV: {len(df)}")
    
    # Parse datetime
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])
    
    # Drop rows with missing temperature or humidity
    df = df.dropna(subset=['temperature', 'humidity'])
    
    # Extract year and month
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month
    
    # Calculate heat index for each row
    print("Calculating heat index for all rows...")
    df['heat_index'] = df.apply(
        lambda row: calculate_heat_index(row['temperature'], row['humidity']), 
        axis=1
    )
    
    # Group by state, year, and month
    print("Aggregating monthly statistics by state...")
    monthly_stats = (
        df.groupby(['state', 'year', 'month'])['heat_index']
        .agg(['min', 'max', 'mean', 'count'])
        .reset_index()
    )
    
    # Rename columns
    monthly_stats.columns = ['state', 'year', 'month', 'min_heat_index', 
                             'max_heat_index', 'avg_heat_index', 'sample_count']
    
    # Round heat index values
    monthly_stats['min_heat_index'] = monthly_stats['min_heat_index'].round(2)
    monthly_stats['max_heat_index'] = monthly_stats['max_heat_index'].round(2)
    monthly_stats['avg_heat_index'] = monthly_stats['avg_heat_index'].round(2)
    
    # Sort by state, year, month
    monthly_stats = monthly_stats.sort_values(['state', 'year', 'month']).reset_index(drop=True)

    t_process = time.time()
    print(f"Calculation Time: {t_process - t_read:.4f} seconds")
    
    # Save to CSV
    monthly_stats.to_csv(output_file, index=False)

    t_end = time.time()
    print(f"Total Execution Time: {t_end - t_start:.4f} seconds")
    
    print(f"\nMonthly heat index calculation completed.")
    print(f"Total records: {len(monthly_stats)}")
    print(f"\nSample output:")
    print(monthly_stats.head(10))
    
    # Summary statistics
    print(f"\n=== Overall Statistics ===")
    print(f"States covered: {monthly_stats['state'].nunique()}")
    print(f"Date range: {monthly_stats['year'].min()}-{monthly_stats['month'].min():02d} to {monthly_stats['year'].max()}-{monthly_stats['month'].max():02d}")
    print(f"Highest heat index recorded: {monthly_stats['max_heat_index'].max():.2f}°C")
    print(f"Lowest heat index recorded: {monthly_stats['min_heat_index'].min():.2f}°C")
    print(f"Average heat index across all months: {monthly_stats['avg_heat_index'].mean():.2f}°C")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python task_heat_index.py <input_csv> <output_dir>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_dir = sys.argv[2]

    base_name = os.path.basename(input_file)
    new_name = base_name.replace("clean_", "heat_index_", 1)

    output_file = os.path.join(output_dir, new_name)
    
    calculate_monthly_heat_index(input_file, output_file)

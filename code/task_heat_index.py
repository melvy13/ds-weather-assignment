import sys
import pandas as pd
import numpy as np

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
    # Read CSV
    df = pd.read_csv(input_file)
    print(f"Rows after reading CSV: {len(df)}")
    
    # Parse datetime
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df = df.dropna(subset=['datetime'])
    print(f"Rows after parsing datetime: {len(df)}")
    
    # Drop rows with missing temperature or humidity
    df = df.dropna(subset=['temperature', 'humidity'])
    print(f"Rows after dropping missing temp/humidity: {len(df)}")
    
    # Extract year and month
    df['year'] = df['datetime'].dt.year
    df['month'] = df['datetime'].dt.month

    print(f"\n=== Data Quality Check ===")
    print(f"Temperature range: {df['temperature'].min():.2f}°C to {df['temperature'].max():.2f}°C")
    print(f"Humidity range: {df['humidity'].min():.2f}% to {df['humidity'].max():.2f}%")
    print(f"Rows with humidity > 100: {(df['humidity'] > 100).sum()}")
    print(f"Rows with temp > 40°C: {(df['temperature'] > 40).sum()}")
    
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
    
    # Save to CSV
    monthly_stats.to_csv(output_file, index=False)
    
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
        print("Usage: python task_heat_index.py <input_csv> <output_csv>")
        print("\nCalculates monthly heat index statistics (min, max, avg) by state.")
        print("Heat index combines temperature and humidity to measure apparent temperature.")
        sys.exit(1)
    
    input_csv = sys.argv[1]
    output_csv = sys.argv[2]
    
    calculate_monthly_heat_index(input_csv, output_csv)

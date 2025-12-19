import sys
import os
import glob
import pandas as pd
from datetime import datetime

def generate_text_summary(df_hw, df_hi, output_file):
    """Generates a human-readable text report."""
    summary = []
    summary.append("=" * 51)
    summary.append("       MALAYSIA CLIMATE RISK ANALYSIS REPORT")
    summary.append("=" * 51)
    summary.append(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    
    # --- SECTION 1: HEAT WAVES ---
    summary.append("\n" + "-" * 30)
    summary.append("1. HEAT WAVES ANALYSIS")
    summary.append("-" * 30)
    
    if not df_hw.empty:
        summary.append(f"Total Heat Waves Detected: {len(df_hw)}")
        summary.append(f"States Affected: {df_hw['state'].nunique()}")
        summary.append(f"Longest Heat Wave: {df_hw['duration_days'].max()} days")
        summary.append(f"Avg Heat Wave Duration: {df_hw['duration_days'].mean():.1f} days")
        summary.append(f"Hottest Wave Avg Temp: {df_hw['avg_temperature'].max():.2f}°C")
        
        summary.append("\nTop 5 Longest Heat Waves:")
        top_waves = df_hw.nlargest(5, 'duration_days')
        for _, row in top_waves.iterrows():
            summary.append(f"  - {row['state']}: {row['duration_days']} days ({row['start_date']} to {row['end_date']}) @ {row['avg_temperature']}°C")

        summary.append("\nHeat Waves by State (Count):")
        state_counts = df_hw['state'].value_counts()
        for state, count in state_counts.head(10).items():
            summary.append(f"  - {state}: {count}")
    else:
        summary.append("No heat waves detected.")

    # --- SECTION 2: HEAT INDEX ---
    summary.append("\n" + "-" * 30)
    summary.append("2. HEAT INDEX ANALYSIS (Monthly)")
    summary.append("-" * 30)
    
    if not df_hi.empty:
        summary.append(f"Total Monthly Records: {len(df_hi)}")
        summary.append(f"Overall Max Heat Index: {df_hi['max_heat_index'].max():.2f}°C")
        summary.append(f"Overall Min Heat Index: {df_hi['min_heat_index'].min():.2f}°C")
        summary.append(f"Overall Avg Heat Index: {df_hi['avg_heat_index'].mean():.2f}°C")
        
        summary.append("\nTop 5 Hottest Months (Max Heat Index):")
        top_hi = df_hi.nlargest(5, 'max_heat_index')
        for _, row in top_hi.iterrows():
            summary.append(f"  - {row['state']} ({row['year']}-{row['month']:02d}): Max {row['max_heat_index']}°C")
            
        summary.append("\nAverage Heat Index by State:")
        avg_by_state = df_hi.groupby('state')['avg_heat_index'].mean().sort_values(ascending=False)
        for state, avg in avg_by_state.items():
            summary.append(f"  - {state}: {avg:.2f}°C")
    else:
        summary.append("No heat index data found.")

    # Save to file
    with open(output_file, 'w') as f:
        f.write("\n".join(summary))
    print(f"Summary report saved to: {output_file}")


def aggregate_results(analysis_dir, output_dir):
    print(f"Starting aggregation from: {analysis_dir}")
    
    # 1. HEAT WAVES (Simple Concat)
    hw_pattern = os.path.join(analysis_dir, "heat_waves_*.csv")
    hw_files = glob.glob(hw_pattern)
    full_hw = pd.DataFrame()

    if hw_files:
        print(f"Merging {len(hw_files)} heat wave files...")
        full_hw = pd.concat((pd.read_csv(f) for f in hw_files), ignore_index=True)
        
        # --- FIX: SANITIZE DATA TYPES ---
        # Force 'duration_days' and 'avg_temperature' to numeric.
        # errors='coerce' turns "bad string" into NaN
        full_hw['duration_days'] = pd.to_numeric(full_hw['duration_days'], errors='coerce')
        full_hw['avg_temperature'] = pd.to_numeric(full_hw['avg_temperature'], errors='coerce')
        
        # Remove any rows that became NaN (garbage data cleanup)
        full_hw = full_hw.dropna(subset=['duration_days', 'avg_temperature'])
        # --------------------------------
        
        # Sort
        if 'start_date' in full_hw.columns:
            full_hw = full_hw.sort_values(['state', 'start_date'])
        
        out_path = os.path.join(output_dir, "FINAL_heat_waves.csv")
        full_hw.to_csv(out_path, index=False)
        print(f"Saved CSV: {out_path}")
    
    # 2. HEAT INDEX (Weighted Average Aggregation)
    hi_pattern = os.path.join(analysis_dir, "heat_index_*.csv")
    hi_files = glob.glob(hi_pattern)
    full_hi = pd.DataFrame()

    if hi_files:
        print(f"Merging {len(hi_files)} heat index files...")
        raw_hi = pd.concat((pd.read_csv(f) for f in hi_files), ignore_index=True)
        
        # --- FIX: SANITIZE DATA TYPES ---
        cols_to_fix = ['min_heat_index', 'max_heat_index', 'avg_heat_index', 'sample_count']
        for col in cols_to_fix:
            if col in raw_hi.columns:
                 raw_hi[col] = pd.to_numeric(raw_hi[col], errors='coerce')
        raw_hi = raw_hi.dropna(subset=cols_to_fix)
        # --------------------------------

        print("Re-aggregating split months...")
        full_hi = raw_hi.groupby(['state', 'year', 'month']).agg({
            'min_heat_index': 'min',
            'max_heat_index': 'max',
            'avg_heat_index': lambda x: (x * raw_hi.loc[x.index, 'sample_count']).sum() / raw_hi.loc[x.index, 'sample_count'].sum(),
            'sample_count': 'sum'
        }).reset_index()

        # Rounding
        cols = ['min_heat_index', 'max_heat_index', 'avg_heat_index']
        full_hi[cols] = full_hi[cols].round(2)
        
        # Sort
        full_hi = full_hi.sort_values(['state', 'year', 'month'])
        
        out_path = os.path.join(output_dir, "FINAL_heat_index.csv")
        full_hi.to_csv(out_path, index=False)
        print(f"Saved CSV: {out_path}")

    # 3. GENERATE TEXT REPORT
    report_path = os.path.join(output_dir, "FINAL_summary_report.txt")
    generate_text_summary(full_hw, full_hi, report_path)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_analysis.py <analysis_dir> <output_dir>")
        sys.exit(1)
        
    aggregate_results(sys.argv[1], sys.argv[2])

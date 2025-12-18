import sys
import pandas as pd
 
def find_heat_waves(input_file, output_file, threshold, min_days): 
    df = pd.read_csv(input_file)
 
    # Parse datetime
    df['datetime'] = pd.to_datetime(
        df['datetime'],
        format='%Y-%m-%d %H:%M:%S',
        errors='coerce'
    )

    df = df.dropna(subset=['datetime'])
 
    # Extract date (as pandas datetime for diff)
    df['date'] = df['datetime'].dt.floor('D')
 
    # 1) Hourly -> daily average temperature per state
    daily = (
        df.groupby(['state', 'date'])['temperature']
        .mean()
        .reset_index()
        .rename(columns={'temperature': 'daily_avg_temp'})
        .sort_values(['state', 'date'])
        .reset_index(drop=True)
    )

    print(f"\nTotal rows in daily dataframe: {len(daily)}")
    print(f"Number of states: {daily['state'].nunique()}")
    print(f"States found: {daily['state'].unique()}")

    heat_waves = []

    # helper to save a wave
    def record_wave(state, wave_df):
        start_date = wave_df.iloc[0]['date']
        end_date = wave_df.iloc[-1]['date']
        duration_days = int((end_date - start_date).days) + 1
        heat_waves.append({
            'state': state,
            'start_date': start_date.date(),
            'end_date': end_date.date(),
            'duration_days': duration_days,
            'avg_temperature': round(wave_df['daily_avg_temp'].mean(), 2)
        })
 
    # 2) Detect heat waves per state with REAL consecutive-day check
    for state, state_df in daily.groupby('state'):
        state_df = state_df.sort_values('date').reset_index(drop=True)

        # ADD THESE DIAGNOSTIC LINES HERE:
        print(f"\n=== State: {state} ===")
        print(f"Date range: {state_df['date'].min()} to {state_df['date'].max()}")
        print(f"Temp range: {state_df['daily_avg_temp'].min():.2f}°C to {state_df['daily_avg_temp'].max():.2f}°C")
        print(f"Days with temp >= 35°C: {(state_df['daily_avg_temp'] >= 35.0).sum()}")
        print(f"Days with temp >= 27°C: {(state_df['daily_avg_temp'] >= 27.0).sum()}")
 
        in_wave = False
        wave_start = 0
 
        for i in range(len(state_df)):
            temp_ok = state_df.loc[i, 'daily_avg_temp'] >= threshold
 
            if not in_wave:
                # start a new wave only if temp meets threshold
                if temp_ok:
                    in_wave = True
                    wave_start = i
            else:
                # we're in a wave: check if streak should continue
                prev_date = state_df.loc[i - 1, 'date']
                cur_date = state_df.loc[i, 'date']
                is_next_day = (cur_date - prev_date).days == 1
 
                if (not temp_ok) or (not is_next_day):
                    # wave ends at i-1
                    wave_df = state_df.loc[wave_start:i - 1]
                    if len(wave_df) >= min_days:
                        record_wave(state, wave_df)
 
                    # if today qualifies, start new wave at i; else end wave
                    if temp_ok:
                        in_wave = True
                        wave_start = i
                    else:
                        in_wave = False
 
        # if wave reaches end
        if in_wave:
            wave_df = state_df.loc[wave_start:]
            if len(wave_df) >= min_days:
                record_wave(state, wave_df)
 
    # 3) Output (with header even if empty)
    if heat_waves:
        result = pd.DataFrame(heat_waves)
    else:
        result = pd.DataFrame(columns=[
            'state', 'start_date', 'end_date', 'duration_days', 'avg_temperature'
        ])
 
    result.to_csv(output_file, index=False)
    print("Heat wave detection completed.")
    print(result)
 
 
if __name__ == "__main__":
    THRESHOLD = 35.0
    MIN_DAYS = 3

    if len(sys.argv) <= 2:
        print("Usage: python task_heat_waves.py <input_csv> <output_csv> <threshold [default 35]> <min_days [default 3]>")
        sys.exit(1)

    if len(sys.argv) > 3:
        THRESHOLD = float(sys.argv[3])
    if len(sys.argv) > 4:
        MIN_DAYS = int(sys.argv[4])
    
    find_heat_waves(sys.argv[1], sys.argv[2], THRESHOLD, MIN_DAYS)

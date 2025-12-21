import os
import re

def parse_logs(log_dir="/mnt/data/use_cases/logs/"):
    # Patterns to match filenames
    clean_pattern = re.compile(r"clean_job_.*\.out")
    hi_pattern = re.compile(r"analysis_HI_.*\.out")
    hw_pattern = re.compile(r"analysis_HW_.*\.out")

    # Patterns to extract time from content
    # Clean: "Time Taken: 13.3932 seconds"
    clean_time_regex = re.compile(r"Time Taken:\s+([\d\.]+)\s+seconds")
    # Analysis: "Total Execution Time: 55.4712 seconds"
    analysis_time_regex = re.compile(r"Total Execution Time:\s+([\d\.]+)\s+seconds")

    total_clean_time = 0.0
    total_hi_time = 0.0
    total_hw_time = 0.0
    
    file_counts = {"clean": 0, "hi": 0, "hw": 0}

    print(f"Scanning directory: {os.path.abspath(log_dir)}\n")

    for filename in os.listdir(log_dir):
        filepath = os.path.join(log_dir, filename)
        
        # 1. Check for Cleaning Logs
        if clean_pattern.match(filename):
            with open(filepath, 'r') as f:
                content = f.read()
                match = clean_time_regex.search(content)
                if match:
                    t = float(match.group(1))
                    total_clean_time += t
                    file_counts["clean"] += 1

        # 2. Check for Heat Index Logs
        elif hi_pattern.match(filename):
            with open(filepath, 'r') as f:
                content = f.read()
                match = analysis_time_regex.search(content)
                if match:
                    t = float(match.group(1))
                    total_hi_time += t
                    file_counts["hi"] += 1

        # 3. Check for Heat Wave Logs
        elif hw_pattern.match(filename):
            with open(filepath, 'r') as f:
                content = f.read()
                match = analysis_time_regex.search(content)
                if match:
                    t = float(match.group(1))
                    total_hw_time += t
                    file_counts["hw"] += 1

    # --- Print Results ---
    grand_total_seconds = total_clean_time + total_hi_time + total_hw_time
    grand_total_minutes = grand_total_seconds / 60
    grand_total_hours = grand_total_minutes / 60

    print("=== Processing Statistics ===")
    print(f"Files Processed: {sum(file_counts.values())}")
    print(f"  - Cleaning Logs:   {file_counts['clean']}")
    print(f"  - Heat Index Logs: {file_counts['hi']}")
    print(f"  - Heat Wave Logs:  {file_counts['hw']}")
    print("-" * 30)
    print(f"Total Cleaning Time:   {total_clean_time:.2f} s")
    print(f"Total Heat Index Time: {total_hi_time:.2f} s")
    print(f"Total Heat Wave Time:  {total_hw_time:.2f} s")
    print("=" * 30)
    print(f"TOTAL SERIAL TIME (Projected): {grand_total_seconds:.2f} seconds")
    print(f"                             = {grand_total_minutes:.2f} minutes")
    print(f"                             = {grand_total_hours:.2f} hours")

if __name__ == "__main__":
    parse_logs()

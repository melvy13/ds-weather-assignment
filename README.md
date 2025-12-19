## Overview
This repo utilizes the dataset [Malaysia's Weather Data (1996-2024)](https://www.kaggle.com/datasets/shahmirvarqha/weather-data-malaysia) to simulate big data and do some processing on it.

The infrastructure is built on **Amazon EC2** (AWS Academy Learner Lab) using **HTCondor** for job scheduling and **NFS** for shared storage.

### Instances Setup
- **All** - Ubuntu Server 22.04 LTS (HVM), SSD Volume Type for Amazon Machine Image (AMI)
- **Submission Host** - t3.micro - 8GB storage gp3
- **Condor Master** - t3.micro - 8GB storage gp3
- **File Server (NFS)** - t3.small - 40GB storage gp3
- **Execution Host (Worker) 1** - t3.large - 8GB storage gp3
- **Execution Host (Worker) 2** - t3.large - 8GB storage gp3

> [!NOTE]
> The File Server (NFS) exports `/srv/data/` and all other nodes mount this export at `/mnt/data/`

## Checklist / Workflow
- Connect to the **Submission Host** and navigate to the shared directory `/mnt/data/use_cases/`
- Ensure the raw dataset `full_weather.csv` exists, all Python & Bash scripts in `code/` & all .sub files in `submit/`
- Create 4 empty directories: `logs`, `clean`, `split` & `analysis`
- Ensure all codes are executable & directories are writable

1. Initial cleaning to reduce duplicating bad data (`initial_clean.sub`) => `clean_weather.csv`

2. Expand data by duplicating & adding some noise (`expand_data.sub`) => `expanded_weather.csv`

3. Sort expanded data by datetime (`sort.sub`) => `sorted_weather.csv`

4. Split data into parts and years (`split_data.sub`) => `split/weather_<year>_part_<num>.csv`
   - e.g. `weather_1996_part_00.csv`, `weather_2023_part_00.csv`, `weather_2023_part_01.csv`, etc.
  
5. Remove CSVs no longer needed to prevent insufficient disk space (`csv_cleanup.sub`)

6. Clean all split data in parallel (`clean_data.sub`) => `clean/clean_<year>_part_<num>.csv`
   - e.g. `clean_2023_part_00.csv`, `clean_2023_part_01.csv`, etc.
  
7. Analyze heat index & heat waves in parallel (`analysis_heat_index.sub` & `analysis_heat_waves.sub`) => `analysis/heat_index_<year>_part_<num>.csv` & `analysis/heat_waves_<year>_part_<num>.csv`
   - e.g. `heat_index_2022_part_01.csv`, `heat_waves_2022_part_01.csv`
   
8. Merge all results to a final summary report (`generate_analysis.sub`) => `FINAL_summary_report.txt`, `FINAL_heat_index.csv` & `FINAL_heat_waves.csv`
   - `FINAL_summary_report.txt` contains a human-readable text report summarizing the results
   - `FINAL_heat_index.csv` contains a statistical summary of heat index for every month
   - `FINAL_heat_waves.csv` contains a list of every single "Heat Wave Event" detected across all years and states

Alternatively, everything can be automated if you use HTCondor DAGMan and run: `condor_submit_dag weather.dag`.

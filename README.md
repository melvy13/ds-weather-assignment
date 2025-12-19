Overview
-
This repo utilizes the dataset [Malaysia's Weather Data (1996-2024)](https://www.kaggle.com/datasets/shahmirvarqha/weather-data-malaysia) to simulate big data and do some processing on it.

Assume HTCondor is set up on Amazon EC2 instances (AWS Academy Learner Lab), and the NFS `/srv/data/` is mounted to other instances `/mnt/data/`

These are the instances info:
- **All** - Ubuntu Server 22.04 LTS (HVM), SSD Volume Type for Amazon Machine Image (AMI)
- **Submission Host** - t3.micro - 8GB storage gp3
- **Condor Master** - t3.micro - 8GB storage gp3
- **File Server (NFS)** - t3.small - 40GB storage gp3
- **Execution Host (Worker) 1** - t3.large - 8GB storage gp3
- **Execution Host (Worker) 2** - t3.large - 8GB storage gp3

Checklist / Workflow:
-
Start in `/mnt/data/use_cases/` with the raw dataset `full_weather.csv`, all codes in `code/`, all .sub files in `submit/`, and 4 empty directories `logs`, `clean`, `split` & `analysis`
> [!NOTE]
> Ensure all codes are executable & the empty directories have write permissions.

1. Initial cleaning to reduce duplicating bad data (`initial_clean.py`[^1] / `initial_clean.sub`[^2])
   - `full_weather.csv` => `clean_weather.csv`

2. Expand data by duplicating & adding some noise (`expand_data.py`[^1] / `expand_data.sub`[^2])
   - `clean_weather.csv` => `expanded_weather.csv`

3. Sort expanded data by datetime (`task_sort.sh` / `sort.sub`[^2])
   - `expanded_weather.csv` => `sorted_weather.csv`

4. Split data into parts and years (`split_data.py`[^1] / `split_data.sub`[^2])
   - `sorted_weather.csv` => `weather_<year>_part_<num>.csv`
   - e.g. `weather_1996_part_00.csv`, `weather_2023_part_00.csv`, `weather_2023_part_01.csv`, etc.
  
5. Remove `clean_weather.csv` & `expanded_weather.csv` to prevent insufficient disk space (`csv_cleanup.sh` / `csv_cleanup.sub`[^2])

6. Clean all split data in parallel (`clean_data.sub`[^2] - to run `clean_data.py` on all split CSVs)
   - `weather_<year>_part_<num>.csv` => `clean_<year>_part_<num>.csv`
   - e.g. `weather_2023_part_00.csv` => `clean_2023_part_00.csv`

7. Calculate heat index & detect heat waves (`analysis.sub`[^2] - to run `task_heat_index.py` & `task_heat_waves.py` on all split CSVs)
   - `clean_<year>_part_<num>.csv` => `heat_index_<year>_part_<num>.csv` & `heat_waves_<year>_part_<num>.csv`
  
8. ... (TO DO)
  
[^1]: If running python files manually, ensure it is run in either one of the **Execution Hosts** to have enough RAM and have the necessary import packages
[^2]: If using .sub files (`condor_submit <file>.sub`), ensure it is run on the **Submission Host**

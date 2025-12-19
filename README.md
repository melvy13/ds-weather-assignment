This repo utilizes the dataset Malaysia's Weather Data (1996-2024) [https://www.kaggle.com/datasets/shahmirvarqha/weather-data-malaysia] to simulate big data and do some processing on it.

Assume HTCondor is set up on Amazon EC2 instances (AWS Academy Learner Lab), and the NFS `/srv/data/` is mounted to other instances `/mnt/data/`

These are the instances info:
- **All** - Ubuntu Server 22.04 LTS (HVM), SSD Volume Type for Amazon Machine Image (AMI)
- **Submission Host** - t3.micro - 8GB storage gp3
- **Condor Master** - t3.micro - 8GB storage gp3
- **File Server (NFS)** - t3.small - 40GB storage gp3
- **Execution Host (Worker) 1** - t3.large - 8GB storage gp3
- **Execution Host (Worker) 2** - t3.large - 8GB storage gp3

Checklist / Workflow:

1. Initial cleaning to reduce bad data duplication later (`initial_clean.py`) - `full_weather.csv` => `clean_weather.csv`

2. Expand data by duplicating & adding some noise (`expand_data.py`) - `clean_weather.csv` => `expanded_weather.csv`

3. Sort expanded data by datetime (`sort.sh`) - `expanded_weather.csv` => `sorted_weather.csv`

4. Split data into parts and years (`split_data.py`) - `sorted_weather.csv` => `weather_<year>_part_<num>`

5. Clean all split data in parallel (clean_data.py) - `weather_<year>_part_<num>` => `clean_<year>_part_<num>`

6. Calculate heat index & detect heat waves (`task_heat_index.py` & `task_heat_waves`) - `clean_<year>_part_<num>`
This repo utilizes the dataset Malaysia's Weather Data (1996-2024) [https://www.kaggle.com/datasets/shahmirvarqha/weather-data-malaysia] to simulate big data and do some processing on it.

Assume HTCondor is set up on Amazon EC2 instances (AWS Academy Learner Lab), and the NFS `/srv/data/` is mounted to other instances `/mnt/data/`

These are the instances info:
- **All** - Ubuntu Server 22.04 LTS (HVM), SSD Volume Type for Amazon Machine Image (AMI)
- **Submission Host** - t3.micro - 8GB storage gp3
- **Condor Master** - t3.micro - 8GB storage gp3
- **File Server (NFS)** - t3.small - 40GB storage gp3
- **Execution Host (Worker) 1** - t3.large - 8GB storage gp3
- **Execution Host (Worker) 2** - t3.large - 8GB storage gp3

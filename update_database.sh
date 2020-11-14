#!/bin/bash
cd /home/halmari/power
source /home/halmari/anaconda3/etc/profile.d/conda.sh
conda activate /home/halmari/anaconda3/envs/notebook_env
python3 main.py
source /home/halmari/anaconda3/etc/profile.d/conda.sh
conda deactivate

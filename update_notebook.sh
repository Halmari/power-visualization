#!/bin/bash
cd /home/halmari/power/notebooks
source /home/halmari/anaconda3/etc/profile.d/conda.sh
conda activate /home/halmari/anaconda3/envs/notebook_env
jupyter nbconvert --to notebook --execute --inplace /home/halmari/power/notebooks/power_info_visualizer.ipynb
source /home/halmari/anaconda3/etc/profile.d/conda.sh
conda deactivate

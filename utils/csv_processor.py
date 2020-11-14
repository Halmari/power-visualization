import sys
import pandas as pd
import numpy as np

# Preprocessing data for database initial population
def load_transform_save(filename):

    data = pd.read_csv(filename)

    data.dropna(axis=0, inplace=True)
    data.drop(data.columns[[1,3]], axis=1, inplace=True)
    data.rename(columns={ 
        data.columns[0]: "Timestamp UTC",
        data.columns[1]: "Timestamp local"}, inplace=True)

    data.to_csv("processed_"+filename, index=False, doublequote=False)

for i in range(len(sys.argv)-1):
    print(sys.argv[i+1])
    load_transform_save(sys.argv[i+1])



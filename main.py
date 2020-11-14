import requests
from datetime import datetime
import time
import pandas as pd
from db_connector import append_db
from pytz import timezone
import os

#### FUNCTIONS AND VARIABLES ####

def get_file_contents(filename):
    try:
        with open(filename, 'r') as f:
            return f.read().strip()
    except FileNotFoundError:
        print("'%s' file not found" % filename)

def variable_ids_to_string(variable_ids):
    variable_string = ''
    for variable in variable_ids:
        if variable_string == '':
            variable_string+=str(variable)
        else:
            variable_string=variable_string+','+str(variable)
    return variable_string

def datetime_from_utc_to_local(utc_datetime):
    return utc_datetime.astimezone(timezone('Europe/Helsinki'))

def format_time(timestamp):
    return datetime.strptime(timestamp,'%Y-%m-%dT%H:%M:%S%z')

def create_dataframe(response_json, input_dict):
    df = pd.DataFrame()
    for i in range(len(response_json)):
        single_source = response_json[i]
        
        df = df.append(
            {"VariableId": str(single_source["variable_id"]),
            "Type": input_dict[single_source["variable_id"]], 
            "Value": single_source["value"],
            "Timestamp UTC": format_time(single_source["start_time"]),
            "Timestamp local": datetime_from_utc_to_local(format_time(single_source["start_time"]))}, 
            ignore_index=True)
    return df

api_key = get_file_contents(os.getcwd()+'/configuration/apikey.txt')

headers = {'x-api-key': api_key} 
format = 'json'

source_dict = {
188: "Nuclear", 
181: "Wind", 
191: "Water", 
205: "Other", 
202: "Industry generation",
201: "District heating"
}

total_prod_consum_dict = {
192: "Overall production", 
193: "Overall consumption"
}

import_export_surplus_deficit_dict = {
194: "Import/export", 
198: "Finnish prod surplus/deficit"
}

countries_dict = {
90: "Aland-Sweden", 
195: "Finland-Russia",  
187: "Finland-Norway", 
87: "Finland-Northern Sweden", 
180: "Finland-Estonia", 
89: "Finland-Central Sweden"
}

#### RETRIEVE DATA FROM FINGRID #### 

# Combine dicts
all_in_one_dict = {**source_dict, **total_prod_consum_dict, **import_export_surplus_deficit_dict, **countries_dict}

response = requests.get('https://api.fingrid.fi/v1/variable/event/'+format+'/'+variable_ids_to_string(all_in_one_dict.keys()), headers=headers)

if response:
    #print('Success! Status code is {}.'.format(response.status_code))
    pass
else:
    print('An error has occurred. Time is {}'.format(datetime.now()))

#### CREATE DATAFRAME #### 

df = create_dataframe(response.json(), all_in_one_dict)

# Check for NaN values in Value column and if found set to zero
nan_indices = df[df['Value'].isnull()].index.tolist()
if nan_indices:
	print("NaN found")
	df.loc[nan_indices, 'Value'] = 0

#### APPEND DATA TO DB ####

append_db(df)


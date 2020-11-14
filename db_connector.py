import psycopg2 as pg2
import pandas as pd
import sys
sys.path.append('.')
from configuration.config import config


def convert_to_timestamp(datetime): 
    return pd.Timestamp(datetime)

def convert_to_timestamp_local(datetime): 
    return pd.Timestamp(datetime, tz='Europe/Helsinki')

def append_db(df):

    conn = None
    try:

        params = config()
        conn = pg2.connect(**params)
       
        cur = conn.cursor()
        
        # TOTAL PRODUCTION AND CONSUMPTION TABLE

        insert_total_prod_consum = ''' 
                INSERT INTO total_prod_consum(time_utc, time_local, total_production, total_consumption) 
                VALUES (%s, %s, %s, %s);

                '''

        records_total_prod_consum = (
        convert_to_timestamp(df.loc[df["VariableId"]=="192", "Timestamp UTC"].values[0]),
        convert_to_timestamp_local(df.loc[df["VariableId"]=="192", "Timestamp local"].values[0]),
        df.loc[df["VariableId"]=="192", "Value"].values[0], 
        df.loc[df["VariableId"]=="193", "Value"].values[0])

        cur.execute(insert_total_prod_consum, records_total_prod_consum)

        ## PRODUCTION SOURCES TABLE
        
        insert_production_sources = ''' 
                INSERT INTO production_sources(time_utc, time_local, wind_production, other_production, industrial_cogeneration,
                cogeneration_district_heating, nuclear_power, hydro_power) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                '''
                
        records_production_sources = (
        convert_to_timestamp(df.loc[df["VariableId"]=="188", "Timestamp UTC"].values[0]),
        convert_to_timestamp_local(df.loc[df["VariableId"]=="188", "Timestamp local"].values[0]),
        df.loc[df["VariableId"]=="181", "Value"].values[0],
        df.loc[df["VariableId"]=="205", "Value"].values[0], 
        df.loc[df["VariableId"]=="202", "Value"].values[0], 
        df.loc[df["VariableId"]=="201", "Value"].values[0], 
        df.loc[df["VariableId"]=="188", "Value"].values[0],  
        df.loc[df["VariableId"]=="191", "Value"].values[0])

        cur.execute(insert_production_sources, records_production_sources)

        ## TRANSMISSION BETWEEN COUNTRIES TABLE

        insert_transmission_between_countries = ''' 
                INSERT INTO transmission_between_countries(time_utc, time_local, sweden_aland, finland_central_sweden, finland_norway, 
                finland_estonia, finland_russia, finland_northern_sweden) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                '''
                
        records_transmission_between_countries = (
        convert_to_timestamp(df.loc[df["VariableId"]=="87", "Timestamp UTC"].values[0]),
        convert_to_timestamp_local(df.loc[df["VariableId"]=="87", "Timestamp local"].values[0]),
        df.loc[df["VariableId"]=="90", "Value"].values[0],
        df.loc[df["VariableId"]=="89", "Value"].values[0],
        df.loc[df["VariableId"]=="187", "Value"].values[0],
        df.loc[df["VariableId"]=="180", "Value"].values[0],
        df.loc[df["VariableId"]=="195", "Value"].values[0], 
        df.loc[df["VariableId"]=="87", "Value"].values[0])

        cur.execute(insert_transmission_between_countries, records_transmission_between_countries)

        ## IMPORT EXPORT SURPLUS DEFICIT TABLE

        insert_import_export_surplus_deficit = ''' 
                INSERT INTO import_export_surplus_deficit(time_utc, time_local, production_surplus_deficit, net_import_export) 
                VALUES (%s, %s, %s, %s);
                '''

        records_import_export_surplus_deficit = (
        convert_to_timestamp(df.loc[df["VariableId"]=="194", "Timestamp UTC"].values[0]),
        convert_to_timestamp_local(df.loc[df["VariableId"]=="194", "Timestamp local"].values[0]),
        df.loc[df["VariableId"]=="194", "Value"].values[0], 
        df.loc[df["VariableId"]=="198", "Value"].values[0])

        cur.execute(insert_import_export_surplus_deficit, records_import_export_surplus_deficit)

        conn.commit()
        cur.close()
    except pg2.DatabaseError as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

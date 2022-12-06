import os
import requests
import pandas as pd
from src.env import user, password, CodeUp_sql_server



def get_CodeUp_db_url(database):
    '''
    Returns a formatted string using credentials stored in env.py that can be passed to a pd.read_sql() function
    '''
    host = CodeUp_sql_server
    return f'mysql+pymysql://{user}:{password}@{host}/{database}'


def get_store_data():
    '''
    Returns a dataframe of all store data in the tsa_item_demand database and saves a local copy as a csv file.
    '''
    query = '''
    SELECT *
    FROM items
    JOIN sales USING(item_id)
    JOIN stores USING(store_id) 
    '''
    
    df = pd.read_sql(query, get_CodeUp_db_url('tsa_item_demand'))
    df.to_csv('data/tsa_item_demand.csv', index=False)
    
    return df


def wrangle_store_data():
    '''
    Checks for a local cache of tsa_store_data.csv and if not present will run the get_store_data() function which acquires data from Codeup's mysql server
    '''
    filename = 'tsa_store_data.csv'
    
    #if os.path.isfile('data/simple_wrangled_zillow_2017.csv'):

    if os.path.isfile(f'data/{filename}'):
        df = pd.read_csv(f'data/{filename}')
    else:
        df = get_store_data()
        
    return df
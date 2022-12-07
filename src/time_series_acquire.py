import os
import requests
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from src.env import user, password, CodeUp_sql_server


########################################################
##############   Acquire DataFrames       ##############
########################################################


def get_CodeUp_db_url(database):
    '''
    Returns a formatted string using credentials stored in env.py that can be passed to a pd.read_sql() function
    '''
    host = CodeUp_sql_server
    return f'mysql+pymysql://{user}:{password}@{host}/{database}'


def set_index(df, index_variable):
    try:
        df[index_variable] = pd.to_datetime(df[index_variable])
    except ValueError as e:
        print('ValueError', e)
    df = df.set_index(index_variable).sort_index() 

    return df

def get_german_power():
    '''
    Returns a dataframe which contains the building of new, invogue forms of power Germany has built. 
    The forms of power include Wind, Solar, and 'wind plus solar' contrasted with the power consumption
    of the country
    '''


########################################################
#######     Simulated Store Data Functions       #######
########################################################

def get_store_df():
    df = wrangle_store_data()
    df = set_index(df, 'sale_date')
    df = prep_store_data(df)

    return df


def query_for_store_data():
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
    df.to_csv('data/tsa_item_data.csv', index=False)
    
    return df


def wrangle_store_data():
    '''
    Checks for a local cache of tsa_store_data.csv and if not present will run the get_store_data() function which acquires data from Codeup's mysql server
    '''
    filename = 'tsa_store_data.csv'
    

    if os.path.isfile(f'data/{filename}'):
        df = pd.read_csv(f'data/{filename}', index_col = 0)
    else:
        df = query_for_store_data()
        
    return df


def prep_store_data(df):
    #df = set_index(wrangle_store_data(), 'sale_date')
    df['month'] = df.index.month_name()
    df['day_of_week'] = df.index.day_name()
    df['sales_total'] = df.sale_amount * df.item_price
    return df



########################################################
#######     German Green Energy DataFrame        #######
########################################################

def get_german_power():
    df = acquire_german_power()
    df = german_power_columns(df)
    df = set_index(df, 'date')

    return df



def acquire_german_power():
    '''
    Returns a dataframe which contains the building of new, invogue forms of power Germany has built. 
    The forms of power include Wind, Solar, and 'wind plus solar' contrasted with the power consumption
    of the country
    '''
    filename = 'german_power.csv'

    if os.path.isfile(f'data/{filename}'):
        df = pd.read_csv(f'data/{filename}', index_col = 0)
    
    else:
        df = pd.read_csv('https://raw.githubusercontent.com/jenfly/opsd/master/opsd_germany_daily.csv')
        df.to_csv(f'data/{filename}')
    
    return df 

def german_power_columns(df):
    '''
    Acquires the german power data frame. Changes column names and returns the dataframe
    '''
    df = df.rename(columns = {
        'Date':'date',
        'Consumption':'consumption',
        'Wind':'wind',
        'Solar':'solar',
        'Wind+Solar':'wind_solar'
    })

    return df 


###############################################################
#######  Plot distributions of columns given dataframe  #######
###############################################################

def plot_distributions(df):
    for col in list(df.columns):
        plt.figure()
        sns.histplot(df[col])
        plt.title('Distribution of {}'.format(col))
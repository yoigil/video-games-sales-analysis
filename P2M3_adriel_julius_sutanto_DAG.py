'''
=================================================
Milestone 3

Nama  : Adriel Julius Sutanto
Batch : FTDS-026-RMT

This program is made to automatically load and transform a dataset from PostgreSQL to ElasticSearch. 
The dataset in question is regarding the sales of video games all across the world.
=================================================
'''

# import libraries
import psycopg2
import re
import pandas as pd
import datetime as dt
import warnings
warnings.filterwarnings('ignore')
from elasticsearch import Elasticsearch

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# --------------------------------------------------------
## STEP 1: OBTAIN DATA FROM POSTGRES

# create a function to fetch data from postgresql
def fetch():
    
    '''
    This function is specifically made to fetch raw data from postgres.
    
    Return:
    data = raw data regarding video game sales, obtained from postgres
    
    '''
    
    # configure database
    db_name = 'airflow'
    db_user = 'airflow'
    db_pass = 'airflow'
    db_host = 'postgres'
    db_port = '5432'
    
    # connect to database
    connection = psycopg2.connect(
        database = db_name,
        user = db_user,
        password = db_pass,
        host = db_host,
        port = db_port
    )
    
    # get all data
    select_query = 'SELECT * FROM table_m3'
    data = pd.read_sql(select_query, connection)
    
    # close the connection
    connection.close()
    
    # save into csv
    data.to_csv('/opt/airflow/dags/P2M3_adriel_julius_sutanto_data_raw.csv', index=False)

# --------------------------------------------------------
## STEP 2: DATA CLEANING

# create a function to clean the data
def data_cleaning():

    '''
    This function is specifically made to clean raw data obtained by function fetch().
    
    Return:
    data = clean data regarding video game sales
    
    '''
    
    # load the dataset
    data = pd.read_csv('/opt/airflow/dags/P2M3_adriel_julius_sutanto_data_raw.csv')
    # drop the column called 'Rank' since there is an identical column name 'index'
    data.drop(columns='Rank')
    # put the column names into a list
    cols = data.columns.to_list()
    # create an empty list to store the new column names
    new_cols = []

    ## renaming column names into lowercase and turn spaces into _
    
    # for every name in column names,
    for col in cols:
        # find special characters on each column name
        new_col = re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', col)
        # turn every letter into lowercase
        new_col = [char.lower() for char in new_col]
        # replace spaces with _
        new_col = '_'.join(new_col)
        # append the new column names into the empty list
        new_cols.append(new_col)
    
    # rename each column in the data by the new column names
    data.columns = new_cols
    
    ## handling missing values
    
    ## column year and publisher have missing values in the dataset
    
    # data imputation with the most frequent data for column publisher
    data.publisher = data.publisher.fillna(data.publisher.mode()[0])
    
    # data imputation with the median data for column year
    data.year = data.year.fillna(data.year.median())
    
    # remove the decimal from the column year
    data.year = data.year.astype(int)
    
    # turn the column year into string so that it could be categorized in visualization
    data.year = data.year.astype(str)
    
    # save the clean data into csv
    data.to_csv('/opt/airflow/dags/P2M3_adriel_julius_sutanto_data_clean.csv', index=False)

# --------------------------------------------------------
## STEP 3: INSERT INTO ELASTIC SEARCH

# create a function to insert the clean data to elastic search with airflow
def insert_to_elastic():
    
    '''
    This function is specifically made to insert the clean data into elastic.
    
    '''
    
    # load the clean data
    data1 = pd.read_csv('/opt/airflow/dags/P2M3_adriel_julius_sutanto_data_clean.csv')
    
    # check connection
    es = Elasticsearch('http://elasticsearch:9200')
    print('Connection Status: ', es.ping())
    
    # insert csv file to elastic search
    failed_insert = []
    for i, r in data1.iterrows():
        doc = r.to_json()
        try:
            print(i, r['name'])
            res = es.index(index='video_game_sales', doc_type='doc', body=doc)
        except:
            print('Failed Index: ', failed_insert)
            pass
            
# --------------------------------------------------------
## STEP 4: DATA PIPELINE

# create a default argument
default_args = {
    'owner' : 'Adriel',
    'start_date' : dt.datetime(2024, 1, 23, 14, 30, 0) - dt.timedelta(hours=8),
    'retries' : 1,
    'retry_delay' : dt.timedelta(minutes=5)
}

# create a dag function
with DAG(
    'milestone3',
    default_args=default_args,
    schedule_interval= '7 * * * *',
    catchup=False) as dag:
    
    start = BashOperator(
        task_id = 'start',
        bash_command = 'echo "Currently reading the .csv file now"')
    
    fetch_data = PythonOperator(
        task_id = 'fetch-data',
        python_callable = fetch)
    
    clean_data = PythonOperator(
        task_id = 'clean-data',
        python_callable = data_cleaning)
    
    elastic = PythonOperator(
        task_id = 'insert-to-elastic',
        python_callable = insert_to_elastic)

# create the order of execution
start >> fetch_data >> clean_data >> elastic
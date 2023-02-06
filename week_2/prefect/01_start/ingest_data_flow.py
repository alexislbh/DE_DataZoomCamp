#!/usr/bin/env python
# coding: utf-8

import os
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from prefect import task, flow
from prefect.tasks import task_input_hash
from datetime import timedelta
#from prefect_sqlalchemy import SQLAlchemyConnector

'''From DTC fmt=csv.gz or NYC fmt=parquet 2019 01 to 2021 07'''
color = 'yellow'
year = 2021
month = 3
file_name = f'{color}_tripdata_{year}-{month:02}'
csv_end= '.csv.gz'
nyc_end = '.parquet'
csv_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{file_name}{csv_end}'
nyc_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}{nyc_end}'
url_file = nyc_url
table = f'{color}_{year}_{month:02}'

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url: str):
    file_name = url.split('/')[-1]
    os.system(f'wget {url} -O {file_name}')
    if file_name.split('.')[1] == 'csv':
        df = pd.read_csv(file_name, compression='gzip', low_memory=False)
    else:
        df = pq.read_table(file_name).to_pandas()
    print(f'Data from {file_name} readed')
    os.remove(file_name)
    return df

@task(log_prints=True)
def transform_data(df):
    print(f'pre: missing passenger count: {df.passenger_count.isin([0]).sum()}')
    df = df[df.passenger_count > 0]
    print(f'post: missing passenger count: {df.passenger_count.isin([0]).sum()}')
    return df

@task(log_prints=True, retries=3)
def ingest_data(user, password, host, port, database, table, df):
    engine = create_engine('postgresql+psycopg2://'+user+':'+password+'@'+host+':'+port+'/'+database)
    df.to_sql(table, engine, index=False, if_exists='append')
    print(f'Data from {file_name} ingested\nDatabase : {database} - Table : {table}')

@flow(name="Ingest Data")
def main_flow(table: str, url_file: str):
    user = 'root'
    password = 'root'
    host = 'pgdatabase'
    port = '5432'
    database = 'ny_taxi'

    raw_data = extract_data(url_file)
    data = transform_data(raw_data)
    ingest_data(user, password, host, port, database, table, data)

if __name__ == '__main__':
    main_flow(table, url_file)
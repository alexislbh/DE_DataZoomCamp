#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import traceback
import spinner

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    url_file = params.url_file

    file_name = url_file.split('/')[-1]
    engine = create_engine('postgresql://'+user+':'+password+'@'+host+':'+port+'/'+database)
    os.system(f'wget {url_file} -O {file_name}')
    if file_name.split('.')[1] == 'csv':
        df = pd.read_csv(file_name)
    else:
        df = pq.read_table(file_name).to_pandas()
    print(f'Data from {file_name} readed')

    try:
        with spinner.Spinner():
            df.to_sql(table, engine, index=False, if_exists='append')
    except Exception:
        traceback.print_exc()
    else:
        print(f'Data from {file_name} ingested\nDatabase : {database} - Table : {table}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Ingest parquet data into postgresql')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host name for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--database', help='database name for postgres')
    parser.add_argument('--table', help='table name for postgres')
    parser.add_argument('--url_file', help='url of file for postgres')
    args = parser.parse_args()

    main(args)

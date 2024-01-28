#!/usr/bin/env python
# coding: utf-8
# ### Dataset Link : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

import pandas as pd
from sqlalchemy import create_engine
from time import time
import argparse
import os


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table
    file_url = params.file_url
    
    csv_name = 'output.csv'
    os.system(f"curl -O {file_url}")
    filename = os.path.basename(file_url)
    print(filename)
    
    df = pd.read_parquet(filename)
    df.to_csv(csv_name, index=False)


    # ## SQL-Alchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    engine.connect()

    df_csv = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)
    df_iter = next(df_csv)

    df_iter.lpep_pickup_datetime = pd.to_datetime(df_iter.lpep_pickup_datetime)
    df_iter.lpep_dropoff_datetime = pd.to_datetime(df_iter.lpep_dropoff_datetime)

    df_iter.head(n=0).to_sql(name=table, con=engine, if_exists='replace')


    # # ## Inserting Records into Table
    df_csv = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    while True:
        t_start = time()
        df_iter = next(df_csv)
        
        df_iter.lpep_pickup_datetime = pd.to_datetime(df_iter.lpep_pickup_datetime)
        df_iter.lpep_dropoff_datetime = pd.to_datetime(df_iter.lpep_dropoff_datetime)

        df_iter.to_sql(name=table, con=engine, if_exists='append')
        t_end = time()

        print('Inserting another chunk... , time taken %.3f seconds ' % (t_end - t_start))
    

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV into Postgres')

    # user, password, host, port, database, table, csv_url
    parser.add_argument('--user', help='Database User')
    parser.add_argument('--password', help='Database Password')
    parser.add_argument('--host', help='Database Host')
    parser.add_argument('--port', help='Database Port')
    parser.add_argument('--database', help='Database Name')
    parser.add_argument('--table', help='Table Name')
    parser.add_argument('--file_url', help='Url of Parquet File')

    args = parser.parse_args()
    main(args)
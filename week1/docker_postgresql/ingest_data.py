#!/usr/bin/env python
# coding: utf-8

import argparse
from time import time

import pandas as pd
from sqlalchemy import create_engine

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name1 = params.table_name[0]
    table_name2 = params.table_name[1]
    url1 = params.url[0]
    url2 = params.url[1]
    
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

    df_iter = pd.read_csv(url1, iterator=True, chunksize=100000)

    # First chunk
    df=next(df_iter)

    # Fixing the date time columns
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    # Replacing table metadata for fresh data import
    df.head(n=0).to_sql(name=table_name1, con=engine, if_exists='replace')

    # Inserting first 100K chunk
    df.to_sql(name=table_name1, con=engine, if_exists='append') 

    # Iterator to finish the data import
    for df in df_iter:
        t_start = time()
        
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.to_sql(name=table_name1, con=engine, if_exists='append')
        t_end = time()
        row_count = len(df)
        print(f'Inserted another {row_count} chunks of data, and took {t_end - t_start:.3f} second')

    taxi_zone = pd.read_csv(url2)
    
    taxi_zone.to_sql(name=table_name2, con=engine, if_exists='replace')
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Ingest csv format data to Postgres")


    parser.add_argument('--user', help="user name for postgres")
    parser.add_argument('--password', help="password for postgres")
    parser.add_argument('--host', help="host for postgres")
    parser.add_argument('--port', help="port for postgres")
    parser.add_argument('--db', help="database name for postgres")
    parser.add_argument('--table_name', nargs=2, help="name of the tables to write results in Postgres")
    parser.add_argument('--url', nargs=2, help="URLs for CSV files")

    args = parser.parse_args()


    main(args)




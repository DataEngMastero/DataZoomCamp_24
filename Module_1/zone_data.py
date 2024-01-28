
import pandas as pd
from sqlalchemy import create_engine
import argparse


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    database = params.database
    table = params.table

    
    zone_file = "../csvs/taxi+_zone_lookup.csv"
    zone_df = pd.read_csv(zone_file)
    print(zone_df)


    # ## SQL-Alchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    engine.connect()

    zone_df.to_sql(name=table, con=engine, if_exists='replace')


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
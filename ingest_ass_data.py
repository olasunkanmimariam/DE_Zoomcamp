import os
import argparse
import pandas as pd

from time import time
from sqlalchemy import create_engine




def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = "output_ass.csv"

    # Download the gzip file
    os.system(f"wget {url} -O {csv_name}.gz")

    # Unzip the downloaded gzip file
    os.system(f"gzip -d {csv_name}.gz")

    # creating connection to postgres
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Now we want to inject data in batches as the whole data is too much
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Inserted another chunk, took %.3f seconds' % (t_end - t_start))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Injest CSV data to postgres')

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name where we will write the result')
    parser.add_argument('--url', help='url of the gzip csv file')

    args = parser.parse_args()

    main(args)

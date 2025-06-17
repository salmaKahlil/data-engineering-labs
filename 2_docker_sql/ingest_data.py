import pandas as pd
from sqlalchemy import create_engine
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    csv_name = 'yellow_tripdata_2021-01.csv'

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    while True:
        df=next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        print('inserted another chunk!')

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data to PostgreSQL")

    parser.add_argument("--user", help="PostgreSQL user")
    parser.add_argument("--password", help="PostgreSQL password")
    parser.add_argument("--host", help="PostgreSQL host")
    parser.add_argument("--port", help="PostgreSQL port")
    parser.add_argument("--db", help="PostgreSQL database name")
    parser.add_argument("--table-name", help="PostgreSQL table name")
    args = parser.parse_args()
    main(args)

#docker run it --network=pg-network taxi_ingest:v001 --user=root --password=root --host=pg-database --port=5432 --db=ny_taxi --table-name=yellow_taxi_trips
     

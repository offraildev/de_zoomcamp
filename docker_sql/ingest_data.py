#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time

import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine


def main(params: dict) -> None:
    engine = create_engine(
        f"postgresql://{params.user}:{params.passw}@{params.host}:{params.port}/{params.db}"
    ).connect()
    trips_downloaded_file = "./raw_data/yellow_tripdata_2023-01.parquet"
    zones_downloaded_file = "./raw_data/taxi+_zone_lookup.csv"
    os.system(f"wget {params.trips_url} -O {trips_downloaded_file}")
    os.system(f"wget {params.zones_url} -O {zones_downloaded_file}")

    zones_df = pd.read_csv(zones_downloaded_file)
    zones_df.head(0).to_sql(
        name=f"{params.zones_table}", con=engine, if_exists="replace"
    )
    zones_df.to_sql(name=f"{params.zones_table}", con=engine, if_exists="append")
    print(f"Inserted zones df!")

    tripdata_pq = pq.ParquetFile(trips_downloaded_file)

    batch_size = 100000
    tripdata_pq_iter = tripdata_pq.iter_batches(batch_size=batch_size)

    # write header columns
    df = next(tripdata_pq_iter).to_pandas()
    df.head(0).to_sql(name=f"{params.trips_table}", con=engine, if_exists="replace")

    # write rows to database
    for batch in tripdata_pq_iter:
        start_time = time()

        df = batch.to_pandas()
        df.to_sql(name=f"{params.trips_table}", con=engine, if_exists="append")

        end_time = time()
        print(f"Inserted another chunk, took time {end_time-start_time:.3f} seconds!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="data_ingest_script",
        description="Ingest yellow cab trip parquet data to postgres.",
    )

    parser.add_argument(
        "--user",
        type=str,
        help="user name for postgres",
        default=os.environ.get("POSTGRES_USER"),
    )
    parser.add_argument(
        "--passw",
        type=str,
        help="passw for postgres",
        default=os.environ.get("POSTGRES_PASSWORD"),
    )
    parser.add_argument(
        "--port",
        type=str,
        help="port for postgres",
        default=os.environ.get("POSTGRES_PORT"),
    )
    parser.add_argument(
        "--host",
        type=str,
        help="host name for postgres",
        default=os.environ.get("POSTGRES_HOST"),
    )
    parser.add_argument(
        "--db",
        type=str,
        help="dataBase name for postgres",
        default=os.environ.get("POSTGRES_DB"),
    )
    parser.add_argument(
        "--zones_table",
        type=str,
        help="table name for zones data",
        default=os.environ.get("ZONES_TABLE"),
    )
    parser.add_argument(
        "--trips_table",
        type=str,
        help="table name for taxi trips data",
        default=os.environ.get("TRIPS_TABLE"),
    )
    parser.add_argument(
        "--zones_url",
        type=str,
        help="url to fetch zones data",
        default=os.environ.get("ZONES_URL"),
    )
    parser.add_argument(
        "--trips_url",
        type=str,
        help="url to fetch trips data",
        default=os.environ.get("TRIPS_URL"),
    )
    args = parser.parse_args()
    main(args)

from dagster import asset, MetadataValue, MaterializeResult
from dagster_duckdb import DuckDBResource
from ..partitions import monthly_partition
from . import constants

import os
import pandas as pd
import requests

@asset(
        partitions_def=monthly_partition,
        group_name="raw_files",
)
def taxi_trips_file(context):
    """
      The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal.
    """
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet",
        timeout=15
    )

    with open(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch), "wb") as output_file:
        output_file.write(raw_trips.content)

    num_rows = len(pd.read_parquet(constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)))

    return MaterializeResult(
        metadata={
            'Number of records': MetadataValue.int(num_rows)
        }
    )

@asset(
    group_name="raw_files",
)
def taxi_zones_file():
    """
      The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal.
    """
    raw_data = requests.get(
        "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD",
        timeout=15
    )

    with open(constants.TAXI_ZONES_FILE_PATH, "wb") as output_file:
        output_file.write(raw_data.content)

    num_rows = len(pd.read_csv(constants.TAXI_ZONES_FILE_PATH))

    return MaterializeResult(
        metadata={
            'Number of records': MetadataValue.int(num_rows)
        }
    )

@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="ingested",
)
def taxi_trips(context, database: DuckDBResource):
    """
      The raw taxi trips dataset, loaded into a DuckDB database
    """
    partition_month = context.partition_key[:-3]
    create_table_query = """
        CREATE TABLE IF NOT EXISTS trips (
          vendor_id INT,
          pickup_zone_id INT,
          dropoff_zone_id INT,
          rate_code_id BIGINT,
          payment_type BIGINT,
          dropoff_datetime TIMESTAMP,
          pickup_datetime TIMESTAMP,
          trip_distance DOUBLE,
          passenger_count BIGINT,
          total_amount DOUBLE,
          partition_date VARCHAR
        );
    """
    sql_query = """
        insert into trips
          select
            VendorID as vendor_id,
            PULocationID as pickup_zone_id,
            DOLocationID as dropoff_zone_id,
            RatecodeID as rate_code_id,
            payment_type as payment_type,
            tpep_dropoff_datetime as dropoff_datetime,
            tpep_pickup_datetime as pickup_datetime,
            trip_distance as trip_distance,
            passenger_count as passenger_count,
            total_amount as total_amount,
            '{dt}' as partition_date,
          from 'data/raw/taxi_trips_{dt}.parquet'
        ;
    """.format(dt=partition_month)

    with database.get_connection() as conn:
      conn.execute(create_table_query)
      conn.execute("DELETE FROM trips WHERE partition_date = '{}' ;".format(partition_month))
      print(partition_month)
      conn.execute(sql_query)

@asset(
    deps=["taxi_zones_file"],
    group_name="ingested",
)
def taxi_zones(database: DuckDBResource):
    """
      The raw taxi zones dataset, loaded into a DuckDB database
    """
    sql_query = f"""
        create or replace table zones as (
          select
            LocationID as zone_id,
            zone,
            borough,
            the_geom as geometry
          from '{constants.TAXI_ZONES_FILE_PATH}'
        );
    """

    with database.get_connection() as conn:
      conn.execute(sql_query)

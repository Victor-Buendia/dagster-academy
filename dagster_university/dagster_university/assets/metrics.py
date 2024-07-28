from dagster import asset

import plotly.express as px
import plotly.io as pio
import geopandas as gpd

from dagster_duckdb import DuckDBResource
from ..partitions import weekly_partition
import datetime
import os

from . import constants

@asset(
    deps=["taxi_trips", "taxi_zones"]
)
def manhattan_stats(database: DuckDBResource):
    query = """
        select
            zones.zone,
            zones.borough,
            zones.geometry,
            count(1) as num_trips,
        from trips
        left join zones on trips.pickup_zone_id = zones.zone_id
        where borough = 'Manhattan' and geometry is not null
        group by all
    """

    with database.get_connection() as conn:
        trips_by_zone = conn.execute(query).fetch_df()

    trips_by_zone["geometry"] = gpd.GeoSeries.from_wkt(trips_by_zone["geometry"])
    trips_by_zone = gpd.GeoDataFrame(trips_by_zone)

    with open(constants.MANHATTAN_STATS_FILE_PATH, 'w') as output_file:
        output_file.write(trips_by_zone.to_json())

@asset(
    deps=["manhattan_stats"],
)
def manhattan_map():
    trips_by_zone = gpd.read_file(constants.MANHATTAN_STATS_FILE_PATH)

    fig = px.choropleth_mapbox(trips_by_zone,
        geojson=trips_by_zone.geometry.__geo_interface__,
        locations=trips_by_zone.index,
        color='num_trips',
        color_continuous_scale='Plasma',
        mapbox_style='carto-positron',
        center={'lat': 40.758, 'lon': -73.985},
        zoom=11,
        opacity=0.7,
        labels={'num_trips': 'Number of Trips'}
    )

    pio.write_image(fig, constants.MANHATTAN_MAP_FILE_PATH)

@asset(
        deps=["taxi_trips", "manhattan_stats"],
        partitions_def=weekly_partition,
)
def trips_by_week(context, database: DuckDBResource):
    week_partition = context.partition_key
    base_query = """
        CREATE OR REPLACE TEMP TABLE tbw{idx} AS (
            SELECT
                DATE_TRUNC('week', pickup_datetime)::DATE AS period
                , COUNT(*)::INT AS num_trips
                , SUM(passenger_count)::INT AS passenger_count
                , ROUND(SUM(total_amount)::FLOAT, 2) AS total_amount
                , ROUND(SUM(trip_distance)::FLOAT, 2) AS trip_distance
            FROM
                trips
            WHERE
                DATE_TRUNC('week', pickup_datetime::DATE) = DATE_TRUNC('week', '{week}'::DATE)
            GROUP BY
                all
        );

        {extra}
    """

    with database.get_connection() as conn:

        start_date = datetime.datetime.strptime(week_partition, constants.DATE_FORMAT)
        end_date = start_date + datetime.timedelta(weeks=1)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS trips_by_week (
                period DATE,
                num_trips INT,
                passenger_count INT,
                total_amount FLOAT,
                trip_distance FLOAT,
            );
        """)
        current_date = start_date
        while current_date < end_date:
            
            query = base_query.format(
                extra='' if current_date == start_date else "CREATE OR REPLACE TEMP TABLE tbw AS ((SELECT * FROM tbw) UNION (SELECT * FROM tbw0));",
                idx='' if current_date == start_date else '0',
                week=datetime.datetime.strftime(current_date, constants.DATE_FORMAT),
            )
            current_date += datetime.timedelta(weeks=1)
            conn.execute(query)

            print('\n\n', current_date)
            print(query)
            print(conn.execute("SHOW TABLES;").fetch_df())
            print(conn.execute("SELECT * FROM tbw;").fetch_df())

        
        conn.execute("INSERT INTO trips_by_week (SELECT * FROM tbw);")
        conn.execute("COPY (SELECT * FROM trips_by_week) TO '{file_path}' (HEADER, DELIMITER ',');".format(file_path=constants.TRIPS_BY_WEEK_FILE_PATH))

if __name__ == "__main__":
    trips_by_week()
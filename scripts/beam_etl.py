import argparse

import numpy as np

import apache_beam as beam
from apache_beam.dataframe.io import read_csv
from apache_beam.dataframe.convert import to_pcollection, to_dataframe
from apache_beam.dataframe.pandas_top_level_functions import DeferredPandasModule

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

def get_station_location(df, df_station, start_or_end):
    if start_or_end == "start":
        left_key = "StartStation Id"
        column_location_name = "start_location"
    elif start_or_end == "end":
        left_key = "EndStation Id"
        column_location_name = "end_location"
    
    df = df.merge(df_station.set_index('id'), left_on=left_key, right_index=True)
    df[column_location_name] = df["latitude"].astype(str) + ", " + df["longitude"].astype(str)
    df = df.drop(["latitude", "longitude"], axis=1)
    return df

def format_datetime(df, column):
    df[column] = DeferredPandasModule.to_datetime(df[column], format=f"%d/%m/%Y %H:%M")
    return df

def df_preparation_to_bq(df):
    df = df.rename(columns={"Rental Id": "rental_id", "Duration": "duration", "Bike Id": "bike_id", "End Date": "end_date",
                        "EndStation Id": "end_station_id", "EndStation Name": "end_station_name", "Start Date": "start_date",
                        "StartStation Id": "start_station_id", "StartStation Name": "start_station_name"})
    df = df[["rental_id", "duration", "bike_id", "start_date", "start_station_id", "start_station_name", "start_location", 
                        "end_date", "end_station_id", "end_station_name", "end_location"]]
    
    pcoll = to_pcollection(df)
    
    schema_hires = {"fields": [
                        {"name": "rental_id", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "duration", "type": "INTEGER", "mode": "NULLABLE"},
                        {"name": "bike_id", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "start_date", "type": "DATE", "mode": "NULLABLE"},
                        {"name": "start_station_id", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "start_station_name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "start_location", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "end_date", "type": "DATE", "mode": "NULLABLE"},
                        {"name": "end_station_id", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "end_station_name", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "end_location", "type": "STRING", "mode": "NULLABLE"}
                    ]
                }
    
    return pcoll, schema_hires

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_folder',
        dest='input_folder',
        help='Input folder with csv files to process.')
    parser.add_argument(
        '--station_data',
        dest='station_data',
        help='Station data to process.')
    parser.add_argument(
        '--dest_table_hires',
        dest='dest_table_hires',
        help='Output table to write results to.')
    parser.add_argument(
        '--dest_table_daily_agg',
        dest='dest_table_daily_agg',
        help='Output table to write results to.')
    
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        
        df_station = p | "ReadStationData" >> read_csv(known_args.station_data, dtype={"id": "str"})
        df_station = df_station[["id", "latitude", "longitude"]]

        df = p | "ReadHiresData" >> read_csv(known_args.input_folder, dtype={"Rental Id": "str", "Bike Id": "str", "EndStation Id": "str", "StartStation Id": "str"})        
        df = df[df["Duration"] != np.nan]
        df = get_station_location(df, df_station, "start")
        df = get_station_location(df, df_station, "end")

        df = format_datetime(df, "Start Date")
        df = format_datetime(df, "End Date")

        pcoll, schema_hires = df_preparation_to_bq(df)

        pcoll | beam.io.WriteToBigQuery(known_args.dest_table_hires,
                                                schema=schema_hires,
                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        
        # load_to_bq(df, args.project_id, args.dest_table_hires)

        # df["start_date"] = df["start_date"].dt.strftime(f"%d-%m-%Y")

        # df_daily_agg = get_daily_agg(df)

        # load_to_bq(df_daily_agg, args.project_id, args.dest_table_daily_agg)
    
if __name__ == '__main__':
    run()
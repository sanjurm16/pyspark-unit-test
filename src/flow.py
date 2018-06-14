
from typing import Dict

from pyspark.sql import SparkSession

from file_processor import load_data, write_data
from flight_analyser import find_the_late_flights, enrich_with_flight_desc


def execute(spark: SparkSession, args: Dict):


    arrival_df = load_data(spark, args.arrival_data_file_location)

    late_flights_df = find_the_late_flights(arrival_df)

    carrier_desc_df = load_data(spark, args.carrier_description_file_location)

    late_flights_enriched_df = enrich_with_flight_desc(late_flights_df, carrier_desc_df)

    write_data( df=late_flights_enriched_df, destination_path="/home/sanjay/devrats/output")



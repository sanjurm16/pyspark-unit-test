from pyspark.sql.functions import *
from pyspark.sql import DataFrame


def find_the_late_flights(df: DataFrame) -> DataFrame:
    late_arriving_flights_df = df.where("ArrDelay> 0").groupBy("UniqueCarrier")\
        .count().where("count>0").orderBy(desc("count"))
    return late_arriving_flights_df


def enrich_with_flight_desc(flight_arrival_df: DataFrame, flight_desc_df: DataFrame) -> DataFrame:
    late_flight_df = flight_arrival_df.join(flight_desc_df, flight_arrival_df.UniqueCarrier == flight_desc_df.Code)
    return late_flight_df

import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from flight_analyser import find_the_late_flights, enrich_with_flight_desc

test_flight_arrival_data = [('BA104',20),
             ('AI101',30),
             ('BA104',10),
             ('AI101', 0)]

test_flight_arrival_schema = StructType([
    StructField("UniqueCarrier", StringType(), True),
    StructField("ArrDelay", IntegerType(), True)
])

test_flight_description_data = [('BA104','British Airways'),
             ('AI101','Air India')]

test_flight_description_schema = StructType([
    StructField("Code", StringType(), True),
    StructField("Description", StringType(), True)
])


@pytest.fixture(scope="module")
def flight_df(spark):
    #type: (SparkSession) -> DataFrame
    df=spark.createDataFrame(test_flight_arrival_data, test_flight_arrival_schema)
    return df


@pytest.fixture(scope="module")
def flight_desc_df(spark):
    df = spark.createDataFrame(test_flight_description_data, test_flight_description_schema)
    return df


def test_find_the_late_flights(flight_df):
    late_flight_df = find_the_late_flights(flight_df)
    assert late_flight_df.count() == 2


def test_enrich_with_flight_desc(flight_df, flight_desc_df):
    late_flight_df = find_the_late_flights(flight_df)
    enriched_df = enrich_with_flight_desc(late_flight_df, flight_desc_df)
    assert enriched_df.select("Description").count() == 2
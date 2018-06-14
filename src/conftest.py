import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").getOrCreate()
    return spark
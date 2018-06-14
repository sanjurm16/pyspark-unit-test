import argparse
from typing import Dict

from pyspark import SparkContext
from pyspark.sql import SparkSession

#Problem statement: based on the flight arrival data, find out the top 3 flights late arriving flights
from flow import execute


def main(spark: SparkSession, args: Dict):
    print("This is spark")
    execute(spark, args)


if __name__ == '__main__':
    spark_session = SparkSession(SparkContext.getOrCreate())
    parser = argparse.ArgumentParser(description='spark some files')
    parser.add_argument("arrival_data_file_location", help="file location of the flight arrival data")
    parser.add_argument("carrier_description_file_location", help="file location of the carrier description")
    args = parser.parse_args()
    main(spark_session, args)

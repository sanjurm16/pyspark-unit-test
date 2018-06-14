from pyspark.sql import SparkSession, DataFrame


def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    spark_df = spark.read.csv(path=file_path,inferSchema=True, header=True)
    return spark_df


def write_data(df: DataFrame, destination_path: str ):
    df.write.csv(path=destination_path, header=True)


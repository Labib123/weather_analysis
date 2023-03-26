from pyspark.sql import SparkSession


def get_spark_session():
    return SparkSession.builder.appName("WeatherAnalysis").getOrCreate()


def save_data_as_parquet(spark, data, file_path):
    data.write.mode("overwrite").parquet(file_path)


def read_data_from_parquet(spark, file_path):
    return spark.read.parquet(file_path)

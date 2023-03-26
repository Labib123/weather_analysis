from pyspark.sql.functions import year, month, dayofmonth
from data_store_client import DataStoreClient
from utils import get_config


def store_data(spark, input_file):
    """
    Stores the preprocessed weather data in a data store.
    """
    # Read the preprocessed data into a DataFrame
    data = spark.read.parquet(input_file)

    # Add additional columns for year, month, and day
    data = data.withColumn("year", year(data.date))
    data = data.withColumn("month", month(data.date))
    data = data.withColumn("day", dayofmonth(data.date))

    # Get the configuration settings for the data store
    config = get_config("data_store")

    # Create a DataStoreClient instance and store the data
    client = DataStoreClient(config)
    client.store_data(data)

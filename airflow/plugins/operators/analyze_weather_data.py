import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from pyspark.sql.functions import mean, max, min, stddev


class AnalyzeWeatherDataOperator(BaseOperator):
    """
    Operator that performs some basic analysis on the weather data.
    """

    @apply_defaults
    def __init__(
            self,
            file_path: str,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_path = file_path

    def execute(self, context):
        logging.info(f"Analyzing weather data from {self.file_path}...")
        df = spark.read.parquet(self.file_path)

        avg_temp = df.select(mean("main.temp")).collect()[0][0]
        max_temp = df.select(max("main.temp")).collect()[0][0]
        min_temp = df.select(min("main.temp")).collect()[0][0]
        temp_stddev = df.select(stddev("main.temp")).collect()[0][0]

        avg_humidity = df.select(mean("main.humidity")).collect()[0][0]
        max_humidity = df.select(max("main.humidity")).collect()[0][0]
        min_humidity = df.select(min("main.humidity")).collect()[0][0]
        humidity_stddev = df.select(stddev("main.humidity")).collect()[0][0]

        logging.info(f"Average temperature: {avg_temp}")
        logging.info(f"Maximum temperature: {max_temp}")
        logging.info(f"Minimum temperature: {min_temp}")
        logging.info(f"Temperature standard deviation: {temp_stddev}")
        logging.info(f"Average humidity: {avg_humidity}")
        logging.info(f"Maximum humidity: {max_humidity}")
        logging.info(f"Minimum humidity: {min_humidity}")
        logging.info(f"Humidity standard deviation: {humidity_stddev}")

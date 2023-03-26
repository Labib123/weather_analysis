import logging
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from openweathermap_client import OpenWeatherMapClient
from data_store_client import save_data_as_parquet

class IngestWeatherDataOperator(BaseOperator):
    """
    Operator that downloads weather data from the OpenWeatherMap API
    and stores it in a Parquet file.
    """
    @apply_defaults
    def __init__(
        self,
        city: str,
        api_key: str,
        file_path: str,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.city = city
        self.api_key = api_key
        self.file_path = file_path

    def execute(self, context):
        logging.info(f"Ingesting weather data for {self.city}...")
        client = OpenWeatherMapClient(api_key=self.api_key)
        data = client.get_weather_data(self.city)
        save_data_as_parquet(data, self.file_path)
        logging.info(f"Finished ingesting weather data for {self.city}.")

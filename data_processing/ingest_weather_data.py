import datetime as dt
from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.dates import days_ago
from openweathermap_client import OpenWeatherMapClient
from data_store_client import DataStoreClient


class IngestWeatherDataTask(BaseOperator):
    def __init__(
        self,
        api_key: str,
        cities: list,
        data_store: str,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.api_key = api_key
        self.cities = cities
        self.data_store = data_store

    def execute(self, context):
        client = OpenWeatherMapClient(api_key=self.api_key)
        data_store_client = DataStoreClient(self.data_store)
        start_date = days_ago(1)
        end_date = dt.datetime.now()

        for city in self.cities:
            data = client.get_city_weather_data(city, start_date, end_date)
            file_name = f"{city}.parquet"
            data_store_client.save_data_as_parquet(data, file_name)

import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StoreWeatherDataOperator(BaseOperator):
    """
    Operator that stores the weather data to a cloud storage system.
    """

    @apply_defaults
    def __init__(
            self,
            data,
            file_path: str,
            data_store_hook,
            *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.data = data
        self.file_path = file_path
        self.data_store_hook = data_store_hook

    def execute(self, context):
        logging.info(f"Storing weather data to {self.file_path}...")
        self.data_store_hook.upload_file(self.data, self.file_path)

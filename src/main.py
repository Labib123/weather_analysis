from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from src.preprocess_weather_data import preprocess_weather_data
from src.store_weather_data import store_weather_data
from src.analyze_weather_data import analyze_weather_data
from airflow.operators.fetch_weather_data import FetchWeatherDataOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('weather_data_pipeline', default_args=default_args, schedule_interval=timedelta(days=1), catchup=False) as dag:
    start_task = DummyOperator(task_id='start_task')

    fetch_weather_data_task = FetchWeatherDataOperator(
        task_id='fetch_weather_data',
        api_key='{{ var.value.OPENWEATHERMAP_API_KEY }}',
        city='London',
        file_path='/path/to/weather_data.parquet'
    )

    preprocess_weather_data_task = PythonOperator(task_id='preprocess_weather_data',
                                                  python_callable=preprocess_weather_data)

    store_weather_data_task = PythonOperator(task_id='store_weather_data', python_callable=store_weather_data)

    analyze_weather_data_task = PythonOperator(task_id='analyze_weather_data', python_callable=analyze_weather_data)

    end_task = DummyOperator(task_id='end_task')

    start_task >> fetch_weather_data_task >> preprocess_weather_data_task >> store_weather_data_task >> analyze_weather_data_task >> end_task

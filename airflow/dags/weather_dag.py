from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from src.openweathermap_client import OpenWeatherMapClient
from src.utils import get_spark_session, save_data_as_parquet, read_data_from_parquet

# Define the DAG configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_analysis",
    default_args=default_args,
    description="A DAG for weather data analysis",
    schedule_interval=timedelta(days=1),
)


# Define the task functions
def fetch_weather_data(city: str):
    """
    Fetches weather data for a given city using the OpenWeatherMap API and saves it as a Parquet file.
    """
    client = OpenWeatherMapClient()
    weather_data = client.get_weather_data(city=city)

    spark = get_spark_session()
    output_file = f"{city}.parquet"
    save_data_as_parquet(spark, weather_data, output_file)
    return output_file


def preprocess_weather_data(input_file: str):
    """
    Reads the weather data from a Parquet file, preprocesses it and saves it as a new Parquet file.
    """
    spark = get_spark_session()
    weather_data = read_data_from_parquet(spark, input_file)

    # Preprocess the data
    weather_data = (
        weather_data
        .select("main.temp", "main.feels_like", "main.humidity", "wind.speed", "clouds.all")
        .withColumnRenamed("main.temp", "temperature")
        .withColumnRenamed("main.feels_like", "feels_like_temperature")
        .withColumnRenamed("clouds.all", "cloud_coverage")
    )

    # Save the preprocessed data as a Parquet file, [:8]  --> .parquet
    preprocessed_file = f"{input_file[:-8]}_preprocessed.parquet"
    save_data_as_parquet(spark, weather_data, preprocessed_file)
    return preprocessed_file


fetch_weather_data_task = PythonOperator(
    task_id="fetch_weather_data",
    python_callable=fetch_weather_data,
    op_kwargs={"city": "London"},
    dag=dag,
)

preprocess_weather_data_task = PythonOperator(
    task_id="preprocess_weather_data",
    python_callable=preprocess_weather_data,
    op_kwargs={"input_file": "{{ task_instance.xcom_pull(task_ids='fetch_weather_data') }}"},
    dag=dag,
)

fetch_weather_data_task >> preprocess_weather_data_task

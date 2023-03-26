# Weather Data Pipeline

This project contains a data pipeline that fetches weather data from the OpenWeatherMap API, preprocesses it, and saves it to an S3 bucket.

## Getting Started

### Prerequisites

To run this project, you will need:

- Python 3.8 or higher
- Apache Airflow 2.2.0 or higher
- An OpenWeatherMap API key
- An AWS account with an S3 bucket configured

### Installing

To install this project, clone the repository and install the required packages:

git clone https://github.com/Labib123/weather-data-pipeline.git
cd weather-data-pipeline
pip install -r requirements.txt


### Configuration

Before running the pipeline, you will need to set the following environment variables:

- `OPENWEATHERMAP_API_KEY`: Your OpenWeatherMap API key
- `AWS_ACCESS_KEY_ID`: Your AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
- `S3_BUCKET_NAME`: The name of the S3 bucket where the data will be stored

You can set these environment variables using the `export` command on Linux and macOS, or the `set` command on Windows.

### Running

To run the pipeline, start the Airflow web server and scheduler:


Then, navigate to `http://localhost:8080` in your web browser and enable the `fetch_weather_data` DAG.

### Contributing

If you would like to contribute to this project, please open an issue or pull request on GitHub.


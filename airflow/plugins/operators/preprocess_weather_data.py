from pyspark.sql.functions import from_unixtime, col, explode


def preprocess_weather_data(spark, input_file_path, output_file_path):
    """
    Preprocesses the weather data by filtering out irrelevant columns and flattening nested JSON structures.
    """
    # Read the input Parquet file into a DataFrame.
    df = spark.read.parquet(input_file_path)

    # Filter out irrelevant columns.
    relevant_columns = [
        "dt",
        "main.temp",
        "main.pressure",
        "main.humidity",
        "wind.speed",
        "wind.deg",
        "clouds.all",
        "weather.id",
        "weather.main",
        "weather.description",
        "weather.icon"
    ]
    df = df.select(*relevant_columns)

    # Flatten the nested JSON structures.
    df = df.withColumn("weather", explode(col("weather")))
    df = df.withColumn("weather_id", col("weather.id"))
    df = df.withColumn("weather_main", col("weather.main"))
    df = df.withColumn("weather_description", col("weather.description"))
    df = df.withColumn("weather_icon", col("weather.icon"))
    df = df.drop("weather")

    # Convert the Unix timestamps to timestamps.
    df = df.withColumn("timestamp", from_unixtime(col("dt")).cast("timestamp"))
    df = df.drop("dt")

    # Save the preprocessed data as a Parquet file.
    df.write.mode("overwrite").parquet(output_file_path)

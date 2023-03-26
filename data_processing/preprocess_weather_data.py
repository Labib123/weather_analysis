from pyspark.sql.functions import col, from_unixtime, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


def preprocess_weather_data(spark, input_file, output_file):
    """
    Preprocesses the weather data by selecting the relevant columns, filtering the data, and adding additional columns.
    """
    # Define the schema for the weather data
    schema = StructType(
        [
            StructField("dt", IntegerType()),
            StructField(
                "main",
                StructType(
                    [
                        StructField("temp", DoubleType()),
                        StructField("feels_like", DoubleType()),
                        StructField("temp_min", DoubleType()),
                        StructField("temp_max", DoubleType()),
                        StructField("pressure", IntegerType()),
                        StructField("humidity", IntegerType()),
                    ]
                ),
            ),
            StructField(
                "weather",
                StructType(
                    [
                        StructField("id", IntegerType()),
                        StructField("main", StringType()),
                        StructField("description", StringType()),
                        StructField("icon", StringType()),
                    ]
                ),
            ),
            StructField("clouds", StructType([StructField("all", IntegerType())])),
            StructField("wind", StructType([StructField("speed", DoubleType())])),
            StructField("visibility", IntegerType()),
            StructField("dt_txt", StringType()),
        ]
    )

    # Read the input file into a DataFrame
    data = spark.read.schema(schema).parquet(input_file)

    # Select the relevant columns and filter the data
    data = (
        data.select(
            col("dt"),
            col("main.temp").alias("temperature"),
            col("main.feels_like").alias("feels_like"),
            col("main.pressure").alias("pressure"),
            col("main.humidity").alias("humidity"),
            col("weather.description").alias("description"),
            col("wind.speed").alias("wind_speed"),
        )
        .filter(col("temperature").isNotNull())
        .filter(col("feels_like").isNotNull())
        .filter(col("pressure").isNotNull())
        .filter(col("humidity").isNotNull())
        .filter(col("description").isNotNull())
        .filter(col("wind_speed").isNotNull())
    )

    # Add additional columns for year, month, and day
    data = data.withColumn("date", from_unixtime(col("dt")).cast(TimestampType()))
    data = data.withColumn("year", year(col("date")))
    data = data.withColumn("month", month(col("date")))
    data = data.withColumn("day", dayofmonth(col("date")))

    # Write the preprocessed data to a new file
    data.write.mode("overwrite").parquet(output_file)

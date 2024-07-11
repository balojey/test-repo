from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import from_json, col
from config import configuration


def main():
    spark: SparkSession = (SparkSession.builder.appName("SmartCity")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
        "org.apache.hadoop:hadoop-aws:3.4.0,"
        "com.amazonaws:aws-java-sdk:1.12.759"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.access.key",
        configuration.get("AWS_ACCESS_KEY")
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        configuration.get("AWS_SECRET_KEY")
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    vehicleSchema = StructType([
        StructField("id", StringType(), False),
        StructField("vehicle_name", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("health_status", StringType(), False),
    ])

    locationSchema = StructType([
        StructField("id", StringType(), False),
        StructField("vehicle_name", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("location", StringType(), False),
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), False),
        StructField("vehicle_name", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("weather_status", StringType(), False),
        StructField("temperature", FloatType(), False),
    ])

    def read_kafka_topic(topic: str, schema: StructType):
        return (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "broker:29092")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
            .withWatermark("timestamp", "2 minutes")
        )

    def stream_writer(input: DataFrame, checkpoint_folder: str, output):
        return (
            input.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpoint_folder)
            .option("path", output)
            .outputMode("append")
            .start()
        )


    vehicleDF = read_kafka_topic("vehicle_data", vehicleSchema).alias("vehicle")
    locationDF = read_kafka_topic("location_data", locationSchema).alias("location")
    weatherDF = read_kafka_topic("weather_data", weatherSchema).alias("weather")

    query1 = stream_writer(vehicleDF, "s3://spark-streaming-1001/checkpoints/vehicle_data", "s3://spark-streaming-1001/data/vehicle_data")
    query2 = stream_writer(locationDF, "s3://spark-streaming-1001/checkpoints/location_data", "s3://spark-streaming-1001/data/location_data")
    query3 = stream_writer(weatherDF, "s3://spark-streaming-1001/checkpoints/weather_data", "s3://spark-streaming-1001/data/weather_data")

    query3.awaitTermination()


if __name__ == "__main__":
    main()
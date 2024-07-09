from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col


table_env: TableEnvironment = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
# table_env.get_config().set("parallelism.default", "1")

# DEFINE DATA SOURCES
vehicle_data_source = f"""
    CREATE TABLE vehicle_source (
        id STRING,
        vehicle_name STRING,
        created_on TIMESTAMP,
        health_status STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'vehicle_data',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
"""
location_data_source = f"""
    CREATE TABLE location_source (
        id STRING,
        vehicle_name STRING,
        created_on TIMESTAMP,
        location MAP<STRING, FLOAT>
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'location_data',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
"""
weather_data_source = f"""
    CREATE TABLE weather_source (
        id STRING,
        vehicle_name STRING,
        created_on TIMESTAMP,
        weather_status STRING,
        temperature FLOAT
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'weather_data',
        'properties.bootstrap.servers' = 'localhost:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
"""

# DEFINE DATA SINKS
vehicle_data_sink = f"""
    CREATE TABLE vehicle_sink (
        id STRING,
        vehicle_name STRING,
        created_on TIMESTAMP,
        health_status STRING
    ) WITH (
        'connector' = 'filesystem',
        'path' = 's3://flink-streaming-bucket-1001/data/vehicle_data',
        'format' = 'parquet',
        'source.monitor-interval' = 10
    )
"""
location_data_sink = f"""
    CREATE TABLE location_sink (
        id STRING,
        vehicle_name STRING,
        created_on TIMESTAMP,
        location MAP<STRING, FLOAT>
    ) WITH (
        'connector' = 'filesystem',
        'path' = 's3://flink-streaming-bucket-1001/data/location_data',
        'format' = 'parquet',
        'source.monitor-interval' = 10
    )
"""
weather_data_sink = f"""
    CREATE TABLE weather_sink (
        id STRING,
        vehicle_name STRING,
        created_on TIMESTAMP,
        weather_status STRING,
        temperature FLOAT
    ) WITH (
        'connector' = 'filesystem',
        'path' = 's3://flink-streaming-bucket-1001/data/weather_data',
        'format' = 'parquet',
        'source.monitor-interval' = 10
    )
"""

if __name__ == "__main__":
    table_env.execute_sql(vehicle_data_source).print()
    # vehicle_data = table_env.sql_query("SELECT * FROM vehicle_source")
    # vehicle_data.execute().print()
    source_data = table_env.from_path("vehicle_source")
    result_table = source_data.select(col("id"), col("vehicle_name"))
    result_table.execute().print()
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DoubleType, BooleanType
)

from pyspark.sql.functions import current_timestamp, from_unixtime, to_timestamp, to_date, col, from_json

def stream_kafka_to_iceberg(spark: SparkSession, table_name: str) -> None:
    # Define schema for the nested BusWayPoint based on ufms.proto
    bus_way_point_schema = StructType([
        StructField("vehicle", StringType(), True),
        StructField("driver", StringType(), True),
        StructField("speed", FloatType(), True),
        StructField("datetime", IntegerType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("z", FloatType(), True),
        StructField("heading", FloatType(), True),
        StructField("ignition", BooleanType(), True),
        StructField("aircon", BooleanType(), True),
        StructField("door_up", BooleanType(), True),
        StructField("door_down", BooleanType(), True),
        StructField("sos", BooleanType(), True),
        StructField("working", BooleanType(), True),
        StructField("analog1", FloatType(), True),
        StructField("analog2", FloatType(), True)
    ])
    
    # Define root schema
    root_schema = StructType([
        StructField("msgType", StringType(), True),
        StructField("msgBusWayPoint", bus_way_point_schema, True)
    ])
    
    # Read from Kafka topic as streaming source
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:29092")
        .option("subscribe", "buswaypoint_json")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    df_json = df_kafka.selectExpr("CAST(value AS STRING) as json")

    # Parse JSON and flatten the struct for tabular Iceberg table
    df = df_json.select(
        from_json(col("json"), root_schema).alias("data")
    ).select("data.*")

    # Filter only BusWayPoint messages
    df_filtered = df.filter(col("msgType") == "MsgType_BusWayPoint")

    df_flattened = df_filtered.select("msgType", "msgBusWayPoint.*")

    # Convert epoch datetime to Spark timestamp and extract Date for partitioning
    df_with_ts = (
        df_flattened
        .withColumn("timestamp", to_timestamp(from_unixtime("datetime")))
        .withColumn("date", to_date(from_unixtime("datetime")))
        .withColumn("load_at", current_timestamp())
    )
    
    def write_batch_to_iceberg(batch_df, batch_id):
        print(f"[Batch {batch_id}] Writing streaming batch to Bronze...", flush=True)
        batch_df.writeTo(table_name).append()
        print(f"[Batch {batch_id}] Successfully written batch to {table_name}.", flush=True)

    # Write to Iceberg with foreachBatch
    query = (
        df_with_ts
        .writeStream
        .foreachBatch(write_batch_to_iceberg)
        .option("checkpointLocation", "s3a://iceberg/lakehouse/checkpoints/bronze/bus_way_point")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("KafkaJsonToMinIO")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    table_name = "catalog_iceberg.bus_bronze.bus_way_point"
    
    stream_kafka_to_iceberg(spark, table_name)
# data/HPCLAB/part2\sub_raw_477.json
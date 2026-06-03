import os
import sys
import json
import redis
import numpy as np
import pandas as pd
from datetime import datetime

# Add silver pipeline directory to path on Driver
sys.path.insert(0, "/opt/spark/apps/pipelines/silver")

from kalman_filter import RedisBackedKalmanFilter

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, to_date, hour, row_number, current_timestamp, broadcast
)

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

def apply_kalman_filter_with_redis(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Apply Kalman Filter to smooth GPS (x, y) coordinates and estimate speed using Redis.
    """
    # Lazy import inside UDF so workers can load it from addPyFile distribution
    from kalman_filter import RedisBackedKalmanFilter
    
    kf = RedisBackedKalmanFilter(
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        R_var=5e-8,         # Optimized Measurement noise
        sigma_a_sq=1.96e-10, # Process noise
        max_dt=15.0         # Max dt threshold for reset
    )
    return kf.process_trajectory(pdf)



def main():
    spark = (
        SparkSession.builder
        .appName("SilverWayPointCleanStreaming")
        .config("spark.driver.memory", "1536m")
        .config("spark.executor.memory", "1536m")
        .config("spark.executor.cores", "1")
        .config("spark.cores.max", "1")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    
    # Distribute the kalman_filter.py dependency to all executor workers
    spark.sparkContext.addPyFile("/opt/spark/apps/pipelines/silver/kalman_filter.py")


    # Read Bronze data as streaming source from Iceberg
    df_bus = spark.readStream.format("iceberg").table("catalog_iceberg.bus_bronze.bus_way_point")
    
    # Read mapping table (static dimension)
    df_map = spark.read.table("catalog_iceberg.bus_bronze.vehicle_bus_mapping")

    # Join streaming raw waypoints with static vehicle mapping
    df_join = df_bus.join(broadcast(df_map), on="vehicle", how="left")

    def write_cleaned_batch(batch_df, batch_id):
        print(f"[Batch {batch_id}] Processing streaming batch to Silver...", flush=True)

        # Step 1: Deduplicate within the micro-batch
        window_spec = Window.partitionBy("vehicle", "timestamp").orderBy(col("load_at").desc())

        df_filtered = (
            batch_df.filter(
                col("vehicle").isNotNull() & 
                col("timestamp").isNotNull() & 
                col("x").isNotNull() & (col("x") != 0) & 
                col("y").isNotNull() & (col("y") != 0)
            )
            .withColumn("rn", row_number().over(window_spec))
            .filter(col("rn") == 1)
            .drop("rn")
        )

        # Step 2: Apply Redis-Backed Kalman filter
        df_kalman = df_filtered.groupBy("vehicle").applyInPandas(
            apply_kalman_filter_with_redis, 
            schema=df_filtered.schema
        )

        # Step 3: Add time details and sort within partitions
        df_clean = (
            df_kalman.withColumn("date", to_date(col("timestamp")))
            .withColumn("hour", hour(col("timestamp")))
            .withColumn("updated_at", current_timestamp())
            .select(
                "vehicle",
                "route_id",
                "route_no",
                "driver",
                "timestamp",
                "date",
                "hour",
                "speed",
                "x",
                "y",
                "z",
                "heading",
                "ignition",
                "aircon",
                "door_up",
                "door_down",
                "sos",
                "working",
                "analog1",
                "analog2",
                "updated_at"
            )
            .sortWithinPartitions(
                col("route_id").asc_nulls_first(),
                col("vehicle").asc_nulls_first(),
                col("timestamp").asc_nulls_first()
            )
        )

        # Step 4: Idempotent append to Silver Iceberg table
        df_clean.writeTo("catalog_iceberg.bus_silver.bus_way_point").append()
        print(f"[Batch {batch_id}] Successfully cleaned and appended batch to bus_silver.bus_way_point.", flush=True)

    # Create Silver table if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.bus_way_point (
            vehicle STRING,
            route_id INT,
            route_no STRING,
            driver STRING,
            timestamp TIMESTAMP,
            date DATE,
            hour INT,
            speed DOUBLE,
            x DOUBLE,
            y DOUBLE,
            z DOUBLE,
            heading FLOAT,
            ignition BOOLEAN,
            aircon BOOLEAN,
            door_up BOOLEAN,
            door_down BOOLEAN,
            sos BOOLEAN,
            working BOOLEAN,
            analog1 FLOAT,
            analog2 FLOAT,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
    """)

    # Run the streaming query
    query = (
        df_join.writeStream
        .foreachBatch(write_cleaned_batch)
        .option("checkpointLocation", "s3a://iceberg/lakehouse/checkpoints/silver/bus_way_point")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
"""
Silver Batch Backfill — Re-process Bronze → Silver for a specific date
======================================================================
Replaces streaming pipeline for historical re-evaluation.
Reads Bronze Iceberg, applies updated Kalman filter (with Adaptive R,
Dual-Model speed<=2.0 fix), overwrites Silver Iceberg for TARGET_DATE.

Usage:
  docker exec spark-master spark-submit /opt/spark/scripts/silver_backfill.py
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, broadcast,
    from_utc_timestamp, to_date, hour, current_timestamp
)

sys.path.insert(0, "/opt/spark/apps/pipelines/silver")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TARGET_DATE = "2025-03-22"


def apply_kalman_filter_with_redis(pdf):
    from kalman_filter import RedisBackedKalmanFilter
    kf = RedisBackedKalmanFilter(
        redis_host=REDIS_HOST,
        redis_port=REDIS_PORT,
        R_var=1e-6,          # fallback if no adaptive R in Redis
        sigma_a_sq=1e-11,    # Q/R≈0.25 @ dt=10s
        max_dt=15.0
    )
    return kf.process_trajectory(pdf)


def main():
    spark = (
        SparkSession.builder
        .appName("SilverBackfill")
        .config("spark.driver.memory", "1536m")
        .config("spark.executor.memory", "1536m")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "4")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Distribute kalman_filter.py to all executors
    spark.sparkContext.addPyFile("/opt/spark/apps/pipelines/silver/kalman_filter.py")

    # Ensure target schema and table exist
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

    print(f"[Backfill] Reading Bronze for {TARGET_DATE}...", flush=True)
    df_bronze = (
        spark.read
        .table("catalog_iceberg.bus_bronze.bus_way_point")
        .filter(col("date") == lit(TARGET_DATE))
    )

    df_map = spark.read.table("catalog_iceberg.bus_bronze.vehicle_bus_mapping")
    df_join = df_bronze.join(broadcast(df_map), on="vehicle", how="inner")

    df_filtered = df_join.filter(
        col("vehicle").isNotNull() &
        col("timestamp").isNotNull() &
        col("x").isNotNull() & (col("x") != 0) &
        col("y").isNotNull() & (col("y") != 0)
    ).dropDuplicates(["vehicle", "timestamp"])

    total = df_filtered.count()
    print(f"[Backfill] {total:,} pings to process for all routes...", flush=True)

    # Apply Kalman filter per vehicle (same logic as streaming pipeline)
    df_kalman = df_filtered.groupBy("vehicle").applyInPandas(
        apply_kalman_filter_with_redis,
        schema=df_filtered.schema
    )

    df_clean = (
        df_kalman
        .withColumn("ts_vn", from_utc_timestamp(col("timestamp"), "Asia/Ho_Chi_Minh"))
        .withColumn("date", to_date(col("ts_vn")))
        .withColumn("hour", hour(col("ts_vn")))
        .drop("ts_vn")
        .withColumn("updated_at", current_timestamp())
        .select(
            "vehicle", "route_id", "route_no", "driver", "timestamp",
            "date", "hour", "speed", "x", "y", "z", "heading",
            "ignition", "aircon", "door_up", "door_down", "sos",
            "working", "analog1", "analog2", "updated_at"
        )
        .sortWithinPartitions(
            col("route_id").asc_nulls_first(),
            col("vehicle").asc_nulls_first(),
            col("timestamp").asc_nulls_first()
        )
    )

    # Step 1: Delete existing Silver data for TARGET_DATE
    print(f"[Backfill] Deleting existing Silver data for {TARGET_DATE}", flush=True)
    spark.sql(f"""
        DELETE FROM catalog_iceberg.bus_silver.bus_way_point
        WHERE date = DATE '{TARGET_DATE}'
    """)

    # Step 2: Write new Silver data
    print("[Backfill] Writing new Silver data...", flush=True)
    df_clean.writeTo("catalog_iceberg.bus_silver.bus_way_point").append()

    count_written = spark.read.table("catalog_iceberg.bus_silver.bus_way_point") \
        .filter(col("date") == lit(TARGET_DATE)).count()

    print(f"\n{'='*50}", flush=True)
    print(f"  BACKFILL COMPLETE", flush=True)
    print(f"{'='*50}", flush=True)
    print(f"  Date       : {TARGET_DATE}", flush=True)
    print(f"  Input pings: {total:,}", flush=True)
    print(f"  Written    : {count_written:,}", flush=True)
    print(f"{'='*50}\n", flush=True)

    spark.stop()


if __name__ == "__main__":
    main()

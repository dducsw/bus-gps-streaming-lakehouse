import os
import redis
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    broadcast,
    col,
    count,
    current_timestamp,
    from_json,
    from_unixtime,
    sum as spark_sum,
    to_timestamp,
    window,
    when,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "buswaypoint_json")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_METRICS_STREAM = os.getenv("REDIS_METRICS_STREAM", "bus_operation_metrics")
MAPPING_CSV = os.getenv("MAPPING_CSV", "/opt/spark/apps/data/vehicle_route_mapping.csv")
CHECKPOINT_LOCATION = os.getenv(
    "WINDOW_CHECKPOINT_LOCATION",
    "/tmp/spark_checkpoints/bus_route_window",
)


bus_way_point_schema = StructType(
    [
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
        StructField("analog2", FloatType(), True),
    ]
)

root_schema = StructType(
    [
        StructField("msgType", StringType(), True),
        StructField("msgBusWayPoint", bus_way_point_schema, True),
    ]
)


def write_metrics_to_redis(batch_df, batch_id):
    row_count = batch_df.count()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[{now}] Batch {batch_id}: Triggered. Row count: {row_count}")
    sys.stdout.flush()
    
    if row_count > 0:
        print(f"[{now}] Batch {batch_id}: Writing to Redis...")
        sys.stdout.flush()
        
        def send_partition(partition):
            redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                socket_keepalive=True,
                socket_timeout=5.0,
            )
            pipeline = redis_client.pipeline(transaction=False)

            for row in partition:
                metric = {
                    "metric_type": "route_window_5m",
                    "route_no": row.route_no,
                    "window_start": row.window_start.isoformat(),
                    "window_end": row.window_end.isoformat(),
                    "avg_speed": f"{row.avg_speed:.2f}" if row.avg_speed is not None else "0.00",
                    "msg_count": str(row.msg_count),
                    "active_vehicle_count": str(row.active_vehicle_count),
                    "moving_msg_count": str(row.moving_msg_count),
                    "stopped_msg_count": str(row.stopped_msg_count),
                    "sos_msg_count": str(row.sos_msg_count),
                    "ignition_on_msg_count": str(row.ignition_on_msg_count),
                    "updated_at": row.updated_at.isoformat(),
                }
                pipeline.xadd(
                    REDIS_METRICS_STREAM,
                    metric,
                    maxlen=50000,
                    approximate=True,
                )

            pipeline.execute()
            redis_client.close()

        batch_df.foreachPartition(send_partition)
        print(f"[{now}] Batch {batch_id}: Successfully committed.")
        sys.stdout.flush()


def main():
    spark = (
        SparkSession.builder.appName("BusRouteWindowMetrics")
        .config("spark.driver.memory", "1536m")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "2")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Starting BusRouteWindowMetrics with MAPPING_CSV={MAPPING_CSV}")
    sys.stdout.flush()

    mapping_df = (
        spark.read.option("header", True)
        .csv(MAPPING_CSV)
        .select("vehicle", "route_no")
        .dropna(subset=["vehicle", "route_no"])
        .dropDuplicates(["vehicle"])
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), root_schema).alias("data"))
        .select("data.msgBusWayPoint.*")
        .withColumn("timestamp", to_timestamp(from_unixtime("datetime")))
        .join(broadcast(mapping_df), on="vehicle", how="left")
        .filter(col("route_no").isNotNull())
        .withWatermark("timestamp", "10 minutes")
    )

    route_metrics = (
        parsed_stream.groupBy(window("timestamp", "5 minutes", "1 minute"), "route_no")
        .agg(
            avg("speed").alias("avg_speed"),
            count("*").alias("msg_count"),
            approx_count_distinct("vehicle").alias("active_vehicle_count"),
            spark_sum(when(col("speed") > 0, 1).otherwise(0)).alias("moving_msg_count"),
            spark_sum(when(col("speed") == 0, 1).otherwise(0)).alias("stopped_msg_count"),
            spark_sum(when(col("sos") == True, 1).otherwise(0)).alias("sos_msg_count"),
            spark_sum(when(col("ignition") == True, 1).otherwise(0)).alias(
                "ignition_on_msg_count"
            ),
        )
        .select(
            "route_no",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_speed",
            "msg_count",
            "active_vehicle_count",
            "moving_msg_count",
            "stopped_msg_count",
            "sos_msg_count",
            "ignition_on_msg_count",
        )
        .withColumn("updated_at", current_timestamp())
    )

    query = (
        route_metrics.writeStream.outputMode("update")
        .foreachBatch(write_metrics_to_redis)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

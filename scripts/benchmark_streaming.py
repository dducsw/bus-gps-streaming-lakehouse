import sys
import time
import os
import math
import json
import csv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, DoubleType, BooleanType
)
from pyspark.sql.functions import (
    current_timestamp, from_unixtime, to_timestamp, to_date, col, from_json,
    min as spark_min, max as spark_max, avg as spark_avg, count as spark_count, lit
)
from pyspark.sql.streaming import StreamingQueryListener

# Configs
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "buswaypoint_json"
CHECKPOINT_LOCATION = "s3a://iceberg/lakehouse/checkpoints/streaming_benchmark"
TARGET_TABLE = "catalog_iceberg.bus_bronze.bus_way_point"
CSV_REPORT_PATH = "/opt/spark/scripts/benchmark_results.csv"

# Shared dictionary to pass precise Iceberg commit durations from foreachBatch to listener
commit_durations = {}

def compute_percentile(data, percent):
    """Computes percentile of a list of numeric values using linear interpolation."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    k = (len(sorted_data) - 1) * percent / 100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_data[int(k)]
    d0 = sorted_data[int(f)] * (c - k)
    d1 = sorted_data[int(c)] * (k - f)
    return d0 + d1

class AcademicBenchmarkListener(StreamingQueryListener):
    """An asynchronous query progress listener that aggregates metrics without block overhead."""
    
    def __init__(self, warmup_batches=3):
        self.warmup_batches = warmup_batches
        self.session_latencies = []
        print("\n" + "="*65)
        print("📊 ACADEMIC BENCHMARK LISTENER INITIALIZED")
        print("="*65)
        print(f"   - Warm-up Batches to Discard: {warmup_batches}")
        print("   - Clock Sync Assumption: Assumes NTP-synchronized clocks between Kafka broker")
        print("     and Spark driver to avoid clock skew measurement errors.")
        print("   - E2E Latency Semantics:")
        print("     * Min Latency = batch completion - max(kafka_ts) [latency of newest record]")
        print("     * Max Latency = batch completion - min(kafka_ts) [latency of oldest record]")
        print("="*65 + "\n")
        sys.stdout.flush()
        
    def onQueryStarted(self, event):
        print(f"🚀 Streaming Benchmark Query Started (ID: {event.id})")
        sys.stdout.flush()

    def onQueryProgress(self, event):
        progress = event.progress
        batch_id = progress.batchId
        
        # 1. Spark Engine Throughput & Duration Metrics
        input_rate = progress.inputRowsPerSecond
        process_rate = progress.processedRowsPerSecond
        trigger_duration = progress.durationMs.get("triggerExecution", 0) / 1000.0
        
        # Retrieve precise Iceberg commit duration measured inside foreachBatch
        iceberg_commit_duration = commit_durations.pop(batch_id, None)
        if iceberg_commit_duration is None:
            # Fallback to engine-reported write duration
            iceberg_commit_duration = progress.durationMs.get("addBatch", 0) / 1000.0
        
        # 2. Extract observed metrics (Computed during batch write with ZERO extra action overhead)
        observed = progress.observedMetrics.get("latency_metrics")
        observed_dict = observed.asDict() if observed else {}
        
        # E2E Latency calculation using batch finish time (current local time)
        # to correctly capture total transit + processing latency.
        batch_finish_time = time.time()
        
        row_count = progress.numInputRows  # Capture directly from engine progress for correctness
        avg_e2e_latency = 0.0
        min_e2e_latency = 0.0
        max_e2e_latency = 0.0
        
        # Extract Kafka lag
        kafka_lag = self._extract_kafka_lag(progress.sources)
        
        is_warmup = batch_id < self.warmup_batches
        
        if observed_dict and observed_dict.get("row_count", 0) > 0:
            min_kafka = observed_dict["min_kafka"]
            max_kafka = observed_dict["max_kafka"]
            avg_kafka = observed_dict["avg_kafka"]
            
            # E2E Processing Latency: from Kafka enqueue to Spark batch processing completion
            avg_e2e_latency = batch_finish_time - avg_kafka
            # Minimum latency is for the newest record (max_kafka)
            min_e2e_latency = batch_finish_time - max_kafka
            # Maximum latency is for the oldest record (min_kafka)
            max_e2e_latency = batch_finish_time - min_kafka
            
            # Store in session metrics if not in warm-up phase
            if not is_warmup:
                self.session_latencies.append(avg_e2e_latency)
        
        # 3. Print stats to console
        print("\n" + "="*65)
        if is_warmup:
            print(f"⚠️  [WARM-UP] BATCH {batch_id} - EXCLUDED FROM SESSION STATISTICS")
        else:
            print(f"⏱️  STREAMING BATCH {batch_id} - ACADEMIC METRICS (Asynchronous)")
        print("="*65)
        print(f"📊 Engine Throughput:")
        print(f"   - Input Rate (Engine): {input_rate:.2f} records/sec")
        print(f"   - Process Rate (Engine): {process_rate:.2f} records/sec")
        print(f"   - Batch Size (Input Rows): {row_count} records")
        if kafka_lag is not None:
            print(f"   - Kafka Consumer Lag: {kafka_lag} records")
        else:
            print(f"   - Kafka Consumer Lag: N/A")
            
        print(f"⏱️  Engine Latencies:")
        print(f"   - Total Trigger Duration: {trigger_duration:.3f} seconds")
        print(f"   - Iceberg Commit Duration (Measured): {iceberg_commit_duration:.3f} seconds")
        
        if row_count > 0:
            print(f"⚡ E2E Pipeline Latency (Kafka Enqueue -> Batch Finish):")
            print(f"   - Avg Latency: {avg_e2e_latency:.3f} seconds")
            print(f"   - Min Latency: {min_e2e_latency:.3f} seconds (newest record in batch)")
            print(f"   - Max Latency: {max_e2e_latency:.3f} seconds (oldest record in batch)")
        else:
            print(f"⚡ E2E Pipeline Latency: No data processed in this batch.")
            
        # Display running percentiles of the session
        if len(self.session_latencies) > 0:
            p50 = compute_percentile(self.session_latencies, 50)
            p95 = compute_percentile(self.session_latencies, 95)
            p99 = compute_percentile(self.session_latencies, 99)
            print(f"📈 Session Running Percentiles (excluding warm-ups):")
            print(f"   - p50: {p50:.3f} seconds")
            print(f"   - p95: {p95:.3f} seconds")
            print(f"   - p99: {p99:.3f} seconds")
            
        print("="*65 + "\n")
        sys.stdout.flush()
        
        # 4. Save to CSV using csv module for safe encoding/quoting
        try:
            header_needed = not os.path.exists(CSV_REPORT_PATH)
            with open(CSV_REPORT_PATH, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if header_needed:
                    writer.writerow([
                        "batch_id", "timestamp", "input_rate", "process_rate", 
                        "trigger_duration", "write_duration", "row_count", "kafka_lag",
                        "avg_e2e_latency", "min_e2e_latency", "max_e2e_latency", "is_warmup"
                    ])
                writer.writerow([
                    batch_id, progress.timestamp, f"{input_rate:.3f}", f"{process_rate:.3f}",
                    f"{trigger_duration:.3f}", f"{iceberg_commit_duration:.3f}", row_count, 
                    kafka_lag if kafka_lag is not None else -1,
                    f"{avg_e2e_latency:.3f}", f"{min_e2e_latency:.3f}", f"{max_e2e_latency:.3f}",
                    1 if is_warmup else 0
                ])
        except Exception as e:
            print(f"⚠️ Failed to write to CSV report: {e}", file=sys.stderr)
            sys.stdout.flush()

    def onQueryTerminated(self, event):
        print(f"🏁 Query terminated (ID: {event.id})")
        sys.stdout.flush()

    def _extract_kafka_lag(self, sources):
        total_lag = 0
        has_lag_info = False
        for source in sources:
            desc = source.description
            if desc and "Kafka" in desc:
                latest = source.latestOffset
                end = source.endOffset
                if latest and end:
                    try:
                        latest_dict = json.loads(latest) if isinstance(latest, str) else latest
                        end_dict = json.loads(end) if isinstance(end, str) else end
                        
                        for topic, partitions in latest_dict.items():
                            if topic in end_dict:
                                end_partitions = end_dict[topic]
                                for partition, latest_off in partitions.items():
                                    if partition in end_partitions:
                                        lag = int(latest_off) - int(end_partitions[partition])
                                        total_lag += max(0, lag)
                                        has_lag_info = True
                    except Exception:
                        pass
        return total_lag if has_lag_info else None

def benchmark_streaming(spark: SparkSession) -> None:
    # Print benchmark runtime metadata to standard out
    print("\n" + "="*65)
    print("📋 STREAMING BENCHMARK RUNTIME METADATA")
    print("="*65)
    print(f"   - Spark Version: {spark.version}")
    print(f"   - Python Version: {sys.version.split()[0]}")
    print(f"   - Target Table: {TARGET_TABLE}")
    print(f"   - Checkpoint: {CHECKPOINT_LOCATION}")
    print(f"   - Core Limit (Cores Max): {spark.conf.get('spark.cores.max', 'N/A')}")
    print(f"   - Executor Memory: {spark.conf.get('spark.executor.memory', 'N/A')}")
    print(f"   - Driver Memory: {spark.conf.get('spark.driver.memory', 'N/A')}")
    print(f"   - Shuffle Partitions: {spark.conf.get('spark.sql.shuffle.partitions', 'N/A')}")
    print("="*65 + "\n")
    sys.stdout.flush()

    # Define schemas
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
    
    root_schema = StructType([
        StructField("msgType", StringType(), True),
        StructField("msgBusWayPoint", bus_way_point_schema, True)
    ])
    
    # Register the asynchronous listener before starting query
    spark.streams.addListener(AcademicBenchmarkListener(warmup_batches=3))
    
    # Read from Kafka
    print(f"Reading stream from Kafka topic: {KAFKA_TOPIC}...")
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    # Extract Kafka metadata
    df_with_metadata = df_kafka.select(
        col("timestamp").alias("kafka_timestamp"),
        col("value")
    )
    
    df_json = df_with_metadata.select(
        "kafka_timestamp",
        col("value").cast("string").alias("json")
    )
    
    # Parse and flatten JSON
    df = df_json.select(
        "kafka_timestamp",
        from_json(col("json"), root_schema).alias("data")
    ).select("kafka_timestamp", "data.*")
    
    df_filtered = df.filter(col("msgType") == "MsgType_BusWayPoint")
    df_flattened = df_filtered.select("kafka_timestamp", "msgType", "msgBusWayPoint.*")
    
    # Add timestamps for table fields
    df_with_ts = (
        df_flattened
        .withColumn("timestamp", to_timestamp(from_unixtime("datetime")))
        .withColumn("date", to_date(from_unixtime("datetime")))
        .withColumn("load_at", current_timestamp())
    )
    
    # Use Spark's observe API to compute metrics inline with zero action overhead!
    df_observed = df_with_ts.observe(
        "latency_metrics",
        spark_min(col("kafka_timestamp").cast("double")).alias("min_kafka"),
        spark_max(col("kafka_timestamp").cast("double")).alias("max_kafka"),
        spark_avg(col("kafka_timestamp").cast("double")).alias("avg_kafka"),
        spark_count(lit(1)).alias("row_count")
    )
    
    # Iceberg write operation inside foreachBatch
    def write_batch_to_iceberg(batch_df, batch_id):
        t0 = time.time()
        # Drop kafka_timestamp metadata column before writing to match target table schema
        batch_df.drop("kafka_timestamp").writeTo(TARGET_TABLE).append()
        duration = time.time() - t0
        commit_durations[batch_id] = duration

    # Start writing stream
    query = (
        df_observed
        .writeStream
        .foreachBatch(write_batch_to_iceberg)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    try:
        # Keep query running until interrupted
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping benchmark stream...")
        query.stop()
        print("Benchmark stream stopped.")

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("StreamingBenchmarkJob")
        .config("spark.driver.memory", "1536m")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "2")
        .config("spark.sql.shuffle.partitions", "4") # Optimize shuffling for small local stream
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    
    # Initialize report directory
    try:
        if not os.path.exists(os.path.dirname(CSV_REPORT_PATH)):
            os.makedirs(os.path.dirname(CSV_REPORT_PATH), exist_ok=True)
    except Exception:
        pass
        
    try:
        benchmark_streaming(spark)
    except Exception as e:
        print(f"Error occurred: {e}", file=sys.stderr)
        sys.exit(1)

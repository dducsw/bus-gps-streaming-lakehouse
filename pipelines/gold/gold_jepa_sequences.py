import json

import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, abs as spark_abs, hash as spark_hash
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SOURCE_TABLE = "catalog_iceberg.bus_gold.jepa_features"

# ---- Output: Parquet on MinIO (consumed directly by PyTorch BusStreamingDataset) ----
OUTPUT_PATH   = "s3a://bus-ai/gold/jepa_sequences"
VOCAB_PATH    = "s3a://bus-ai/gold/route_vocab.json"

# Mirrors notebook constants exactly
SEQ_LENGTH         = 10
MAX_DELTA_TIME_SEC = 300  # gap filter: same as gold_jepa_features.py


# Features consumed by BusStreamingDataset.__iter__ in the exact same order
FEATURE_COLS = [
    "delta_x",
    "delta_y",
    "speed",
    "acceleration",
    "delta_heading",
    "door_up",
    "door_down",
    "working",
    "Time_Sin",
    "Time_Cos",
]

# Schema returned by applyInPandas — one row per sliding window
_SEQUENCE_SCHEMA = StructType(
    [StructField("vehicle", StringType(), False)]
    + [StructField("route_id", IntegerType(), True)]
    + [StructField(f"seq_{feat}_{t}", DoubleType(), True)
       for feat in FEATURE_COLS for t in range(SEQ_LENGTH)]
    + [StructField(f"seq_delta_time_{t}", DoubleType(), True) for t in range(SEQ_LENGTH)]
    + [StructField("waypoint_count", LongType(), False)]
    + [StructField("updated_at", TimestampType(), True)]
)


def build_sequences(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Convert a single vehicle's time-ordered pings into sliding windows of
    SEQ_LENGTH consecutive pings.

    Mirrors BusStreamingDataset.__iter__:
        for i in range(len(vals) - seq_length - 1):
            obs_t = vals[i : i + seq_length]

    Rows with a gap > MAX_DELTA_TIME_SEC break continuity and are treated as
    sequence boundaries (the window cannot span the break).
    """
    pdf = pdf.sort_values("timestamp").reset_index(drop=True)

    if len(pdf) < SEQ_LENGTH + 1:
        return pd.DataFrame(columns=[f.name for f in _SEQUENCE_SCHEMA])

    feature_vals = pdf[FEATURE_COLS].astype(float).values   # shape: (N, 10)
    delta_times  = pdf["delta_time"].astype(float).values   # shape: (N,)
    route_id_val = int(pdf["route_id"].iloc[0]) if pd.notna(pdf["route_id"].iloc[0]) else None
    vehicle_val  = pdf["vehicle"].iloc[0]

    rows = []
    n = len(pdf)

    for i in range(n - SEQ_LENGTH):
        window_slice = feature_vals[i : i + SEQ_LENGTH]      # (SEQ_LENGTH, 10)
        dt_slice     = delta_times[i : i + SEQ_LENGTH]       # (SEQ_LENGTH,)

        # Reject windows that span a GPS blackout gap
        if np.any(dt_slice > MAX_DELTA_TIME_SEC):
            continue

        row: dict = {"vehicle": vehicle_val, "route_id": route_id_val}

        for feat_idx, feat_name in enumerate(FEATURE_COLS):
            for t in range(SEQ_LENGTH):
                row[f"seq_{feat_name}_{t}"] = float(window_slice[t, feat_idx])

        for t in range(SEQ_LENGTH):
            row[f"seq_delta_time_{t}"] = float(dt_slice[t])

        row["waypoint_count"] = SEQ_LENGTH
        row["updated_at"] = None  # filled with current_timestamp() after collection
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=[f.name for f in _SEQUENCE_SCHEMA])

    return pd.DataFrame(rows)


def main():
    spark = SparkSession.builder.appName("GoldJepaSequences").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # -------------------------------------------------------------------------
    # 1. Read physics delta features from Gold Iceberg table
    # -------------------------------------------------------------------------
    df = spark.read.table(SOURCE_TABLE)

    # -------------------------------------------------------------------------
    # 2. Build Route Vocabulary  (mirrors build_route_vocab() in notebook cell 10)
    #    route_id (int) → sequential index used by nn.Embedding
    # -------------------------------------------------------------------------
    route_ids = (
        df.select("route_id")
        .filter(col("route_id").isNotNull())
        .distinct()
        .orderBy("route_id")
        .toPandas()["route_id"]
        .astype(str)
        .tolist()
    )
    route_vocab = {rid: idx for idx, rid in enumerate(route_ids)}
    print(f"Route vocabulary built: {len(route_vocab)} unique routes.")

    # -------------------------------------------------------------------------
    # 3. Sliding Window Sequences  (applyInPandas, group by vehicle)
    # -------------------------------------------------------------------------
    df_sequences = (
        df.groupBy("vehicle")
        .applyInPandas(build_sequences, schema=_SEQUENCE_SCHEMA)
        .withColumn("updated_at", current_timestamp())
        .withColumn("vehicle_bucket", spark_abs(spark_hash(col("vehicle"))) % 50)
    )

    # -------------------------------------------------------------------------
    # 4. Write sequences as Parquet on MinIO using bucket-partitioning to avoid
    #    small file metadata overhead on object storage (MinIO).
    # -------------------------------------------------------------------------
    (
        df_sequences
        .repartition(50, col("vehicle_bucket"))
        .write
        .mode("overwrite")
        .partitionBy("vehicle_bucket")
        .parquet(OUTPUT_PATH)
    )
    print(f"WRITE jepa_sequences SUCCESS (Bucket-Partitioned) → {OUTPUT_PATH}")

    # -------------------------------------------------------------------------
    # 5. Persist Route Vocabulary as JSON alongside sequences
    #    (notebook: route_to_idx dict passed to BusStreamingDataset)
    # -------------------------------------------------------------------------
    vocab_rdd = spark.sparkContext.parallelize([json.dumps(route_vocab)])
    vocab_rdd.coalesce(1).saveAsTextFile(VOCAB_PATH)
    print(f"WRITE route_vocab SUCCESS → {VOCAB_PATH}")


if __name__ == "__main__":
    main()

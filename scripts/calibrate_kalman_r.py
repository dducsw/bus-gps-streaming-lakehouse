"""
Kalman Filter R Calibration Script (Phase 1 — One-time Batch Job)
==================================================================
Reads Bronze GPS data, detects stationary segments (speed < 1 km/h,
≥ 10 consecutive pings), computes GPS measurement noise variance
(var_x, var_y) per vehicle, and stores results into Redis.

Redis keys written:
  kalman_r:{vehicle}  → hash {var_x, var_y}  (per-vehicle calibrated R)
  kalman_r:global     → hash {var_x, var_y}  (global fallback R)

Usage:
  docker exec spark-master spark-submit /opt/spark/scripts/calibrate_kalman_r.py
"""

import os
import json
import numpy as np
import pandas as pd
import redis

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
TARGET_DATE = "2025-03-22"
MIN_STATIONARY_PINGS = 5    # ~50s minimum stop (5 pings × 10s interval)
SPEED_THRESHOLD = 2.0       # km/h — GPS device min=1.0; ≤2.0 captures truly stopped buses


def compute_r_for_vehicle(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Pandas UDF: receives all GPS pings for ONE vehicle, sorted by timestamp.
    Returns a 1-row DataFrame: vehicle, var_x, var_y, n_segments.
    """
    vehicle = pdf["vehicle"].iloc[0]
    pdf = pdf.sort_values("timestamp").reset_index(drop=True)

    is_stationary = pdf["speed"] <= SPEED_THRESHOLD
    # label consecutive runs of speed < 1 km/h
    streak_id = (is_stationary != is_stationary.shift()).cumsum()

    var_x_list = []
    var_y_list = []

    for _, segment in pdf[is_stationary].groupby(streak_id[is_stationary]):
        if len(segment) >= MIN_STATIONARY_PINGS:
            vx = segment["x"].var()
            vy = segment["y"].var()
            # avoid zero variance (numerical stability)
            var_x_list.append(max(vx, 1e-12))
            var_y_list.append(max(vy, 1e-12))

    if var_x_list:
        mean_var_x = float(np.mean(var_x_list))
        mean_var_y = float(np.mean(var_y_list))
        n_seg = len(var_x_list)
    else:
        mean_var_x = float("nan")
        mean_var_y = float("nan")
        n_seg = 0

    return pd.DataFrame([{
        "vehicle": vehicle,
        "var_x": mean_var_x,
        "var_y": mean_var_y,
        "n_segments": n_seg
    }])


def store_to_redis(vehicle_r_df: pd.DataFrame, redis_host: str, redis_port: int):
    """Store per-vehicle R values and global fallback into Redis."""
    r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

    valid = vehicle_r_df.dropna(subset=["var_x", "var_y"])
    # Exclude vehicles whose variance is at/near the floor (1e-12).
    # These GPS devices freeze coordinates when stopped → var≈0, unreliable for moving periods.
    MIN_RELIABLE_VAR = 1e-10  # ~1.1m std dev — below this, use global R
    valid = valid[(valid["var_x"] > MIN_RELIABLE_VAR) | (valid["var_y"] > MIN_RELIABLE_VAR)]
    stored = 0

    for _, row in valid.iterrows():
        key = f"kalman_r:{row['vehicle']}"
        r.hset(key, mapping={
            "var_x": str(row["var_x"]),
            "var_y": str(row["var_y"]),
        })
        # no expiry — R calibration is semi-permanent
        stored += 1

    # global fallback
    if len(valid) > 0:
        global_var_x = float(valid["var_x"].mean())
        global_var_y = float(valid["var_y"].mean())
        r.hset("kalman_r:global", mapping={
            "var_x": str(global_var_x),
            "var_y": str(global_var_y),
        })
    else:
        global_var_x = 1e-9
        global_var_y = 1e-9
        r.hset("kalman_r:global", mapping={
            "var_x": str(global_var_x),
            "var_y": str(global_var_y),
        })

    r.close()
    return global_var_x, global_var_y, stored


def main():
    spark = SparkSession.builder.appName("KalmanRCalibration").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"[Calibration] Reading Bronze data for {TARGET_DATE}...")
    df = (
        spark.read
        .table("catalog_iceberg.bus_bronze.bus_way_point")
        .filter(col("date") == lit(TARGET_DATE))
        .select("vehicle", "timestamp", "x", "y", "speed")
        .filter(col("x").isNotNull() & (col("x") != 0))
        .filter(col("y").isNotNull() & (col("y") != 0))
    )

    total_pings = df.count()
    total_vehicles = df.select("vehicle").distinct().count()
    print(f"[Calibration] {total_pings:,} pings | {total_vehicles} vehicles")

    # output schema for applyInPandas
    result_schema = "vehicle STRING, var_x DOUBLE, var_y DOUBLE, n_segments INT"

    print("[Calibration] Computing stationary variance per vehicle...")
    result_df = (
        df.groupBy("vehicle")
        .applyInPandas(compute_r_for_vehicle, schema=result_schema)
    )

    # collect — result is small (1 row per vehicle)
    result_pd = result_df.toPandas()

    valid_count = result_pd.dropna(subset=["var_x"]).shape[0]
    no_data_count = result_pd.shape[0] - valid_count

    print(f"\n[Calibration] Results:")
    print(f"  Vehicles with valid stationary data : {valid_count}")
    print(f"  Vehicles with no stationary data    : {no_data_count} (will use global fallback)")

    # show top examples
    valid = result_pd.dropna(subset=["var_x"]).sort_values("var_x")
    if len(valid) > 0:
        print(f"\n  Sample calibrated R values (in degrees²):")
        print(f"  {'Vehicle':<20} {'var_x':>12} {'var_y':>12} {'segments':>10}")
        print("  " + "-"*56)
        for _, row in valid.head(10).iterrows():
            print(f"  {row['vehicle']:<20} {row['var_x']:>12.2e} {row['var_y']:>12.2e} {int(row['n_segments']):>10}")

    print("\n[Calibration] Storing results to Redis...")
    global_var_x, global_var_y, stored = store_to_redis(result_pd, REDIS_HOST, REDIS_PORT)

    # convert to approximate meters for readability
    deg_to_m = 111320.0
    std_x_m = (global_var_x ** 0.5) * deg_to_m
    std_y_m = (global_var_y ** 0.5) * deg_to_m

    print(f"\n{'='*55}")
    print(f"  KALMAN R CALIBRATION COMPLETE")
    print(f"{'='*55}")
    print(f"  Vehicles calibrated : {stored}")
    print(f"  Global fallback R   : var_x={global_var_x:.2e}, var_y={global_var_y:.2e}")
    print(f"  GPS std dev (approx): X ≈ {std_x_m:.1f} m, Y ≈ {std_y_m:.1f} m")
    print(f"  Redis key (global)  : kalman_r:global")
    print(f"  Redis key (vehicle) : kalman_r:{{vehicle_id}}")
    print(f"{'='*55}\n")
    print("[Calibration] Done. Silver streaming pipeline will now use adaptive R.")

    spark.stop()


if __name__ == "__main__":
    main()

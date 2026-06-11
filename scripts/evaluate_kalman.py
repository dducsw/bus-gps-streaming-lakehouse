from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, radians, sin, cos, asin, sqrt, lag, unix_timestamp, avg, stddev, lit, sum as spark_sum, broadcast

def haversine_spark(lat1, lng1, lat2, lng2):
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371.0 * c  # Output in km

def main():
    spark = (
        SparkSession.builder
        .appName("KalmanEvaluation")
        .config("spark.driver.memory", "1536m")
        .config("spark.executor.memory", "1536m")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "4")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    target_date = "2025-03-22"
    print(f"Evaluating Kalman Filter Performance for date: {target_date}...")

    # Read Raw (Bronze) and Cleaned (Silver) waypoints
    df_raw = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point") \
        .filter(col("date") == lit(target_date)) \
        .dropDuplicates(["vehicle", "timestamp"])
    df_clean = spark.read.table("catalog_iceberg.bus_silver.bus_way_point").filter(col("date") == lit(target_date))

    # Filter only vehicles that run on route '1' or '50'
    df_map = spark.read.table("catalog_iceberg.bus_bronze.vehicle_bus_mapping")
    vehicles_filtered = df_map.select("vehicle").distinct()

    df_raw = df_raw.join(broadcast(vehicles_filtered), on="vehicle", how="inner")
    df_clean = df_clean.join(broadcast(vehicles_filtered), on="vehicle", how="inner")

    # Rename raw coordinate columns to avoid naming collision after join
    df_raw_sel = df_raw.select(
        col("vehicle"),
        col("timestamp"),
        col("x").alias("raw_x"),
        col("y").alias("raw_y"),
        col("speed").alias("raw_speed")
    )

    # Join clean and raw coordinates on vehicle and timestamp
    df_joined = df_clean.join(df_raw_sel, on=["vehicle", "timestamp"], how="inner")

    # Haversine distance between Raw and Filtered (representing the displacement introduced by the filter)
    df_eval = df_joined.withColumn("deviation_meters", haversine_spark(col("y"), col("x"), col("raw_y"), col("raw_x")) * 1000.0)

    # Define window specification to compute delta metrics
    w = Window.partitionBy("vehicle").orderBy("timestamp")

    df_kinematics = df_eval \
        .withColumn("prev_timestamp", lag("timestamp").over(w)) \
        .withColumn("prev_x", lag("raw_x").over(w)) \
        .withColumn("prev_y", lag("raw_y").over(w)) \
        .withColumn("prev_x_f", lag("x").over(w)) \
        .withColumn("prev_y_f", lag("y").over(w)) \
        .withColumn("dt", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))

    # Filter out records where time difference is too small (avoiding high-frequency division noise) or invalid
    df_kinematics = df_kinematics.filter((col("dt") >= 5) & col("prev_timestamp").isNotNull())

    # Calculate step distances
    df_kinematics = df_kinematics \
        .withColumn("dist_raw_m", haversine_spark(col("prev_y"), col("prev_x"), col("raw_y"), col("raw_x")) * 1000.0) \
        .withColumn("dist_filtered_m", haversine_spark(col("prev_y_f"), col("prev_x_f"), col("y"), col("x")) * 1000.0)

    # Calculate coordinate-based speeds (m/s)
    df_kinematics = df_kinematics \
        .withColumn("speed_raw_coords", col("dist_raw_m") / col("dt")) \
        .withColumn("speed_filtered_coords", col("dist_filtered_m") / col("dt"))

    # Calculate coordinate-based accelerations (m/s^2)
    df_kinematics = df_kinematics \
        .withColumn("prev_speed_raw_coords", lag("speed_raw_coords").over(w)) \
        .withColumn("prev_speed_filtered_coords", lag("speed_filtered_coords").over(w)) \
        .withColumn("accel_raw_coords", (col("speed_raw_coords") - col("prev_speed_raw_coords")) / col("dt")) \
        .withColumn("accel_filtered_coords", (col("speed_filtered_coords") - col("prev_speed_filtered_coords")) / col("dt"))

    # Compute global aggregates
    metrics = df_kinematics.agg(
        avg("deviation_meters").alias("avg_deviation_m"),
        stddev("deviation_meters").alias("std_deviation_m"),
        avg("dist_raw_m").alias("avg_raw_dist_m"),
        avg("dist_filtered_m").alias("avg_filtered_dist_m"),
        stddev("accel_raw_coords").alias("raw_accel_std"),
        stddev("accel_filtered_coords").alias("filtered_accel_std")
    ).collect()[0]

    # Compute actual total trajectory distances
    trajectory_totals = df_kinematics.groupBy("vehicle").agg(
        spark_sum("dist_raw_m").alias("total_raw"),
        spark_sum("dist_filtered_m").alias("total_filtered")
    ).agg(
        avg("total_raw").alias("avg_traj_raw_km"),
        avg("total_filtered").alias("avg_traj_filtered_km")
    ).collect()[0]

    # Convert traj totals to km
    avg_traj_raw_km = trajectory_totals["avg_traj_raw_km"] / 1000.0
    avg_traj_filtered_km = trajectory_totals["avg_traj_filtered_km"] / 1000.0

    print("\n" + "="*50)
    print("      KALMAN FILTER PERFORMANCE METRICS (CORRECTED)")
    print("="*50)
    print(f"1. Coordinate Displacement (Shift introduced by filter):")
    print(f"   - Mean Shift: {metrics['avg_deviation_m']:.2f} meters")
    print(f"   - Std Shift:  {metrics['std_deviation_m']:.2f} meters")
    print(f"\n2. Trajectory Distance Smoothness (Total length):")
    print(f"   - Raw average ping-to-ping step:      {metrics['avg_raw_dist_m']:.2f} meters")
    print(f"   - Filtered average ping-to-ping step: {metrics['avg_filtered_dist_m']:.2f} meters")
    print(f"   - Average Raw Trajectory Length:      {avg_traj_raw_km:.2f} km")
    print(f"   - Average Filtered Trajectory Length: {avg_traj_filtered_km:.2f} km")
    reduction = ((avg_traj_raw_km - avg_traj_filtered_km) / avg_traj_raw_km) * 100.0
    print(f"   - Zig-zag noise path reduction:       {reduction:.2f}%")
    print(f"\n3. Kinematic Smoothness (Coordinate-based Acceleration):")
    print(f"   - Raw Acceleration Standard Dev:      {metrics['raw_accel_std']:.4f} m/s^2")
    print(f"   - Filtered Acceleration Standard Dev: {metrics['filtered_accel_std']:.4f} m/s^2")
    accel_reduction = ((metrics['raw_accel_std'] - metrics['filtered_accel_std']) / metrics['raw_accel_std']) * 100.0
    print(f"   - Acceleration jitter reduction:      {accel_reduction:.2f}%")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()

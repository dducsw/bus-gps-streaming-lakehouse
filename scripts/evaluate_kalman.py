from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, radians, sin, cos, asin, sqrt, lag, unix_timestamp, avg, stddev, lit

def haversine_spark(lat1, lng1, lat2, lng2):
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371.0 * c  # Output in km

def main():
    spark = SparkSession.builder.appName("KalmanEvaluation").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    target_date = "2025-03-22"
    print(f"Evaluating Kalman Filter Performance for date: {target_date}...")

    # Read Raw (Bronze) and Cleaned (Silver) waypoints
    df_raw = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point").filter(col("date") == lit(target_date))
    df_clean = spark.read.table("catalog_iceberg.bus_silver.bus_way_point").filter(col("date") == lit(target_date))

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
        .withColumn("prev_speed", lag("raw_speed").over(w)) \
        .withColumn("prev_speed_f", lag("speed").over(w)) \
        .withColumn("dt", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))

    # Filter out records where time difference is zero or invalid
    df_kinematics = df_kinematics.filter((col("dt") > 0) & col("prev_timestamp").isNotNull())

    # Calculate step distances and acceleration
    df_kinematics = df_kinematics \
        .withColumn("dist_raw_m", haversine_spark(col("prev_y"), col("prev_x"), col("raw_y"), col("raw_x")) * 1000.0) \
        .withColumn("dist_filtered_m", haversine_spark(col("prev_y_f"), col("prev_x_f"), col("y"), col("x")) * 1000.0) \
        .withColumn("accel_raw", (col("raw_speed") - col("prev_speed")) / col("dt")) \
        .withColumn("accel_filtered", (col("speed") - col("prev_speed_f")) / col("dt"))

    # Compute global aggregates
    metrics = df_kinematics.agg(
        avg("deviation_meters").alias("avg_deviation_m"),
        stddev("deviation_meters").alias("std_deviation_m"),
        avg("dist_raw_m").alias("avg_raw_dist_m"),
        avg("dist_filtered_m").alias("avg_filtered_dist_m"),
        stddev("accel_raw").alias("raw_accel_std"),
        stddev("accel_filtered").alias("filtered_accel_std")
    ).collect()[0]

    # Compute cumulative trajectory distance overestimation
    trajectory_totals = df_kinematics.groupBy("vehicle").agg(
        avg("dist_raw_m").alias("total_raw"),
        avg("dist_filtered_m").alias("total_filtered")
    ).agg(
        avg("total_raw").alias("avg_traj_raw_m"),
        avg("total_filtered").alias("avg_traj_filtered_m")
    ).collect()[0]

    print("\n" + "="*50)
    print("      KALMAN FILTER PERFORMANCE METRICS")
    print("="*50)
    print(f"1. Coordinate Displacement (Shift introduced by filter):")
    print(f"   - Mean Shift: {metrics['avg_deviation_m']:.2f} meters")
    print(f"   - Std Shift:  {metrics['std_deviation_m']:.2f} meters")
    print(f"\n2. Trajectory Distance Smoothness (Total length):")
    print(f"   - Raw average ping-to-ping step:      {metrics['avg_raw_dist_m']:.2f} meters")
    print(f"   - Filtered average ping-to-ping step: {metrics['avg_filtered_dist_m']:.2f} meters")
    reduction = ((metrics['avg_raw_dist_m'] - metrics['avg_filtered_dist_m']) / metrics['avg_raw_dist_m']) * 100.0
    print(f"   - Zig-zag noise path reduction:       {reduction:.2f}%")
    print(f"\n3. Kinematic Smoothness (Acceleration Variance):")
    print(f"   - Raw Acceleration Standard Dev:      {metrics['raw_accel_std']:.4f} m/s^2")
    print(f"   - Filtered Acceleration Standard Dev: {metrics['filtered_accel_std']:.4f} m/s^2")
    accel_reduction = ((metrics['raw_accel_std'] - metrics['accel_filtered_std']) / metrics['raw_accel_std']) * 100.0 if 'accel_filtered_std' in metrics else 0.0
    # Wait, let's use key names matching alias
    accel_reduction = ((metrics['raw_accel_std'] - metrics['filtered_accel_std']) / metrics['raw_accel_std']) * 100.0
    print(f"   - Acceleration jitter reduction:      {accel_reduction:.2f}%")
    print("="*50 + "\n")

if __name__ == "__main__":
    main()

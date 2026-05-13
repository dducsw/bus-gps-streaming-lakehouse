from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, row_number, current_timestamp
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

def apply_kalman_filter(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Apply Kalman Filter to smooth GPS (x, y) coordinates for a single vehicle's data.
    """
    # Ensure chronological order
    pdf = pdf.sort_values("timestamp").reset_index(drop=True)
    if len(pdf) == 0:
        return pdf

    # 1. Calculate Measurement Noise Variance (R) from stationary segments
    # Logic based on scripts/kalman_filter_demo.py
    is_zero = (pdf['speed'] == 0)
    streaks = (is_zero != is_zero.shift()).cumsum()
    zero_segments = pdf[is_zero].groupby(streaks)
    
    var_x_list = []
    var_y_list = []
    for _, segment in zero_segments:
        if len(segment) >= 10:
            var_x_list.append(segment['x'].var())
            var_y_list.append(segment['y'].var())
            
    # Default variances if no stationary data found (safety fallback)
    var_x = np.mean(var_x_list) if var_x_list else 1e-7
    var_y = np.mean(var_y_list) if var_y_list else 1e-7
    var_x, var_y = max(var_x, 1e-10), max(var_y, 1e-10)
    R = np.array([[var_x, 0], [0, var_y]])

    # 2. Kalman Filter Initialization
    H = np.array([[1, 0, 0, 0], [0, 1, 0, 0]])
    I = np.eye(4)
    sigma_a_sq = 1.96e-10 # Acceleration variance from demo script
    
    raw_x = pdf['x'].values
    raw_y = pdf['y'].values
    times = pd.to_datetime(pdf['timestamp'])
    
    filtered_x = []
    filtered_y = []
    
    # State: [x, y, vx, vy]^T
    x_hat = np.array([[raw_x[0]], [raw_y[0]], [0], [0]])
    P = np.eye(4)
    
    for i in range(len(raw_x)):
        if i == 0:
            dt = 1.0
        else:
            dt = (times[i] - times[i-1]).total_seconds()
            
        if dt <= 0: dt = 1.0 
        
        # Transition matrix F
        F = np.array([[1, 0, dt, 0],
                      [0, 1, 0, dt],
                      [0, 0, 1, 0],
                      [0, 0, 0, 1]])
        
        # Process noise matrix Q
        dt2, dt3, dt4 = dt**2, dt**3, dt**4
        Q = np.array([
            [(dt4/4)*sigma_a_sq, 0,                  (dt3/2)*sigma_a_sq, 0                 ],
            [0,                  (dt4/4)*sigma_a_sq, 0,                  (dt3/2)*sigma_a_sq],
            [(dt3/2)*sigma_a_sq, 0,                  dt2*sigma_a_sq,     0                 ],
            [0,                  (dt3/2)*sigma_a_sq, 0,                  dt2*sigma_a_sq    ]
        ])
        
        # Predict
        x_hat = F @ x_hat
        P = F @ P @ F.T + Q
        
        # Update
        Z = np.array([[raw_x[i]], [raw_y[i]]])
        Y = Z - (H @ x_hat)
        S = (H @ P @ H.T) + R
        K = P @ H.T @ np.linalg.inv(S)
        
        x_hat = x_hat + (K @ Y)
        P = (I - (K @ H)) @ P
        
        filtered_x.append(x_hat[0, 0])
        filtered_y.append(x_hat[1, 0])
    
    pdf['x'] = filtered_x
    pdf['y'] = filtered_y
    return pdf

def main():
    spark = SparkSession.builder.appName("SilverWayPointClean").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Read bronze tables
    df_bus = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point")
    df_map = spark.read.table("catalog_iceberg.bus_bronze.vehicle_bus_mapping")

    # Join vehicle -> route
    df_join = df_bus.join(df_map, on="vehicle", how="left")

    # Step 1 & 2: Filter and Deduplicate (Using Window to get the latest record)
    window_spec = Window.partitionBy("vehicle", "timestamp").orderBy(col("load_at").desc())

    df_filtered = (
        df_join.filter(
            col("vehicle").isNotNull() & 
            col("timestamp").isNotNull() & 
            col("x").isNotNull() & (col("x") != 0) & 
            col("y").isNotNull() & (col("y") != 0)
        )
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Step 3: Apply Kalman Filter to remove noise from (x, y) coordinates
    # We use applyInPandas for efficient group-by processing
    df_kalman = df_filtered.groupBy("vehicle").applyInPandas(
        apply_kalman_filter, 
        schema=df_filtered.schema
    )

    # Clean + add time columns + Step 5: Sorting
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

    # Create Silver table
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_silver
    """)

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

    # Step 4: Write to Silver with overwritePartitions
    df_clean.writeTo("catalog_iceberg.bus_silver.bus_way_point").overwritePartitions()

    print("WRITE bus_silver.bus_way_point SUCCESS (Idempotent, Cleaned, Sorted)")


if __name__ == "__main__":
    main()
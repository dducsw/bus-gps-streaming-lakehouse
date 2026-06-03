import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, radians, sin, cos, asin, sqrt, lag, unix_timestamp, avg, stddev, lit
from pyspark.sql.window import Window

# Define Haversine distance in python/numpy
def haversine_np(lat1, lon1, lat2, lon2):
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    c = 2.0 * np.arcsin(np.sqrt(a))
    return 6371.0 * c * 1000.0  # meters

def run_kalman_filter(df_pdf, R_var, sigma_a_sq, max_dt=15.0):
    pdf = df_pdf.sort_values("timestamp").reset_index(drop=True)
    if len(pdf) == 0:
        return pdf
    
    H = np.array([[1.0, 0.0, 0.0, 0.0], [0.0, 1.0, 0.0, 0.0]])
    I = np.eye(4)
    R = np.array([[R_var, 0.0], [0.0, R_var]])
    
    x_hat = np.array([[pdf["x"].iloc[0]], [pdf["y"].iloc[0]], [0.0], [0.0]])
    P = np.eye(4) * 0.1
    P[2, 2] = 1.0
    P[3, 3] = 1.0
    
    filtered_x = [pdf["x"].iloc[0]]
    filtered_y = [pdf["y"].iloc[0]]
    filtered_speed = [float(pdf["speed"].iloc[0])]
    
    last_timestamp = pd.to_datetime(pdf["timestamp"].iloc[0])
    times = pd.to_datetime(pdf["timestamp"])
    
    for i in range(1, len(pdf)):
        dt = (times[i] - last_timestamp).total_seconds()
        
        if dt > max_dt or dt <= 0:
            x_hat[0, 0] = pdf["x"].iloc[i]
            x_hat[1, 0] = pdf["y"].iloc[i]
            x_hat[2, 0] = 0.0
            x_hat[3, 0] = 0.0
            P = np.eye(4) * 0.1
            P[2, 2] = 1.0
            P[3, 3] = 1.0
            dt = 1.0
            
        F = np.array([
            [1.0, 0.0, dt,  0.0],
            [0.0, 1.0, 0.0, dt ],
            [0.0, 0.0, 1.0, 0.0],
            [0.0, 0.0, 0.0, 1.0]
        ])
        
        dt2, dt3, dt4 = dt**2, dt**3, dt**4
        Q = np.array([
            [(dt4/4)*sigma_a_sq, 0.0,                    (dt3/2)*sigma_a_sq, 0.0                    ],
            [0.0,                    (dt4/4)*sigma_a_sq, 0.0,                    (dt3/2)*sigma_a_sq],
            [(dt3/2)*sigma_a_sq, 0.0,                    dt2*sigma_a_sq,     0.0                    ],
            [0.0,                    (dt3/2)*sigma_a_sq, 0.0,                    dt2*sigma_a_sq    ]
        ])
        
        x_hat = F @ x_hat
        P = F @ P @ F.T + Q
        
        raw_x = pdf["x"].iloc[i]
        raw_y = pdf["y"].iloc[i]
        Z = np.array([[raw_x], [raw_y]])
        
        Y = Z - (H @ x_hat)
        S = (H @ P @ H.T) + R
        K = P @ H.T @ np.linalg.inv(S)
        
        x_hat = x_hat + (K @ Y)
        P = (I - (K @ H)) @ P
        
        # Speed calculation
        lat_rad = np.radians(x_hat[1, 0])
        vy_mps = x_hat[3, 0] * 111320.0
        vx_mps = x_hat[2, 0] * 111320.0 * np.cos(lat_rad)
        speed_mps = np.sqrt(vx_mps**2 + vy_mps**2)
        speed_kmh = speed_mps * 3.6
        
        if speed_kmh > 120.0:
            speed_kmh = float(pdf["speed"].iloc[i])
        elif speed_kmh < 0.1 and float(pdf["speed"].iloc[i]) < 1.0:
            speed_kmh = 0.0
            
        filtered_x.append(x_hat[0, 0])
        filtered_y.append(x_hat[1, 0])
        filtered_speed.append(speed_kmh)
        last_timestamp = times[i]
        
    pdf["filtered_x"] = filtered_x
    pdf["filtered_y"] = filtered_y
    pdf["filtered_speed"] = filtered_speed
    return pdf

def main():
    spark = SparkSession.builder.appName("KalmanGridSearch").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    target_date = "2025-03-22"
    print(f"Loading raw Bronze waypoints for {target_date}...")
    df_raw = spark.read.table("catalog_iceberg.bus_bronze.bus_way_point").filter(col("date") == lit(target_date))
    
    # Collect to Pandas for fast local evaluation of grid parameters
    print("Collecting records to driver...")
    pdf_all = df_raw.select("vehicle", "timestamp", "x", "y", "speed").toPandas()
    
    # Sort and clean
    pdf_all = pdf_all.sort_values(["vehicle", "timestamp"]).reset_index(drop=True)
    pdf_all["timestamp"] = pd.to_datetime(pdf_all["timestamp"])
    
    # Define parameter grid
    # R_var range: 1e-9 (~3.5m std) to 1e-6 (~110m std)
    R_grid = [1e-9, 5e-9, 1e-8, 5e-8, 1e-7]
    # sigma_a_sq range: 1e-11 to 1e-9
    sigma_grid = [1e-11, 5e-11, 1e-10, 1.96e-10, 1e-9]
    
    print("\nStarting Grid Search...")
    print(f"{'R_var':<10} | {'sigma_a_sq':<12} | {'Mean Shift (m)':<15} | {'Std Shift (m)':<15} | {'Path Red. %':<12} | {'Filtered Accel Std':<20}")
    print("-" * 85)
    
    for R_var in R_grid:
        for sigma_a_sq in sigma_grid:
            # Process trajectories
            results = []
            for vehicle, group in pdf_all.groupby("vehicle"):
                if len(group) < 5:
                    continue
                res = run_kalman_filter(group, R_var, sigma_a_sq)
                results.append(res)
            
            if not results:
                continue
                
            pdf_res = pd.concat(results, ignore_index=True)
            
            # Compute evaluation metrics
            # 1. Displacement
            pdf_res["deviation_meters"] = haversine_np(pdf_res["y"], pdf_res["x"], pdf_res["filtered_y"], pdf_res["filtered_x"])
            mean_shift = pdf_res["deviation_meters"].mean()
            std_shift = pdf_res["deviation_meters"].std()
            
            # 2. Path reduction
            # Calculate step distances
            pdf_res["prev_x"] = pdf_res.groupby("vehicle")["x"].shift(1)
            pdf_res["prev_y"] = pdf_res.groupby("vehicle")["y"].shift(1)
            pdf_res["prev_fx"] = pdf_res.groupby("vehicle")["filtered_x"].shift(1)
            pdf_res["prev_fy"] = pdf_res.groupby("vehicle")["filtered_y"].shift(1)
            
            pdf_res["dist_raw"] = haversine_np(pdf_res["prev_y"], pdf_res["prev_x"], pdf_res["y"], pdf_res["x"])
            pdf_res["dist_filtered"] = haversine_np(pdf_res["prev_fy"], pdf_res["prev_fx"], pdf_res["filtered_y"], pdf_res["filtered_x"])
            
            avg_raw_dist = pdf_res["dist_raw"].mean()
            avg_filt_dist = pdf_res["dist_filtered"].mean()
            path_reduction = ((avg_raw_dist - avg_filt_dist) / avg_raw_dist) * 100.0 if avg_raw_dist > 0 else 0.0
            
            # 3. Kinematic smoothness
            pdf_res["prev_time"] = pdf_res.groupby("vehicle")["timestamp"].shift(1)
            pdf_res["dt"] = (pdf_res["timestamp"] - pdf_res["prev_time"]).dt.total_seconds()
            pdf_res["prev_speed"] = pdf_res.groupby("vehicle")["filtered_speed"].shift(1)
            
            # Filter valid rows for acceleration
            pdf_accel = pdf_res[(pdf_res["dt"] > 0) & (pdf_res["prev_speed"].notnull())].copy()
            # Speed is in km/h, convert to m/s
            pdf_accel["accel"] = ((pdf_accel["filtered_speed"] / 3.6) - (pdf_accel["prev_speed"] / 3.6)) / pdf_accel["dt"]
            accel_std = pdf_accel["accel"].std()
            
            print(f"{R_var:<10.1e} | {sigma_a_sq:<12.1e} | {mean_shift:<15.2f} | {std_shift:<15.2f} | {path_reduction:<12.2f}% | {accel_std:<20.4f}")
            
    spark.stop()

if __name__ == "__main__":
    main()

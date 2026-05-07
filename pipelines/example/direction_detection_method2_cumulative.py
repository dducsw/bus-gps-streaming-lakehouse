"""
Direction Detection - Method 2: Cumulative Distance Tracking
Detect direction bằng cách theo dõi quãng đường tích lũy so với chiều dài tuyến
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, radians, sin, cos, sqrt, asin, round as spark_round,
    row_number, when, count, sum as spark_sum, avg, max as spark_max, lag,
    from_unixtime, greatest
)
from pyspark.sql.window import Window
import pandas as pd
import json
import time
import sys


def load_sample_json(filepath):
    """Load sample.json with fallback methods"""
    print(f"⏳ Loading sample.json (71MB, may take 1-2 minutes)...")
    start_time = time.time()
    
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        
        df_raw = pd.DataFrame(data)
        elapsed = time.time() - start_time
        print(f"✅ Loaded successfully in {elapsed:.1f}s")
        
    except Exception as e:
        print(f"⚠️ Error: {e}, trying alternative...")
        df_raw = pd.read_json(filepath)
    
    return df_raw


def extract_waypoints(df_raw):
    """Extract and clean waypoint data from nested msgBusWayPoint"""
    print("\n📋 Extracting msgBusWayPoint data...")
    
    # Flatten nested msgBusWayPoint structure using json_normalize
    df_waypoint = pd.json_normalize(df_raw['msgBusWayPoint'])
    
    # Select required columns that exist in the data
    required_cols = ['vehicle', 'speed', 'datetime', 'x', 'y']
    available_cols = [col for col in required_cols if col in df_waypoint.columns]
    df_waypoint = df_waypoint[available_cols].copy()
    
    # Rename columns
    df_waypoint = df_waypoint.rename(columns={
        'datetime': 'unix_timestamp',
        'x': 'lng',
        'y': 'lat'
    })
    
    # Drop rows with null vehicle ID
    df_waypoint = df_waypoint.dropna(subset=['vehicle'])
    
    # Convert all columns to string first to handle mixed types
    for col in df_waypoint.columns:
        if df_waypoint[col].dtype != 'object':
            df_waypoint[col] = df_waypoint[col].astype(str)
        else:
            df_waypoint[col] = df_waypoint[col].apply(
                lambda x: str(x) if not isinstance(x, (dict, list)) else None
            )
    
    # Now convert specific columns to numeric types
    numeric_cols = ['speed', 'lng', 'lat', 'unix_timestamp']
    for col in numeric_cols:
        if col in df_waypoint.columns:
            df_waypoint[col] = pd.to_numeric(df_waypoint[col], errors='coerce')
    
    # Convert vehicle to string
    if 'vehicle' in df_waypoint.columns:
        df_waypoint['vehicle'] = df_waypoint['vehicle'].astype('string')
    
    # Final cleanup - remove any rows with null critical values
    df_waypoint = df_waypoint.dropna(subset=['vehicle', 'lng', 'lat', 'unix_timestamp'])
    
    print(f"✅ Extracted {len(df_waypoint)} waypoints")
    return df_waypoint


def haversine_distance(lat1, lng1, lat2, lng2):
    """Calculate haversine distance in km"""
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c


def detect_direction_by_cumulative_distance(spark, df_waypoint_pd, df_mapping_pd):
    """
    Method 2: Cumulative Distance Tracking
    Detect direction by tracking cumulative GPS distance
    """
    print("\n🔍 METHOD 2: Cumulative Distance Tracking")
    print("=" * 70)
    
    # Convert to Spark DataFrame
    df_waypoint = spark.createDataFrame(df_waypoint_pd, verifySchema=False)
    df_waypoint = df_waypoint.withColumn(
        "timestamp",
        from_unixtime(col("unix_timestamp")).cast("timestamp")
    ).drop("unix_timestamp").dropna(subset=["speed"])
    
    df_mapping = spark.createDataFrame(df_mapping_pd, verifySchema=False)
    
    print(f"✅ Spark DataFrames created")
    print(f"   Waypoints: {df_waypoint.count()}")
    print(f"   Mappings: {df_mapping.count()}")
    
    # Join with route mapping
    df_with_route = df_waypoint.join(df_mapping, on="vehicle", how="left").dropna(subset=["route_id"])
    
    print(f"✅ Joined {df_with_route.count()} waypoints with routes")
    
    # Calculate segment distances
    window_spec = Window.partitionBy("vehicle", "route_id").orderBy("timestamp")
    
    df_with_segment = df_with_route \
        .withColumn("prev_lat", lag("lat").over(window_spec)) \
        .withColumn("prev_lng", lag("lng").over(window_spec)) \
        .withColumn(
            "segment_distance_km",
            when(
                (col("prev_lat").isNotNull()) & (col("prev_lng").isNotNull()),
                haversine_distance(col("prev_lat"), col("prev_lng"), col("lat"), col("lng"))
            ).otherwise(0)
        )
    
    print(f"✅ Calculated segment distances")
    
    # Calculate cumulative distance
    df_cumulative = df_with_segment \
        .withColumn(
            "cumulative_distance_km",
            spark_sum("segment_distance_km").over(
                Window.partitionBy("vehicle", "route_id")
                .orderBy("timestamp")
                .rangeBetween(Window.unboundedPreceding, 0)
            )
        )
    
    print(f"✅ Calculated cumulative distances")
    
    # Detect direction based on forward movement ratio
    df_distance_trend = df_cumulative \
        .withColumn(
            "distance_change",
            col("cumulative_distance_km") - 
            lag("cumulative_distance_km", 1, 0).over(
                Window.partitionBy("vehicle", "route_id").orderBy("timestamp")
            )
        )
    
    # Count forward vs backward movements
    df_direction_detection = df_distance_trend \
        .withColumn(
            "is_forward",
            when(col("distance_change") > 0.01, 1).otherwise(0)  # 10m threshold
        ) \
        .groupBy("vehicle", "route_id", "route_no") \
        .agg(
            spark_sum("is_forward").alias("forward_moves"),
            (count("*") - spark_sum("is_forward")).alias("non_forward_moves"),
            spark_max("cumulative_distance_km").alias("max_distance_km")
        ) \
        .withColumn(
            "forward_ratio",
            spark_round(
                col("forward_moves") / (col("forward_moves") + col("non_forward_moves")) * 100, 2
            )
        ) \
        .withColumn(
            "detected_direction",
            when(col("forward_ratio") > 70, "OUTBOUND")
            .when(col("forward_ratio") < 30, "INBOUND")
            .otherwise("UNCERTAIN")
        ) \
        .withColumn(
            "confidence",
            spark_round(
                greatest(col("forward_ratio"), 100 - col("forward_ratio")),
                2
            )
        )
    
    print(f"\n✅ RESULTS:")
    df_direction_detection.show(20, truncate=False)
    
    # Statistics
    results_df = df_direction_detection.toPandas()
    print(f"\n📊 Summary:")
    print(f"   Total trips: {len(results_df)}")
    print(f"\n   Direction Distribution:")
    print(results_df["detected_direction"].value_counts())
    print(f"\n   Forward Ratio Statistics:")
    print(results_df["forward_ratio"].describe())
    print(f"\n   Max Distance Statistics:")
    print(results_df["max_distance_km"].describe())
    
    return results_df


def main():
    """Main execution"""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DirectionDetectionMethod2") \
        .config("spark.memory.fraction", "0.6") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Spark initialized\n")
    
    try:
        # Load data
        df_raw = load_sample_json("/opt/spark/apps/data/HPCLAB/sample.json")
        df_waypoint = extract_waypoints(df_raw)
        
        df_mapping = pd.read_csv("/opt/spark/apps/data/HPCLAB/vehicle_route_mapping.csv")
        
        print(f"✅ Loaded {len(df_mapping)} vehicle mappings")
        
        # Run direction detection
        results = detect_direction_by_cumulative_distance(spark, df_waypoint, df_mapping)
        
        # Save results
        output_path = "/opt/spark/apps/pipelines/example/results_method2.csv"
        results.to_csv(output_path, index=False)
        print(f"\n✅ Results saved to {output_path}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

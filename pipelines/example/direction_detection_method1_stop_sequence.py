"""
Direction Detection - Method 1: Stop Sequence Matching
Detect direction bằng cách match GPS points với trạm gần nhất
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, radians, sin, cos, sqrt, asin, round as spark_round,
    row_number, when, count, sum as spark_sum, max as spark_max,
    from_unixtime, lag, avg, greatest
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
        
    except MemoryError:
        print("⚠️ Memory error, trying with pd.read_json...")
        try:
            df_raw = pd.read_json(filepath)
            elapsed = time.time() - start_time
            print(f"✅ Loaded with pd.read_json in {elapsed:.1f}s")
        except Exception as e:
            print(f"❌ Failed: {e}")
            raise
    
    return df_raw


def extract_waypoints(df_raw):
    """Extract and clean waypoint data from nested msgBusWayPoint"""
    print("\n📋 Extracting msgBusWayPoint data...")
    
    # Flatten nested msgBusWayPoint structure using json_normalize
    df_waypoint = pd.json_normalize(df_raw['msgBusWayPoint'])
    
    # Select required columns that exist in the data
    required_cols = ['vehicle', 'driver', 'speed', 'datetime', 'x', 'y']
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
    # This ensures consistent types before Spark conversion
    for col in df_waypoint.columns:
        # Skip if already string
        if df_waypoint[col].dtype != 'object':
            df_waypoint[col] = df_waypoint[col].astype(str)
        else:
            # For object columns, convert all values to string, handling nested structures
            df_waypoint[col] = df_waypoint[col].apply(
                lambda x: str(x) if not isinstance(x, (dict, list)) else None
            )
    
    # Now convert specific columns to numeric types
    numeric_cols = ['speed', 'lng', 'lat', 'unix_timestamp']
    for col in numeric_cols:
        if col in df_waypoint.columns:
            df_waypoint[col] = pd.to_numeric(df_waypoint[col], errors='coerce')
    
    # Convert vehicle and driver to string (pandas StringDtype)
    string_cols = ['vehicle', 'driver']
    for col in string_cols:
        if col in df_waypoint.columns:
            df_waypoint[col] = df_waypoint[col].astype('string')
    
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


def detect_direction_by_stop_sequence(spark, df_waypoint_pd, df_mapping_pd, df_stops_pd):
    """
    Method 1: Stop Sequence Matching
    Detect direction by matching GPS points with stop order
    """
    print("\n🔍 METHOD 1: Stop Sequence Matching")
    print("=" * 70)
    
    # Convert to Spark DataFrames
    df_waypoint = spark.createDataFrame(df_waypoint_pd, verifySchema=False)
    df_waypoint = df_waypoint.withColumn(
        "timestamp",
        from_unixtime(col("unix_timestamp")).cast("timestamp")
    ).drop("unix_timestamp")
    
    df_mapping = spark.createDataFrame(df_mapping_pd, verifySchema=False)
    df_stops = spark.createDataFrame(df_stops_pd, verifySchema=False)
    
    print(f"✅ Spark DataFrames created")
    print(f"   Waypoints: {df_waypoint.count()}")
    print(f"   Mappings: {df_mapping.count()}")
    print(f"   Stops: {df_stops.count()}")
    
    # Join with route mapping
    df_with_route = df_waypoint.join(df_mapping, on="vehicle", how="left")
    
    # Filter to only rows with route info
    df_with_route = df_with_route.filter(col("route_id").isNotNull())
    
    # Rename stop columns to avoid conflicts
    df_stops_renamed = df_stops.select(
        col("stopId").alias("stop_id"),
        col("stop_name"),
        col("routes").alias("route_list"),
        col("lng").alias("stop_lng"),
        col("lat").alias("stop_lat")
    )
    
    # Find closest stop for each waypoint (within 500m)
    # Using a cross join with distance filtering since we can't use route matching
    df_with_distance = df_with_route.join(
        df_stops_renamed,
        how="cross"
    ).withColumn(
        "distance_to_stop_km",
        haversine_distance(
            col("lat"), col("lng"),
            col("stop_lat"), col("stop_lng")
        )
    ).filter(col("distance_to_stop_km") < 0.5)  # Within 500m
    
    # Find closest stop  
    df_closest_stop = df_with_distance \
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("vehicle", "timestamp").orderBy("distance_to_stop_km")
            )
        ) \
        .filter(col("rn") == 1) \
        .select(
            "vehicle", "timestamp", "route_id", "route_no", "stop_id", "stop_name", 
            "distance_to_stop_km", "stop_lat", "stop_lng"
        )
    
    print(f"✅ Found {df_closest_stop.count()} waypoints near stops")
    
    # For direction detection, use GPS coordinate progression
    # Group by vehicle and calculate movement direction
    window_spec = Window.partitionBy("vehicle").orderBy("timestamp")
    
    df_movement = df_closest_stop.withColumn(
        "prev_lat", lag("stop_lat").over(window_spec)
    ).withColumn(
        "prev_lng", lag("stop_lng").over(window_spec)
    ).withColumn(
        "lat_diff", col("stop_lat") - col("prev_lat")
    ).withColumn(
        "lng_diff", col("stop_lng") - col("prev_lng")
    )
    
    # Detect direction based on coordinate progression
    # For HCMC: outbound typically moves north/east, inbound moves south/west
    df_with_direction = df_movement \
        .withColumn(
            "moving_north", when(col("lat_diff") > 0, 1).otherwise(0)
        ) \
        .withColumn(
            "moving_east", when(col("lng_diff") > 0, 1).otherwise(0)
        ) \
        .groupBy("vehicle", "route_id", "route_no") \
        .agg(
            spark_sum("moving_north").alias("north_count"),
            spark_sum("moving_east").alias("east_count"),
            count("*").alias("total_moves")
        ) \
        .withColumn(
            "detected_direction",
            when(
                (col("north_count") + col("east_count")) > col("total_moves") / 2,
                "OUTBOUND"
            ).otherwise("INBOUND")
        ) \
        .withColumn(
            "confidence",
            spark_round(
                greatest(col("north_count"), col("east_count")) / col("total_moves") * 100,
                2
            )
        )
    
    
    print(f"\n✅ RESULTS:")
    df_with_direction.show(20, truncate=False)
    
    # Statistics
    results_df = df_with_direction.toPandas()
    print(f"\n📊 Summary:")
    print(f"   Total trips: {len(results_df)}")
    print(f"\n   Direction Distribution:")
    print(results_df["detected_direction"].value_counts())
    print(f"\n   Confidence Statistics:")
    print(results_df.groupby("detected_direction")["confidence"].describe())
    
    return df_with_direction.toPandas()


def main():
    """Main execution"""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DirectionDetectionMethod1") \
        .config("spark.memory.fraction", "0.6") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✅ Spark initialized\n")
    
    try:
        # Load data
        df_raw = load_sample_json("/opt/spark/apps/data/HPCLAB/sample.json")
        df_waypoint = extract_waypoints(df_raw)
        
        df_mapping = pd.read_csv("/opt/spark/apps/data/HPCLAB/vehicle_route_mapping.csv")
        df_stops = pd.read_csv("/opt/spark/apps/data/HPCLAB/hcmc_stops_agu_2025.csv")
        
        print(f"✅ Loaded {len(df_mapping)} vehicle mappings")
        print(f"✅ Loaded {len(df_stops)} route stops")
        
        # Run direction detection
        results = detect_direction_by_stop_sequence(spark, df_waypoint, df_mapping, df_stops)
        
        # Save results
        output_path = "/opt/spark/apps/pipelines/example/results_method1.csv"
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

"""
Direction Detection - Method 3: Path Geometry Matching
Detect direction bằng cách so sánh GPS points với path của tuyến
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, radians, sin, cos, sqrt, asin, round as spark_round,
    row_number, when, count, sum as spark_sum, avg, max as spark_max, lead, lag,
    from_unixtime, collect_list, greatest
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
    required_cols = ['vehicle', 'datetime', 'x', 'y']
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
    numeric_cols = ['lng', 'lat', 'unix_timestamp']
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


def detect_direction_by_path_geometry(spark, df_waypoint_pd, df_mapping_pd):
    """
    Method 3: Path Geometry Matching
    Detect direction by matching GPS progression along path geometry
    """
    print("\n🔍 METHOD 3: Path Geometry Matching")
    print("=" * 70)
    
    # Convert to Spark DataFrame
    df_waypoint = spark.createDataFrame(df_waypoint_pd, verifySchema=False)
    df_waypoint = df_waypoint.withColumn(
        "timestamp",
        from_unixtime(col("unix_timestamp")).cast("timestamp")
    ).drop("unix_timestamp")
    
    df_mapping = spark.createDataFrame(df_mapping_pd, verifySchema=False)
    
    print(f"✅ Spark DataFrames created")
    print(f"   Waypoints: {df_waypoint.count()}")
    print(f"   Mappings: {df_mapping.count()}")
    
    # Join with route mapping
    df_with_route = df_waypoint.join(df_mapping, on="vehicle", how="left").dropna(subset=["route_id"])
    
    print(f"✅ Joined {df_with_route.count()} waypoints with routes")
    
    # Add waypoint sequence
    window_spec = Window.partitionBy("vehicle", "route_id").orderBy("timestamp")
    df_with_seq = df_with_route.withColumn(
        "waypoint_seq",
        row_number().over(window_spec)
    )
    
    print(f"✅ Added waypoint sequence")
    
    # Create synthetic route path profiles from median positions
    df_path_profile = df_with_seq \
        .groupBy("route_id", "waypoint_seq") \
        .agg(
            avg("lat").alias("median_lat"),
            avg("lng").alias("median_lng")
        )
    
    print(f"✅ Created route path profiles")
    
    # Match waypoints to path profile by route and sequence
    df_with_path_match = df_with_seq.join(
        df_path_profile,
        on=["route_id", "waypoint_seq"],
        how="left"
    )
    
    # Calculate distance to path
    df_distance_to_path = df_with_path_match.withColumn(
        "distance_to_path_km",
        haversine_distance(
            col("lat"), col("lng"),
            col("median_lat"), col("median_lng")
        )
    )
    
    print(f"✅ Calculated distance to path")
    
    # Find closest path sequence for each waypoint
    df_closest_path_seq = df_distance_to_path \
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("vehicle", "timestamp").orderBy("distance_to_path_km")
            )
        ) \
        .filter(col("rn") == 1) \
        .select(
            "vehicle", "timestamp", "route_id",
            "waypoint_seq", "distance_to_path_km"
        ) \
        .dropna()
    
    print(f"✅ Matched {df_closest_path_seq.count()} waypoints to path sequences")
    
    # Detect direction from sequence progression
    window_trip = Window.partitionBy("vehicle", "route_id").orderBy("timestamp")
    
    df_sequence_trend = df_closest_path_seq \
        .withColumn(
            "next_path_seq",
            lead(col("waypoint_seq")).over(window_trip)
        ) \
        .withColumn(
            "is_forward",
            when(col("next_path_seq") > col("waypoint_seq"), 1).otherwise(0)
        )
    
    # Calculate direction based on trend
    df_direction_final = df_sequence_trend \
        .groupBy("vehicle", "route_id") \
        .agg(
            spark_sum("is_forward").alias("forward_movements"),
            count("*").alias("total_movements"),
            avg("distance_to_path_km").alias("avg_path_match_distance_km")
        ) \
        .withColumn(
            "forward_ratio",
            spark_round(col("forward_movements") / col("total_movements") * 100, 2)
        ) \
        .withColumn(
            "detected_direction",
            when(col("forward_ratio") > 60, "OUTBOUND")
            .when(col("forward_ratio") < 40, "INBOUND")
            .otherwise("UNCERTAIN")
        ) \
        .withColumn(
            "confidence",
            spark_round(greatest(col("forward_ratio"), 100 - col("forward_ratio")), 2)
        )
    
    print(f"\n✅ RESULTS:")
    df_direction_final.show(20, truncate=False)
    
    # Statistics
    results_df = df_direction_final.toPandas()
    print(f"\n📊 Summary:")
    print(f"   Total trips: {len(results_df)}")
    print(f"\n   Direction Distribution:")
    print(results_df["detected_direction"].value_counts())
    print(f"\n   Confidence Statistics:")
    print(results_df["confidence"].describe())
    print(f"\n   Path Match Distance Statistics:")
    print(results_df["avg_path_match_distance_km"].describe())
    print(f"\n   Forward Ratio by Direction:")
    print(results_df.groupby("detected_direction")["forward_ratio"].describe())
    
    return results_df


def main():
    """Main execution"""
    # Initialize Spark
    spark = SparkSession.builder \
        .appName("DirectionDetectionMethod3") \
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
        results = detect_direction_by_path_geometry(spark, df_waypoint, df_mapping)
        
        # Save results
        output_path = "/opt/spark/apps/pipelines/example/results_method3.csv"
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

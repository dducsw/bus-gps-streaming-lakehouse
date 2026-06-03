import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, radians, sin, cos, asin, sqrt, row_number, element_at,
    lag, unix_timestamp, when, sum as spark_sum, min as spark_min,
    max as spark_max, count, round, least, to_date, lit, current_timestamp
)

PROCESS_DATE = os.getenv("PROCESS_DATE", "2025-03-22")

def haversine(lat1, lng1, lat2, lng2):
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c

def main():
    spark = SparkSession.builder.appName("GoldTripSummary").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Running Trip Segmentation for date: {PROCESS_DATE}")

    # 1. Read Waypoints for the target processing date
    df_bw = (
        spark.read.table("catalog_iceberg.bus_silver.bus_way_point")
        .filter(col("date") == lit(PROCESS_DATE))
    )
    # Ensure columns and types are correct
    df_bw = df_bw.withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # 2. Read Route Path and Extract Terminals
    df_route = spark.read.table("catalog_iceberg.bus_silver.route_path")
    
    # Take the first Route Path variation per RouteId (min RouteVarId) to define Terminal A and B
    w_route = Window.partitionBy("RouteId").orderBy("RouteVarId")
    df_route_min = df_route.withColumn("rn", row_number().over(w_route)).filter("rn = 1").drop("rn")
    
    # Convert path array to start and end coordinates
    df_terminals = df_route_min.select(
        col("RouteId").alias("route_id"),
        element_at(col("path"), 1).alias("start_pt"),
        element_at(col("path"), -1).alias("end_pt")
    ).select(
        "route_id",
        col("start_pt")[0].alias("lng_A"),
        col("start_pt")[1].alias("lat_A"),
        col("end_pt")[0].alias("lng_B"),
        col("end_pt")[1].alias("lat_B")
    )

    # 3. Join waypoints with their route terminals
    # We join df_bw and df_terminals on route_id
    df_bw_term = df_bw.join(df_terminals, on="route_id", how="inner")

    # Compute distances to terminals
    df_bw_term = df_bw_term.withColumn("dist_A", haversine(col("y"), col("x"), col("lat_A"), col("lng_A"))) \
                           .withColumn("dist_B", haversine(col("y"), col("x"), col("lat_B"), col("lng_B")))

    # A waypoint is at a terminal if it is within 300 meters
    df_bw_term = df_bw_term.withColumn("is_at_terminal", (col("dist_A") <= 0.3) | (col("dist_B") <= 0.3))

    # 4. Detect transitions and split trips
    w = Window.partitionBy("vehicle").orderBy("timestamp")
    w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    df_sorted = df_bw_term.withColumn("prev_timestamp", lag("timestamp").over(w)) \
                          .withColumn("prev_x", lag("x").over(w)) \
                          .withColumn("prev_y", lag("y").over(w)) \
                          .withColumn("prev_route_id", lag("route_id").over(w)) \
                          .withColumn("prev_is_at_terminal", lag("is_at_terminal").over(w))
    
    df_sorted = df_sorted.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
                     
    # A transition triggers a new trip segment:
    # - First point of the vehicle
    # - Telemetry gap > 10 mins (600s)
    # - Route ID changes
    # - Leaving terminal (prev is_at_terminal was True, current is False)
    # - Entering terminal (prev is_at_terminal was False, current is True)
    df_sorted = df_sorted.withColumn("is_new_trip", 
        when(col("prev_timestamp").isNull(), 1)
        .when(col("time_diff") > 600, 1)
        .when(col("prev_route_id") != col("route_id"), 1)
        .when(col("prev_is_at_terminal") & ~col("is_at_terminal"), 1)
        .when(~col("prev_is_at_terminal") & col("is_at_terminal"), 1)
        .otherwise(0)
    )
    
    df_sorted = df_sorted.withColumn("segment_dist", 
        when(col("is_new_trip") == 1, 0.0)
        .when(col("prev_x").isNotNull() & col("prev_y").isNotNull(),
             haversine(col("prev_y"), col("prev_x"), col("y"), col("x"))).otherwise(0.0)
    )

    df_sorted = df_sorted.withColumn("trip_id", spark_sum("is_new_trip").over(w_cum))
    
    # 5. Summarize Trips
    trip_summary = df_sorted.groupBy("vehicle", "route_id", "route_no", "trip_id").agg(
        spark_min("timestamp").alias("start_time"),
        spark_max("timestamp").alias("end_time"),
        spark_sum("segment_dist").alias("total_distance_km"),
        count("*").alias("point_count")
    )
    
    # Filter out very short noise or layover trips (distance <= 1.5 km)
    trip_summary = trip_summary.filter(col("total_distance_km") > 1.5)
    trip_summary = trip_summary.withColumn("trip_duration_minutes", round((unix_timestamp("end_time") - unix_timestamp("start_time")) / 60, 2))

    # 6. Retrieve Start and End Coordinates for Direction Classification
    df_start_loc = df_sorted.select(
        "vehicle", "route_id", "trip_id",
        col("timestamp").alias("start_time"),
        col("x").alias("trip_start_lng"),
        col("y").alias("trip_start_lat")
    )
    
    df_end_loc = df_sorted.select(
        "vehicle", "route_id", "trip_id",
        col("timestamp").alias("end_time"),
        col("x").alias("trip_end_lng"),
        col("y").alias("trip_end_lat")
    )

    trip_detail = trip_summary \
        .join(df_start_loc, on=["vehicle", "route_id", "trip_id", "start_time"], how="left") \
        .join(df_end_loc, on=["vehicle", "route_id", "trip_id", "end_time"], how="left")

    # Join with Outbound/Inbound terminals (already has lat_A, lng_A, lat_B, lng_B)
    trip_detail = trip_detail.join(df_terminals, on="route_id", how="left")

    # Calculate mismatch distance to Outbound path logic and Inbound path logic
    trip_detail = trip_detail.withColumn("outbound_dist_err",
        haversine(col("trip_start_lat"), col("trip_start_lng"), col("lat_A"), col("lng_A")) + 
        haversine(col("trip_end_lat"), col("trip_end_lng"), col("lat_B"), col("lng_B"))
    )
    
    trip_detail = trip_detail.withColumn("inbound_dist_err",
        haversine(col("trip_start_lat"), col("trip_start_lng"), col("lat_B"), col("lng_B")) + 
        haversine(col("trip_end_lat"), col("trip_end_lng"), col("lat_A"), col("lng_A"))
    )

    # Classify circular routes and directions
    trip_detail = trip_detail.withColumn("is_circular",
        haversine(col("lat_A"), col("lng_A"), col("lat_B"), col("lng_B")) < 1.0
    )

    trip_detail = trip_detail.withColumn("direction",
        when(col("is_circular") == True, "CIRCULAR")
        .when(col("outbound_dist_err") < col("inbound_dist_err"), "OUTBOUND")
        .otherwise("INBOUND")
    )
    
    # Confidence could be driven by how small the distance error is
    trip_detail = trip_detail.withColumn("confidence_score",
        when(col("is_circular") == True, "HIGH")
        .when(least(col("outbound_dist_err"), col("inbound_dist_err")) < 2.0, "HIGH")
        .when(least(col("outbound_dist_err"), col("inbound_dist_err")) < 5.0, "MEDIUM")
        .otherwise("LOW")
    )
    
    final_cols = [
        "date", "vehicle", "route_id", "route_no", "trip_id", 
        "start_time", "end_time", "trip_duration_minutes", 
        "total_distance_km", "direction", "confidence_score"
    ]
    trip_detail = trip_detail.withColumn("date", to_date(col("start_time"))).select(final_cols)
    trip_detail = trip_detail.withColumn("updated_at", current_timestamp())

    # Create Gold table if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_gold")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_gold.trip_summary (
            date DATE,
            vehicle STRING,
            route_id INT,
            route_no STRING,
            trip_id INT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            trip_duration_minutes DOUBLE,
            total_distance_km DOUBLE,
            direction STRING,
            confidence_score STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
    """)

    # Idempotent overwrite partition
    trip_detail.select(
        "date", "vehicle", "route_id", "route_no", "trip_id", 
        "start_time", "end_time", "trip_duration_minutes", 
        "total_distance_km", "direction", "confidence_score", "updated_at"
    ).writeTo("catalog_iceberg.bus_gold.trip_summary").overwritePartitions()

    print("WRITE bus_gold.trip_summary SUCCESS")

if __name__ == "__main__":
    main()
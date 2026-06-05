import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, radians, sin, cos, asin, sqrt, row_number, element_at,
    lag, unix_timestamp, when, sum as spark_sum, min as spark_min,
    max as spark_max, count, round, least, to_date, lit, current_timestamp,
    broadcast
)

PROCESS_DATE = os.getenv("PROCESS_DATE", "2025-03-22")

def haversine(lat1, lng1, lat2, lng2):
    c_lat1 = col(lat1) if isinstance(lat1, str) else lit(lat1)
    c_lng1 = col(lng1) if isinstance(lng1, str) else lit(lng1)
    c_lat2 = col(lat2) if isinstance(lat2, str) else lit(lat2)
    c_lng2 = col(lng2) if isinstance(lng2, str) else lit(lng2)
    
    dlat = radians(c_lat2 - c_lat1)
    dlng = radians(c_lng2 - c_lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(c_lat1)) * cos(radians(c_lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c

def main():
    spark = SparkSession.builder.appName("GoldTripSummary").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Running Simplified Trip Segmentation for date: {PROCESS_DATE}")

    # 1. Read Waypoints for the target processing date
    df_bw = (
        spark.read.table("catalog_iceberg.bus_silver.bus_way_point")
        .filter(col("date") == lit(PROCESS_DATE))
        .dropDuplicates(["vehicle", "timestamp"])
    )
    df_bw = df_bw.withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # 2. Read Route Path and Extract Terminals
    df_route = spark.read.table("catalog_iceberg.bus_silver.route_path")
    
    # Take the first Route Path variation per RouteId (min RouteVarId) to define Terminal A and B
    w_route = Window.partitionBy("RouteId").orderBy("RouteVarId")
    df_route_min = df_route.withColumn("rn", row_number().over(w_route)).filter("rn = 1").drop("rn")
    
    # Convert path array to start and end coordinates
    df_route_min = df_route_min.select(
        col("RouteId").alias("route_id"),
        col("RouteNo").alias("route_no"),
        element_at(col("path"), 1).alias("start_pt"),
        element_at(col("path"), -1).alias("end_pt")
    )

    # Extract route coordinates for join
    df_route_coords = df_route_min.select(
        col("route_id").alias("r_id"),
        col("route_no").alias("r_no"),
        col("start_pt")[0].alias("r_lng_A"),
        col("start_pt")[1].alias("r_lat_A"),
        col("end_pt")[0].alias("r_lng_B"),
        col("end_pt")[1].alias("r_lat_B")
    ).withColumn("r_term_dist", haversine("r_lat_A", "r_lng_A", "r_lat_B", "r_lng_B"))

    # 3. Join waypoints directly with their route terminals (1-to-1 join based on pre-corrected route_id)
    df_bw_active = df_bw.join(broadcast(df_route_coords), df_bw.route_id == df_route_coords.r_id, "inner")
    
    # Calculate proximity to the route terminals
    df_bw_active = df_bw_active.withColumn("is_near_A", haversine("y", "x", "r_lat_A", "r_lng_A") <= 0.5) \
                               .withColumn("is_near_B", haversine("y", "x", "r_lat_B", "r_lng_B") <= 0.5) \
                               .withColumn("is_at_terminal", col("is_near_A") | col("is_near_B"))

    # 4. Detect transitions and split trips per vehicle
    w = Window.partitionBy("vehicle").orderBy("timestamp")
    w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    df_sorted = df_bw_active.withColumn("prev_timestamp", lag("timestamp").over(w)) \
                      .withColumn("prev_x", lag("x").over(w)) \
                      .withColumn("prev_y", lag("y").over(w)) \
                      .withColumn("prev_is_at_terminal", lag("is_at_terminal").over(w))
    
    df_sorted = df_sorted.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
    
    # A transition triggers a new trip segment:
    # - First point of the vehicle
    # - Telemetry gap > 20 mins (1200s)
    # - Leaving terminal (prev is_at_terminal was True, current is False)
    # - Entering terminal (prev is_at_terminal was False, current is True)
    df_sorted = df_sorted.withColumn("is_new_trip", 
        when(col("prev_timestamp").isNull(), 1)
        .when(col("time_diff") > 1200, 1)
        .when(col("prev_is_at_terminal") & ~col("is_at_terminal"), 1)
        .when(~col("prev_is_at_terminal") & col("is_at_terminal"), 1)
        .otherwise(0)
    )
    
    df_sorted = df_sorted.withColumn("segment_dist", 
        when(col("is_new_trip") == 1, 0.0)
        .when(col("prev_x").isNotNull() & col("prev_y").isNotNull(),
             haversine("y", "x", "prev_y", "prev_x")).otherwise(0.0)
    )

    df_sorted = df_sorted.withColumn("trip_id", spark_sum("is_new_trip").over(w_cum))
    
    # 5. Summarize Trips
    w_trip = Window.partitionBy("vehicle", "trip_id").orderBy("timestamp")
    df_sorted = df_sorted.withColumn("rn_trip_asc", row_number().over(w_trip))
    w_trip_desc = Window.partitionBy("vehicle", "trip_id").orderBy(col("timestamp").desc())
    df_sorted = df_sorted.withColumn("rn_trip_desc", row_number().over(w_trip_desc))

    # Collect trip starts and ends
    df_starts = df_sorted.filter("rn_trip_asc = 1").select(
        "vehicle", "trip_id", 
        col("x").alias("trip_start_lng"), 
        col("y").alias("trip_start_lat"),
        col("timestamp").alias("start_time")
    )
    df_ends = df_sorted.filter("rn_trip_desc = 1").select(
        "vehicle", "trip_id", 
        col("x").alias("trip_end_lng"), 
        col("y").alias("trip_end_lat"),
        col("timestamp").alias("end_time")
    )

    df_trips = df_sorted.groupBy("vehicle", "trip_id", "route_id", "route_no", "r_lat_A", "r_lng_A", "r_lat_B", "r_lng_B").agg(
        spark_sum("segment_dist").alias("total_distance_km"),
        count("*").alias("point_count"),
        round(spark_sum(when(~col("is_at_terminal"), 1).otherwise(0)) / count("*"), 2).alias("pct_outside")
    )

    df_trips = df_trips.join(df_starts, on=["vehicle", "trip_id"], how="inner") \
                       .join(df_ends, on=["vehicle", "trip_id"], how="inner")

    # Filter out layovers (pct_outside <= 0.5) and very short noise/depot parking (distance <= 1.5 km)
    df_trips = df_trips.filter("total_distance_km > 1.5 and pct_outside > 0.5")
    df_trips = df_trips.withColumn("trip_duration_minutes", round((unix_timestamp("end_time") - unix_timestamp("start_time")) / 60, 2))

    # 7. Direction and Confidence Classification
    df_trips = df_trips.withColumn("outbound_dist_err",
        haversine(col("trip_start_lat"), col("trip_start_lng"), col("r_lat_A"), col("r_lng_A")) + 
        haversine(col("trip_end_lat"), col("trip_end_lng"), col("r_lat_B"), col("r_lng_B"))
    ).withColumn("inbound_dist_err",
        haversine(col("trip_start_lat"), col("trip_start_lng"), col("r_lat_B"), col("r_lng_B")) + 
        haversine(col("trip_end_lat"), col("trip_end_lng"), col("r_lat_A"), col("r_lng_A"))
    )

    df_trips = df_trips.withColumn("is_circular",
        haversine(col("r_lat_A"), col("r_lng_A"), col("r_lat_B"), col("r_lng_B")) < 1.0
    )

    df_trips = df_trips.withColumn("direction",
        when(col("is_circular") == True, "CIRCULAR")
        .when(col("outbound_dist_err") < col("inbound_dist_err"), "OUTBOUND")
        .otherwise("INBOUND")
    )

    df_trips = df_trips.withColumn("err", 
        when(col("is_circular") == True, col("outbound_dist_err"))
        .otherwise(least(col("outbound_dist_err"), col("inbound_dist_err")))
    )

    df_trips = df_trips.withColumn("confidence_score",
        when(col("is_circular") == True, "HIGH")
        .when(col("err") < 2.0, "HIGH")
        .when(col("err") < 5.0, "MEDIUM")
        .otherwise("LOW")
    )

    trip_detail = df_trips.withColumn("date", to_date(col("start_time"))).select(
        col("date"),
        col("vehicle"),
        col("route_id"),
        col("route_no"),
        col("trip_id"),
        col("start_time"),
        col("end_time"),
        col("trip_duration_minutes"),
        col("total_distance_km"),
        col("direction"),
        col("confidence_score")
    )
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

    print(f"WRITE bus_gold.trip_summary SUCCESS for date {PROCESS_DATE}", flush=True)
    spark.stop()

if __name__ == "__main__":
    main()
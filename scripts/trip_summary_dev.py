from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

def haversine(lat1, lng1, lat2, lng2):
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c

def main():
    spark = SparkSession.builder.appName("TripSummaryDev").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read Waypoints for a single day
    df_bw = spark.read.table("catalog_iceberg.bus_silver.bus_way_point").filter("date = '2025-04-03'")
    df_bw = df_bw.withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # 2. Read Route Path and Extract Terminals
    df_route = spark.read.table("catalog_iceberg.bus_silver.route_path")
    
    # Take the first Route Path variation per RouteId and Outbound boolean
    w_route = Window.partitionBy("RouteId", "Outbound").orderBy("RouteVarId")
    df_route = df_route.withColumn("rn", row_number().over(w_route)).filter("rn = 1").drop("rn")
    
    # Convert path array to start and end coordinates
    df_terminals = df_route.select(
        col("RouteId").alias("route_id"),
        col("Outbound"),
        element_at(col("path"), 1).alias("start_pt"),
        element_at(col("path"), -1).alias("end_pt")
    ).select(
        "route_id",
        "Outbound",
        col("start_pt")[0].alias("start_x"),
        col("start_pt")[1].alias("start_y"),
        col("end_pt")[0].alias("end_x"),
        col("end_pt")[1].alias("end_y")
    )

    outbound_terminals = df_terminals.filter(col("Outbound") == True).select(
        "route_id",
        col("start_x").alias("out_start_lng"),
        col("start_y").alias("out_start_lat"),
        col("end_x").alias("out_end_lng"),
        col("end_y").alias("out_end_lat")
    )
    
    inbound_terminals = df_terminals.filter(col("Outbound") == False).select(
        "route_id",
        col("start_x").alias("in_start_lng"),
        col("start_y").alias("in_start_lat"),
        col("end_x").alias("in_end_lng"),
        col("end_y").alias("in_end_lat")
    )

    terminals = outbound_terminals.join(inbound_terminals, on="route_id", how="left")

    # 3. Calculate distance and split trips for waypoints
    w = Window.partitionBy("vehicle", "route_id").orderBy("timestamp")
    
    df_sorted = df_bw.withColumn("prev_timestamp", lag("timestamp").over(w)) \
                     .withColumn("prev_x", lag("x").over(w)) \
                     .withColumn("prev_y", lag("y").over(w))
    
    df_sorted = df_sorted.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
                     
    df_sorted = df_sorted.withColumn("segment_dist", 
        when(col("prev_x").isNotNull() & col("prev_y").isNotNull(),
             haversine(col("prev_y"), col("prev_x"), col("y"), col("x"))).otherwise(0.0)
    )

    # A gap of > 10 mins (600s) = new trip
    df_sorted = df_sorted.withColumn("is_new_trip", 
        when(col("time_diff") > 600, 1)
        .when(col("prev_timestamp").isNull(), 1)
        .otherwise(0)
    )
    
    df_sorted = df_sorted.withColumn("trip_id", sum("is_new_trip").over(w))
    
    # 4. Summarize Trips
    w_trip = Window.partitionBy("vehicle", "route_id", "trip_id")
    
    trip_summary = df_sorted.groupBy("vehicle", "route_id", "route_no", "trip_id").agg(
        min("timestamp").alias("start_time"),
        max("timestamp").alias("end_time"),
        sum("segment_dist").alias("total_distance_km"),
        count("*").alias("point_count")
    )
    
    # Filter out very short noise trips
    trip_summary = trip_summary.filter(col("total_distance_km") > 1.0)
    trip_summary = trip_summary.withColumn("trip_duration_minutes", round((unix_timestamp("end_time") - unix_timestamp("start_time")) / 60, 2))

    # To classify direction, get the first point and last point of the trip
    
    # Join with start/end time to get start and end locations
    df_start_loc = df_sorted.select("vehicle", "route_id", "trip_id", "timestamp", "x", "y")
    df_start_loc = df_start_loc.withColumnRenamed("x", "trip_start_lng").withColumnRenamed("y", "trip_start_lat")
    
    df_end_loc = df_sorted.select("vehicle", "route_id", "trip_id", "timestamp", "x", "y")
    df_end_loc = df_end_loc.withColumnRenamed("x", "trip_end_lng").withColumnRenamed("y", "trip_end_lat")

    trip_detail = trip_summary \
        .join(df_start_loc, (trip_summary.vehicle == df_start_loc.vehicle) & (trip_summary.route_id == df_start_loc.route_id) & (trip_summary.trip_id == df_start_loc.trip_id) & (trip_summary.start_time == df_start_loc.timestamp), "left") \
        .drop(df_start_loc.vehicle).drop(df_start_loc.route_id).drop(df_start_loc.trip_id).drop(df_start_loc.timestamp) \
        .join(df_end_loc, (trip_summary.vehicle == df_end_loc.vehicle) & (trip_summary.route_id == df_end_loc.route_id) & (trip_summary.trip_id == df_end_loc.trip_id) & (trip_summary.end_time == df_end_loc.timestamp), "left") \
        .drop(df_end_loc.vehicle).drop(df_end_loc.route_id).drop(df_end_loc.trip_id).drop(df_end_loc.timestamp)

    # Join with Outbound/Inbound terminals
    trip_detail = trip_detail.join(terminals, on="route_id", how="left")

    # Calculate mismatch distance to Outbound path logic and Inbound path logic
    trip_detail = trip_detail.withColumn("outbound_dist_err",
        haversine(col("trip_start_lat"), col("trip_start_lng"), col("out_start_lat"), col("out_start_lng")) + 
        haversine(col("trip_end_lat"), col("trip_end_lng"), col("out_end_lat"), col("out_end_lng"))
    )
    
    trip_detail = trip_detail.withColumn("inbound_dist_err",
        haversine(col("trip_start_lat"), col("trip_start_lng"), col("in_start_lat"), col("in_start_lng")) + 
        haversine(col("trip_end_lat"), col("trip_end_lng"), col("in_end_lat"), col("in_end_lng"))
    )

    trip_detail = trip_detail.withColumn("direction",
        when(col("outbound_dist_err") < col("inbound_dist_err"), "OUTBOUND")
        .otherwise("INBOUND")
    )
    
    # Confidence could be driven by how small the distance error is
    trip_detail = trip_detail.withColumn("confidence_score",
        when(least(col("outbound_dist_err"), col("inbound_dist_err")) < 5.0, "HIGH")
        .when(least(col("outbound_dist_err"), col("inbound_dist_err")) < 15.0, "MEDIUM")
        .otherwise("LOW")
    )
    
    final_cols = ["date", "vehicle", "route_id", "route_no", "trip_id", "start_time", "end_time", "trip_duration_minutes", "total_distance_km", "direction", "confidence_score"]
    trip_detail = trip_detail.withColumn("date", to_date(col("start_time"))).select(final_cols)
    
    trip_detail.show(20, truncate=False)

if __name__ == "__main__":
    main()

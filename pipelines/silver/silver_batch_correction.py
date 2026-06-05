import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, radians, sin, cos, asin, sqrt, row_number, min as spark_min,
    round, lit, coalesce, broadcast, when, element_at
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
    spark = SparkSession.builder.appName("SilverBatchCorrection").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"Running Daily Silver Batch Correction Job for date: {PROCESS_DATE}")

    # 1. Read Silver Waypoints for the target date
    df_bw = (
        spark.read.table("catalog_iceberg.bus_silver.bus_way_point")
        .filter(col("date") == lit(PROCESS_DATE))
    )

    # 2. Read Route Path (which has corrected Terminal A coordinates for Route 1)
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

    # 3. Dynamic Route Assignment: Identify the single true route for each vehicle
    # Group waypoints by rounded grid coordinates (3 decimal places, ~111m) to optimize cross join
    df_bw_grid = df_bw.select("vehicle", "route_id", round(col("x"), 3).alias("wx"), round(col("y"), 3).alias("wy")).distinct()
    df_bw_routes = df_bw_grid.crossJoin(broadcast(df_route_coords))
    df_bw_routes = df_bw_routes.withColumn("dist_A", haversine("wy", "wx", "r_lat_A", "r_lng_A")) \
                               .withColumn("dist_B", haversine("wy", "wx", "r_lat_B", "r_lng_B"))
    
    # Candidate routes that the vehicle got within 600m of both terminals
    df_candidates = df_bw_routes.groupBy("vehicle", "route_id", "r_id", "r_no", "r_term_dist").agg(
        spark_min("dist_A").alias("min_dist_A"),
        spark_min("dist_B").alias("min_dist_B")
    ).filter("min_dist_A <= 0.6 and min_dist_B <= 0.6")

    # Select best route for each vehicle:
    # 1. Statically assigned route (route_id == r_id) gets highest priority
    # 2. Otherwise, order by straight-line terminal-to-terminal distance (r_term_dist) descending
    w_rank = Window.partitionBy("vehicle").orderBy(
        when(col("route_id") == col("r_id"), 1).otherwise(0).desc(),
        col("r_term_dist").desc()
    )
    df_vehicle_route = df_candidates.withColumn("rank", row_number().over(w_rank)).filter("rank = 1").select(
        "vehicle", col("r_id").alias("true_route_id"), col("r_no").alias("true_route_no")
    )

    # 4. Correct the route_id and route_no in the original waypoints
    df_corrected = df_bw.join(df_vehicle_route, on="vehicle", how="left")
    df_final = df_corrected.withColumn("route_id", coalesce(col("true_route_id"), col("route_id"))) \
                           .withColumn("route_no", coalesce(col("true_route_no"), col("route_no")))

    # 5. Select columns in the exact Iceberg table order
    final_cols = [
        "vehicle", "route_id", "route_no", "driver", "timestamp", "date", "hour",
        "speed", "x", "y", "z", "heading", "ignition", "aircon", "door_up", "door_down",
        "sos", "working", "analog1", "analog2", "updated_at"
    ]
    df_final = df_final.select(final_cols)

    # 6. Overwrite the partition for PROCESS_DATE in Iceberg
    df_final.writeTo("catalog_iceberg.bus_silver.bus_way_point").overwritePartitions()

    print(f"Daily Silver Batch Correction Job completed successfully for date {PROCESS_DATE}", flush=True)
    
    spark.stop()

if __name__ == "__main__":
    main()

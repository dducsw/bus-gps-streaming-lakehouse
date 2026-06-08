from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, trim, split, explode, when, broadcast,
    round, abs, hour, dayofweek, last, current_timestamp,
    from_utc_timestamp
)

def main():
    spark = SparkSession.builder.appName("GoldGpsStatsOverview").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    bus_df = (
        spark.read.table("catalog_iceberg.bus_silver.bus_way_point")
        .filter(col("date").between("2025-03-21", "2025-04-06"))
        .withColumn("x_round", round(col("x"), 4))
        .withColumn("y_round", round(col("y"), 4))
    )

    # Lọc xe hợp lệ + lấy các tuyến được phép từ mapping
    df_mapping = (
        spark.read.table("catalog_iceberg.bus_bronze.vehicle_bus_mapping")
        .select("vehicle", col("route_no").alias("mapped_route_no"))
    )

    bus_df = bus_df.join(
        broadcast(df_mapping.select("vehicle").distinct()),
        on="vehicle", how="inner"
    )

    stop_df = spark.read.table("catalog_iceberg.bus_silver.route_stop")
    route_info_df = (
        spark.read.table("catalog_iceberg.bus_silver.route_info")
        .select(
            col("route_id").alias("RouteId"),
            col("route_var_id").alias("RouteVarId")
        )
        .dropDuplicates(["RouteId", "RouteVarId"])
    )

    stop_expand = (
        stop_df.alias("s")
        .join(broadcast(route_info_df).alias("r"),
            (col("s.route_id") == col("r.RouteId")) &
            (col("s.route_var_id") == col("r.RouteVarId")),
            how="left"
        )
        .withColumn("RouteNo", explode(split(col("s.routes"), ",")))
        .withColumn("RouteNo", trim(col("RouteNo")))
        .withColumn("Lng_round", round(col("s.lng"), 4))
        .withColumn("Lat_round", round(col("s.lat"), 4))
        .select("s.lng", "s.lat", "RouteNo", "Lng_round", "Lat_round")
    )

    # Left join: điểm dừng có RouteNo, giữa đường null
    df_with_stop = (
        bus_df.alias("b")
        .join(
            broadcast(stop_expand).alias("s"),
            (col("b.x_round") == col("s.Lng_round")) &
            (col("b.y_round") == col("s.Lat_round")),
            "left"
        )
        .filter(
            col("s.Lng_round").isNull() |
            (
                (abs(col("b.x") - col("s.lng")) < 0.0001) &
                (abs(col("b.y") - col("s.lat")) < 0.0001)
            )
        )
        .drop("x_round", "y_round", "Lng_round", "Lat_round", "lng", "lat")
        .withColumnRenamed("RouteNo", "route_no_detected")
    )

    # Forward-fill route_no theo vehicle + timestamp
    w = Window.partitionBy("vehicle").orderBy("timestamp").rowsBetween(Window.unboundedPreceding, 0)
    df = (
        df_with_stop
        .withColumn("route_no", last("route_no_detected", ignorenulls=True).over(w))
        .filter(col("route_no").isNotNull())
        .drop("route_no_detected")
    )

    # Validate: chỉ giữ route_no khớp với mapping
    df = (
        df.join(broadcast(df_mapping), on="vehicle", how="inner")
        .filter(col("route_no") == col("mapped_route_no"))
        .drop("mapped_route_no")
    )

    # Feature engineering
    df = df.withColumn("hour", hour(from_utc_timestamp(col("timestamp"), "Asia/Ho_Chi_Minh")))
    df = df.withColumn("day_of_week", dayofweek("date"))
    df = df.withColumn("day_type", when(col("day_of_week").isin([1, 7]), "Weekend").otherwise("Weekday"))
    df = df.withColumn("is_peak_hour", when(col("hour").isin([7, 8, 17, 18]), 1).otherwise(0))
    df = df.withColumn(
        "speed_level",
        when(col("speed") == 0, "stopped")
        .when(col("speed") < 20, "slow")
        .when(col("speed") < 60, "normal")
        .otherwise("fast")
    )
    df = df.withColumn("is_moving", when(col("speed") > 0, 1).otherwise(0))
    df = df.withColumn("is_stopped", when(col("speed") == 0, 1).otherwise(0))

    df.withColumn("updated_at", current_timestamp()).select(
        "vehicle", "route_no", "date", "hour",
        "day_of_week", "day_type", "is_peak_hour",
        "speed", "speed_level", "is_moving", "is_stopped",
        col("b.x").alias("x"),
        col("b.y").alias("y"),
        "updated_at"
    ).writeTo("catalog_iceberg.bus_gold.gps_stats_overview").createOrReplace()

    print("WRITE GOLD SUCCESS")

if __name__ == "__main__":
    main()
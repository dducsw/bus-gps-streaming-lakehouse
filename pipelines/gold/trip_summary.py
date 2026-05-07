from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, radians, sin, cos, sqrt, asin,
    row_number, when, sum as spark_sum, min as spark_min,
    max as spark_max, avg as spark_avg, count, lag, 
    unix_timestamp, to_date, element_at, md5, concat_ws, current_timestamp,
    broadcast
)

def haversine(lat1, lng1, lat2, lng2):
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c

def main():
    spark = SparkSession.builder.appName("GoldTripSummary_DepartureLogic").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Đọc dữ liệu Silver
    df_bw = spark.read.table("catalog_iceberg.bus_silver.bus_way_point").filter("date = '2025-04-03'")
    df_bw = df_bw.withColumn("timestamp", col("timestamp").cast("timestamp"))
    
    # 2. Lấy tọa độ bến A (đầu) và B (cuối)
    df_route = spark.read.table("catalog_iceberg.bus_silver.route_path")
    w_route = Window.partitionBy("RouteId", "Outbound").orderBy("RouteVarId")
    df_route = df_route.withColumn("rn", row_number().over(w_route)).filter("rn = 1").drop("rn")
    
    terminals = df_route.groupBy("RouteId").agg(
        spark_min(when(col("Outbound") == True, col("RouteVarId"))).alias("out_var_id"),
        spark_min(when(col("Outbound") == True, element_at(col("path"), 1)[0])).alias("lng_A"),
        spark_min(when(col("Outbound") == True, element_at(col("path"), 1)[1])).alias("lat_A"),
        spark_min(when(col("Outbound") == False, col("RouteVarId"))).alias("in_var_id"),
        spark_min(when(col("Outbound") == True, element_at(col("path"), -1)[0])).alias("lng_B"),
        spark_min(when(col("Outbound") == True, element_at(col("path"), -1)[1])).alias("lat_B")
    ).withColumnRenamed("RouteId", "route_id")

    # 3. Tính khoảng cách tới A và B
    df_dist = df_bw.join(broadcast(terminals), on="route_id", how="left")
    df_dist = df_dist.withColumn("d_A", haversine(col("y"), col("x"), col("lat_A"), col("lng_A")))
    df_dist = df_dist.withColumn("d_B", haversine(col("y"), col("x"), col("lat_B"), col("lng_B")))

    # Bán kính 300m để xác định đang ở trong bến
    df_dist = df_dist.withColumn("at_A", col("d_A") < 0.1)
    df_dist = df_dist.withColumn("at_B", col("d_B") < 0.1)
    df_dist = df_dist.withColumn("at_terminal", (col("at_A")) | (col("at_B")))

    # # 4. Window Function để so sánh điểm trước và điểm sau
    # w_veh = Window.partitionBy("vehicle", "route_id").orderBy("timestamp")
    
    # df_split = df_dist.withColumn("prev_at_A", lag("at_A").over(w_veh)) \
    #                   .withColumn("prev_at_B", lag("at_B").over(w_veh)) \
    #                   .withColumn("prev_at_terminal", lag("at_terminal").over(w_veh)) \
    #                   .withColumn("prev_timestamp", lag("timestamp").over(w_veh)) \
    #                   .withColumn("prev_x", lag("x").over(w_veh)) \
    #                   .withColumn("prev_y", lag("y").over(w_veh))

    # df_split = df_split.withColumn("time_diff", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
    # df_split = df_split.withColumn("seg_dist", 
    #     when(col("prev_x").isNotNull(), haversine(col("prev_y"), col("prev_x"), col("y"), col("x"))).otherwise(0.0))  
    # # LOGIC TÁCH CHUYẾN MỚI: TÁCH KHI RỜI BẾN
    # # Một chuyến mới bắt đầu khi xe rời khỏi bến A hoặc bến B sau khi đã ở trong đó
    # df_split = df_split.withColumn("is_new_trip", 
    #     when(col("prev_timestamp").isNull(), 1)
    #     .when(col("time_diff") > 600, 1) # Nghỉ lâu > 10p
    #     .when((col("at_terminal") == False) & (col("prev_at_terminal") == True), 1) # RỜI BẾN
    #     .otherwise(0)
    # )
    
    # df_split = df_split.withColumn("trip_temp_id", spark_sum("is_new_trip").over(w_veh))
    
    # set logic tách chuyến: nếu xe rời bến (từ at_terminal = True sang at_terminal = False) thì bắt đầu chuyến mới.
    


    # # 5. Gom nhóm theo Trip ID tạm thời
    # trip_summary = df_split.groupBy("vehicle", "route_id", "route_no", "trip_temp_id").agg(
    #     spark_min("timestamp").alias("start_time"),
    #     spark_max("timestamp").alias("end_time"),
    #     spark_sum("seg_dist").alias("distance_km"),
    #     spark_avg("speed").alias("avg_speed"),
    #     spark_max("speed").alias("max_speed"),
    #     count("*").alias("waypoint_count"),
    #     spark_sum(when(col("speed") < 2.0, 1).otherwise(0)).alias("stop_count"),
    #     # Xác định xe rời bến nào bằng cách lấy giá trị prev_at_A/B của điểm đầu tiên trong chuyến
    #     spark_min(when(col("is_new_trip") == 1, col("prev_at_A"))).alias("left_from_A"),
    #     spark_min(when(col("is_new_trip") == 1, col("prev_at_B"))).alias("left_from_B"),
    #     # Dự phòng: nếu không có prev (đầu ngày), dùng khoảng cách thực tế
    #     spark_min("d_A").alias("start_d_A"),
    #     spark_min("d_B").alias("start_d_B"),
    #     spark_min("out_var_id").alias("out_var_id"),
    #     spark_min("in_var_id").alias("in_var_id")
    # )

    # # 6. Xác định hướng (Direction)
    # # Nếu rời bến A -> OUTBOUND. Nếu rời bến B -> INBOUND. 
    # # Nếu là chuyến đầu ngày (isNull), so sánh khoảng cách tới A và B.
    # trip_summary = trip_summary.withColumn("direction", 
    #     when(col("left_from_A") == True, "OUTBOUND")
    #     .when(col("left_from_B") == True, "INBOUND")
    #     .when(col("start_d_A") < col("start_d_B"), "OUTBOUND")
    #     .otherwise("INBOUND")
    # )
    
    # trip_summary = trip_summary.withColumn("route_var_id", 
    #     when(col("direction") == "OUTBOUND", col("out_var_id")).otherwise(col("in_var_id")).cast("int"))

    # trip_summary = trip_summary.withColumn("duration_minutes", 
    #     ((unix_timestamp("end_time") - unix_timestamp("start_time")) / 60).cast("int"))

    # # Lọc bỏ "chuyến rác" (xe nổ máy tại chỗ trong bến hoặc chạy quãng ngắn < 5km)
    # df_final_filtered = trip_summary.filter(col("distance_km") > 5.0)

    # =========================================================================
    # 4. TRIP SEGMENTATION (ROBUST VERSION) 
    # =========================================================================
    w_veh = Window.partitionBy("vehicle", "route_id").orderBy("timestamp")
    df_split = df_dist.select(
    "*",
    lag("at_A").over(w_veh).alias("prev_at_A"),
    lag("at_B").over(w_veh).alias("prev_at_B"),
    lag("at_terminal").over(w_veh).alias("prev_at_terminal"),
    lag("timestamp").over(w_veh).alias("prev_timestamp"),
    lag("x").over(w_veh).alias("prev_x"),
    lag("y").over(w_veh).alias("prev_y"),
    lag("speed").over(w_veh).alias("prev_speed")
    )
    # Time & distance
    df_split = df_split.withColumn(
        "time_diff",
        unix_timestamp("timestamp") - unix_timestamp("prev_timestamp")
    )

    df_split = df_split.withColumn(
        "seg_dist",
        when(col("prev_x").isNotNull(),
            haversine(col("prev_y"), col("prev_x"), col("y"), col("x"))
        ).otherwise(0.0)
    )
    # =========================================================================
    # 🔹 LOGIC TÁCH CHUYẾN (Đã fix Nghịch lý)
    # =========================================================================
    df_split = df_split.withColumn(
        "is_new_trip",
        when(col("prev_timestamp").isNull(), 1)
        .when(col("time_diff") > 600, 1) # Mất sóng hoặc nghỉ quá 10 phút
        .when(
            (col("at_terminal") == False) & 
            (col("prev_at_terminal") == True) & 
            (col("speed") > 3) # Chống nhiễu: Phải chạy > 3km/h mới tính là rời bến thực sự
        , 1)
        .otherwise(0)
    )
    df_split = df_split.withColumn("trip_temp_id", spark_sum("is_new_trip").over(w_veh))
    # =========================================================================
    # 5. TRIP SUMMARY (CLEAN METRICS)
    # =========================================================================
    trip_summary = df_split.groupBy("vehicle", "route_id", "route_no", "trip_temp_id").agg(

        spark_min("timestamp").alias("start_time"),

        # end_time: điểm cuối KHÔNG nằm trong bến
        spark_max(when(col("at_terminal") == False, col("timestamp"))).alias("end_time"),

        # distance: chỉ tính khi xe chạy ngoài bến + lọc GPS jump
        spark_sum(
            when((col("at_terminal") == False) & (col("seg_dist") < 0.5), col("seg_dist"))
            .otherwise(0.0)
        ).alias("distance_km"),

        spark_max("speed").alias("max_speed"),

        count("*").alias("waypoint_count"),

        # 🚀 stop event (không đếm duplicate)
        spark_sum(
            when((col("speed") < 2) & (col("prev_speed") >= 2), 1).otherwise(0)
        ).alias("stop_count"),

        spark_min(when(col("is_new_trip") == 1, col("prev_at_A"))).alias("left_from_A"),
        spark_min(when(col("is_new_trip") == 1, col("prev_at_B"))).alias("left_from_B"),

        spark_min("d_A").alias("start_d_A"),
        spark_min("d_B").alias("start_d_B"),

        spark_min("out_var_id").alias("out_var_id"),
        spark_min("in_var_id").alias("in_var_id")
    )

    # fix null end_time
    trip_summary = trip_summary.withColumn(
        "end_time",
        when(col("end_time").isNull(), col("start_time")).otherwise(col("end_time"))
    )
    # =========================================================================
    # 6. DIRECTION + METRICS
    # =========================================================================

    # direction logic (fallback thông minh hơn)
    trip_summary = trip_summary.withColumn(
        "direction",
        when(col("left_from_A") == True, "OUTBOUND")
        .when(col("left_from_B") == True, "INBOUND")
        .when(col("start_d_A") < col("start_d_B"), "OUTBOUND")
        .otherwise("INBOUND")
    )

    trip_summary = trip_summary.withColumn(
        "route_var_id",
        when(col("direction") == "OUTBOUND", col("out_var_id"))
        .otherwise(col("in_var_id")).cast("int")
    )

    # duration
    trip_summary = trip_summary.withColumn(
        "duration_minutes",
        ((unix_timestamp("end_time") - unix_timestamp("start_time")) / 60).cast("int")
    )

    # avg_speed chuẩn vật lý
    trip_summary = trip_summary.withColumn(
        "avg_speed",
        when(col("duration_minutes") > 0,
            col("distance_km") / (col("duration_minutes") / 60)
        ).otherwise(0.0)
    )
    df_final_filtered = trip_summary.filter((col("distance_km") > 10.0) & (col("duration_minutes") > 10))
    #Metadata & Schema matching
    df_final_filtered = df_final_filtered.withColumn("is_completed", col("distance_km") > 12.0)
    df_final_filtered = df_final_filtered.withColumn("trip_id", md5(concat_ws("_", col("vehicle"), col("start_time").cast("string"))))
    df_final_filtered = df_final_filtered.withColumn("date", to_date(col("start_time")))
    df_final_filtered = df_final_filtered.withColumn("updated_at", current_timestamp())

    final_cols = [
        "trip_id", "vehicle", "date", "route_id", "route_no", "route_var_id", "direction",
        "start_time", "end_time", "duration_minutes", "avg_speed", "max_speed", "distance_km",
        "waypoint_count", "stop_count", "is_completed", "updated_at"
    ]
    
    df_final = df_final_filtered.select(*final_cols)
    
    print(f"📊 Kết quả: Đã tách thành {df_final.count()} chuyến đi.")

    df_final.writeTo("catalog_iceberg.bus_gold.trip_summary") \
        .partitionedBy("date", "route_id") \
        .overwritePartitions()

if __name__ == "__main__":
    main()


#Kha##
##at_terminal = True nghĩa là xe đang ở trong bến sau đó chuyển sang at_terminal = False và speed > 3km/h thì mới tính là rời bến thực sự. Điều này giúp tránh việc tách chuyến giả do GPS nhảy hoặc xe đứng yên trong bến nhưng tín hiệu GPS không ổn định.

import os
# docker exec -it spark-master /opt/spark/apps/pipelines/silver/silver_route_terminal_density.py

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    max as spark_max,
    lag,
    lit,
    min as spark_min,
    radians,
    row_number,
    sin,
    cos,
    sqrt,
    asin,
    sum as spark_sum,
    unix_timestamp,
    when,
)

# ================== CONFIG ==================
APP_NAME = "SilverRouteTerminalDensity"
SOURCE_TABLE = "catalog_iceberg.bus_silver.bus_way_point"
TARGET_TABLE = "catalog_iceberg.bus_silver.route_terminal_density"
PROCESS_DATE = os.getenv("PROCESS_DATE", "2025-03-20")

STOP_SPEED_KMH = 0.0 
MAX_STOP_GAP_SECONDS = 300
MIN_STOP_DURATION_SECONDS = 600
NEIGHBOR_RADIUS_KM = 0.25
TOP_DENSE_CANDIDATES = 8


def haversine(lat1, lng1, lat2, lng2):
    dlat = radians(lat2 - lat1)
    dlng = radians(lng2 - lng1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlng / 2) ** 2
    c = 2 * asin(sqrt(a))
    return 6371 * c


def create_spark_session():
    return SparkSession.builder.appName(APP_NAME).getOrCreate()


def build_long_stop_sessions(df_bw):
    w = Window.partitionBy("route_id", "vehicle").orderBy("timestamp")
    w_cum = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_seq = (
        df_bw.select("route_id", "vehicle", "timestamp", "x", "y", "speed")
        .filter(
            col("route_id").isNotNull()
            & col("vehicle").isNotNull()
            & col("timestamp").isNotNull()
            & col("x").isNotNull()
            & col("y").isNotNull()
        )
        .withColumn("timestamp", col("timestamp").cast("timestamp"))
        .withColumn("is_stop", ((col("speed") == lit(STOP_SPEED_KMH)) | col("speed").isNull()).cast("int"))
        .withColumn("prev_timestamp", lag("timestamp").over(w))
        .withColumn("prev_is_stop", lag("is_stop").over(w))
        .withColumn("time_gap_seconds", unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"))
    )
    
    # Debug: check stop points
    stop_points = df_seq.filter(col("is_stop") == 1).count()
    
    # Debug: check speed distribution
    speed_stats = df_seq.groupBy("speed").count().orderBy("count").collect()

    df_seq = df_seq.withColumn(
        "new_stop_session",
        when(
            (col("is_stop") == 1)
            & (
                col("prev_timestamp").isNull()
                | (col("prev_is_stop") != 1)
                | (col("time_gap_seconds") > lit(MAX_STOP_GAP_SECONDS))
            ),
            1,
        ).otherwise(0),
    )

    df_stop = (
        df_seq.withColumn("stop_session_id", spark_sum("new_stop_session").over(w_cum))
        .filter(col("is_stop") == 1)
        .groupBy("route_id", "vehicle", "stop_session_id")
        .agg(
            avg("x").alias("stop_lat"),
            avg("y").alias("stop_lng"),
            count("*").alias("stop_points"),
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time"),
        )
        .withColumn("duration_seconds", unix_timestamp("end_time") - unix_timestamp("start_time"))
    )
    return df_stop.filter(col("duration_seconds") >= lit(MIN_STOP_DURATION_SECONDS))


def detect_terminal_candidates(df_long_stops):
    w_stop_id = Window.partitionBy("route_id").orderBy("vehicle", "stop_session_id")

    df_points = df_long_stops.withColumn("stop_id", row_number().over(w_stop_id)).cache()

    a = df_points.alias("a")
    b = df_points.alias("b")

    neighbors = (
        a.join(
            b,
            (col("a.route_id") == col("b.route_id"))
            & (
                haversine(
                    col("a.stop_lat"),
                    col("a.stop_lng"),
                    col("b.stop_lat"),
                    col("b.stop_lng"),
                )
                <= lit(NEIGHBOR_RADIUS_KM)
            ),
            "inner",
        )
        .select(
            col("a.route_id").alias("route_id"),
            col("a.stop_id").alias("anchor_id"),
            col("b.stop_lng").alias("neighbor_lng"),
            col("b.stop_lat").alias("neighbor_lat"),
        )
    )

    clusters = neighbors.groupBy("route_id", "anchor_id").agg(
        count("*").alias("density"),
        avg("neighbor_lng").alias("cluster_lng"),
        avg("neighbor_lat").alias("cluster_lat"),
    )

    w_dense = Window.partitionBy("route_id").orderBy(col("density").desc(), col("anchor_id").asc())
    return clusters.withColumn("dense_rank", row_number().over(w_dense)).filter(
        col("dense_rank") <= lit(TOP_DENSE_CANDIDATES)
    )


def pick_terminal_pair(df_candidates):
    left = df_candidates.alias("l")
    right = df_candidates.alias("r")

    pairs = (
        left.join(
            right,
            (col("l.route_id") == col("r.route_id")) & (col("l.anchor_id") < col("r.anchor_id")),
            "inner",
        )
        .select(
            col("l.route_id").alias("route_id"),
            col("l.cluster_lng").alias("lng_t1"),
            col("l.cluster_lat").alias("lat_t1"),
            col("l.density").alias("density_t1"),
            col("r.cluster_lng").alias("lng_t2"),
            col("r.cluster_lat").alias("lat_t2"),
            col("r.density").alias("density_t2"),
        )
        .withColumn(
            "inter_terminal_km",
            haversine(col("lat_t1"), col("lng_t1"), col("lat_t2"), col("lng_t2")),
        )
    )

    w_pair = Window.partitionBy("route_id").orderBy(
        col("inter_terminal_km").desc(),
        (col("density_t1") + col("density_t2")).desc(),
    )

    return (
        pairs.withColumn("pair_rank", row_number().over(w_pair))
        .filter(col("pair_rank") == 1)
        .select(
            "route_id",
            col("lng_t1").alias("lng_A"),
            col("lat_t1").alias("lat_A"),
            col("density_t1").alias("density_A"),
            col("lng_t2").alias("lng_B"),
            col("lat_t2").alias("lat_B"),
            col("density_t2").alias("density_B"),
            "inter_terminal_km",
        )
        .withColumn("method", lit("density-neighborhood"))
        .withColumn("updated_at", current_timestamp())
    )


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df_bw = spark.read.table(SOURCE_TABLE).filter(col("date") == lit(PROCESS_DATE))
    df_long_stops = build_long_stop_sessions(df_bw)


    df_candidates = detect_terminal_candidates(df_long_stops)


    df_terminals = pick_terminal_pair(df_candidates)

    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_terminal_density (
            route_id INT,
            lng_A DOUBLE,
            lat_A DOUBLE,
            density_A BIGINT,
            lng_B DOUBLE,
            lat_B DOUBLE,
            density_B BIGINT,
            inter_terminal_km DOUBLE,
            method STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (route_id)
        """
    )

    # Debug: show schema before write
    df_terminals.printSchema()

    try:
        print(f"[DEBUG] Writing to {TARGET_TABLE}...")
        df_terminals.writeTo(TARGET_TABLE).overwritePartitions()
    except Exception as e:
        print(f"[ERROR] Failed to write: {str(e)}")
        print(f"[DEBUG] Trying alternative write method...")
        try:
            df_terminals.write.mode("overwrite").option("merge-on-read", "true").saveAsTable(TARGET_TABLE)
            print("[SUCCESS] Data written using alternative method")
        except Exception as e2:
            print(f"[ERROR] Alternative write also failed: {str(e2)}")

    try:
        result = spark.sql(f"SELECT COUNT(*) as row_count FROM {TARGET_TABLE}").collect()
        written_count = result[0]["row_count"]
        print(f"[VERIFY] Records in {TARGET_TABLE}: {written_count}")
    except Exception as e:
        print(f"[ERROR] Failed to verify: {str(e)}")


if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    cos,
    coalesce,
    current_timestamp,
    lag,
    lit,
    pi,
    sin,
    sqrt,
    unix_timestamp,
    when,
)
from pyspark.sql.window import Window


SOURCE_TABLE  = "catalog_iceberg.bus_silver.bus_way_point"
MAPPING_TABLE = "catalog_iceberg.bus_bronze.vehicle_bus_mapping"
TARGET_TABLE  = "catalog_iceberg.bus_gold.jepa_features"

# Maximum allowed time gap between consecutive pings (seconds).
# Mirrors the notebook's `WHERE delta_time > 0` filter extended to drop
# large GPS blackout gaps that would corrupt delta features.
MAX_DELTA_TIME_SEC = 300


def main():
    spark = SparkSession.builder.appName("GoldJepaFeatures").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # -------------------------------------------------------------------------
    # 1. Read Silver GPS (already Kalman-filtered) + route mapping
    # -------------------------------------------------------------------------
    df_silver = spark.read.table(SOURCE_TABLE)

    df_mapping = (
        spark.read.table(MAPPING_TABLE)
        .select("vehicle", col("route_id").alias("mapped_route_id"))
        .dropDuplicates(["vehicle"])
    )

    # Left join: vehicles without a mapping keep their existing route_id from silver.
    # The notebook uses route_id from vehicle_route_mapping.csv with the same logic.
    df = df_silver.join(df_mapping, on="vehicle", how="left").withColumn(
        "route_id",
        coalesce(col("mapped_route_id"), col("route_id")),
    ).drop("mapped_route_id")

    # -------------------------------------------------------------------------
    # 2. Physics Delta Features  (mirrors DuckDB LAG window in notebook cell 3/5)
    #    Window: PARTITION BY vehicle ORDER BY timestamp ASC
    # -------------------------------------------------------------------------
    w = Window.partitionBy("vehicle").orderBy("timestamp")

    df_lagged = (
        df
        .withColumn("prev_x",         lag("x").over(w))
        .withColumn("prev_y",         lag("y").over(w))
        .withColumn("prev_speed",     lag("speed").over(w))
        .withColumn("prev_heading",   lag("heading").over(w))
        .withColumn("prev_timestamp", lag("timestamp").over(w))
    )

    # delta_time in seconds  (notebook: datetime - LAG(datetime))
    df_delta = df_lagged.withColumn(
        "delta_time",
        unix_timestamp("timestamp") - unix_timestamp("prev_timestamp"),
    )

    # delta_x, delta_y  (notebook: x - LAG(x), y - LAG(y))
    df_delta = (
        df_delta
        .withColumn("delta_x", col("x") - col("prev_x"))
        .withColumn("delta_y", col("y") - col("prev_y"))
    )

    # acceleration = delta_speed / delta_time  (notebook: delta_speed / NULLIF(delta_time, 0))
    df_delta = df_delta.withColumn(
        "acceleration",
        coalesce(
            (col("speed") - col("prev_speed")) / when(col("delta_time") > 0, col("delta_time")),
            lit(0.0),
        ),
    )

    # delta_heading (notebook: COALESCE(delta_heading, 0.0))
    df_delta = df_delta.withColumn(
        "delta_heading",
        coalesce(col("heading") - col("prev_heading"), lit(0.0)),
    )

    # Time_Sin / Time_Cos — Fourier encoding of time-of-day
    # Notebook: SIN(2*PI() * MOD(datetime_unix, 86400) / 86400)
    # We derive seconds-since-midnight from the timestamp column.
    seconds_in_day = lit(86400.0)
    seconds_since_midnight = (
        unix_timestamp("timestamp") % seconds_in_day.cast("long")
    ).cast("double")

    df_delta = (
        df_delta
        .withColumn("Time_Sin", sin(lit(2.0) * pi() * seconds_since_midnight / seconds_in_day))
        .withColumn("Time_Cos", cos(lit(2.0) * pi() * seconds_since_midnight / seconds_in_day))
    )

    # -------------------------------------------------------------------------
    # 3. Filter  (notebook: WHERE delta_time > 0)
    #    Drop the first ping per vehicle (delta_time is null) and large gaps.
    # -------------------------------------------------------------------------
    df_features = (
        df_delta
        .filter(col("delta_time") > 0)
        .filter(col("delta_time") <= MAX_DELTA_TIME_SEC)
        .drop("prev_x", "prev_y", "prev_speed", "prev_heading", "prev_timestamp")
        .withColumn("updated_at", current_timestamp())
    )

    # -------------------------------------------------------------------------
    # 4. Create table and write (same pattern as vehicle_daily_stats.py)
    # -------------------------------------------------------------------------
    spark.sql("CREATE NAMESPACE IF NOT EXISTS catalog_iceberg.bus_gold")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_gold.jepa_features (
            vehicle        STRING,
            route_id       INT,
            route_no       STRING,
            timestamp      TIMESTAMP,
            date           DATE,
            hour           INT,
            -- Raw kinematics (kept for reference / debugging)
            x              DOUBLE,
            y              DOUBLE,
            speed          DOUBLE,
            heading        FLOAT,
            door_up        BOOLEAN,
            door_down      BOOLEAN,
            working        BOOLEAN,
            -- Physics delta features (used by BusStreamingDataset)
            delta_time     DOUBLE,
            delta_x        DOUBLE,
            delta_y        DOUBLE,
            acceleration   DOUBLE,
            delta_heading  DOUBLE,
            Time_Sin       DOUBLE,
            Time_Cos       DOUBLE,
            updated_at     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (date)
    """)

    df_features.select(
        "vehicle",
        "route_id",
        "route_no",
        "timestamp",
        "date",
        "hour",
        "x",
        "y",
        "speed",
        "heading",
        "door_up",
        "door_down",
        "working",
        "delta_time",
        "delta_x",
        "delta_y",
        "acceleration",
        "delta_heading",
        "Time_Sin",
        "Time_Cos",
        "updated_at",
    ).writeTo(TARGET_TABLE).overwritePartitions()

    print("WRITE bus_gold.jepa_features SUCCESS")


if __name__ == "__main__":
    main()

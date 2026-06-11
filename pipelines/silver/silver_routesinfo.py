from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, current_timestamp

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverRouteInfo")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table("catalog_iceberg.bus_bronze.route_info")

    df_clean = (
        df
        .select(
            col("RouteId").cast("int").alias("route_id"),
            col("RouteNo").cast("string").alias("route_no"),
            col("RouteVarId").cast("int").alias("route_var_id"),
            col("RouteVarName").cast("string").alias("route_var_name"),
            col("Outbound").cast("boolean").alias("outbound"),
            col("Distance").cast("double").alias("distance_km"),
            col("RunningTime").cast("int").alias("running_time"),
            col("StartStop").cast("string").alias("start_stop"),
            col("EndStop").cast("string").alias("end_stop")
        )
        .withColumn(
            "direction",
            when(col("outbound") == True, "Outbound")
            .otherwise("Inbound")
        )
        .withColumn("updated_at", current_timestamp())
        .dropDuplicates(["route_id", "route_var_id"])
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_info (
            route_id INT,
            route_no STRING,
            route_var_id INT,
            route_var_name STRING,
            outbound BOOLEAN,
            direction STRING,
            distance_km DOUBLE,
            running_time INT,
            start_stop STRING,
            end_stop STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
    """)

    df_clean.createOrReplaceTempView("source_route_info")

    spark.sql("""
        MERGE INTO catalog_iceberg.bus_silver.route_info t
        USING source_route_info s
        ON t.route_id = s.route_id AND t.route_var_id = s.route_var_id
        WHEN MATCHED THEN UPDATE SET 
            t.route_no = s.route_no,
            t.route_var_name = s.route_var_name,
            t.outbound = s.outbound,
            t.direction = s.direction,
            t.distance_km = s.distance_km,
            t.running_time = s.running_time,
            t.start_stop = s.start_stop,
            t.end_stop = s.end_stop,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("MERGE route_info SUCCESS")

if __name__ == "__main__":
    main()
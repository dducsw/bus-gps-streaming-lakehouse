from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverRouteStop")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table("catalog_iceberg.bus_bronze.route_stop")

    df_clean = (
        df.select(
            col("RouteId").cast("int").alias("route_id"),
            col("RouteVarId").cast("int").alias("route_var_id"),
            col("StopId").cast("int").alias("stop_id"),
            col("Code").cast("string").alias("code"),
            col("Name").cast("string").alias("stop_name"),
            col("StopType").cast("string").alias("stop_type"),
            col("Zone").cast("string").alias("zone"),
            col("Ward").cast("string").alias("ward"),
            col("AddressNo").cast("string").alias("address_no"),
            col("Street").cast("string").alias("street"),
            col("Lng").cast("double").alias("lng"),
            col("Lat").cast("double").alias("lat"),
            col("Routes").cast("string").alias("routes")
        )
        .withColumn("updated_at", current_timestamp())
        .dropDuplicates(["route_id", "route_var_id", "stop_id"])
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_stop (
            route_id INT,
            route_var_id INT,
            stop_id INT,
            code STRING,
            stop_name STRING,
            stop_type STRING,
            zone STRING,
            ward STRING,
            address_no STRING,
            street STRING,
            lng DOUBLE,
            lat DOUBLE,
            routes STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
    """)

    df_clean.createOrReplaceTempView("source_route_stop")

    spark.sql("""
        MERGE INTO catalog_iceberg.bus_silver.route_stop t
        USING source_route_stop s
        ON t.route_id = s.route_id AND t.route_var_id = s.route_var_id AND t.stop_id = s.stop_id
        WHEN MATCHED THEN UPDATE SET 
            t.code = s.code,
            t.stop_name = s.stop_name,
            t.stop_type = s.stop_type,
            t.zone = s.zone,
            t.ward = s.ward,
            t.address_no = s.address_no,
            t.street = s.street,
            t.lng = s.lng,
            t.lat = s.lat,
            t.routes = s.routes,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("MERGE route_stop SUCCESS")

if __name__ == "__main__":
    main()
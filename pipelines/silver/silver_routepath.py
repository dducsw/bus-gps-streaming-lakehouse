from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_timestamp

def create_spark_session():
    return (
        SparkSession.builder
        .appName("SilverRoutePath")
        .getOrCreate()
    )

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.table("catalog_iceberg.bus_bronze.route_path")

    df_clean = (
        df.select(
            col("RouteId").cast("int"),
            col("RouteNo").cast("string"),
            col("RouteVarId").cast("int"),
            col("RouteVarName").cast("string"),
            col("Outbound").cast("boolean"),
            expr("""
                CASE 
                    WHEN RouteId = 1 AND RouteVarId = 1 THEN concat(array(array(106.6983, 10.7716)), slice(Path, 2, size(Path)))
                    WHEN RouteId = 1 AND RouteVarId = 2 THEN concat(slice(Path, 1, size(Path) - 1), array(array(106.6983, 10.7716)))
                    ELSE Path
                END
            """).alias("path")
        )
        .dropDuplicates(["RouteId", "RouteVarId", "Outbound"])
        .withColumn("updated_at", current_timestamp())
    )

    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_iceberg.bus_silver.route_path (
            RouteId INT,
            RouteNo STRING,
            RouteVarId INT,
            RouteVarName STRING,
            Outbound BOOLEAN,
            path ARRAY<ARRAY<DOUBLE>>,
            updated_at TIMESTAMP
        )
        USING iceberg
    """)

    df_clean.createOrReplaceTempView("source_route_path")

    spark.sql("""
        MERGE INTO catalog_iceberg.bus_silver.route_path t
        USING source_route_path s
        ON t.RouteId = s.RouteId AND t.RouteVarId = s.RouteVarId AND t.Outbound = s.Outbound
        WHEN MATCHED THEN UPDATE SET 
            t.RouteNo = s.RouteNo,
            t.RouteVarName = s.RouteVarName,
            t.path = s.path,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print("MERGE route_path SILVER SUCCESS")

if __name__ == "__main__":
    main()
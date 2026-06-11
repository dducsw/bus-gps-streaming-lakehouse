from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("QueryRoute50").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- Route Info for Route 50 ---")
    try:
        df_info = spark.read.table("catalog_iceberg.bus_silver.route_info").filter("route_no = '50'")
        df_info.show(20, truncate=False)
    except Exception as e:
        print(f"Error reading route_info: {e}")

    print("\n--- Route Path for Route 50 ---")
    try:
        df_path = spark.read.table("catalog_iceberg.bus_silver.route_path").filter("RouteNo = '50'")
        # Let's show route path info and start/end coordinates of route 50
        from pyspark.sql.functions import element_at
        df_terminals = df_path.select(
            "RouteId", "RouteVarId", "Outbound",
            element_at("path", 1).alias("start_pt"),
            element_at("path", -1).alias("end_pt")
        )
        df_terminals.show(20, truncate=False)
    except Exception as e:
        print(f"Error reading route_path: {e}")

    spark.stop()

if __name__ == "__main__":
    main()

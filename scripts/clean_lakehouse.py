from pyspark.sql import SparkSession

TABLES = [
    "catalog_iceberg.bus_bronze.bus_way_point",
    "catalog_iceberg.bus_bronze.vehicle_bus_mapping",
    "catalog_iceberg.bus_bronze.route_path",
    "catalog_iceberg.bus_bronze.route_info",
    "catalog_iceberg.bus_bronze.route_stop",
    "catalog_iceberg.bus_bronze.vehicle",
    "catalog_iceberg.bus_silver.bus_way_point",
    "catalog_iceberg.bus_silver.route_path",
    "catalog_iceberg.bus_silver.route_info",
    "catalog_iceberg.bus_silver.route_stop",
    "catalog_iceberg.bus_silver.route_terminal_density",
    "catalog_iceberg.bus_gold.trip_summary",
    "catalog_iceberg.bus_gold.gold_bus_dashboard",
    "catalog_iceberg.bus_gold.vehicle_daily_stats",
    "catalog_iceberg.bus_gold.vehicle_latest_status",
    "catalog_iceberg.bus_gold.jepa_features"
]

def main():
    spark = SparkSession.builder.appName("CleanLakehouseTables").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- DROPPING ICEBERG TABLES ---")
    for table in TABLES:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"Dropped table: {table}")
        except Exception as e:
            print(f"Failed to drop table {table}: {e}")

    print("\nLakehouse drop tables complete!")
    spark.stop()

if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession
import sys

def apply_optimized_sort_order(spark: SparkSession, table_name: str):
    print(f"--- Applying Optimized Sort Order for: {table_name} ---")
    
    # 1. Set Table Property
    print("Setting table sort order: vehicle, timestamp")
    spark.sql(f"ALTER TABLE {table_name} WRITE ORDERED BY vehicle, timestamp")
    
    # 2. Re-organize EXISTING data
    print("Executing rewrite_data_files with SORT strategy...")
    spark.sql(f"""
        CALL catalog_iceberg.system.rewrite_data_files(
            table => '{table_name}',
            strategy => 'sort',
            sort_order => 'vehicle ASC NULLS FIRST, timestamp ASC NULLS FIRST'
        )
    """)
    
    print("--- Optimized Sort Order Applied! ---")
    sys.stdout.flush()

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("IcebergSortOrderOptimization")
        .config("spark.sql.catalog.catalog_iceberg.type", "rest")
        .config("spark.sql.catalog.catalog_iceberg.uri", "http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg")
        .getOrCreate()
    )
    
    target_table = sys.argv[1] if len(sys.argv) > 1 else "catalog_iceberg.bus_bronze.buswaypoint_test"
    apply_optimized_sort_order(spark, target_table)

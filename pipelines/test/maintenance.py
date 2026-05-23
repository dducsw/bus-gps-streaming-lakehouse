from pyspark.sql import SparkSession
import sys

def run_full_maintenance(spark: SparkSession, table_name: str):
    print(f"--- Starting Full Maintenance for: {table_name} ---")
    
    # Configuration matches pipelines/maintenance/iceberg_maintenance.py
    MIN_SIZE = 3 * 1024 * 1024       # 3MB
    TARGET_SIZE = 64 * 1024 * 1024   # 64MB
    MAX_SIZE = 128 * 1024 * 1024     # 128MB

    print(f"Step 1: Rewriting data files (Compaction to {TARGET_SIZE/1024/1024}MB)...")
    spark.sql(f"""
        CALL catalog_iceberg.system.rewrite_data_files(
            table => '{table_name}',
            options => map(
                'min-file-size-bytes', '{MIN_SIZE}',
                'target-file-size-bytes', '{TARGET_SIZE}',
                'max-file-size-bytes', '{MAX_SIZE}'
            )
        )
    """)
    
    print("Step 2: Rewriting manifest files...")
    spark.sql(f"CALL catalog_iceberg.system.rewrite_manifests('{table_name}')")
    
    print("Step 3: Expiring old snapshots (Retaining last 3)...")
    spark.sql(f"CALL catalog_iceberg.system.expire_snapshots(table => '{table_name}', retain_last => 3)")
    
    print("Step 4: Removing orphan files...")
    spark.sql(f"CALL catalog_iceberg.system.remove_orphan_files(table => '{table_name}')")
    
    print("--- Full Maintenance Completed Successfully! ---")
    sys.stdout.flush()

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("IcebergFullMaintenance")
        .config("spark.sql.catalog.catalog_iceberg.type", "rest")
        .config("spark.sql.catalog.catalog_iceberg.uri", "http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg")
        .getOrCreate()
    )
    
    # Get table name from argument or default to test table
    target_table = sys.argv[1] if len(sys.argv) > 1 else "catalog_iceberg.bus_bronze.buswaypoint_test"
    run_full_maintenance(spark, target_table)

"""Iceberg maintenance and benchmark runner for bus_way_point.

The script performs maintenance in this order:
1. Compact data files.
2. Rewrite manifests.
3. Expire snapshots.
4. Remove orphan files.

It also reports table health before and after maintenance and runs
representative benchmark queries to compare execution time.
"""

import os
from datetime import datetime, timedelta, timezone
from time import perf_counter

from pyspark.sql import SparkSession

# ================== CONFIG ==================
APP_NAME = "IcebergMaintenance"
CATALOG_NAME = "catalog_iceberg"
TABLE = os.getenv("ICEBERG_TABLE", "catalog_iceberg.bus_silver.bus_way_point")

ICEBERG_REST_URI = "http://gravitino:9001/iceberg/"
ICEBERG_WAREHOUSE = "s3a://iceberg/lakehouse"

ENABLE_COMPACT_DATA_FILES = True
ENABLE_REWRITE_MANIFESTS = True
ENABLE_EXPIRE_SNAPSHOTS = True
ENABLE_REMOVE_ORPHANS = True
SHOW_OPTIMIZATION_RECOMMENDATIONS = True

MIN_FILE_SIZE_BYTES = 3 * 1024 * 1024
TARGET_FILE_SIZE_BYTES = 64 * 1024 * 1024
MAX_FILE_SIZE_BYTES = 128 * 1024 * 1024
RETAIN_LAST_SNAPSHOTS = 3
ORPHAN_RETENTION_DAYS = 10

BENCHMARK_QUERIES = [
    (
        "daily_vehicle_volume",
        f"""
        SELECT date, vehicle, COUNT(*) AS point_count, AVG(speed) AS avg_speed
        FROM {TABLE}
        WHERE date BETWEEN DATE_SUB(CURRENT_DATE(), 7) AND CURRENT_DATE()
        GROUP BY date, vehicle
        ORDER BY date, vehicle
        """,
    ),
    (
        "vehicle_summary",
        f"""
        SELECT vehicle, COUNT(*) AS point_count, AVG(speed) AS avg_speed, MAX(timestamp) AS last_seen
        FROM {TABLE}
        GROUP BY vehicle
        ORDER BY point_count DESC
        LIMIT 50
        """,
    ),
]

spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", ICEBERG_REST_URI)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", ICEBERG_WAREHOUSE)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", "http://minio:9000")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access", "true")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id", "minioadmin")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key", "minioadmin123")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.client.region", "us-east-1")
    .config("spark.sql.defaultCatalog", CATALOG_NAME)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def execute_sql(sql: str, show: bool = True):
    print("\n=== RUN SQL ===")
    print(sql.strip())
    result = spark.sql(sql)
    if show:
        print("=== RESULT ===")
        result.show(truncate=False)
    return result


def show_table_health(stage: str):
    print(f"\n=== TABLE HEALTH: {stage.upper()} ===")
    execute_sql(f"SELECT COUNT(*) AS row_count FROM {TABLE}")
    execute_sql(f"SELECT COUNT(*) AS snapshot_count FROM {TABLE}.snapshots")
    execute_sql(f"SELECT COUNT(*) AS manifest_count FROM {TABLE}.manifests")
    execute_sql(f"DESCRIBE TABLE EXTENDED {TABLE}")


def benchmark_query(name: str, sql: str):
    print(f"\n=== BENCHMARK: {name} ===")
    print(sql.strip())
    start = perf_counter()
    rows = spark.sql(sql).count()
    elapsed = perf_counter() - start
    print(f"Rows returned: {rows}")
    print(f"Elapsed seconds: {elapsed:.3f}")
    return elapsed


def run_benchmarks(stage: str):
    print(f"\n=== BENCHMARKS: {stage.upper()} ===")
    timings = {}
    for name, sql in BENCHMARK_QUERIES:
        timings[name] = benchmark_query(name, sql)
    return timings


def compact_data_files():
    if not ENABLE_COMPACT_DATA_FILES:
        print("Skip compact_data_files (disabled).")
        return

    print("\n>>> [1] Compact small data files")
    sql = f"""
        CALL {CATALOG_NAME}.system.rewrite_data_files(
            table => '{TABLE}',
            options => map(
                'min-file-size-bytes',    '{MIN_FILE_SIZE_BYTES}',
                'target-file-size-bytes', '{TARGET_FILE_SIZE_BYTES}',
                'max-file-size-bytes',    '{MAX_FILE_SIZE_BYTES}'
            )
        )
    """
    execute_sql(sql)


def rewrite_manifests():
    if not ENABLE_REWRITE_MANIFESTS:
        print("Skip rewrite_manifests (disabled).")
        return

    print("\n>>> [2] Rewrite manifests")
    sql = f"""
        CALL {CATALOG_NAME}.system.rewrite_manifests(
            table => '{TABLE}'
        )
    """
    execute_sql(sql)


def expire_snapshots():
    if not ENABLE_EXPIRE_SNAPSHOTS:
        print("Skip expire_snapshots (disabled).")
        return

    print("\n>>> [3] Expire old snapshots")
    sql = f"""
        CALL {CATALOG_NAME}.system.expire_snapshots(
            table => '{TABLE}',
            retain_last => {RETAIN_LAST_SNAPSHOTS}
        )
    """
    execute_sql(sql)


def remove_orphan_files():
    if not ENABLE_REMOVE_ORPHANS:
        print("Skip remove_orphan_files (disabled).")
        return

    print("\n>>> [4] Remove orphan files")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ORPHAN_RETENTION_DAYS)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    sql = f"""
        CALL {CATALOG_NAME}.system.remove_orphan_files(
            table => '{TABLE}',
            older_than => TIMESTAMP '{cutoff}'
        )
    """
    execute_sql(sql)


def print_optimization_recommendations():
    if not SHOW_OPTIMIZATION_RECOMMENDATIONS:
        return

    print("\n=== OPTIMIZATION RECOMMENDATIONS ===")
    print("1. Partition by date if the dashboard usually filters on date.")
    print("2. Consider sorting by date, vehicle, timestamp to improve data skipping.")
    print("3. Reduce small files at ingest by repartitioning before write and tuning file size.")
    print("4. Benchmark with real dashboard or analytics queries instead of only COUNT(*).")
    print("5. Tune snapshot retention by environment: fewer in dev/test, more in production if rollback is important.")


# ================== MAIN ==================
if __name__ == "__main__":
    try:
        show_table_health("before")
        run_benchmarks("before")

        compact_data_files()
        rewrite_manifests()
        expire_snapshots()
        remove_orphan_files()

        show_table_health("after")
        run_benchmarks("after")
        print_optimization_recommendations()
    finally:
        spark.stop()
        print("\n>>> Spark session stopped.")

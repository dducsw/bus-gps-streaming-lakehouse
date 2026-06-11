from pyspark.sql import SparkSession
import sys

# Minimal Spark script for getting stats
# Config is passed via spark-submit --conf flags
table_name = sys.argv[1]

spark = SparkSession.builder \
    .appName("StatsCollector") \
    .getOrCreate()

try:
    # Query Iceberg metadata table .files
    # Using full table name (e.g. catalog.db.table.files)
    df = spark.sql(f"SELECT count(*) as file_count, sum(record_count) as total_records FROM {table_name}.files")
    row = df.collect()[0]
    
    file_count = row['file_count'] if row['file_count'] is not None else 0
    total_records = row['total_records'] if row['total_records'] is not None else 0
    
    print(f"FILE_COUNT:{file_count} | TOTAL_RECORDS:{total_records}")
except Exception as e:
    print(f"Stats collection failed: {e}")
    sys.exit(1)
finally:
    spark.stop()

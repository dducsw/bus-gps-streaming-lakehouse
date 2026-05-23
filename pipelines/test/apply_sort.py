from pyspark.sql import SparkSession
import sys

spark = SparkSession.builder \
    .appName("SortOrderBenchmark") \
    .getOrCreate()

try:
    print("Reading table...")
    df = spark.read.format("iceberg").load("catalog_iceberg.bus_bronze.buswaypoint_test")

    print("Writing table with sort order (timestamp, x, y)...")
    df.writeTo("catalog_iceberg.bus_bronze.buswaypoint_test") \
      .option("write.distribution-mode", "range") \
      .option("write.ordering", "timestamp,x,y") \
      .overwritePartitions()
    
    print("Successfully applied sort order and overwritten partitions.")
except Exception as e:
    print(f"Failed: {e}")
    sys.exit(1)
finally:
    spark.stop()

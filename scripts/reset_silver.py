from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("ResetSilverWaypoints").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Drop Silver waypoints table
    print("Dropping table catalog_iceberg.bus_silver.bus_way_point...")
    try:
        spark.sql("DROP TABLE IF EXISTS catalog_iceberg.bus_silver.bus_way_point")
        print("Table dropped successfully!")
    except Exception as e:
        print(f"Failed to drop table: {e}")

    # Delete MinIO checkpoints
    print("Deleting MinIO checkpoints for silver/bus_way_point...")
    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        Path = spark._jvm.org.apache.hadoop.fs.Path
        URI = spark._jvm.java.net.URI
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem
        
        fs = FileSystem.get(URI("s3a://iceberg"), hadoop_conf)
        checkpoint_path = Path("s3a://iceberg/lakehouse/checkpoints/silver/bus_way_point")
        
        if fs.exists(checkpoint_path):
            fs.delete(checkpoint_path, True)
            print("Checkpoint directory deleted successfully!")
        else:
            print("Checkpoint directory does not exist.")
    except Exception as e:
        print(f"Failed to delete checkpoint directory: {e}")

    spark.stop()

if __name__ == "__main__":
    main()

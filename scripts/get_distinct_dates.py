from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("GetDistinctDates").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.read.table("catalog_iceberg.bus_silver.bus_way_point")
        dates = df.select("date").distinct().orderBy("date").collect()
        print("DISTINCT DATES IN SILVER:")
        for r in dates:
            print(r["date"])
        print("TOTAL ROWS IN SILVER:", df.count())
    except Exception as e:
        print("Error reading silver table:", str(e))

if __name__ == "__main__":
    main()

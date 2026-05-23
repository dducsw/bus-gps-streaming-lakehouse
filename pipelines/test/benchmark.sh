#!/bin/bash

# Configuration
RAW_TABLE="catalog_iceberg.bus_bronze.buswaypoint_raw"
TEST_TABLE="catalog_iceberg.bus_bronze.buswaypoint_test"
QUERY_FILE="pipelines/test/query.sql"
SPATIAL_QUERY_FILE="pipelines/test/spatial_query.sql"
RESULT_LOG="pipelines/test/benchmark_results.log"

rm -f $RESULT_LOG

echo "===========================================" | tee -a $RESULT_LOG
echo "ICEBERG FULL OPTIMIZATION BENCHMARK" | tee -a $RESULT_LOG
echo "Date: $(date)" | tee -a $RESULT_LOG
echo "===========================================" | tee -a $RESULT_LOG

run_trino_query() {
    local query=$1
    docker exec -t trino trino --execute "$query" --output-format CSV
}

get_stats() {
    local label=$1
    echo "--- Stats: $label ---" | tee -a $RESULT_LOG
    local stats_query="SELECT count(*) as file_count, sum(record_count) as total_records, sum(file_size_in_bytes)/1024 as size_kb FROM catalog_iceberg.bus_bronze.\"${TEST_TABLE##*.}\$files\""
    run_trino_query "$stats_query" | tee -a $RESULT_LOG
}

measure_perf() {
    local label=$1
    local file=$2
    echo "--- Measuring Performance ($label) using $file ---" | tee -a $RESULT_LOG
    
    local start_time=$(date +%s%N)
    docker exec -t trino trino --file "/opt/spark/apps/$file" > /dev/null 2>&1
    local end_time=$(date +%s%N)
    
    local duration=$(( (end_time - start_time) / 1000000 ))
    echo "Execution Time: ${duration}ms" | tee -a $RESULT_LOG
}

## Function to reset the test table to the original 'messy' state
reset_test_table() {
    echo -e "\n--- Resetting $TEST_TABLE to 'Extreme Messy' State (~22,000 files) ---"
    run_trino_query "DROP TABLE IF EXISTS $TEST_TABLE"
    
    # 1. Create a clean copy first (fast)
    run_trino_query "CREATE TABLE $TEST_TABLE WITH (partitioning = ARRAY['date']) AS SELECT * FROM $RAW_TABLE" > /dev/null 2>&1
    
    # 2. Shred it into 22,000 small files (48KB each)
    # This simulates the exact fragmentation of the raw table
    docker exec spark-master spark-sql \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.catalog_iceberg=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.catalog_iceberg.type=rest \
        --conf spark.sql.catalog.catalog_iceberg.uri=http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg \
        -e "CALL catalog_iceberg.system.rewrite_data_files(table => '${TEST_TABLE#*.}', options => map('target-file-size-bytes', '49152'))" > /dev/null 2>&1
}

get_stats() {
    local label=$1
    echo "--- Stats: $label ---" | tee -a $RESULT_LOG
    docker exec spark-master spark-submit /opt/spark/apps/pipelines/test/get_stats.py "catalog_iceberg.${TEST_TABLE#*.}" | grep "FILE_COUNT" | tee -a $RESULT_LOG
}

# [STEP 1] INITIAL STATE
reset_test_table
echo -e "\n[CASE 1] INITIAL STATE (Extreme Messy - 22,000 files)" | tee -a $RESULT_LOG
get_stats "INITIAL"
measure_perf "INITIAL_NORMAL" "$QUERY_FILE"
measure_perf "INITIAL_SPATIAL" "$SPATIAL_QUERY_FILE"

# [STEP 2] BASIC MAINTENANCE (Compaction Only)
reset_test_table
echo -e "\n[CASE 2] BASIC MAINTENANCE (Compaction Only)" | tee -a $RESULT_LOG
get_stats "BEFORE_COMPACTION"
T_START=$(date +%s)
docker exec spark-master spark-submit \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.catalog_iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.catalog_iceberg.type=rest \
    --conf spark.sql.catalog.catalog_iceberg.uri=http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg \
    /opt/spark/apps/pipelines/test/maintenance.py "$TEST_TABLE" > /dev/null 2>&1
T_END=$(date +%s)
echo "Action Execution Time: $((T_END-T_START))s" | tee -a $RESULT_LOG
get_stats "AFTER_COMPACTION"
measure_perf "POST_COMPACTION_NORMAL" "$QUERY_FILE"

# [STEP 3] HIDDEN PARTITION + COMPACTION
reset_test_table
echo -e "\n[CASE 3] HIDDEN PARTITION + COMPACTION" | tee -a $RESULT_LOG
get_stats "BEFORE_HIDDEN_EVOLUTION"
START=$(date +%s)
# Evolve Metadata
docker exec spark-master spark-submit \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.catalog_iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.catalog_iceberg.type=rest \
    --conf spark.sql.catalog.catalog_iceberg.uri=http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg \
    /opt/spark/apps/pipelines/test/hidden_partition.py "$TEST_TABLE" > /dev/null 2>&1
# Compact into new partitions
docker exec spark-master spark-submit \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.catalog_iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.catalog_iceberg.type=rest \
    --conf spark.sql.catalog.catalog_iceberg.uri=http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg \
    /opt/spark/apps/pipelines/test/maintenance.py "$TEST_TABLE" > /dev/null 2>&1
END=$(date +%s)
echo "Action Execution Time: $((END-START))s" | tee -a $RESULT_LOG
get_stats "AFTER_HIDDEN_EVOLUTION"
measure_perf "POST_HIDDEN_NORMAL" "$QUERY_FILE"

# [STEP 4] SORT ORDER (Compaction + Hierarchical Sort)
reset_test_table
echo -e "\n[CASE 4] SORT ORDER (Compaction + Vehicle Sort)" | tee -a $RESULT_LOG
get_stats "BEFORE_SORT_ORDER"
START=$(date +%s)
docker exec spark-master spark-submit \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.catalog_iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.catalog_iceberg.type=rest \
    --conf spark.sql.catalog.catalog_iceberg.uri=http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg \
    /opt/spark/apps/pipelines/test/sort_order.py "$TEST_TABLE" > /dev/null 2>&1
END=$(date +%s)
echo "Action Execution Time: $((END-START))s" | tee -a $RESULT_LOG
get_stats "AFTER_SORT_ORDER"
measure_perf "POST_SORT_NORMAL" "$QUERY_FILE"

# [STEP 5] Z-ORDER (Compaction + Spatial Sort)
reset_test_table
echo -e "\n[CASE 5] Z-ORDER (Compaction + Spatial X/Y Sort)" | tee -a $RESULT_LOG
get_stats "BEFORE_ZORDER"
START=$(date +%s)
docker exec spark-master spark-submit \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.catalog_iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.catalog_iceberg.type=rest \
    --conf spark.sql.catalog.catalog_iceberg.uri=http://gravitino:8090/api/metalakes/metalake/catalogs/catalog_iceberg \
    /opt/spark/apps/pipelines/test/z_order.py "$TEST_TABLE" > /dev/null 2>&1
END=$(date +%s)
echo "Action Execution Time: $((END-START))s" | tee -a $RESULT_LOG
get_stats "AFTER_ZORDER"
measure_perf "POST_ZORDER_SPATIAL" "$SPATIAL_QUERY_FILE"

# FINAL SUMMARY
echo -e "\n===========================================" | tee -a $RESULT_LOG
echo "INDEPENDENT BENCHMARK COMPLETED" | tee -a $RESULT_LOG
echo "===========================================" | tee -a $RESULT_LOG




import subprocess
import time
import os
import re

# === CONFIGURATION ===
CATALOG = "catalog_iceberg"
SCHEMA = "bus_bronze"
# TARGETING RAW TABLE DIRECTLY
TABLE = f"{CATALOG}.{SCHEMA}.buswaypoint_raw"
QUERY_FILE = "pipelines/test/query.sql"
SPATIAL_QUERY_FILE = "pipelines/test/spatial_query.sql"
REPORT_FILE = "pipelines/test/benchmark_report.txt"

def log_to_report(message, clear=False):
    mode = 'w' if clear else 'a'
    with open(REPORT_FILE, mode) as f:
        f.write(message + "\n")
        f.flush()
        try:
            os.fsync(f.fileno())
        except:
            pass
    print(message)

def run_command(cmd, shell=True):
    result = subprocess.run(cmd, shell=shell, capture_output=True, text=True)
    return result.stdout.strip(), result.stderr.strip()

def run_trino(query):
    cmd = f"docker exec -t trino trino --execute \"{query}\" --output-format CSV"
    out, _ = run_command(cmd)
    return out

def run_spark_sql(query):
    cmd = (
        f"docker exec spark-master spark-sql "
        f"--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        f"--conf spark.sql.catalog.{CATALOG}=org.apache.iceberg.spark.SparkCatalog "
        f"--conf spark.sql.catalog.{CATALOG}.type=rest "
        f"--conf spark.sql.catalog.{CATALOG}.uri=http://gravitino:9001/iceberg/ "
        f"-e \"{query}\""
    )
    out, _ = run_command(cmd)
    return out

def run_spark_script(script_path, args=""):
    cmd = (
        f"docker exec spark-master spark-submit "
        f"--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
        f"--conf spark.sql.catalog.{CATALOG}=org.apache.iceberg.spark.SparkCatalog "
        f"--conf spark.sql.catalog.{CATALOG}.type=rest "
        f"--conf spark.sql.catalog.{CATALOG}.uri=http://gravitino:9001/iceberg/ "
        f"/opt/spark/apps/{script_path} {args}"
    )
    start = time.time()
    out, err = run_command(cmd)
    end = time.time()
    return round(end - start, 2), out

def get_stats():
    out = run_spark_sql(f"SELECT count(*) as file_count, sum(record_count) as total_records FROM {TABLE}.files")
    for line in out.split("\n"):
        if "|" in line:
            parts = [p.strip() for p in line.split("|") if p.strip()]
            if parts and parts[0].isdigit():
                return int(parts[0]), int(parts[1])
    return -1, -1

def measure_perf(query_file):
    with open(query_file, 'r') as f:
        query = f.read().strip().replace(';', '')
    times = []
    # Warm up
    run_trino(query) 
    for _ in range(2):
        start = time.time()
        run_trino(query)
        end = time.time()
        times.append((end - start) * 1000)
    return round(sum(times) / len(times), 2)

def main():
    log_to_report("="*60, clear=True)
    log_to_report("ICEBERG DIRECT RAW TABLE BENCHMARK (CUMULATIVE)")
    log_to_report(f"Started at: {time.ctime()}")
    log_to_report("="*60)
    
    results = []

    # CASE 1: INITIAL STATE (22k files)
    log_to_report(f"\n--- [CASE 1] Measuring Initial State (Messy) ---")
    f_count, r_count = get_stats()
    perf_norm = measure_perf(QUERY_FILE)
    perf_spat = measure_perf(SPATIAL_QUERY_FILE)
    res = ["INITIAL", f_count, 0, perf_norm, perf_spat]
    results.append(res)
    log_to_report(f"DONE: CASE 1 | Files: {f_count} | Normal: {perf_norm}ms | Spatial: {perf_spat}ms")

    # CASE 2: COMPACTION
    log_to_report(f"\n--- [CASE 2] Applying Compaction Only ---")
    duration, _ = run_spark_script("pipelines/test/maintenance.py", TABLE)
    f_count, _ = get_stats()
    perf_norm = measure_perf(QUERY_FILE)
    perf_spat = measure_perf(SPATIAL_QUERY_FILE)
    res = ["COMPACTION", f_count, duration, perf_norm, perf_spat]
    results.append(res)
    log_to_report(f"DONE: CASE 2 | Files: {f_count} | Action: {duration}s | Normal: {perf_norm}ms")

    # CASE 3: HIDDEN PARTITION
    log_to_report(f"\n--- [CASE 3] Applying Hidden Partition Evolution ---")
    duration1, _ = run_spark_script("pipelines/test/hidden_partition.py", TABLE)
    duration2, _ = run_spark_script("pipelines/test/maintenance.py", TABLE)
    f_count, _ = get_stats()
    perf_norm = measure_perf(QUERY_FILE)
    res = ["HIDDEN_PART", f_count, duration1+duration2, perf_norm, "N/A"]
    results.append(res)
    log_to_report(f"DONE: CASE 3 | Files: {f_count} | Action: {duration1+duration2}s | Normal: {perf_norm}ms")

    # CASE 4: SORT ORDER
    log_to_report(f"\n--- [CASE 4] Applying Sort Order ---")
    duration, _ = run_spark_script("pipelines/test/sort_order.py", TABLE)
    f_count, _ = get_stats()
    perf_norm = measure_perf(QUERY_FILE)
    res = ["SORT_ORDER", f_count, duration, perf_norm, "N/A"]
    results.append(res)
    log_to_report(f"DONE: CASE 4 | Files: {f_count} | Action: {duration}s | Normal: {perf_norm}ms")

    # CASE 5: Z-ORDER
    log_to_report(f"\n--- [CASE 5] Applying Z-ORDER ---")
    duration, _ = run_spark_script("pipelines/test/z_order.py", TABLE)
    f_count, _ = get_stats()
    perf_spat = measure_perf(SPATIAL_QUERY_FILE)
    res = ["Z_ORDER", f_count, duration, "N/A", perf_spat]
    results.append(res)
    log_to_report(f"DONE: CASE 5 | Files: {f_count} | Action: {duration}s | Spatial: {perf_spat}ms")

    # FINAL SUMMARY TABLE
    log_to_report("\n" + "="*80)
    log_to_report("FINAL CUMULATIVE SUMMARY TABLE (Markdown)")
    log_to_report("="*80)
    log_to_report("| Case | Files | Action(s) | Normal Query (ms) | Spatial Query (ms) |")
    log_to_report("| :--- | :--- | :--- | :--- | :--- |")
    for r in results:
        log_to_report(f"| {r[0]} | {r[1]} | {r[2]}s | {r[3]}ms | {r[4]}ms |")
    log_to_report("="*80)

if __name__ == "__main__":
    main()

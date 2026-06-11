# Hướng dẫn Benchmark Streaming: Throughput, Latency & End-to-End Latency

Tài liệu này hướng dẫn cách đo lường, phân tích và tối ưu hóa các chỉ số hiệu năng (Throughput, Latency, End-to-End Latency) cho hệ thống streaming dữ liệu GPS xe buýt từ **Kafka → Spark Structured Streaming → Apache Iceberg / Redis**.

---

## 1. Các Chỉ Số Quan Trọng Trong Streaming

Khi đánh giá hiệu năng hệ thống streaming, chúng ta cần tập trung vào 3 chỉ số chính:

| Chỉ số | Định nghĩa | Cách đo lường | Đơn vị |
| :--- | :--- | :--- | :--- |
| **Throughput (Băng thông)** | Số lượng bản ghi hệ thống có thể tiếp nhận và xử lý trong một đơn vị thời gian. | Tốc độ ghi vào Kafka (Kafka Ingestion Rate) và tốc độ Spark xử lý (`processedRowsPerSecond`). | Records/sec hoặc MB/s |
| **Processing Latency (Độ trễ xử lý)** | Thời gian cần thiết để xử lý một lô dữ liệu (Micro-batch) trong Spark. | Thời gian thực thi trigger trong Spark Structured Streaming (`triggerExecution`). | Milliseconds / Seconds |
| **End-to-End Latency (Độ trễ toàn trình)** | Thời gian từ khi sự kiện xảy ra ngoài thực tế (GPS timestamp) cho đến khi dữ liệu nằm trong tầng phục vụ (Iceberg/Redis) và sẵn sàng truy vấn. | Hiệu số giữa thời gian ghi nhận ở sink và thời gian sinh sự kiện ($t_{sink} - t_{event}$). | Seconds |

---

## 2. Phương Pháp Đo Lường Chi Tiết

### A. Throughput & Processing Latency trong Spark
Spark Structured Streaming tự động thống kê hiệu năng qua cấu trúc `StreamingQueryProgress` ở mỗi Micro-batch. 

Bạn có thể cấu hình lắng nghe trực tiếp thông qua API `StreamingQueryListener` hoặc truy xuất thuộc tính `query.lastProgress`. Các chỉ số chính bao gồm:
*   `inputRowsPerSecond`: Tốc độ dữ liệu đi vào Spark từ Kafka.
*   `processedRowsPerSecond`: Tốc độ Spark xử lý và ghi xuống Iceberg / Redis.
*   `durationMs.triggerExecution`: Tổng thời gian thực hiện một Micro-batch.
*   `durationMs.addBatch`: Thời gian thực hiện việc ghi dữ liệu vào Sink (MinIO/Iceberg hoặc Redis). Đây là nhân tố chính ảnh hưởng đến độ trễ ghi (Write Latency).

### B. Đo Lường End-to-End Latency (Độ trễ toàn trình)
Khi làm việc với hệ thống streaming, chúng ta cần phân biệt hai loại độ trễ toàn trình tùy thuộc vào nguồn dữ liệu đầu vào:

1.  **Độ trễ truyền tải hệ thống (Pipeline Transit Latency - Kafka to Sink)**:
    *   **Áp dụng cho**: Cả dữ liệu realtime và replay dữ liệu tĩnh từ file lịch sử (Historical Replay).
    *   **Cách thức**: Đo thời gian từ lúc bản ghi được ghi nhận vào Kafka broker cho đến khi được lưu trữ thành công ở Sink (Iceberg/Redis).
    *   **Công thức**: $\text{Latency}_{pipeline} = t_{sink} - t_{kafka\_ingest}$
    *   **Cách lấy trong Spark**: Spark Structured Streaming cung cấp cột siêu dữ liệu (metadata) ẩn `timestamp` từ Kafka đại diện cho thời điểm tin nhắn được Kafka ghi nhận. Ta ánh xạ cột này thành `kafka_timestamp` và so sánh với clock ghi của Spark.

2.  **Độ trễ nghiệp vụ toàn trình (Business End-to-End Latency - GPS to Sink)**:
    *   **Áp dụng cho**: Chỉ chính xác khi nhận luồng dữ liệu realtime thực tế từ thiết bị xe buýt.
    *   **Cách thức**: Đo thời gian từ khi sự kiện GPS xảy ra đến khi lưu trữ thành công.
    *   **Công thức**: $\text{Latency}_{e2e} = t_{sink} - t_{gps\_event}$ (với $t_{gps\_event}$ là trường `datetime` trong payload JSON).
    *   *Lưu ý*: Khi replay dữ liệu tĩnh cũ từ file, chỉ số này sẽ cực kỳ lớn (hàng giờ, hàng ngày) và không phản ánh đúng tốc độ của đường truyền hệ thống. Do đó, khi test bằng replay file tĩnh ta nên bỏ qua/giám sát riêng chỉ số này và tập trung vào **Pipeline Transit Latency**.

---

### 3. Chạy Thử Nghiệm Benchmark Streaming

Hệ thống đã được tích hợp script kiểm thử hiệu năng tại [scripts/benchmark_streaming.py](file:///d:/Projects/mp-252/scripts/benchmark_streaming.py).

### Bước 1: Khởi chạy Kafka Producer (để giả lập dòng dữ liệu)
Chạy Kafka Producer với tốc độ cao (ví dụ: 1000 records/giây) để tạo tải:
```bash
# Sửa records_per_second=1000 trong scripts/json_producer.py trước khi chạy nếu muốn tải nặng hơn
python scripts/json_producer.py
```

### Bước 2: Chạy Job Streaming Benchmark trên Spark Cluster
Gửi job benchmark vào Spark Master container:
```bash
docker exec -it spark-master spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.catalog_iceberg=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.catalog_iceberg.type=rest \
  --conf spark.sql.catalog.catalog_iceberg.uri=http://gravitino:9001/iceberg/ \
  /opt/spark/scripts/benchmark_streaming.py
```

Khi chạy, màn hình console sẽ liên tục cập nhật thông số hiệu năng sau mỗi batch (chạy hoàn toàn bất đồng bộ không gây nghẽn luồng):
```text
=================================================================
⏱️ STREAMING BATCH 12 - ACADEMIC METRICS (Asynchronous)
=================================================================
📊 Engine Throughput:
   - Input Rate: 2450.50 records/sec
   - Process Rate: 2310.20 records/sec
⏱️ Engine Latencies:
   - Total Trigger Duration: 2.150 seconds
   - Write/Sink (Iceberg Commit) Duration: 1.120 seconds
⚡ E2E Pipeline Transit Latency (Kafka -> Iceberg):
   - Batch Size: 5000 records
   - Avg Latency: 1.85 seconds
   - Min Latency: 0.45 seconds
   - Max Latency: 3.20 seconds
=================================================================
```

### Bước 3: Xuất báo cáo kết quả và vẽ đồ thị
Sau khi chạy benchmark, toàn bộ thông số lịch sử của từng batch sẽ tự động được ghi nhận vào file CSV tại:
*   Đường dẫn trong Spark: `/opt/spark/scripts/benchmark_results.csv`
*   Đường dẫn trên máy Host: [`scripts/benchmark_results.csv`](file:///d:/Projects/mp-252/scripts/benchmark_results.csv)

Định dạng file kết quả:
```csv
batch_id,timestamp,input_rate,process_rate,trigger_duration,write_duration,row_count,avg_pipeline_latency,min_pipeline_latency,max_pipeline_latency
0,2026-06-07T07:48:38.123Z,0.000,0.000,0.000,0.000,0,0.000,0.000,0.000
1,2026-06-07T07:48:48.123Z,2450.500,2310.200,2.150,1.120,5000,1.850,0.450,3.200
```
Bạn có thể dễ dàng load file này vào **Pandas** hoặc **Excel** để vẽ đồ thị biểu diễn tương quan giữa:
1.  **Throughput và Processing Latency** (Đánh giá giới hạn của Spark).
2.  **Throughput và Pipeline Transit Latency** (Đánh giá giới hạn truyền tải và thời gian commit của Iceberg).

---

## 4. Giám Sát Bằng Prometheus & Grafana (Ưu tiên sản xuất)

Hệ thống của chúng ta đi kèm bộ exporter hoàn chỉnh để visualize tự động lên Grafana (`http://localhost:3000`):

1.  **Kafka Exporter (`kafka-exporter:9308`)**:
    *   `kafka_consumergroup_lag`: Theo dõi lượng message bị dồn ứ (Lag) trên Kafka. Nếu lag tăng liên tục, đồng nghĩa với việc Spark xử lý không kịp tốc độ đẩy dữ liệu vào.
    *   `kafka_topic_partition_current_offset`: Đo lượng message Kafka sinh ra theo giây để tính toán Input Throughput gốc.
2.  **Spark JMX Prometheus Metrics**:
    *   Spark Master & Workers xuất metrics về CPU, JVM Heap Memory và GC time. Sử dụng để phát hiện nghẽn tài nguyên phần cứng.
3.  **Redis Exporter (`redis-exporter:9121`)**:
    *   `redis_instantaneous_ops_per_sec`: Theo dõi tốc độ Spark ghi dữ liệu cập nhật vào Redis phục vụ Dashboard thời gian thực.
    *   `redis_connected_clients`: Số lượng client đang đọc metrics từ tầng phục vụ.
4.  **cAdvisor & Node Exporter**:
    *   Theo dõi tài nguyên Docker containers và hệ điều hành máy chủ trong quá trình chịu tải benchmark.

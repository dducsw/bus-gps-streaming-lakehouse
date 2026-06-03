# Dữ liệu Phỏng vấn Chuyên sâu: GPS Data Lakehouse

Tài liệu này cung cấp các câu trả lời **chuyên sâu (Deep-dive)**, kết hợp giữa nền tảng **Lý thuyết (Theory)** và **Thực hành (Practice)** trong project HCMC Bus GPS Streaming.

---

## 1. Kiến trúc & Thiết kế (Architecture & Design)

### Q1: Tại sao chọn Data Lakehouse thay vì Data Warehouse truyền thống?
**Lý thuyết:**
- **Data Warehouse (DWH):** Kiến trúc đóng (tightly coupled), tính toán và lưu trữ dính liền nhau (như BigQuery, Snowflake, Teradata). Chi phí lưu trữ cao.
- **Data Lake:** Lưu trữ rẻ (S3, HDFS) nhưng thiếu ACID transactions, khó update/delete, không đảm bảo tính nhất quán (Consistency).
- **Data Lakehouse:** Kết hợp lưu trữ rẻ của Data Lake và các tính năng quản trị của DWH thông qua **Open Table Formats** (như Apache Iceberg, Delta Lake, Hudi).

**Thực hành trong Project:**
- Hệ thống nhận 1,000 GPS pings/giây, lượng dữ liệu thô cực lớn. Nếu đẩy thẳng vào DWH sẽ tốn rất nhiều tiền lưu trữ.
- Ta dùng **MinIO** (S3-compatible) làm Object Storage để lưu Parquet files với chi phí rẻ.
- Dùng **Apache Iceberg** phủ lên trên để cung cấp tính năng ACID (đảm bảo lúc Spark Streaming đang ghi file thì Trino vẫn đọc được dữ liệu cũ mà không bị lỗi file corruption).
- Tách biệt hoàn toàn Storage (MinIO) và Compute (Spark để xử lý, Trino để query).

### Q2: Chi tiết luồng dữ liệu qua các tầng Medallion?
**Lý thuyết:**
- Kiến trúc Medallion chia dữ liệu thành 3 tầng: **Bronze** (Nguyên bản, append-only) -> **Silver** (Đã làm sạch, chuẩn hóa schema) -> **Gold** (Tổng hợp theo logic Business).

**Thực hành trong Project:**
- **Bronze Layer:** Lưu nguyên bản message JSON từ Kafka xuống MinIO. Phục vụ việc "replay" lại dữ liệu nếu logic code ở Silver bị sai.
- **Silver Layer:** 
    - Khử nhiễu tọa độ. Trong project này, ta áp dụng **Kalman Filter** để làm mượt các điểm GPS bị giật lag (nhảy vị trí đột ngột do mất sóng tòa nhà).
    - Map với bảng `route_path` (lộ trình tuyến).
- **Gold Layer (được thể hiện rõ ở `trip_summary_dev.py`):**
    - Sử dụng **Window Functions** (`Window.partitionBy("vehicle", "route_id").orderBy("timestamp")`) kết hợp hàm `lag()` để tính chênh lệch thời gian (`time_diff`) giữa 2 điểm GPS.
    - Nếu `time_diff > 600s` (10 phút), thuật toán tự động tách đó thành một `trip_id` mới.
    - Tính toán khoảng cách bằng công thức **Haversine** và phân loại hướng đi (Inbound/Outbound) dựa trên độ lệch so với trạm đầu/cuối.

---

## 2. Xử lý Streaming (Kafka & Spark)

### Q3: Cơ chế xử lý 1,000 msgs/s và Backpressure?
**Lý thuyết:**
- **Backpressure / Rate Limiting:** Khi Consumer (Spark) xử lý không kịp tốc độ của Producer (Kafka), ta cần giới hạn lượng dữ liệu đọc vào trong mỗi batch để tránh lỗi Out Of Memory (OOM). Đối với Spark Structured Streaming, cơ chế này gọi là Rate Limiting thay vì cơ chế Backpressure PID của DStreams cũ.

**Thực hành trong Project:**
- Trong file `pipelines/test/buswaypoint_streaming.py` (dòng 86), hệ thống sử dụng `.option("maxOffsetsPerTrigger", "5000")`. Điều này giới hạn Spark chỉ đọc tối đa 5000 record từ Kafka trong mỗi micro-batch.
- Trong file `pipelines/bronze/buswaypoint_consumer.py` (dòng 86), ta kết hợp dùng **Micro-batching** thông qua `.trigger(processingTime="5 seconds")` để tạo ra nhịp xử lý ổn định, ghi file Parquet hiệu quả hơn xuống Iceberg.

### Q4: Đảm bảo Fault-tolerance (Chịu lỗi) như thế nào?
**Lý thuyết:**
- Để đạt được **Exactly-once semantics** (Xử lý chính xác một lần) trong Streaming, cần kết hợp: Source có khả năng replay (Kafka) + Checkpointing + Idempotent Sink (Iceberg).

**Thực hành trong Project:**
- **Spark Checkpointing:** Spark lưu trữ thư mục `_checkpoint` trên MinIO. Thư mục này chứa metadata về offset của Kafka. Nếu Spark job bị chết, khi restart, nó đọc file checkpoint và chạy tiếp từ offset bị gián đoạn.
- **Idempotent Writes với Iceberg:** Nếu job chết lúc đang ghi dở file, file đó sẽ ở trạng thái "mồ côi" (uncommitted). Iceberg chỉ tạo Snapshot mới khi toàn bộ batch thành công (ACID Transaction), nên Trino sẽ không bao giờ đọc phải dữ liệu bị rác hay nhân đôi.

### Q4.5: Điều gì xảy ra nếu bạn thay đổi logic (ví dụ: đổi Window từ 5 phút thành 10 phút) nhưng lại resume từ Checkpoint cũ?
**Lý thuyết & Thực hành:**
- **Crash hệ thống!** Spark Structured Streaming lưu trữ state schema (cấu trúc của các stateful operations như aggregations, window) vào Checkpoint. Nếu bạn sửa code làm thay đổi schema của state (đổi kích thước window, thêm một cột agg mới) và cố gắng chạy tiếp từ checkpoint cũ, Spark sẽ văng lỗi `State schema mismatch`.
- **Cách giải quyết:** Bạn buộc phải trỏ Checkpoint sang một thư mục mới (hoặc xóa thư mục cũ). Đánh đổi là bạn sẽ bị mất toàn bộ state (các phép tính tổng/trung bình đang dở dang) của batch trước đó.

### Q4.6: Dữ liệu GPS rất dễ bị "Data Skew" (ví dụ: một tuyến xe bus dài gửi dữ liệu gấp 10 lần tuyến khác). Bạn xử lý thế nào trong Spark?
**Lý thuyết & Thực hành:**
- Nếu bạn thực hiện `groupBy("route_no")` (như trong `buswaypoint_window.py`), các Executor xử lý tuyến xe đông sẽ bị quá tải (OOM) trong khi các Executor khác rảnh rỗi.
- **Cách giải quyết (Salting):** Thêm một chuỗi random (salt) vào `route_no` (ví dụ `route_1_A`, `route_1_B`) để ép Spark phân tán dữ liệu ra nhiều node tính toán, sau đó mới agg lần 2 (Double Aggregation). Tuy nhiên, với dữ liệu 1000 msg/s, Spark's AQE (Adaptive Query Execution) có thể tự động handle Skew Join khá tốt nếu kích hoạt.

---

## 3. Tối ưu hóa Lưu trữ (Apache Iceberg)

### Q5: Phân vùng (Partitioning) và tối ưu hóa file nhỏ?
**Lý thuyết:**
- Streaming luôn sinh ra **Small Files Problem** (rất nhiều file Parquet dung lượng vài KB/MB). Quá nhiều file nhỏ làm NameNode (hoặc Object Storage) bị chậm, và các Engine truy vấn (Trino) tốn quá nhiều thời gian mở/đóng file thay vì đọc dữ liệu.

**Thực hành trong Project:**
- **Partitioning:** Sử dụng `event_time` để phân vùng theo ngày (`day(timestamp)`). Iceberg sử dụng **Hidden Partitioning**, nó tự động suy luận ra ngày từ cột timestamp gốc.
- **Compaction:** Viết một script Spark chạy định kỳ (batch job) gọi thủ tục `CALL catalog.system.rewrite_data_files('db.table')` với chiến lược **bin-packing**. Nó sẽ gom các file 1MB thành các file chuẩn 128MB. Điều này đã giúp giảm 40% query latency.

### Q6: Sort-order/Z-Order (Clustering) là gì?
**Lý thuyết:**
- Parquet lưu trữ dữ liệu theo dạng cột (Columnar) và giữ các thống kê (min, max, count) ở phần footer của file. **Data Skipping** là cơ chế Engine bỏ qua không đọc file nếu điều kiện `WHERE` nằm ngoài khoảng (min, max).

**Thực hành trong Project:**
- Nếu dữ liệu không được sort, một xe bus sẽ nằm rải rác ở hàng trăm file.
- Ta cấu hình Iceberg sort order theo `vehicle_id` và `timestamp`. 
- Khi Trino query `WHERE vehicle_id = 'A'`, nhờ Iceberg Manifests đã lưu min-max của `vehicle_id` cho từng file, Trino sẽ **bỏ qua** toàn bộ các file không chứa xe 'A', giảm đáng kể I/O.

### Q6.5 (Câu hỏi hóc búa): So sánh Copy-on-Write (CoW) và Merge-on-Read (MoR) trong Iceberg? Dự án này nên dùng loại nào?
**Lý thuyết:**
- **Copy-on-Write (CoW):** Bất cứ khi nào bạn Update/Delete 1 dòng, Iceberg phải đọc lại toàn bộ file Parquet chứa dòng đó, sửa, và ghi lại thành 1 file hoàn toàn mới. -> Write cực chậm, Read cực nhanh.
- **Merge-on-Read (MoR):** Khi Update/Delete, Iceberg chỉ ghi vào các "Delete Files" nhỏ lẻ. Khi Trino đọc, nó sẽ load Data file gốc + Delete file và gộp lại trên RAM (Merge). -> Write cực nhanh, Read chậm hơn.

**Thực hành trong Project:**
- Tầng **Bronze** (Raw GPS) là `append-only` nên mặc định là CoW (không có update).
- Nếu tầng **Silver/Gold** cần thực hiện thao tác **UPSERT** (ví dụ cập nhật trạng thái chuyến đi chậm trễ), ta **BẮT BUỘC** phải cấu hình bảng thành **Merge-on-Read** (`write.update.mode=merge-on-read`). Nếu dùng CoW cho Streaming có update, Spark job sẽ chết vì quá tải I/O (Write Amplification).

### Q6.6 (Câu hỏi hóc búa): Đang chạy Compaction (`rewrite_data_files`) thì server sập, bảng Iceberg có bị lỗi (corrupt) không? Trino đang query có bị ảnh hưởng không?
**Lý thuyết & Thực hành:**
- **Tuyệt đối không!** Iceberg sử dụng **Optimistic Concurrency Control (OCC)**. Quá trình Compaction chỉ đang âm thầm tạo ra các file Parquet mới (Snapshot Mới) ở background.
- Trino vẫn tiếp tục đọc dữ liệu từ **Snapshot Cũ** (bình thường).
- Chỉ khi Compaction xong 100%, Iceberg mới thực hiện thao tác **Pointer Swap** (đổi con trỏ metadata sang Snapshot mới). Nếu server sập giữa chừng, Snapshot mới bị bỏ hoang (orphan files) và sẽ tự động bị xóa đi trong đợt chạy dọn rác (Vacuum/Expire Snapshots) lần sau. Zero downtime!

---

## 4. Serving Layer & Data Quality

### Q7: Kiến trúc kết hợp Redis và Trino?
**Lý thuyết:**
- Bài toán Real-time (Low Latency) cần In-memory Database (Redis).
- Bài toán Analytics (OLAP) cần Distributed SQL Engine (Trino/Iceberg).

**Thực hành trong Project:**
- Tại Spark Streaming, ta dùng hàm `foreachBatch`. Một nhánh ghi dữ liệu lịch sử xuống Iceberg. Nhánh còn lại cập nhật (UPSERT) trạng thái tọa độ mới nhất của xe vào **Redis** (cấu trúc `HSET bus_state {vehicle_id} {json_data}`).
- Các API của Web App / Grafana khi cần hiện bản đồ realtime sẽ lấy từ Redis (độ trễ < 5ms).
- Khi Superset cần vẽ biểu đồ "Tốc độ trung bình tuần qua", nó query vào Trino -> Iceberg (độ trễ 1-2s).

### Q8: Đảm bảo Data Quality và phục vụ AI ETA Prediction?
**Lý thuyết:**
- Dữ liệu rác (Garbage In) sẽ dẫn tới Model AI kém (Garbage Out). Cần có các feature có tính đại diện cao cho Machine Learning.

**Thực hành trong Project:**
- Trong `trip_summary_dev.py`, ta tính toán **Confidence Score** cho mỗi chuyến đi:
    - Nếu tổng độ lệch khoảng cách giữa điểm đầu/cuối của chuyến đi và trạm xe bus thực tế `(outbound_dist_err + inbound_dist_err) < 5.0 km`, hệ thống đánh giá dữ liệu là "HIGH" confidence.
    - Lọc bỏ các chuyến đi nhiễu (distance < 1km).
- Các trường `trip_duration_minutes`, `total_distance_km` và chuỗi tọa độ (đã qua Kalman Filter) tạo thành các dataset dạng Sequence Window cực kỳ sạch, làm input chuẩn xác cho các mạng Neural Network dự đoán thời gian đến (ETA - Estimated Time of Arrival).

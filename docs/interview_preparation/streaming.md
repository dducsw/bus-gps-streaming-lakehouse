# Câu Hỏi Phỏng Vấn Chuyên Sâu: Spark Streaming Resources & Tuning

## Tình huống (Scenario)
Trong pipeline `buswaypoint_window.py`, ứng dụng Spark được cấu hình tài nguyên như sau:

```python
spark = (
    SparkSession.builder.appName("BusRouteWindowMetrics")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.executor.cores", "2")
    .config("spark.cores.max", "2")
    .getOrCreate()
)
```

**Câu hỏi:** Với cấu hình toàn bộ ứng dụng chỉ dùng tối đa `2 CPU Cores` như trên, hệ thống có đủ sức xử lý lưu lượng **1,000 messages/giây** từ Kafka hay không? Hãy phân tích các rủi ro (nếu có).

---

## Trả lời (Dành cho Senior Data Engineer)

Về mặt tính toán thuần túy thì **ĐỦ**, nhưng mang lên môi trường Production thực tế thì cấu hình này **RẤT DỄ CRASH** do thiếu các cơ chế bảo vệ.

### 1. Phân tích tại sao "ĐỦ" (Trong điều kiện lý tưởng)
*   **Throughput của Spark:** 1,000 messages/giây (JSON) tương đương khoảng 1-2 MB dữ liệu/giây. Việc parse JSON (`from_json`) và thực hiện Broadcast Join với dữ liệu nhỏ (file mapping) trên Spark tiêu tốn rất ít CPU. 1 core có thể xử lý hàng chục nghìn record JSON mỗi giây. Do đó, 2 cores là đủ cho lượng tính toán này.
*   **Tối ưu Shuffle (`shuffle.partitions = 4`):** Đây là một điểm cấu hình rất thông minh cho lượng dữ liệu nhỏ. Mặc định của Spark là 200 partitions, sẽ tạo ra quá nhiều task rác. Giảm xuống 4 partitions rất vừa vặn với 2 cores (mỗi core xử lý 2 task song song), giảm tối đa Scheduling Overhead.

### 2. Phân tích tại sao "RẤT DỄ CRASH" (3 Tử huyệt trên Production)

Dù CPU đủ, nhưng code pipeline hiện tại đang thiếu các cấu hình bảo vệ (Safeguards) quan trọng, dẫn đến 3 rủi ro chí mạng:

#### Tử huyệt 1: Lỗi dồn Data (Data Burst) khi Restart (Thiếu Rate Limiting)
*   **Vấn đề:** Hiện tại `readStream` chưa cấu hình `maxOffsetsPerTrigger`. Nếu ứng dụng sập hoặc bảo trì trong 10 phút, Kafka sẽ tích tụ `1,000 * 60 * 10 = 600,000` messages. Khi bật lại, 2 Cores này sẽ cố nạp toàn bộ 600,000 messages vào RAM trong **một micro-batch duy nhất**.
*   **Hậu quả:** Chắc chắn dẫn đến **Out-Of-Memory (OOM)** hoặc kẹt cứng do Garbage Collection (GC Pause).

#### Tử huyệt 2: State Store quá nặng do Sliding Window
*   **Vấn đề:** Pipeline sử dụng `window("timestamp", "5 minutes", "1 minute")` (Cửa sổ 5 phút, trượt mỗi 1 phút). Mỗi sự kiện GPS gửi tới sẽ phải tồn tại cùng lúc trong **5 cửa sổ tính toán khác nhau** (ví dụ: điểm lúc 08:02 sẽ nằm trong cửa sổ 07:58-08:03, 07:59-08:04,...).
*   **Hậu quả:** Spark phải lưu toàn bộ các "trạng thái" (State) này xuống Checkpoint Directory. Với 2 Cores, áp lực I/O để ghi State xuống disk/MinIO liên tục sẽ trở thành nút thắt cổ chai cực lớn.

#### Tử huyệt 3: Không cấu hình Trigger Interval
*   **Vấn đề:** Lệnh `writeStream.start()` đang không được truyền `.trigger(...)`.
*   **Hậu quả:** Mặc định Spark sẽ chạy batch tiếp theo *ngay lập tức* khi batch trước vừa hoàn thành. Nếu 1 batch chạy tốn 200ms, Spark sẽ liên tục spam task mới 5 lần/giây. Lượng tài nguyên điều phối (Scheduling Overhead) sẽ vắt kiệt sức của 2 Cores thay vì dùng nó để xử lý dữ liệu.

---

## 🛠 Giải pháp Tối ưu (Best Practices)

Để biến pipeline này đạt chuẩn Production và chạy mượt mà không bao giờ sập với 2 Cores, ta cần cập nhật code như sau:

1.  **Thêm Rate Limiting (Bắt buộc):** 
    Thêm `.option("maxOffsetsPerTrigger", "10000")` vào lúc đọc Kafka để bảo vệ hệ thống khỏi Data Burst lúc restart.
2.  **Thiết lập Micro-batching (Bắt buộc):** 
    Thêm `.trigger(processingTime="10 seconds")` vào lúc `writeStream`. Thay vì chạy lẻ tẻ, ta gom 10,000 messages để xử lý gọn gàng 1 lần trong mỗi 10 giây.
3.  **Tăng nhẹ Core (Khuyến nghị):** 
    Nên nâng `spark.cores.max = 4`. Lý do: Xử lý CPU là đủ, nhưng phần cuối của pipeline gọi lệnh `foreachBatch` ghi đồng bộ (synchronous) vào Redis và Checkpoint I/O. Có 4 cores sẽ giúp các I/O task không làm nghẽn quá trình đọc luồng dữ liệu mới.

---

## 🔥 Câu Hỏi Bổ Sung (Tầm cỡ Senior)

### Q1: Vấn đề Data Consistency (Tính nhất quán) khi ghi vào Redis Stream
**Câu hỏi:** Trong `buswaypoint_window.py`, bạn đang dùng `redis_client.pipeline().xadd(...)` trong hàm `foreachPartition`. Nếu hệ thống bị đứt mạng giữa chừng khi đang `pipeline.execute()`, Spark sẽ báo lỗi task và tự động Retry. Việc này gây ra rủi ro gì?
**Trả lời:**
- Rủi ro cực lớn là sinh ra **Duplicate Data (Dữ liệu lặp)** trong Redis Stream. Khác với Iceberg (Idempotent), lệnh `XADD` của Redis không có tính Idempotent. Khi Spark retry lại cái partition bị lỗi, nó sẽ gửi lại toàn bộ các message đã gửi thành công trước khi đứt mạng.
- **Hậu quả:** Hệ thống chỉ đạt mức **At-least-once** (Ít nhất một lần) thay vì Exactly-once.
- **Cách khắc phục chuẩn Senior:** Thay vì dùng `XADD` (append-only stream), nếu bài toán chỉ cần lưu trạng thái mới nhất của metric trong Window, ta nên dùng `HSET` với key chứa `batch_id` hoặc `window_start`. Hoặc ta phải chủ động lưu `batch_id` thành công vào Redis để deduplicate.

### Q2: Quản lý Connection Pool với Redis
**Câu hỏi:** Tại sao bạn lại tạo `redis_client = redis.Redis(...)` bên trong hàm `send_partition` (nằm trong `foreachPartition`) thay vì tạo 1 connection duy nhất ở ngoài hàm `main()`?
**Trả lời:**
- Connection objects (như Redis Client, Kafka Producer, JDBC Connection) **không thể Serialize** (chuyển đổi thành bytes) để gửi từ Spark Driver sang các Spark Executors qua mạng.
- Nếu khởi tạo ở ngoài `main()`, Spark sẽ văng lỗi `Task not serializable`. Khởi tạo bên trong `foreachPartition` đảm bảo mỗi Executor (Worker) sẽ tự mở Connection riêng biệt tới Redis, xử lý xong cụm data của partition đó rồi đóng lại (`redis_client.close()`). Điều này tối ưu và an toàn nhất cho kiến trúc phân tán.

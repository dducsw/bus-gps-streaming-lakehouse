# Câu Hỏi Phỏng Vấn Chuyên Sâu: Apache Iceberg & Lakehouse

Tài liệu này tập trung vào kiến trúc lưu trữ Lakehouse và sự tối ưu hóa của Apache Iceberg so với các giải pháp truyền thống.

---

## 1. Cơ Chế Hoạt Động (Architecture)

### Q1: Iceberg giải quyết bài toán "Small Files Problem" và "Directory Listing" tốt hơn Hive như thế nào?
**Câu hỏi:** Với hệ thống Streaming 1,000 msg/s ghi xuống MinIO (S3), việc tạo ra hàng ngàn file nhỏ là không thể tránh khỏi. Tại sao dùng Hive Format lại bị chậm rề rà khi query, còn Iceberg thì không?

**Trả lời (Senior):**

**1. Lý thuyết cốt lõi:**
- **Bản chất của Object Storage:** MinIO/S3 không phải là một File System thực sự (POSIX). Khái niệm "thư mục" (directory) chỉ là giả lập dựa trên prefix của key. Lệnh `ListObjects` (duyệt thư mục) trên S3 là thao tác có độ phức tạp O(N), tốn rất nhiều API calls và cực kỳ chậm.
- **Hive Format:** Thiết kế dựa trên cấu trúc thư mục. Khi Trino/Spark đọc bảng Hive, nó BẮT BUỘC phải gọi `ListObjects` để xem trong thư mục partition có bao nhiêu file Parquet. Nếu có 10,000 file nhỏ, việc List này có thể mất vài phút trước khi việc đọc dữ liệu thực sự bắt đầu.
- **Iceberg Architecture:** Thiết kế dựa trên cây metadata (Metadata Tree). Bao gồm: `Metadata File` -> trỏ tới `Snapshot` -> trỏ tới `Manifest List` -> trỏ tới các `Manifest Files`. Mỗi Manifest File là một tệp Avro lưu trực tiếp đường dẫn tuyệt đối (URI) của các file Parquet cùng với số liệu thống kê (min/max/null_count).

**2. Thực hành & Phân tích trong dự án:**
- Trong dự án GPS của bạn, dòng dữ liệu chảy liên tục và ghi xuống MinIO mỗi vài giây (Micro-batch), sinh ra hàng ngàn file Parquet kích thước chỉ vài KB đến vài MB.
- Khi Trino query, thay vì quét MinIO mù quáng (như Hive), nó chỉ việc tải 1 file Iceberg Manifest (rất nhẹ) để lấy danh sách đường dẫn tuyệt đối của các file Parquet cần đọc (độ phức tạp O(1)).
- Hơn nữa, nhờ các thống kê Min/Max có sẵn trong Manifest, nếu Trino đang tìm `vehicle_id = 123`, nó sẽ tự động bỏ qua (Skip) toàn bộ các file Parquet không chứa xe này mà không cần mở file ra xem. Điều này giảm triệt để I/O và bù đắp lại nhược điểm của Small Files.

### Q2: Phân tích "Hidden Partitioning" (Phân vùng ẩn)
**Câu hỏi:** Trong bảng `bus_bronze.buswaypoint_raw`, bạn phân vùng theo `date`. Sự khác biệt giữa Partitioning của Hive và Hidden Partitioning của Iceberg là gì?

**Trả lời (Senior):**

**1. Lý thuyết cốt lõi:**
- **Physical Partitioning (Hive):** Người kỹ sư phải tự tạo thêm một cột vật lý (ví dụ: `date_string`) chỉ để phục vụ cho việc tạo thư mục (vd: `/date_string=2025-04-03/`). Đây là sự rò rỉ cấu trúc vật lý (physical layout) lên tầng logic. Người phân tích dữ liệu (Data Analyst) khi query **bắt buộc** phải nhớ filter bằng cột `date_string` này. Nếu họ quên và filter bằng `timestamp`, Hive sẽ không nhận diện được và tiến hành Full Table Scan.
- **Logical Partitioning (Iceberg):** Tách bạch hoàn toàn giữa việc lưu trữ vật lý và câu query logic. Iceberg lưu thông tin phân vùng dưới dạng một hàm biến đổi (Transform function), ví dụ: `partitioned_by = day(timestamp)`. Không cần tạo thêm cột mới.

**2. Thực hành & Phân tích trong dự án:**
- Dữ liệu GPS của bạn có cột gốc là `timestamp` (thời gian nguyên bản của GPS). 
- Khi khai báo Iceberg table, bạn chỉ cần set rule: Dùng hàm `day()` lên cột `timestamp`. 
- Khi team Data Analyst/AI chạy truy vấn: `SELECT * FROM buswaypoint_raw WHERE timestamp >= '2025-04-03 00:00:00' AND timestamp < '2025-04-04 00:00:00'`, Iceberg tự động dịch điều kiện này, đối chiếu với metadata và chỉ đọc đúng partition của ngày mùng 3.
- Điều này loại bỏ hoàn toàn rủi ro Human Error (quên thêm partition filter) làm treo hệ thống MinIO.

---

## 2. Đồng thời (Concurrency) và Cập nhật (Updates)

### Q3: Optimistic Concurrency Control (OCC) là gì?
**Câu hỏi:** Điều gì xảy ra nếu có 2 Spark jobs (hoặc 1 job Streaming và 1 job Compaction) cùng ghi/cập nhật vào một bảng Iceberg cùng một lúc?

**Trả lời (Senior):**

**1. Lý thuyết cốt lõi:**
- **Pessimistic Locking (Khóa bi quan):** Giống như các RDBMS truyền thống, khi một job đang ghi, nó khóa (lock) luôn cái bảng đó. Job thứ 2 phải đứng đợi. Điều này tạo ra nút thắt cổ chai cực lớn.
- **Optimistic Concurrency Control - OCC (Kiểm soát đồng thời lạc quan):** Giả định rằng xung đột (conflict) rất hiếm khi xảy ra. Iceberg cho phép tất cả các job cứ thoải mái đọc dữ liệu và viết ra các tệp Parquet mới ở background. Sự phân xử chỉ diễn ra ở giây phút cuối cùng: Thao tác **Compare-And-Swap (CAS)** trên Catalog.

**2. Thực hành & Phân tích trong dự án:**
- Giả sử Job A (Streaming) và Job B (Compaction - dọn dẹp file nhỏ) cùng chạy. Cả 2 cùng dựa trên Snapshot gốc là `V1`.
- Job A chạy xong trước, nó yêu cầu Catalog (REST/Hive Catalog) trỏ con trỏ từ `V1 -> V2`. Thao tác CAS thành công.
- 5 giây sau, Job B chạy xong. Nó yêu cầu Catalog trỏ từ `V1 -> V3`. Catalog từ chối vì hiện tại bảng đã là `V2` (Snapshot gốc `V1` đã lỗi thời).
- **Iceberg Magic:** Lúc này Job B không hề bị crash ngay lập tức! Nó sẽ âm thầm tải metadata của `V2` về, kiểm tra xem việc Job A thêm data vào có bị đụng chạm tới các file mà Job B đang Compaction hay không. 
    - Nếu Job A ghi vào thư mục của hôm nay, còn Job B đang dọn dẹp thư mục của hôm qua (không đụng chạm nhau) -> Iceberg tự động Merge metadata lại và đẩy lên thành `V3`.
    - Chỉ khi nào có xung đột vật lý trên cùng 1 file, hệ thống mới ném lỗi `ValidationException` để bảo vệ tính ACID.

### Q4: Copy-on-Write (CoW) vs Merge-on-Read (MoR)
**Câu hỏi:** Tại tầng Silver/Gold, nếu thuật toán Kalman Filter của bạn cần **UPSERT** (sửa lại tọa độ cũ), bạn sẽ thiết kế bảng Iceberg như thế nào để hệ thống Streaming không bị quá tải?

**Trả lời (Senior):**

**1. Lý thuyết cốt lõi:**
- **Copy-on-Write (CoW - Ghi đè toàn bộ):** Mỗi khi có thao tác Update/Delete 1 dòng, Iceberg phải đọc lại toàn bộ nội dung của file Parquet chứa dòng đó lên RAM, chỉnh sửa, rồi lưu thành 1 file Parquet mới toanh. 
    - *Ưu điểm:* Tốc độ đọc (Read) cực nhanh vì dữ liệu luôn nguyên vẹn. 
    - *Nhược điểm:* Hiện tượng **Write Amplification (Khuếch đại I/O)**. Sửa 1 byte cũng mất công ghi lại file 128MB.
- **Merge-on-Read (MoR - Gộp khi đọc):** Khi Update/Delete, Iceberg giữ nguyên file Data cũ. Nó chỉ tạo ra một file "Delete File" siêu nhỏ (vài KB), lưu thông tin ID của dòng bị xóa (Position Delete) hoặc giá trị bị xóa (Equality Delete). Đồng thời ghi dòng mới tinh vào một Data File mới.
    - *Ưu điểm:* Tốc độ viết (Write) siêu nhanh, cực kỳ phù hợp cho Streaming.
    - *Nhược điểm:* Tốn CPU lúc Trino truy vấn vì nó phải tải cả Data File và Delete File lên RAM để gộp (Merge On-the-fly).

**2. Thực hành & Phân tích trong dự án:**
- Tầng **Bronze** (lưu dữ liệu GPS thô từ Kafka): Vì tính chất chỉ có thêm vào (`append-only`), mặc định dùng CoW là tối ưu nhất vì không có thao tác Update/Delete nào diễn ra.
- Tầng **Silver/Gold**: Giả sử Kalman Filter phát hiện tín hiệu nhiễu và cần sửa lại một tọa độ của 10 giây trước. Nếu bạn để CoW, Spark Streaming sẽ phải nạp cả file Parquet đang ghi dở ra để sửa -> Job sẽ bị đơ/lag ngay lập tức.
- Do đó, đối với bảng cần UPSERT liên tục từ Streaming, **BẮT BUỘC** phải cấu hình bảng thành MoR (`ALTER TABLE ... SET TBLPROPERTIES ('write.update.mode'='merge-on-read')`).
- Sau đó, để bù đắp cho nhược điểm đọc chậm của MoR, ta phải setup một job chạy ngầm (Maintenance Job) để định kỳ Compaction các file Delete này vào Data File chính vào ban đêm.

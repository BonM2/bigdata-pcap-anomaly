
# Kiểm thử Pipeline Big Data — Storage → Processing

## 1. Mục tiêu

Kiểm tra các thành phần đã triển khai trước khi chạy pipeline trên 8 file CSV CICIDS2017 thật.

Các thành phần cần kiểm tra:

- Docker Environment
- Apache Spark & MinIO
- Data Ingestion
- Data Transformation
- Data Validation
- Data Cleaning
- Source Audit

---

## 2. Quy trình kiểm thử

| Bước | Công việc | Kết quả mong đợi |
|---|---|---|
| 1 | Docker Environment | MinIO, Spark Master và 2 Workers hoạt động |
| 2 | Unit Testing | 12 tests PASS |
| 3 | MinIO Smoke Test | Spark xử lý đúng 12 dòng dữ liệu mẫu |
| 4 | Source Audit | Kiểm tra 8 CSV thật, tổng 2.830.743 records |
| 5 | Upload Dry Run | Kiểm tra kế hoạch upload, chưa upload thật |

---

## 3. Các bước thực hiện

### Bước 1 — Khởi động Docker Environment

Mục đích: Khởi động hạ tầng MinIO và Spark Cluster.

```powershell
cd D:\bigdata-pcap-anomaly

docker compose -f docker/docker-compose.yml up -d

docker compose -f docker/docker-compose.yml ps
```

Kết quả mong đợi:

- `bigdata-minio`: healthy
- `spark-master`: running
- `spark-worker-1`: running
- `spark-worker-2`: running

### Bước 2 — Unit Testing

Mục đích: Kiểm tra từng module xử lý dữ liệu.

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m unittest discover -s tests -v
```

Kết quả mong đợi:

```text
Ran 12 tests
OK
```

Các chức năng được kiểm tra:

- Configuration: Cấu hình Spark/MinIO.
- Transformation: Ép kiểu dữ liệu và phát hiện lỗi cast.
- Validation: Kiểm tra dữ liệu theo Data Contract.
- Cleaning: Xử lý NaN, Infinity và sentinel `-1`.
- Ingestion: Kiểm tra header và mapping CSV.
- Source Audit: Kiểm tra dữ liệu nguồn.

Chỉ chạy test Transformation và Cleaning:

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m unittest tests.test_transform tests.test_clean -v
```

### Bước 3 — MinIO Smoke Test

Mục đích: Kiểm tra khả năng kết nối Spark ↔ MinIO và xử lý dữ liệu mẫu.

**3.1. Tạo dữ liệu mẫu 12 dòng**

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m tests.seed_minio
```

**3.2. Chạy Spark Job**

```powershell
docker compose -f docker/docker-compose.yml exec -e INPUT_PATH=s3a://bigdata-network/smoke/cicids2017/synthetic_flow.csv spark-master python3 -m processing.check_minio --report /tmp/minio-smoke.json
```

Kết quả mong đợi:

```json
{
  "status": "PASS",
  "raw_columns": 79,
  "core_columns": 22,
  "input_records": 12,
  "valid_records": 4,
  "quarantined_records": 6,
  "rejected_records": 2
}
```

**3.3. Lưu báo cáo về máy**

```powershell
docker cp spark-master:/tmp/minio-smoke.json docs/minio-smoke.local.json
```

Ý nghĩa:

- `raw_columns`: Số cột dữ liệu CSV gốc.
- `core_columns`: Số trường lõi theo Data Contract.
- `input_records`: Tổng số bản ghi đầu vào.
- `valid_records`: Bản ghi hợp lệ.
- `quarantined_records`: Bản ghi cần kiểm tra/xử lý.
- `rejected_records`: Bản ghi không được chấp nhận.

### Bước 4 — Source Audit trên dữ liệu thật

Mục đích: Kiểm toán 8 CSV CICIDS2017 trước khi xử lý.

```powershell
python -m processing.audit_source --source-dir data/raw/cicids2017
```

Các nội dung kiểm tra:

- Số lượng file và bản ghi.
- Header và số cột.
- Cột trùng tên.
- Giá trị NULL, NaN, Infinity.
- Giá trị không hợp lệ.
- Phân bố nhãn (Label Distribution).
- SHA-256 checksum.

Kết quả kỳ vọng:

```text
Files   : 8
Records : 2,830,743
```

Báo cáo:

```text
docs/source-audit.local.json
```

Lưu ý: Source Audit chỉ đọc và kiểm tra, không sửa dữ liệu CSV gốc.

### Bước 5 — Upload Dry Run

Mục đích: Kiểm tra kế hoạch upload dữ liệu lên MinIO trước khi upload thật.

```powershell
python -m storage.upload_minio --source-dir data/raw/cicids2017
```

Kết quả mong đợi:

```text
Mode: DRY_RUN
Files: 8
```

DRY_RUN dùng để:

- Kiểm tra danh sách file sẽ upload.
- Kiểm tra đường dẫn đích trên MinIO.
- Kiểm tra checksum.
- Phát hiện lỗi trước khi upload.

**Quan trọng:** DRY_RUN không upload dữ liệu thật.

---

## 4. Công cụ theo dõi

| Công cụ | Địa chỉ / File | Mục đích |
|---|---|---|
| Spark UI | http://localhost:8080 | Theo dõi Spark Cluster |
| MinIO Console | http://localhost:9001 | Kiểm tra Object Storage |
| Source Audit Report | `docs/source-audit.local.json` | Kiểm toán CSV gốc |
| Smoke Test Report | `docs/minio-smoke.local.json` | Kết quả kiểm tra Spark/MinIO |

Credentials MinIO được cấu hình trong `docker/.env`, không đưa vào Git hoặc báo cáo.

---

## 5. Phân biệt các loại kiểm thử

### Unit Testing

Kiểm tra từng module riêng lẻ.

Ví dụ: `transform.py` có chuyển String sang Long đúng không?

### Smoke Testing

Kiểm tra nhanh các thành phần chính có kết nối và hoạt động cơ bản không.

Ví dụ: Spark có đọc được 12 dòng CSV từ MinIO không?

### Integration Testing

Kiểm tra các module phối hợp với nhau.

Ví dụ:

```text
MinIO
  ↓
Data Ingestion
  ↓
Data Transformation
  ↓
Data Validation
  ↓
Data Cleaning
```

### Real Data Testing

Kiểm tra pipeline bằng 8 CSV CICIDS2017 thật.

Cần kiểm chứng số bản ghi, chất lượng dữ liệu, kết quả xử lý và log.

---

## 6. Trạng thái hiện tại

### Đã triển khai code

- [x] Spark/MinIO Configuration
- [x] Source Audit
- [x] Data Ingestion
- [x] MinIO Smoke Check
- [x] Data Transformation
- [x] Data Validation
- [x] Data Cleaning

Các mục trên đã có code theo báo cáo triển khai, nhưng vẫn cần kiểm chứng bằng test thực tế.

### Chưa hoàn thành

- [ ] Kiểm chứng tất cả Unit Tests.
- [ ] Kiểm chứng MinIO Smoke Test.
- [ ] Upload 8 CSV thật lên MinIO.
- [ ] Chạy Processing trên dữ liệu thật.
- [ ] Hoàn thiện `main.py`.
- [ ] Hoàn thiện `features.py`.
- [ ] Hoàn thiện `quality_report.py`.
- [ ] Ghi Cleaned Parquet lên MinIO.
- [ ] Thực hiện Benchmark.

---

## 7. Kết luận

Quy trình kiểm thử:

```text
Docker Environment
        ↓
Unit Testing
        ↓
MinIO Smoke Testing
        ↓
Source Audit
        ↓
Upload Dry Run
        ↓
Upload Real Dataset (sau khi xác nhận)
        ↓
Real Data Processing Test
```

**Nguyên tắc cần nhớ:**

- Unit Test PASS: Chức năng được kiểm thử hoạt động đúng.
- Smoke Test PASS: Kết nối và xử lý mẫu hoạt động.
- Real Data Test PASS: Pipeline hoạt động trên dữ liệu thực tế theo các tiêu chí đã kiểm chứng.

Mục tiêu tiếp theo: Xác minh toàn bộ các bước hiện tại hoạt động chính xác trước khi triển khai Feature Engineering, Aggregation và Benchmark.

# Big Data Storage → Processing

Đồ án môn **Nhập môn Dữ liệu lớn**, thực hiện quy trình **Storage → Processing** trên bộ dữ liệu network flow **CICIDS2017** bằng **Apache Spark/PySpark 3.5.0** và **MinIO**.

Hệ thống đọc CSV, chuẩn hóa và làm sạch dữ liệu, tạo đặc trưng, tổng hợp và lưu kết quả dưới dạng **Parquet + Snappy**.

Pipeline đã được kiểm chứng end-to-end với dữ liệu **synthetic**. Với 8 CSV thật, hồ sơ thực nghiệm hiện có xác nhận xử lý ở chế độ **chỉ đọc**; chưa có bằng chứng nghiệm thu việc ghi output thật lên MinIO.

## 1. Yêu cầu môi trường

- Windows PowerShell và Git.
- Docker Desktop (Linux containers/WSL2) và Docker Compose v2.
- Máy có đủ RAM, dung lượng đĩa và kết nối mạng để build Docker images. Môi trường thực nghiệm đã cấp khoảng 8 GiB RAM cho Docker; đây **không phải** cấu hình tối thiểu.
- Không cần cài Spark, Java hay Python riêng trên Windows để thực hiện các lệnh dưới đây.

## 2. Cấu trúc thư mục

```text
bigdata-pcap-anomaly/
├── README.md
├── requirements.txt
├── data/          # Dữ liệu mẫu, CSV nguồn và Data Contract
├── docker/        # Docker Compose, Dockerfiles và cấu hình môi trường
├── processing/    # Pipeline Spark: ingestion, validation, cleaning, output
├── storage/       # Upload dữ liệu lên MinIO
├── experiments/   # Benchmark hiệu năng
├── tests/         # Unit tests và synthetic smoke test
├── docs/          # Tài liệu và kết quả thực nghiệm
└── extraction/    # Mã khung ngoài phạm vi pipeline hiện tại
```

## 3. Cài đặt và khởi động

**Bước 1: Clone repository** và mở thư mục dự án trong PowerShell:

```powershell
git clone https://github.com/BonM2/bigdata-pcap-anomaly.git
cd bigdata-pcap-anomaly
```

**Bước 2: Tạo và chỉnh sửa cấu hình MinIO:**

```powershell
if (-not (Test-Path docker/.env)) { Copy-Item docker/.env.example docker/.env }
notepad docker/.env
```

Đặt `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` theo môi trường local; giữ `MINIO_BUCKET=bigdata-network` để sử dụng các đường dẫn ví dụ. **Không chia sẻ hoặc commit file `.env`.**

**Bước 3: Build và chạy Docker:**

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml up -d --build
```

**Bước 4: Kiểm tra dịch vụ:**

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml ps
```

Hệ thống cần có 4 dịch vụ: `minio`, `spark-master`, `spark-worker-1`, `spark-worker-2`. Kiểm tra:

- **MinIO Console:** http://localhost:9001 — đăng nhập bằng tài khoản trong `.env`.
- **Spark Master UI:** http://localhost:8080 — có **2 Workers** ở trạng thái `ALIVE`.

## 4. Chạy thử với dữ liệu synthetic

Chạy pipeline mẫu gồm 12 bản ghi để kiểm tra Spark, MinIO và thao tác ghi/đọc lại Parquet:

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master python3 -m tests.smoke_pipeline --execute
```

**Kết quả mong đợi:** job thành công, báo cáo có `run_status: VERIFIED` và **12 bản ghi = 4 VALID + 6 QUARANTINED + 2 REJECTED**.

Pipeline tạo 5 dataset Parquet đầu ra và kiểm tra đọc lại. Có thể xem các object mẫu trong MinIO Console tại `bigdata-network/smoke/pipeline/`.

> Lệnh này **có ghi dữ liệu mẫu lên MinIO**, nhưng không ghi đè CSV nguồn thật. Dữ liệu synthetic chỉ dùng để kiểm thử, không phải kết quả của CICIDS2017 thật.

## 5. Chạy với 8 CSV CICIDS2017 thật

**Bước 1: Chuẩn bị dữ liệu.**

Tải các file CSV từ [CICIDS2017 — UNB](https://www.unb.ca/cic/datasets/ids-2017.html), giải nén và đặt **đúng 8 file** vào `data/raw/cicids2017/`. Giữ nguyên tên file, header và nội dung; không commit CSV lớn lên GitHub.

```powershell
New-Item -ItemType Directory -Force data/raw/cicids2017 | Out-Null
(Get-ChildItem data/raw/cicids2017 -File -Filter '*.csv').Count
```

Kết quả đếm mong đợi là `8`. Danh sách tên file có trong [báo cáo kiểm toán nguồn](docs/reports/source-audit-report.md).

**Bước 2: Kiểm tra CSV bằng Source Audit:**

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master python3 -m processing.audit_source --source-dir /workspace/data/raw/cicids2017 --report /tmp/source-audit.json
```

Với bộ dữ liệu đã kiểm toán trong đồ án, tổng số là **2.830.743 bản ghi**. Chỉ tiếp tục khi audit thành công.

**Bước 3: Upload CSV lên MinIO.**

Xem kế hoạch upload trước (`DRY_RUN`, chưa ghi dữ liệu):

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master python3 -m storage.upload_minio --source-dir /workspace/data/raw/cicids2017 --audit-report /tmp/source-audit.json
```

Nếu thông tin đúng, thực hiện upload:

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master python3 -m storage.upload_minio --source-dir /workspace/data/raw/cicids2017 --audit-report /tmp/source-audit.json --execute
```

Lệnh `--execute` **ghi dữ liệu thật** vào `bigdata-network/raw/cicids2017/`. Kiểm tra đủ 8 object trong MinIO Console trước khi chạy Spark.

**Bước 4: Chạy pipeline.**

Kiểm tra dữ liệu trước, **chưa ghi output**:

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m processing.main --input 's3a://bigdata-network/raw/cicids2017/*.csv' --validate-only
```

Khi trạng thái `VALIDATED_NO_OUTPUT` và hậu kiểm đạt `PASS`, chạy để **ghi output thật**:

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m processing.main --input 's3a://bigdata-network/raw/cicids2017/*.csv' --execute
```

Pipeline sẽ đọc, chuẩn hóa, phân loại, làm sạch, hậu kiểm, tạo feature, tổng hợp và lưu kết quả trên MinIO.

Mỗi lần `--execute` tạo một batch mới; nếu job lỗi có thể còn output dở dang. Chỉ xác nhận hoàn thành khi có marker cuối hợp lệ.

## 6. Kiểm tra kết quả trên MinIO

Truy cập **http://localhost:9001**, vào bucket `bigdata-network`.

Với lần chạy dữ liệu thật, kiểm tra các prefix:

| Prefix | Nội dung |
|---|---|
| `cleaned/flows/` | Flow hợp lệ đã làm sạch |
| `processed/derived_features/` | Flow cùng các feature dẫn xuất |
| `processed/aggregates/` | Dữ liệu tổng hợp |
| `quality/quarantine/` | Bản ghi cần cách ly |
| `quality/rejected/` | Bản ghi bị từ chối |
| `reports/data_quality/` | Báo cáo chất lượng dữ liệu |
| `reports/performance/` | Báo cáo thời gian xử lý |

Mỗi lượt chạy có `batch_id` riêng.

Trong `reports/data_quality/<batch_id>/`, kiểm tra `quality.json`, `summary.csv` và **`_SUCCESS.json` có `run_status: COMPLETE`**. Báo cáo cần cho thấy hậu kiểm `PASS` và các output được đọc lại thành công.

Với dữ liệu mẫu, các kết quả nằm trong nhánh `smoke/pipeline/` thay vì các prefix đầu ra dữ liệu thật ở bảng trên.

## 7. Chạy kiểm thử

```powershell
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master python3 -m unittest discover -s tests -v
```

Kết quả mong đợi: `OK` và exit code `0`. Bộ kiểm thử không tự upload 8 CSV thật.

**Tài liệu chi tiết:** [Hướng dẫn xử lý](docs/processing.md) · [Data Contract](data/schemas/feature_dictionary.md) · [Benchmark](docs/benchmark-guide.md) · [Hồ sơ kiểm chứng](docs/reports/pipeline-validation.md).
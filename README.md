# Big Data Storage → Processing

Đồ án xử lý batch dữ liệu network flow CICIDS2017 bằng **Spark/PySpark 3.5.0 và MinIO**:
đọc CSV, chuẩn hóa, kiểm tra/làm sạch, tạo feature, tổng hợp và lưu **Parquet + Snappy**.
Đã kiểm chứng end-to-end bằng synthetic; 8 CSV thật mới được kiểm chứng **chỉ đọc**, chưa có bằng chứng ghi output thật lên MinIO.

## 1. Điều kiện môi trường

- Windows PowerShell, Git và Docker Desktop dùng **Linux containers/WSL2**, có Docker Compose v2.
- Docker Desktop đang chạy; có mạng để build image và đủ RAM/đĩa cho dữ liệu, cache và output.
- Môi trường thực nghiệm cấp khoảng 8 GiB RAM cho Docker; đây không phải cấu hình tối thiểu đã xác định.
- Spark, Java và Python chạy trong image; không cần cài riêng trên Windows cho các lệnh dưới.

## 2. Cấu trúc repository

```text
bigdata-pcap-anomaly/
├── README.md, requirements.txt
├── data/          # sample, schemas; CSV thật đặt tại raw/cicids2017/
├── docker/        # Compose, .env.example và Dockerfiles
├── processing/    # Pipeline, validation, cleaning, output và reports
├── storage/       # Upload CSV lên MinIO
├── extraction/    # Placeholder, ngoài pipeline đã triển khai
├── experiments/   # Benchmark và vẽ kết quả
├── tests/         # Unit/integration tests và synthetic smoke test
└── docs/          # Hướng dẫn, giải thích và bằng chứng thực nghiệm
```

## 3. Clone, cấu hình và khởi động

Mở PowerShell, chạy lần lượt; nếu đã clone thì chỉ vào thư mục gốc dự án.
```powershell
git clone https://github.com/BonM2/bigdata-pcap-anomaly.git
Set-Location bigdata-pcap-anomaly
if (-not (Test-Path docker/.env)) { Copy-Item docker/.env.example docker/.env }
notepad docker/.env
```
Trong Notepad, đặt `MINIO_ROOT_USER` và `MINIO_ROOT_PASSWORD` riêng, thay password mẫu;
giữ `MINIO_BUCKET=bigdata-network` để dùng nguyên các URI bên dưới. Không commit/chia sẻ `.env`.

Tiếp tục trong **cùng cửa sổ PowerShell, tại root**. `$dc` rút gọn tùy chọn Compose;
`$runTag` là UUID tạo tên báo cáo mới. Mở cửa sổ mới thì khai báo lại các biến.
```powershell
$dc = @('compose', '--env-file', 'docker/.env', '-f', 'docker/docker-compose.yml')
$runTag = [guid]::NewGuid().ToString('N')
$evidenceDir = 'docs/evidence/local'
New-Item -ItemType Directory -Force $evidenceDir | Out-Null
docker info --format '{{.OSType}}'
docker @dc config --quiet
docker @dc up -d --build
docker @dc ps
```
Mong đợi Docker in `linux`, cấu hình không lỗi, bốn services chạy và MinIO `healthy`.
Mở **Spark Master UI: http://localhost:8080**; đợi đủ **2 Workers ALIVE**.
Mở **MinIO Console: http://localhost:9001**, đăng nhập bằng tài khoản trong `.env`.
Sau mỗi lệnh Docker, chỉ tiếp tục khi `$LASTEXITCODE` bằng `0`; lỗi thì xem [hướng dẫn vận hành](docs/processing.md).

## 4. Chạy thử bằng synthetic

**Có ghi MinIO:** lệnh sau tự tạo bucket nếu thiếu, seed fixture và ghi batch mới dưới `smoke/`.
Object mẫu khác nội dung bị từ chối; không ghi đè raw.
```powershell
$sampleReport = "/tmp/sample-$runTag.json"
docker @dc exec -T spark-master python3 -m tests.smoke_pipeline --execute --report $sampleReport
docker cp "spark-master:$sampleReport" "$evidenceDir/sample-$runTag.local.json"
$result = Get-Content "$evidenceDir/sample-$runTag.local.json" -Raw -Encoding UTF8 | ConvertFrom-Json
$result | Select-Object run_status,batch_id,input_records,valid_records,quarantined_records,rejected_records
```
Mong đợi exit code `0`, `VERIFIED` và **12 input = 4 VALID + 6 QUARANTINED + 2 REJECTED**.
Đây là fixture tổng hợp, không phải CSV thật. Kiểm tra thêm output ở mục 6 trước khi tiếp tục.
`/tmp` là đường dẫn **trong container**; `docker cp` lưu report về Windows. `/workspace` được mount chỉ đọc.

## 5. Chạy với 8 CSV CICIDS2017 thật

Tải CSV từ [CICIDS2017 chính thức](https://www.unb.ca/cic/datasets/ids-2017.html), giải nén đúng
8 file theo [danh sách nguồn](docs/reports/source-audit-report.md). Tạo thư mục chứa dữ liệu:
```powershell
New-Item -ItemType Directory -Force data/raw/cicids2017 | Out-Null
```
Copy CSV vào `data/raw/cicids2017/`, giữ nguyên tên/header/nội dung; không đưa CSV lớn vào Git.
Hoàn tất bước synthetic trước để có bucket, rồi kiểm tra và audit:
```powershell
(Get-ChildItem data/raw/cicids2017 -File -Filter '*.csv').Count
$audit = "/tmp/audit-$runTag.json"
docker @dc exec -T spark-master python3 -m processing.audit_source --report $audit
docker @dc exec -T spark-master python3 -m storage.upload_minio --audit-report $audit
```
Count phải là `8`. Audit kiểm đúng tập tên/header và hash.
Uploader mặc định **DRY_RUN**: in 8 object dự kiến, chưa kết nối/ghi MinIO. Chỉ tiếp tục khi cả hai lệnh thành công.

**Ghi dữ liệu thật:** sau khi xem kế hoạch, lệnh dưới upload vào `raw/cicids2017/`.
Chỉ chạy một uploader; object có sẵn khác metadata hash/kích thước bị từ chối ghi đè.
```powershell
docker @dc exec -T spark-master python3 -m storage.upload_minio --audit-report $audit --execute
```
Trong Console, xác nhận bucket `bigdata-network` có đủ 8 object đúng tên tại `raw/cicids2017/`.

Kiểm tra một CSV trước, rồi đủ tám CSV; `--validate-only` tính pipeline nhưng không ghi output MinIO.
```powershell
$oneCsv = 's3a://bigdata-network/raw/cicids2017/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv'
docker @dc exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m processing.main --input $oneCsv --validate-only
docker @dc exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m processing.main --validate-only
```
Mong đợi `VALIDATED_NO_OUTPUT` và hậu kiểm PASS; `outputs` rỗng là đúng. Mặc định input là glob 8 CSV trên MinIO.
Không có cờ mode thì main chỉ in `PLAN_ONLY`. Dùng executor 2g vì mức 1g từng OOM trên đủ tám CSV.

**Ghi output dữ liệu thật:** chỉ chạy sau khi validate-only đạt và bạn quyết định xuất kết quả.
```powershell
$realReport = "/tmp/real-$runTag.json"
docker @dc exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m processing.main --execute --local-report $realReport
docker cp "spark-master:$realReport" "$evidenceDir/real-$runTag.local.json"
$result = Get-Content "$evidenceDir/real-$runTag.local.json" -Raw -Encoding UTF8 | ConvertFrom-Json
```
Mong đợi `VERIFIED`, năm output read-back PASS và marker cuối. Mỗi lượt tạo batch mới;
không overwrite nguồn/batch cũ. Job lỗi có thể để lại output dở dang, chưa được xem là hoàn thành.

## 6. Kiểm tra output trên MinIO

Dùng `$result` của lần synthetic hoặc lần execute thật vừa chạy:
```powershell
$result.batch_id
$result.post_cleaning_validation | Select-Object status,records_with_errors
$result.outputs.PSObject.Properties.Value | Select-Object path,records,read_back_status
$result.performance | Select-Object processing_duration_seconds,throughput_flows_per_second
```
Mong đợi hậu kiểm PASS, 0 lỗi hậu kiểm và **5 output** read-back PASS: cleaned, features, aggregates, quarantine, rejected.
Mở bucket `bigdata-network` trong Console, theo các `path` in ra để kiểm tra Parquet.
Report mẫu: `smoke/pipeline/report/<batch_id>/`; report thật: `reports/data_quality/<batch_id>/`.
Trong đúng batch phải có `quality.json`, `summary.csv` và **`_SUCCESS.json` có COMPLETE**.
Chỉ marker JSON cuối xác nhận toàn batch; `_SUCCESS` trong một thư mục Parquet không thay thế được.

<a id="testing"></a>

## 7. Chạy unit/integration tests

```powershell
docker @dc exec -T spark-master python3 -m unittest discover -s tests -v
```
Mong đợi unittest kết thúc `OK`, exit code `0`. Suite dùng Spark local và file tạm, không tự upload CSV thật.
Không chạy tests đồng thời benchmark; kết quả kiểm thử lịch sử nằm trong tài liệu bên dưới.

## 8. Tài liệu chuyên sâu

- [Vận hành, cấu hình và đọc lại Parquet](docs/processing.md) · [Data Contract](data/schemas/feature_dictionary.md).
- [Hồ sơ kiểm chứng](docs/reports/pipeline-validation.md) · [Báo cáo triển khai](docs/reports/storage-processing-report.md).
- [Hướng dẫn benchmark](docs/benchmark-guide.md) · [Kiến trúc hệ thống](docs/explanations/system-overview-explained.md).

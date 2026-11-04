# Chạy và kiểm tra processing

Bốn module `config.py`, `transform.py`, `validation.py`, `clean.py` triển khai
Data Contract v2.3.0. `ingestion.py` kiểm tra header trước khi đọc, giữ 79 trường
String theo vị trí (kể cả hai cột Fwd Header Length), kiểm tra độ rộng mỗi dòng,
và tính SHA-256 từng file bằng stream. Mỗi dòng vật lý là một record; CSV có
trường chứa newline không thuộc định dạng hỗ trợ của bộ CICIDS2017 này.

## Docker

Chạy tại root repository. Docker Desktop phải dùng Linux containers.

```powershell
if (-not (Test-Path docker/.env)) { Copy-Item docker/.env.example docker/.env }
New-Item -ItemType Directory -Force docs/evidence/local | Out-Null
# Sửa MINIO_ROOT_USER và MINIO_ROOT_PASSWORD trong docker/.env trước khi chạy.
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml ps
```

Không chép đè `.env` đã có. File này được gitignore. MinIO đọc credentials từ
`.env`; Spark nhận cùng credentials qua môi trường. Endpoint trong container
là `http://minio:9000`, ngoài host là `http://localhost:9000`.

Image Bitnami Spark 3.5.0 và MinIO trong Compose cũ không còn tải được ở lần kiểm
tra này. Compose mới build Spark từ `apache/spark:3.5.0`, kèm Hadoop AWS 3.3.4 và
AWS SDK 1.12.262; MinIO được build từ mã nguồn upstream release
`RELEASE.2025-10-15T17-29-55Z`. Lần build đầu cần mạng và có thể mất vài phút.

## Job kiểm tra MinIO

Kiểm toán CSV thật tại local trước khi upload (không sửa nguồn):

```powershell
python -m processing.audit_source --source-dir data/raw/cicids2017 --report docs/evidence/local/source-audit.local.json
python -m storage.upload_minio --source-dir data/raw/cicids2017 --audit-report docs/evidence/local/source-audit.local.json
```

Lệnh upload ở trên mặc định là **DRY_RUN**, kiểm tra lại SHA-256 và in kế hoạch,
không kết nối mạng hay ghi object. Sau khi đọc báo cáo và quyết định tiếp tục,
chạy lệnh sau mới upload vào `bigdata-network/raw/cicids2017/`:

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m storage.upload_minio --audit-report /workspace/docs/evidence/local/source-audit.local.json --execute
```

Upload yêu cầu đủ 8 CSV đã qua kiểm tra cấu trúc, schema/contract không đổi và
SHA-256 hiện tại khớp audit. Object raw có sẵn khác hash/kích thước sẽ gây lỗi;
không chạy nhiều uploader đồng thời. Bất thường giá trị được giữ nguyên để
pipeline xử lý theo contract, không sửa CSV trước upload.

Sau khi upload, chạy job chỉ đọc:

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m processing.check_minio --report /tmp/minio-check.json
docker cp spark-master:/tmp/minio-check.json docs/evidence/local/minio-check.local.json
```

Job xuất số cột raw/core, số record từng file, SHA-256, số malformed, số
VALID/QUARANTINED/REJECTED và schema cleaned. Không có object CSV hoặc không có
record sẽ trả exit code lỗi. `--skip-checksum` bỏ lượt đọc bổ sung tính SHA-256;
chế độ này không đủ kiểm toán lineage. File JSON local được gitignore.

Chưa có CSV thật thì dùng fixture tổng hợp 12 dòng (không phải dữ liệu kiểm toán
CICIDS2017): 4 VALID, 6 QUARANTINED, 2 REJECTED. Dữ liệu có header 79 cột, nhãn
`SYNTHETIC_TEST`, các lỗi rate/window/cast/port/flag/label và dòng thiếu/thừa cột.

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m tests.seed_minio
docker compose -f docker/docker-compose.yml exec -e INPUT_PATH=s3a://bigdata-network/smoke/cicids2017/synthetic_flow.csv spark-master python3 -m processing.check_minio --report /tmp/minio-smoke.json
docker cp spark-master:/tmp/minio-smoke.json docs/evidence/local/minio-smoke.local.json
```

Script seed chỉ ghi `smoke/`, không ghi `raw/`, không ghi đè object khác nội dung.

## Cấu hình môi trường

| Biến | Mặc định / ý nghĩa |
|---|---|
| `SPARK_MASTER_URL` | `local[2]`; Compose dùng `spark://spark-master:7077` |
| `SPARK_APP_NAME` | `cicids2017-processing` |
| `SPARK_SHUFFLE_PARTITIONS` | `4` |
| `SPARK_CACHE_BATCH_SIZE` | `1000`; giảm đỉnh bộ nhớ khi cache raw evidence có 79 cột |
| `SPARK_STORAGE_LEVEL` | `DISK_ONLY`; có thể chọn `MEMORY_AND_DISK` cho workload vừa bộ nhớ executor |
| `SPARK_EXECUTOR_MEMORY`, `SPARK_EXECUTOR_CORES` | `1g`, `2` |
| `SPARK_PACKAGES` | `org.apache.hadoop:hadoop-aws:3.3.4`; rỗng trong image đã cài JAR |
| `MINIO_ENDPOINT` | `http://localhost:9000` khi chạy ngoài Docker |
| `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Bắt buộc; fallback `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| `MINIO_BUCKET`, `MINIO_REGION` | `bigdata-network`, `us-east-1` |
| `SCHEMA_PATH` | `data/schemas/raw_flow_schema.json` tính từ vị trí module |
| `INPUT_PATH` | `s3a://<bucket>/raw/cicids2017/*.csv` |
| `CLEANED_PATH`, `QUARANTINE_PATH`, `REJECTED_PATH` | Prefix `cleaned/flows`, `quality/quarantine`, `quality/rejected` |
| `AGGREGATES_PATH`, `REPORT_PATH` | Prefix `processed/aggregates`, `reports/data_quality` |
| `FEATURES_PATH`, `PERFORMANCE_PATH` | Prefix `processed/derived_features`, `reports/performance` |
| `BATCH_ID`, `PIPELINE_VERSION` | UUID sinh mỗi cấu hình, `0.1.0` |

## API và chính sách

```python
from processing.config import ProcessingConfig, create_spark_session
from processing.ingestion import read_raw_flows
from processing.clean import clean_flows

config = ProcessingConfig.from_env()
spark = create_spark_session(config)
raw, manifest = read_raw_flows(spark, config.input_path)
result = clean_flows(raw, config)
result.valid       # 22 trường lõi + 12 metadata/cờ chất lượng
result.quarantined # Có raw values và reason codes để điều tra
result.rejected    # Có raw record lỗi cấu trúc
result.counts()    # input = valid + quarantined + rejected
```

- Integer/Long chỉ nhận chuỗi số nguyên có dấu tùy chọn. Chuỗi thập phân, số mũ,
  lỗi cú pháp và overflow được ghi lỗi; không làm tròn hay cắt phần thập phân.
- Rate null/rỗng/NaN/Infinity/âm/lỗi cast → NULL + invalid flag; không tự loại row.
- Window `-1` → NULL + sentinel flag; âm khác `-1` → QUARANTINED.
- Các feature bắt buộc null/rỗng/lỗi cast/âm/không hữu hạn → QUARANTINED;
  port phải trong 0–65535, TCP flag phải 0/1. Window null/rỗng không có sentinel
  cũng bị cách ly để xác minh nguồn thiếu.
- Label null/rỗng → QUARANTINED; ký tự thay thế Unicode → ghi mã lỗi mềm,
  giữ nguyên label. Không trim hay tự gán nhãn.
- Lỗi cấu trúc ưu tiên REJECTED kể cả khi cùng row có lỗi miền giá trị.
- Giữ duplicate và backward packet = 0. Không có khóa quan sát để deduplicate.
- Reason code có dạng `CAST_FAILURE:field`, `NULL:field`, `EMPTY:field`,
  `NAN:field`, `INFINITY:field`, `NEGATIVE:field`, `SENTINEL:field`,
  `OUT_OF_RANGE:destination_port`, `INVALID_FLAG:field`, `INVALID_LABEL`,
  `LABEL_ENCODING_REPLACEMENT`, `MALFORMED_CSV`. Một record có thể có nhiều mã.

`main.py` thực hiện đầy đủ ingest → transform → validate → clean → post-validation
→ features → aggregates → Parquet/read-back → report. `clean_flows()` vẫn tương
thích API cũ; pipeline dùng `clean_classified_flows()` để không cast/validate lặp.

## Chạy pipeline hoàn chỉnh

Mặc định chỉ in kế hoạch, không đọc CSV hoặc ghi output:

```powershell
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m processing.main
```

Kiểm thử synthetic qua MinIO và Spark cluster; chỉ dùng `smoke/`, không dùng raw:

```powershell
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m tests.smoke_pipeline
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m tests.smoke_pipeline --execute --report /tmp/pipeline-smoke.json
docker cp spark-master:/tmp/pipeline-smoke.json docs/evidence/local/pipeline-smoke.local.json
```

Kiểm thử sample hoàn toàn local trong container tạm, không cần MinIO:

```powershell
docker compose -f docker/docker-compose.yml run --rm --no-deps -T spark-master python3 -m processing.main --local --input file:///workspace/data/sample/sample_flow.csv --local-output /tmp/pipeline-sample --execute
```

Với **một CSV thật đã có trên MinIO**, kiểm tra chỉ đọc trước:

```powershell
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m processing.main --input s3a://bigdata-network/raw/cicids2017/Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv --validate-only --local-report /tmp/one-file-validation.json
```

Nếu file chưa được upload, có thể kiểm tra bản local với `--local`,
`--input file:///workspace/data/raw/cicids2017/<file>.csv`, và
`--local-output /tmp/one-file-output-unused`. `--validate-only` không tạo output
Parquet; `--local-report` chỉ ghi báo cáo đo đạc vào file mới.

Sau khi được phép ghi kết quả dữ liệu thật, dùng cùng `--input` với `--execute`.
Chỉ mở rộng sang glob 8 CSV khi kiểm tra một CSV đạt. Pipeline không tự upload
input; `storage.upload_minio` vẫn là bước riêng cần quyết định rõ ràng.

Với 8 CSV, dùng `SPARK_EXECUTOR_MEMORY=2g` và `SPARK_STORAGE_LEVEL=DISK_ONLY`
(mỗi worker Compose đã cấp 2 GB). Mức executor 1 GB đã gặp OOM ở thử nghiệm
toàn bộ input; lưu cache trên đĩa vẫn cần heap để tạo từng batch trung gian.

```powershell
docker compose -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g -e SPARK_STORAGE_LEVEL=DISK_ONLY spark-master python3 -m processing.main --input "file:///workspace/data/raw/cicids2017/*.csv" --validate-only --local-report /tmp/eight-files-validation.json
```

### Chính sách output và hoàn thành

- Mỗi prefix được nối thêm `/<BATCH_ID>`; UUID mới là mặc định. Không chạy hai
  writer cùng batch ID. Batch đã tồn tại bị từ chối, không overwrite/append.
- Chặn output vào `raw/`, root, đường dẫn traversal/glob và các prefix chồng lấn
  nhau hoặc chồng lấn thư mục nguồn. Object nguồn phải bất biến suốt job.
- Cleaned: 22 trường lõi + 12 metadata. Derived features nằm riêng ở
  `processed/derived_features/<batch>`. Quarantine/reject giữ `_raw_values`,
  `_raw_record`, lineage, mã lỗi và trạng thái trước cleaning.
- Cả 5 output (cleaned, features, aggregates, quarantine, rejected) đều Parquet
  Snappy; kiểm tra schema/types và count bằng Spark sau ghi, kể cả output rỗng.
  Các bảng flow còn được kiểm tra phân bố label/source/status sau đọc lại.
- Report có `quality.json`, `summary.csv`; performance có `performance.json`.
  **Chỉ `_SUCCESS.json` trong thư mục quality report xác nhận batch hoàn thành.**
  `_SUCCESS` do Spark tự tạo trong từng thư mục Parquet không đại diện toàn job.
- Job lỗi trả exit code khác 0, cố ghi `_FAILED.json`, giữ output dở dang để
  điều tra; chạy lại bằng batch ID mới. Không tự xóa hoặc tiếp tục batch lỗi.
- `quality.json` dùng trạng thái `VERIFIED`; consumer phải kiểm tra marker cuối.
  Chế độ chỉ đọc trả `VALIDATED_NO_OUTPUT`, không tuyên bố storage đã kiểm chứng.

### Post-validation, features và số liệu báo cáo

Post-validation kiểm tra schema/type, NULL hợp lệ có cờ, NaN/Infinity, miền giá
trị, nhãn so với raw và cờ so với raw. Lỗi giá trị cứng còn trong quarantine hoặc
reject là bằng chứng cần giữ; bất kỳ vi phạm ở tập VALID hoặc sai nhãn/cờ đều
làm job thất bại. Không tự nâng trạng thái quarantine thành valid.

Đối soát trước/sau kiểm tra `input = valid + quarantined + rejected`, bảng
chuyển trạng thái và multiset raw evidence bằng `exceptAll` (giữ đúng số lần
xuất hiện của duplicate). Pipeline persist tại các ranh giới để tránh tính lại
và tránh biểu thức Spark lồng quá lớn; mặc định dùng `DISK_ONLY` để giảm áp lực
heap; đủ 8 CSV đã chạy thành công với executor 2 GB. Caller của API hậu
kiểm cũng nên materialize annotated input khi kết hợp nhiều bước.

| Feature | Định nghĩa/chính sách |
|---|---|
| `total_packets` | forward + backward, Decimal(20,0) tránh overflow Long |
| `total_bytes` | tổng payload bytes forward + backward, Decimal(20,0); không phải wire bytes |
| `fwd_packet_ratio` | forward / total_packets; NULL nếu tổng = 0 hoặc nguồn lỗi |
| `bwd_packet_ratio` | backward / total_packets; NULL nếu tổng = 0 hoặc nguồn lỗi |

Aggregate table có `grouping` = `batch`, `label`, `source_file`,
`source_file_label`. `flow_count` và tỷ lệ trạng thái dùng **tất cả** input;
`total_packets_sum`, `total_bytes_sum`, `avg_flow_duration` chỉ dùng **VALID**.
Duration giữ đơn vị microsecond. Tổng rỗng = 0, average và tỷ lệ có mẫu số 0 =
NULL. Cột dimension NULL ở nhóm roll-up được phân biệt bằng `grouping`.
Không dùng timestamp/IP hoặc cộng TCP flag thành số packet.

Report trước/sau đếm cell của 22 trường lõi theo từng field/file, không cộng chúng thành số dòng
lỗi duy nhất. `null_count` là NULL sau cast (bao gồm NULL do lỗi cast và NULL
được phép sau cleaning). `cast_error_count` ở cả hai giai đoạn là lịch sử lỗi
nguồn được giữ; `post_cleaning_validation.error_counts` mới là vi phạm hậu kiểm.
`records_with_reason_codes` bao gồm cả lỗi mềm ở VALID. `invalid_range_count`
đếm mỗi cell một lần dù cell vi phạm nhiều quy tắc miền. Label giữ nguyên,
bao gồm khoảng trắng và ký tự thay thế Unicode.

## Benchmark có số liệu thực

Hướng dẫn mới cho **size, 1/2 workers và 4/8/16/32 partitions**, warmup, speedup,
CSV/JSON và biểu đồ: [benchmark-guide.md](benchmark-guide.md). Kết quả đo và các
giới hạn được tổng hợp trong [storage-processing-report.md](reports/storage-processing-report.md).

Các runner mặc định in kế hoạch, cần `--execute` để chạy; không thay đổi topology
Docker hoặc upload input. Mỗi trial ghi batch mới và report riêng. Ví dụ nhỏ:

```powershell
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m experiments.benchmark_partitions --local --input file:///workspace/data/sample/sample_flow.csv --local-output /tmp/benchmark-output --dataset-kind synthetic --partitions 2 4 --repeats 2 --results /tmp/partitions.json --execute
docker cp spark-master:/tmp/partitions.json docs/evidence/local/benchmark-partitions.local.json
python -m experiments.plot_results docs/evidence/local/benchmark-partitions.local.json --csv docs/evidence/local/benchmark-summary.local.csv
```

- `experiments.benchmark_size`: truyền `--inputs <URI1> <URI2> ...`; dùng byte
  size và SHA-256 đo được, không tự gọi workload là 1/3/5 GB.
- `experiments.benchmark_workers`: truyền `--master-ui http://spark-master:8080`
  và `--expected-workers 1` hoặc `2`; xác nhận worker ALIVE từ Master trước/sau
  mỗi trial. Người vận hành cấu hình topology trước. Chạy cùng input, partitions,
  cores/memory và số repeat ở hai topology, lưu vào hai result file khác nhau.
- Runner tái sử dụng một Spark session; mặc định chạy một warmup lưu riêng và loại
  khỏi thống kê, đảo thứ tự cấu hình giữa các lần lặp. Thời gian bao gồm đọc,
  checksum, xử lý, tính QA, ghi/đọc Parquet; không bao gồm khởi tạo Spark và ghi
  JSON/CSV cuối. Công bố thứ tự trial vì cache/JIT có thể ảnh hưởng sample nhỏ.
- Báo cáo pipeline để `worker_count = null` nếu chưa có bằng chứng topology;
  benchmark có `--master-ui` và `--expected-workers` điền số đã xác minh ở mọi dimension.
  Không đồng nhất executor với worker.
- Summarizer từ chối benchmark FAILED, tách nhóm theo checksum, kích thước,
  cấu hình và phạm vi đo; xuất min/median/max, throughput và speedup nếu có baseline
  tương thích. `--png` tùy chọn cần
  matplotlib. Sample 12 dòng chỉ kiểm tra chức năng, không chứng minh scalability.

## Kiểm thử

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m unittest discover -s tests -v
```

Tests dùng Spark local và file tạm trong container, bật ANSI để kiểm tra lỗi
cast, kiểm tra kiểu/precision/label, quy tắc chất lượng, đối soát record, SHA-256,
header/row width và ghi–đọc lại Parquet Snappy.

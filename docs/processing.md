# Chạy và kiểm tra processing

Bốn module `config.py`, `transform.py`, `validation.py`, `clean.py` triển khai
Data Contract v2.3.0. `ingestion.py` kiểm tra header trước khi đọc, giữ 79 trường
String theo vị trí (kể cả hai cột Fwd Header Length), kiểm tra độ rộng mỗi dòng,
và tính SHA-256 từng file bằng stream. Mỗi dòng vật lý là một record; CSV có
trường chứa newline không thuộc định dạng hỗ trợ của bộ CICIDS2017 này.

## Docker

Chạy tại root repository. Docker Desktop phải dùng Linux containers.

```powershell
Copy-Item docker/.env.example docker/.env
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
python -m processing.audit_source --source-dir data/raw/cicids2017
python -m storage.upload_minio --source-dir data/raw/cicids2017
```

Lệnh upload ở trên mặc định là **DRY_RUN**, kiểm tra lại SHA-256 và in kế hoạch,
không kết nối mạng hay ghi object. Sau khi đọc báo cáo và quyết định tiếp tục,
chạy lệnh sau mới upload vào `bigdata-network/raw/cicids2017/`:

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m storage.upload_minio --execute
```

Upload yêu cầu đủ 8 CSV đã qua kiểm tra cấu trúc, schema/contract không đổi và
SHA-256 hiện tại khớp audit. Object raw có sẵn khác hash/kích thước sẽ gây lỗi;
không chạy nhiều uploader đồng thời. Bất thường giá trị được giữ nguyên để
pipeline xử lý theo contract, không sửa CSV trước upload.

Sau khi upload, chạy job chỉ đọc:

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m processing.check_minio --report /tmp/minio-check.json
docker cp spark-master:/tmp/minio-check.json docs/minio-check.local.json
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
docker cp spark-master:/tmp/minio-smoke.json docs/minio-smoke.local.json
```

Script seed chỉ ghi `smoke/`, không ghi `raw/`, không ghi đè object khác nội dung.

## Cấu hình môi trường

| Biến | Mặc định / ý nghĩa |
|---|---|
| `SPARK_MASTER_URL` | `local[2]`; Compose dùng `spark://spark-master:7077` |
| `SPARK_APP_NAME` | `cicids2017-processing` |
| `SPARK_SHUFFLE_PARTITIONS` | `4` |
| `SPARK_EXECUTOR_MEMORY`, `SPARK_EXECUTOR_CORES` | `1g`, `2` |
| `SPARK_PACKAGES` | `org.apache.hadoop:hadoop-aws:3.3.4`; rỗng trong image đã cài JAR |
| `MINIO_ENDPOINT` | `http://localhost:9000` khi chạy ngoài Docker |
| `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Bắt buộc; fallback `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| `MINIO_BUCKET`, `MINIO_REGION` | `bigdata-network`, `us-east-1` |
| `SCHEMA_PATH` | `data/schemas/raw_flow_schema.json` tính từ vị trí module |
| `INPUT_PATH` | `s3a://<bucket>/raw/cicids2017/*.csv` |
| `CLEANED_PATH`, `QUARANTINE_PATH`, `REJECTED_PATH` | Prefix `cleaned/flows`, `quality/quarantine`, `quality/rejected` |
| `AGGREGATES_PATH`, `REPORT_PATH` | Prefix `processed/aggregates`, `reports/data_quality` |
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

`main.py`, aggregation và báo cáo đầy đủ theo batch/file vẫn là phần pipeline
chưa triển khai; job smoke chỉ kiểm tra đọc và xử lý trong bộ nhớ. Parquet
round-trip được kiểm tra trong unit tests, chưa phải xuất toàn bộ dataset thật.

## Kiểm thử

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m unittest discover -s tests -v
```

Tests dùng Spark local và file tạm trong container, bật ANSI để kiểm tra lỗi
cast, kiểm tra kiểu/precision/label, quy tắc chất lượng, đối soát record, SHA-256,
header/row width và ghi–đọc lại Parquet Snappy.

# Big Data PCAP Pipeline

**Đồ án học phần:** Nhập môn Dữ liệu lớn  
**Quy trình được chọn:** **Storage → Processing**  
**Dữ liệu:** CICIDS2017 — dữ liệu network flow từ các tệp CSV đã trích xuất từ PCAP  
**Công nghệ chính:** MinIO (S3-compatible Data Lake), Apache Spark / PySpark, Spark SQL, Apache Parquet, Snappy và Docker Compose

> **Phạm vi:** Repository tập trung vào đọc dữ liệu từ Storage, làm sạch, chuẩn hóa, chuyển đổi, tổng hợp bằng Spark và ghi kết quả trở lại Data Lake. Huấn luyện mô hình ML, dự đoán tấn công, cảnh báo, dashboard và hoạt động nghiên cứu khoa học **không phải sản phẩm bắt buộc của đồ án này**. Các script PCAP extraction và upload chỉ hỗ trợ việc chuẩn bị dữ liệu đầu vào.
>
> **Trạng thái tài liệu:** README mô tả kiến trúc và cách triển khai dự kiến theo cấu trúc repository. Vì chưa đối chiếu nội dung thực tế của các file Python, Docker Compose và schema, những lệnh phụ thuộc entry point/CLI được ghi rõ là **mẫu cần xác minh**, không được xem là log chạy thành công.

## 1. Mục tiêu

Xây dựng một pipeline batch **Storage → Processing** có thể:

1. Đọc dữ liệu luồng mạng đã lưu trong **MinIO** bằng Spark và giao thức S3A.
2. Ánh xạ tên trường, ép kiểu, kiểm tra và làm sạch dữ liệu theo **Data Contract**.
3. Tạo các đặc trưng dẫn xuất hợp lệ và thực hiện tổng hợp bằng Spark DataFrame / Spark SQL.
4. Lưu dữ liệu đầu ra dạng **Parquet + Snappy** mà không thay đổi dữ liệu raw.
5. Sinh thống kê chất lượng dữ liệu, thời gian xử lý, log và kết quả benchmark có thể tái lập.

**Sản phẩm tối thiểu:** Spark job xử lý phân tán, minh chứng dữ liệu trước/sau, Data Quality Report và số liệu thời gian xử lý.

### 1.1. Đặc trưng Big Data được thể hiện

- **Volume:** nhiều tệp CSV với tổng khoảng **2.830.743 network flow** (xấp xỉ **843,66 MiB** trong bộ dữ liệu đã được kiểm toán); đánh giá khả năng mở rộng khi tăng tải. Dung lượng này là quy mô bộ dữ liệu thực tế, không tự đồng nhất với quy mô sản xuất rất lớn.
- **Veracity:** dữ liệu có Null/NaN/Infinity, giá trị thời lượng âm, sentinel `-1`, header trùng và nhãn có vấn đề mã hóa; cần cơ chế kiểm soát chất lượng và truy vết.

Pipeline hiện tại là **batch processing**; không khẳng định có xử lý streaming hoặc tốc độ dữ liệu phát sinh liên tục.

## 2. Kiến trúc và luồng dữ liệu

```text
[8 CSV CICIDS2017 (raw network flow)]
                   |
                   | Storage: MinIO / S3-compatible
                   v
      s3a://<bucket>/raw/cicids2017/
                   |
                   | Spark read CSV (S3A)
                   v
         [Apache Spark / PySpark]
          1. Validate raw header
          2. Map schema + cast types
          3. Validate + clean + quarantine
          4. Transform / derived features
          5. Aggregate by source_file / label
                   |
             Parquet + Snappy
                   v
      s3a://<bucket>/cleaned/flows/
      s3a://<bucket>/processed/aggregates/
      s3a://<bucket>/quality/quarantine/
      s3a://<bucket>/reports/data_quality/
      s3a://<bucket>/reports/performance/
```

**Ranh giới thực hiện:** bắt đầu khi CSV đã tồn tại tại Storage, kết thúc khi Spark ghi được đầu ra đã chuẩn hóa/tổng hợp kèm số liệu kiểm tra. `extraction/` và `storage/upload_minio.py` là phần hỗ trợ chuẩn bị input, không phải trọng tâm đánh giá Storage → Processing.

Xem thêm sơ đồ: [`docs/architecture.png`](docs/architecture.png) và [`docs/data_flow.png`](docs/data_flow.png) (cần cập nhật để phản ánh đúng phiên bản code chạy thực tế).

## 3. Cấu trúc repository

```text
bigdata-pcap-pipeline/
├── .gitignore
├── README.md
├── requirements.txt
│
├── docker/
│   ├── docker-compose.yml             # MinIO + Spark Master + 2 Workers (theo thiết kế)
│   └── spark/
│       └── Dockerfile                 # Tuỳ chọn: JAR, dependency cho Spark image
│
├── data/
│   ├── sample/
│   │   ├── sample.pcap                # PCAP nhỏ để kiểm thử extraction
│   │   └── sample_flow.csv            # CSV nhỏ để kiểm thử processing
│   └── schemas/
│       ├── raw_flow_schema.json       # Schema đầu vào / ánh xạ nguồn
│       └── feature_dictionary.md      # Data Contract / từ điển feature
│
├── extraction/
│   ├── extract_flow.py                # PCAP -> flow (phần hỗ trợ đầu vào)
│   └── utils.py
│
├── storage/
│   ├── minio_client.py                # Kết nối client MinIO/S3
│   └── upload_minio.py                # Upload CSV lên vùng raw
│
├── processing/
│   ├── __init__.py
│   ├── main.py                        # Điều phối Spark job
│   ├── config.py                      # Cấu hình Spark, MinIO, paths
│   ├── clean.py                       # Làm sạch và chính sách lỗi
│   ├── transform.py                   # Chuẩn hoá dữ liệu, ép kiểu
│   └── features.py                    # Derived features / aggregation hợp lệ
│
├── experiments/
│   ├── benchmark_size.py              # Đánh giá theo kích thước input
│   ├── benchmark_workers.py           # Đánh giá khi thay đổi số worker
│   ├── benchmark_partitions.py        # Đánh giá khi thay đổi partition
│   └── plot_results.py               # Vẽ biểu đồ từ số đo thực tế
│
└── docs/
    ├── architecture.png
    ├── data_flow.png
    └── report_draft.md
```

### 3.1. Trách nhiệm và giao diện giữa các module

| Module | Trách nhiệm | Giao diện dự kiến |
|---|---|---|
| `storage/minio_client.py` | Tạo client, thao tác với MinIO | Bucket / object key / kết nối |
| `storage/upload_minio.py` | Đẩy raw CSV, không ghi đè thiếu kiểm soát | Local CSV → MinIO `raw/` |
| `processing/config.py` | Tập trung cấu hình Spark, S3A, input/output | Env + cấu hình pipeline |
| `processing/transform.py` | Map tên cột, resolve header trùng, cast kiểu | Raw Spark DataFrame → standardized DataFrame |
| `processing/clean.py` | Kiểm tra và xử lý bản ghi lỗi theo chính sách | Standardized DataFrame → cleaned / quarantined |
| `processing/features.py` | Tính feature dẫn xuất có ngữ nghĩa và bảng aggregate | Cleaned DataFrame → derived/aggregate DataFrame |
| `processing/main.py` | Điều phối đọc, xử lý, ghi và đo thời gian | MinIO raw → Parquet + reports |
| `experiments/*.py` | Benchmark theo kích thước, worker, partition | Cấu hình → thời gian, throughput, log |

## 4. Dữ liệu đầu vào

### 4.1. Nguồn và quy mô

Nguồn đầu vào chính của môn Big Data là **8 CSV CICIDS2017** đã trích xuất đặc trưng flow, không phải gói tin PCAP trực tiếp:

| File | Số flow (baseline kiểm toán) |
|---|---:|
| `Monday-WorkingHours.pcap_ISCX.csv` | 529.918 |
| `Tuesday-WorkingHours.pcap_ISCX.csv` | 445.909 |
| `Wednesday-workingHours.pcap_ISCX.csv` | 692.703 |
| `Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv` | 170.366 |
| `Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv` | 288.602 |
| `Friday-WorkingHours-Morning.pcap_ISCX.csv` | 191.033 |
| `Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv` | 286.467 |
| `Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv` | 225.745 |
| **Tổng** | **2.830.743** |

Các giá trị trên là số liệu nền của bộ dữ liệu đã cung cấp, **không phải đầu ra của job Spark**. Khi chạy thật cần đối chiếu lại counts và checksum của input.

### 4.2. Schema và các giới hạn quan trọng

- Mỗi CSV nguồn có **79 cột header**, trong đó `Fwd Header Length` lặp ở vị trí cột **35 và 56** (đếm từ 1). Không đổi tên cột hàng loạt khi chưa xử lý trùng header.
- **Core cleaned schema** trong Data Contract v2.3.0 gồm **21 feature mạng + 1 `label`**. Trường metadata và flags kiểm soát chất lượng nằm ngoài 22 trường lõi.
- CSV hiện có **không chứa** `source_ip`, `destination_ip`, `source_port`, `protocol`, `timestamp`, `flow_id` hoặc `ip_version`. Không tự tạo giá trị cho các trường không có sẵn; tên ngày trong filename không thay thế event timestamp.
- Nhóm TCP flag trong bộ nguồn có thể có giá trị 0/1; **không diễn giải tổng các trường này thành số packet SYN/ACK thực**.
- Phân tích trong môn học tập trung vào bộ dữ liệu CICIDS2017 liên quan đến IPv4; không tuyên bố đã kiểm định `ip_version` theo từng flow khi CSV không có trường này.

**Tài liệu tham chiếu:** [`data/schemas/feature_dictionary.md`](data/schemas/feature_dictionary.md) — phiên bản áp dụng đề xuất: *Network Flow Feature Dictionary — Storage → Processing, v2.3.0*; [`data/schemas/raw_flow_schema.json`](data/schemas/raw_flow_schema.json). Cần kiểm tra hai tài liệu khớp nhau trước khi xử lý.

## 5. Quy tắc Storage → Processing

### 5.1. Standardization

- Validate số lượng/vị trí cột và header trước khi parse.
- Mapping tên nguồn → `snake_case` theo bảng tường minh.
- Cast đúng kiểu Spark SQL (`Integer`, `Long`, `Double`, `String`) và ghi nhận lỗi cast.
- Giữ nguồn dữ liệu raw bất biến; dữ liệu cleaned phải có metadata truy vết được đến source.

### 5.2. Data Quality / Cleaning

- Kiểm tra `NULL`, `NaN`, `Infinity`, giá trị âm không hợp lệ, nhãn lỗi và duplicate có căn cứ.
- Không mặc định thay `Infinity` bằng `0` hay xóa mọi bản ghi có `0` packet backward.
- Với `Init_Win_bytes_* = -1`, xử lý theo ngữ nghĩa sentinel được Data Contract mô tả; không coi là kích thước cửa sổ hợp lệ.
- Giữ nguyên nhãn nguồn; mọi mapping thêm phải có mô tả rõ ràng.
- Cần phân biệt các trạng thái `VALID`, `QUARANTINED`, `REJECTED` và đối soát số lượng bản ghi.

### 5.3. Transformation / Aggregation

- Chỉ tạo feature có công thức và điều kiện dữ liệu xác định (ví dụ `total_packets = total_fwd_packets + total_backward_packets` khi cả hai hợp lệ).
- Aggregate **theo `source_file`, `label`** hoặc các trường thực sự có trong schema.
- **Không** tính thống kê theo `source_ip` hoặc time window của sự kiện từ bộ CSV này do thiếu trường cần thiết.
- `features.py` không được suy diễn TCP flag 0/1 thành số lượng packet thật.

### 5.4. Đầu ra và Data Lake layout (đề xuất theo Data Contract)

Tên bucket và prefix bên dưới là **quy ước mục tiêu**, phải được đồng bộ với `processing/config.py` và `docker/docker-compose.yml` trước khi chạy:

```text
bigdata-network/                      # Tên bucket đề xuất
├── raw/
│   └── cicids2017/                   # 8 CSV không chỉnh sửa
├── cleaned/
│   └── flows/                        # Parquet + Snappy
├── processed/
│   ├── derived_features/             # Tuỳ chọn
│   └── aggregates/                   # Parquet + Snappy
├── quality/
│   ├── quarantine/                   # Nếu có record cách ly
│   └── rejected/                     # Nếu có record bị từ chối
└── reports/
    ├── data_quality/                 # JSON/CSV theo batch
    └── performance/                  # Time / throughput / config
```

**Bắt buộc kiểm chứng:** Spark đọc lại được file Parquet đã ghi, kiểm tra `schema`, số lượng bản ghi và `label` sau xử lý. Không dùng `overwrite` lên prefix `raw/`.

### 5.5. Data Quality Report

Báo cáo theo batch và theo source file nên có:

```text
batch_id, source_file, schema_version, pipeline_version
input_records, valid_records, quarantined_records, rejected_records
null_count, nan_count, infinity_count, invalid_range_count
cast_failure_count, label_distribution_before, label_distribution_after
cleaned_output_records, aggregate_output_records
processing_duration_seconds, throughput_flows_per_second
spark_app_id, worker_count, partition_count, run_status
```

Quy tắc kiểm kê khi mỗi input record có đúng một trạng thái:

```text
input_records = valid_records + quarantined_records + rejected_records
```

Phải công bố rõ mẫu số và phạm vi đo throughput; báo cáo parser corrupt records riêng nếu có.

## 6. Chuẩn bị môi trường

### 6.1. Yêu cầu

- Docker Engine và Docker Compose v2.
- Python với phiên bản tương thích với PySpark trong `requirements.txt` và Docker image.
- Java, Spark, Hadoop S3A connector và AWS SDK **tương thích với phiên bản Spark/Hadoop thực tế** nếu chạy Spark ngoài container đã chuẩn bị.
- Có đủ CPU/RAM/dung lượng lưu trữ cho bộ CSV và output trung gian.
- Quyền truy cập tới MinIO bucket và endpoint từ mạng container chạy Spark.

Không cần cài mọi công cụ `Zeek`, `TShark`, `NFStream` để chạy Storage → Processing nếu đã sử dụng CSV flow có sẵn.

### 6.2. Kiểm tra và khởi động hạ tầng

Chạy từ thư mục gốc repository:

```bash
# Kiểm tra cú pháp file Compose trước khi chạy
docker compose -f docker/docker-compose.yml config

# Khởi động các dịch vụ được định nghĩa trong Compose
docker compose -f docker/docker-compose.yml up -d --build

# Xem dịch vụ và trạng thái
docker compose -f docker/docker-compose.yml ps

# Xem log (tên service thực tế lấy từ kết quả 'docker compose ... config --services')
docker compose -f docker/docker-compose.yml logs --tail=100
```

**Lưu ý:** Các lệnh Compose phía trên dùng được khi Docker và file Compose hợp lệ. Không giả định tên container, cổng UI, tài khoản MinIO hay địa chỉ Spark Master: cần lấy chúng từ file `docker/docker-compose.yml` hiện tại. Để xem service:

```bash
docker compose -f docker/docker-compose.yml config --services
```

### 6.3. Cấu hình kết nối

Không commit access key/secret key. Các giá trị cần cấu hình (đặt qua môi trường hoặc cơ chế cấu hình thực tế trong repository):

| Nhóm | Thông tin cần có |
|---|---|
| MinIO | Endpoint từ **mạng Spark container** và bucket |
| S3A | Access key, secret key, `s3a://` path, path-style access, HTTP/HTTPS |
| Spark | Master URL, executor/worker cores, memory, số partitions |
| Pipeline | Raw CSV prefix, cleaned prefix, aggregates prefix, batch ID |

Nếu `processing/config.py` chưa hỗ trợ biến môi trường, cần bổ sung trước khi dùng một bộ lệnh chạy cố định. Tránh đưa khóa thật vào ảnh chụp và log nộp bài.

## 7. Hướng dẫn chạy pipeline

### Bước 1 — Kiểm tra dữ liệu đầu vào

- Dùng `data/sample/sample_flow.csv` để kiểm thử nhanh schema và quy tắc làm sạch.
- Sử dụng `storage/upload_minio.py` để upload 8 CSV vào `raw/cicids2017/` **sau khi kiểm tra tham số/entry point thực tế của script**.
- Đối chiếu số object, tên file, kích thước và (nếu có) checksum trên MinIO.

### Bước 2 — Chạy Spark job Storage → Processing

`processing/main.py` là entry point theo cấu trúc dự án. Lệnh Spark dưới đây chỉ là **khung tham khảo**, không bảo đảm chạy được nguyên văn nếu `main.py` đang dùng tham số/đường dẫn khác:

```bash
# Ví dụ — thay URL master, package/JAR, path và tham số theo môi trường thật
spark-submit \
  --master spark://<spark-master-host>:<port> \
  processing/main.py
```

Khi chạy bằng Docker, vị trí file Python phải tồn tại **trong container driver** (bind mount hoặc copy image); endpoint S3A phải dùng địa chỉ container truy cập được, không mặc định dùng `localhost`.

Quy trình logic kỳ vọng:

```text
Read raw CSV
  → Verify source header & schema
  → Cast / standardize
  → Validate / clean / quarantine
  → Derive features / aggregates
  → Write Parquet + technical report
  → Read-back verification
```

### Bước 3 — Kiểm tra kết quả

Cần thu thập bằng chứng rằng:

- Có các object trong `cleaned/flows/` và `processed/aggregates/`.
- Dữ liệu Parquet đọc được và có schema 22 trường lõi (cộng metadata được khai báo).
- Số lượng input/output và trạng thái chất lượng được đối soát.
- Có thời gian hoàn thành job và Spark application log.
- Dữ liệu `raw/` không bị chỉnh sửa.

Nếu pipeline chưa tự tạo báo cáo chất lượng và thời gian, các đầu ra đó vẫn là **hạng mục cần hoàn thiện** trước khi nghiệm thu, không phải mục đã thực hiện.

## 8. Thực nghiệm và benchmark

Các script trong `experiments/` phục vụ kiểm chứng yêu cầu khả năng mở rộng; CLI của từng script cần được đọc trực tiếp trước khi chạy:

| Script | Câu hỏi cần trả lời | Ví dụ cấu hình so sánh (mục tiêu) |
|---|---|---|
| `benchmark_size.py` | Thời gian/throughput thay đổi khi tăng input? | 1 GB, 3 GB, 5 GB *nếu tạo được tải tương ứng* |
| `benchmark_workers.py` | Có speedup khi tăng số worker không? | 1 worker so với 2 workers |
| `benchmark_partitions.py` | Bao nhiêu partition phù hợp workload? | 4 / 8 / 16 / 32 |
| `plot_results.py` | Kết quả đo thể hiện xu hướng nào? | Biểu đồ từ log/CSV thực nghiệm |

Mỗi lần benchmark cần lưu **input size**, số record, worker count, cores, memory, số partition, phạm vi thời gian đo, duration và throughput. Chạy nhiều lần để tránh kết luận dựa vào một phép đo ngẫu nhiên. Nếu nhân bản flow chỉ nhằm tạo tải benchmark, phải ghi nhãn **synthetic benchmark load**; không dùng các bản sao đó như dữ liệu quan sát an ninh mạng mới.

Công thức tham khảo:

```text
Throughput (flows/second) = Số flow đầu vào / Thời gian xử lý (giây)
Speedup(N workers) = Thời gian xử lý ở baseline / Thời gian xử lý với N workers
```

Hai công thức chỉ có ý nghĩa khi cùng workload và định nghĩa thời gian đo. Không điền số liệu minh họa vào báo cáo dưới dạng kết quả chạy thật.

## 9. Tiêu chí hoàn thành và minh chứng nộp bài

- [ ] Có mô tả rõ phạm vi **Storage → Processing**, 2 đặc trưng Big Data trở lên và sơ đồ architecture/data flow.
- [ ] MinIO lưu raw CSV; Spark đọc input trực tiếp qua S3A.
- [ ] Có Spark job thực thi làm sạch, chuẩn hóa, chuyển đổi và tổng hợp.
- [ ] Có cleaned Parquet + Snappy, aggregate và cơ chế quarantine/reject.
- [ ] Có thống kê chất lượng dữ liệu trước/sau xử lý và record reconciliation.
- [ ] Có processing time, throughput và ít nhất một phép so sánh hiệu năng có giải thích; ưu tiên cả 3 hướng size/workers/partitions theo kế hoạch.
- [ ] Có Spark log/Spark UI screenshot, MinIO raw/output screenshot và bằng chứng đọc lại Parquet.
- [ ] README ghi đủ môi trường, cấu hình và lệnh chạy thực tế sau khi kiểm chứng.
- [ ] Có `docs/report_draft.md` hoàn thiện thành báo cáo, phân công nhóm và tài liệu tham khảo.

**Minh chứng cần nộp theo đề bài:** báo cáo PDF, source code/config, dữ liệu mẫu hoặc nguồn dữ liệu/script sinh dữ liệu, log/ảnh/video demo nếu cần, bảng phân công và đóng góp. Bài nộp phải phân biệt mô tả thiết kế với kết quả thật sự đã chạy.

## 10. Phân công theo thành phần

| Nhóm công việc | Phụ trách theo cây thư mục đã cung cấp | Đầu ra cần phối hợp |
|---|---|---|
| Data schema, Data Contract | TV1 | `data/schemas/feature_dictionary.md` và mapping schema |
| Extraction / source preparation | TV2 | `extraction/`, sample flow nhất quán schema |
| MinIO + Docker infrastructure | TV3 | `docker/`, `storage/`, raw object & endpoint |
| PySpark processing + benchmark | TV4 phối hợp TV1 | `processing/`, `experiments/`, cleaned Parquet và báo cáo |

Đây là phân công **theo chú thích trong cấu trúc thư mục**, cần đối chiếu với phân công thực tế của nhóm trong báo cáo.

## 11. Tài liệu và quy tắc làm việc

- [Data Contract — Feature Dictionary](data/schemas/feature_dictionary.md): nguồn quy định tên feature, kiểu dữ liệu, ý nghĩa, chất lượng và storage output.
- [Raw Flow Schema](data/schemas/raw_flow_schema.json): schema đầu vào của CSV/network flow.
- [Sơ đồ kiến trúc](docs/architecture.png) và [sơ đồ data flow](docs/data_flow.png).
- [Bản nháp báo cáo](docs/report_draft.md).

**Quy ước:** commit source code, config mẫu, dữ liệu sample đủ nhỏ và tài liệu; không commit dataset lớn, PCAP lớn, output Parquet nhiều file, file `.env` chứa secrets hoặc thông tin truy cập MinIO. Nên giữ môi trường chạy cố định (phiên bản Spark/Python/JAR), ghi batch ID và source_file để có thể tái lập kết quả.

---

### Ghi chú về khả năng tái lập

Các cổng/host, biến môi trường, cú pháp CLI, tên bucket và thời gian benchmark sẽ chỉ được chốt sau khi kiểm tra `docker/docker-compose.yml`, `processing/config.py`, `processing/main.py`, `storage/upload_minio.py` và mã nguồn các script benchmark. Không coi những đoạn lệnh ví dụ trong README là bằng chứng pipeline đã chạy thành công.

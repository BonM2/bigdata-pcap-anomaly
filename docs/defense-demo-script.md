# Kịch bản demo và cách xử lý phản biện

Soạn từ source/artifact ngày 25/10/2026. **Không có lệnh chạy job hoặc thay đổi
hạ tầng nào được thực thi khi soạn tài liệu.** Các lệnh dưới là hướng dẫn để người
thuyết trình chủ động chạy sau này. Phần A chỉ đọc bằng chứng có sẵn; phần B là
demo Spark nhỏ, chỉ đọc object synthetic và output cũ. Không dùng uploader/benchmark
`--execute`, không sửa Compose/CSV/contract, không in `.env` hoặc full Docker config.

## 1. Chuẩn bị cửa sổ và lời mở đầu

Mở VS Code tại `D:\bigdata-pcap-anomaly`. Chuẩn bị các tab:

| Tab | Nội dung cần chỉ | Lời nói |
|---|---|---|
| `docs/system-overview-explained.md` preview | Mermaid kiến trúc | “Driver khác master daemon; đây là một host chạy các container.” |
| [main.py:34](../processing/main.py#L34) | `run_pipeline` và các ranh giới | “Đây là đường gọi thật.” |
| [ingestion.py:94](../processing/ingestion.py#L94) | textFile/mapPartitionsWithIndex | “Reader actual không dùng inferSchema CSV.” |
| [clean.py:41](../processing/clean.py#L41) | Hai vòng rate/window và split | “Clean policy mềm; hard lỗi giữ ở nhánh lỗi.” |
| [validation.py:43](../processing/validation.py#L43) | exceptAll/transitions | “Count đúng chưa đủ, duplicate multiplicity được bảo toàn.” |
| [quality-example.json](benchmarks/20261021/quality-example.json) | before/after/post/reconciliation/outputs | “Đây là report thực nghiệm synthetic.” |
| [summary.csv](benchmarks/20261021/summary.csv) và [PNG](benchmarks/20261021/benchmark.png) | 9 nhóm cấu hình | “Mỗi nhóm hai scored trials, warmup lưu riêng.” |
| [test-results.json](benchmarks/20261021/test-results.json) | 36 tests/log SHA | “Đây là PASS lịch sử ngày 21/10, không phải vừa rerun.” |

VS Code `Ctrl+P`, gõ `processing/main.py:34` để tới đúng dòng; preview Markdown để
render Mermaid. Hai PNG architecture/data_flow cũ chỉ 1 byte, không mở chúng như
sơ đồ đã hoàn thiện. Không mở toàn bộ 8 CSV lớn khi chỉ cần fixture.

Lời mở đầu gợi ý: “Em sẽ chỉ code kiểm soát dữ liệu, một record trước/sau và bằng
chứng storage/benchmark. Sau đó, nếu cluster đang sẵn sàng, em chạy đọc synthetic
nhỏ và kiểm executor tasks; em không upload hay ghi lại dataset thật trong demo này.”

## 2. Phần A — Demo bằng chứng có sẵn, không chạy Spark

### A1. Một record Before/After (khoảng 1–2 phút)

Mở [sample_flow.csv dòng 3](../data/sample/sample_flow.csv#L3): rate `Infinity` ở
fixture. Chỉ [transform:32](../processing/transform.py#L32) để giải thích Double Inf
cast thành công, không phải cast failure; [validation:160](../processing/validation.py#L160)
để giải thích soft; [clean:45](../processing/clean.py#L45) để giải thích NULL+flag;
[post-check:104](../processing/validation.py#L104) để kiểm raw/cờ/value.

Chuyển sang [sample dòng 6](../data/sample/sample_flow.csv#L6): duration -1, hard →
quarantine, không sửa thành 0. Dùng [Before/After tables](data-quality-explained.md)
để cho thấy mỗi stage gọi hàm nào và đi output nào. Nói rõ các row là synthetic.

### A2. Report và marker (khoảng 1 phút)

Mở [quality-example.json](benchmarks/20261021/quality-example.json), tìm lần lượt:

```text
batch_id = bench-fb612f14128542ce908752b50444ed91
input_records = 120000
valid_records / quarantined_records / rejected_records = 40000 / 60000 / 20000
post_cleaning_validation.status = PASS
record_reconciliation.evidence_multiset_preserved = true
outputs.<name>.read_back_status = PASS
```

NULL typed cells 20k→50k, NaN/Inf về 0, historical cast failures vẫn 10k. Đó là số
ô, không cộng thành số record lỗi. Mở [minio-audit.json](benchmarks/20261021/minio-audit.json):
22 batch/110 output datasets; audit tải lại marker/report, không count toàn bộ
Parquet lần mới. Giải thích final marker ở [main:136](../processing/main.py#L136).

### A3. Bộ thật và test (khoảng 1 phút)

Mở [pipeline-eight-real.local.json](pipeline-eight-real.local.json):
2.830.743 = 2.830.628 + 115 + 0, `VALIDATED_NO_OUTPUT`, `outputs={}`. Nói đúng:
“Bộ thật đã được xử lý/hậu kiểm, nhưng lần đó chưa ghi storage output.” Artifact
local này không được commit như các JSON benchmark; cần bảo đảm đã copy vào máy
trình bày nếu dùng clone mới. Nếu thiếu, nói chỉ còn hồ sơ Markdown, không nhận
là vừa mở được report gốc.

Mở [log tests](pipeline-tests-20261021.local.log) cuối file: 36 tests, OK, exit=0.
Có stack trace do failure injection và ResourceWarning; không coi mọi chữ ERROR
trong log là unittest FAIL. [test_pipeline:36](../tests/test_pipeline.py#L36) cố ý
test lỗi ghi và không có final success. Không rerun bộ test nặng giữa demo trừ khi
đã chủ động lên kế hoạch và có thời gian.

### A4. Benchmark (khoảng 1–2 phút)

Hiện [PNG](benchmarks/20261021/benchmark.png), nói ba điểm:

1. Size tăng 12k→120k, median throughput 771→4.484 records/s; input low entropy,
   highest 20,34 MB, không gọi benchmark GB.
2. Worker 1/2 median 22,77/24,85 s, speedup=0,916×, không đạt tăng tốc.
3. 16 shuffle partitions median thấp nhất 26,16 s; hai repeats không chứng minh
   optimal tổng quát. Input/features vẫn hai partitions, không phải toàn DAG có 16.

Chỉ error bars là min–max và timer gồm Parquet read-back; không so trực tiếp với
8.975 records/s của full-real validate-only. Nguồn [summary.csv](benchmarks/20261021/summary.csv)
và [benchmark-explained.md](benchmark-explained.md).

## 3. Kiểm tra hạ tầng trước phần live

Các lệnh chỉ đọc trạng thái, chạy tại root repository:

```powershell
docker compose -f docker/docker-compose.yml ps
docker compose -f docker/docker-compose.yml config --services
```

Mong đợi có Master, hai Workers và MinIO; chưa xác nhận chúng đang chạy tại thời
điểm soạn tài liệu. Nếu Docker không chạy thì dùng phần A. Không tự sửa credentials,
build image hoặc khởi động/dừng services ngay trong buổi demo khi chưa chuẩn bị.
Compose phụ thuộc `.env` đã cấu hình trước; `config --services` chỉ in tên services.

Mở Master `http://localhost:8080`, kiểm **ALIVE workers** và **Active Applications**.
Đếm ALIVE thay vì tổng cả DEAD worker history. Nếu có job khác dùng resource, tránh
đưa số thời gian demo vào bảng benchmark và không dừng job của người khác.
Worker UIs được map `http://localhost:8081`, `http://localhost:8082`; MinIO Console
`http://localhost:9001`. Các port này có trong [Compose:23](../docker/docker-compose.yml#L23),
[Compose:43](../docker/docker-compose.yml#L43), [Compose:53](../docker/docker-compose.yml#L53).

## 4. Phần B — Chạy Spark chỉ đọc và giữ driver để xem task metrics

Mục đích là chứng minh actual executor/task execution; không tạo benchmark mới.
Workload 12k dưới smoke đã được tạo trong đợt trước; nếu object không còn thì
chuyển về artifact, không âm thầm seed/upload để biến thất bại thành thành công.

Mở Python REPL trong container driver, chủ động chạy từng dòng:

```powershell
docker compose -f docker/docker-compose.yml exec -e SPARK_EXECUTOR_MEMORY=2g spark-master python3
```

Trong Python:

```python
from dataclasses import replace
from processing.config import ProcessingConfig, create_spark_session
from processing.main import run_pipeline

cfg = ProcessingConfig.from_env()
cfg = replace(cfg, input_path=f"s3a://{cfg.minio_bucket}/smoke/benchmark-scaling/v1/copies-1000/synthetic.csv")
spark = create_spark_session(cfg)
spark.sparkContext.setLogLevel("WARN")
print(spark.sparkContext.master, spark.sparkContext.applicationId)
report = run_pipeline(spark, cfg, write_outputs=False)
print({k: report[k] for k in ("run_status", "input_records", "valid_records", "quarantined_records", "rejected_records")})
print(report["post_cleaning_validation"]["status"], report["outputs"])
```

Kỳ vọng dựa trên fixture/đợt chạy trước: master `spark://spark-master:7077`,
`VALIDATED_NO_OUTPUT`, 12.000 / 4.000 / 6.000 / 2.000, post PASS, outputs={}. Đây
là **kỳ vọng**, chỉ gọi live PASS sau khi lệnh thực sự thành công. Không print
`os.environ` hoặc SparkConf đầy đủ vì có thể chứa credentials.

Nguồn API: [config:126](../processing/config.py#L126),
[run_pipeline:34](../processing/main.py#L34),
[no-output branch:140](../processing/main.py#L140).

### B1. Chứng minh tasks đã chạy ở executor nào

Giữ nguyên REPL, không stop Spark. Driver UI còn sống ở `spark.sparkContext.uiWebUrl`.
Compose chưa publish 4040; từ host có thể không mở được URL container. Có thể dùng
VS Code kết nối container và forward port driver UI tạm thời nếu đã chuẩn bị, hoặc
đọc REST JSON ngay trong driver mà không sửa Compose:

```python
import json
from urllib.request import urlopen

app_id = spark.sparkContext.applicationId
ui_url = spark.sparkContext.uiWebUrl
print("Driver UI:", ui_url)
with urlopen(f"{ui_url}/api/v1/applications/{app_id}/executors", timeout=10) as response:
    executor_rows = json.load(response)
for row in executor_rows:
    if row["id"] != "driver":
        print({k: row.get(k) for k in ("id", "hostPort", "totalTasks", "completedTasks", "failedTasks")})
```

Không dùng số executor làm số worker: đối chiếu `hostPort` với worker hosts ở Master.
Muốn nói cả hai worker tham gia data work, cần executor trên cả hai hosts có tasks
completed >0. Nếu không có, trình bày đúng quan sát thay vì tuyên bố chia đều.
Master cấp 4 cores là resource evidence, completed tasks là execution evidence.
Code benchmark hiện mới lưu lớp đầu, [benchmark:145](../experiments/benchmark.py#L145).
Đường REST executors tham chiếu
[Spark 3.5.0 Monitoring](https://raw.githubusercontent.com/apache/spark/v3.5.0/docs/monitoring.md).

Nếu đã mở được UI: Jobs → job vừa chạy → Stages → executor/task IDs và shuffle;
Executors → completed tasks; SQL → physical plan có Exchange/InMemoryTableScan;
Environment → kiểm đúng master/cores/memory. Không hứa tên/số stage cố định, vì
optimizer và AQE có thể thay plan. REST snippet là kịch bản chuẩn bị, chưa được
thực thi trong đợt đọc source này.

### B2. Đọc lại một output Parquet có sẵn

Trong cùng REPL, dùng URI có trong quality-example artifact:

```python
base = f"s3a://{cfg.minio_bucket}/smoke/benchmarks/20261021"
batch = "bench-fb612f14128542ce908752b50444ed91"
cleaned = spark.read.parquet(f"{base}/cleaned/{batch}")
cleaned.printSchema()
print("columns", len(cleaned.columns), "records", cleaned.count())
cleaned.groupBy("quality_status").count().show()
cleaned.select("label", "flow_duration").filter("flow_duration >= 0").explain("formatted")
```

Kỳ vọng artifact: 34 cột / 40.000 rows / VALID. `explain` cho thấy read schema và
filter pushdown nếu plan hỗ trợ; không lấy sự hiện diện của pushed filter làm số
đo tiết kiệm I/O. Nguồn kiểm write/read-back actual:
[write_verified_parquet:79](../processing/output.py#L79),
[quality-example.json](benchmarks/20261021/quality-example.json).
Nếu object bị mất hoặc không truy cập được, live read là FAIL; artifact lịch sử
vẫn có giá trị lịch sử nhưng không biến live failure thành PASS.

Kết thúc khi đã xem/chụp bằng chứng cần thiết:

```python
spark.stop()
exit()
```

Không gọi `docker compose down -v`; MinIO source/output nằm ở volume. Driver UI
sẽ không còn khi app dừng. Repository hiện chưa có History Server/event log để
khôi phục timeline sau đó.

## 5. MinIO Console: chỉ đúng objects và report

Đăng nhập bằng thông tin đã được người vận hành quản lý; không chiếu hoặc chép
credentials vào slide. Mở bucket `bigdata-network` (hoặc bucket config thực tế):

```text
smoke/benchmark-scaling/v1/copies-10000/synthetic.csv    (input đo tải)
smoke/benchmarks/20261021/
  cleaned/bench-fb612f14128542ce908752b50444ed91/        (Parquet dataset)
  features/bench-fb612f14128542ce908752b50444ed91/
  aggregates/bench-fb612f14128542ce908752b50444ed91/
  quarantine/bench-fb612f14128542ce908752b50444ed91/
  rejected/bench-fb612f14128542ce908752b50444ed91/
  report/bench-fb612f14128542ce908752b50444ed91/
      quality.json  summary.csv  _STARTED.json  _SUCCESS.json
  performance/bench-fb612f14128542ce908752b50444ed91/performance.json
```

Đây là đường đã lưu trong artifact, không phải xác nhận object còn tồn tại ngày
25/10. Chỉ cho xem part-files và metadata object, không tuyên bố mỗi prefix là
một physical file. Preview quality JSON và final marker; giải thích `raw/` là
prefix production đề xuất, còn demo đã kiểm chứng ở `smoke/`. Không tự chuyển
synthetic sang raw để “đủ hình minh chứng”.

## 6. Trả lời nhanh khi demo gặp sự cố

| Tình huống | Chẩn đoán và câu trả lời đúng |
|---|---|
| Docker engine không kết nối | Hạ tầng hiện chưa sẵn sàng; trình bày artifact lịch sử, không tự nhận live PASS |
| Master hai ALIVE, app vẫn local | Kiểm `spark.sparkContext.master`; local[2] không dùng standalone workers |
| Không có S3AFileSystem class | Kiểm image/JAR Hadoop AWS theo [Dockerfile:4](../docker/spark/Dockerfile#L4); không sửa version ngẫu nhiên trong buổi bảo vệ |
| Connection refused/unknown host MinIO | Phân biệt endpoint container `minio:9000` với host localhost; chưa kết luận credentials sai |
| Access denied | Kiểm cấu hình môi trường qua người vận hành, không print secret để debug công khai |
| No CSV input objects | Kiểm URI/prefix thực tế; không khẳng định upload đã chạy chỉ vì dry-run có plan |
| OOM ở real input | Nêu đúng historical 1g OOM/2g PASS và giới hạn cache; không gọi DISK_ONLY là không cần RAM |
| Job thất bại sau vài Parquet writes | Một số part output có thể tồn tại; batch chưa COMPLETE nếu thiếu final JSON marker |
| Driver UI mất sau stop | Chưa có history/event log; giữ driver sống ở REPL và xem metrics trước stop ở lần demo chuẩn bị |

## 7. Tự kiểm trước khi kết thúc

- Có phân biệt synthetic vs real, measured vs expectation, live vs historical?
- Chỉ đúng raw 79 → core 22 + metadata 12, không gọi đó là 22 cột tổng cộng?
- Giải thích được Infinity NULL+flag khác duration âm quarantine?
- Chứng minh được count conservation và giới hạn raw_df boundary?
- Có giữ đúng thuật ngữ worker/executor/task và giới hạn resource evidence?
- Có nói speedup 0,916× trung thực và hai repeats chưa đủ kết luận tổng quát?
- Có tránh nói `partitionBy`, compaction, HA, exactly-once hoặc ML như tính năng đã có?

Khi muốn luyện trực tiếp, yêu cầu “Bắt đầu luyện vấn đáp”. Quy trình hỏi một câu,
chờ trả lời, chấm/giải thích rồi tăng độ khó nằm ở [defense-questions.md](defense-questions.md).

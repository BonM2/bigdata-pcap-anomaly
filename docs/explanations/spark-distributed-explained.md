# Hiểu Spark từ cấu hình và đường chạy thực tế

Đọc source ngày 25/10/2026; không khởi động cluster hay chạy job mới. Những gì là
kiến thức chung được phân biệt với số đo/triển khai của repository.

## 1. Bảy khái niệm phải phân biệt được

| Khái niệm | Nghĩa dễ hiểu | Ví dụ thực tế ở đây |
|---|---|---|
| Worker | Daemon quản lý tài nguyên trên node của Spark Standalone | Hai container worker, mỗi daemon khai báo 2 cores/2g |
| Executor | Process của một application, chạy task và giữ cache của application | App benchmark xin executor 2 cores/2g; Master ghi tổng app 2 hoặc 4 cores |
| Core | Đơn vị slot CPU Spark dùng để phân bổ task | Một executor 2 cores thường có thể chạy 2 task đồng thời khi mỗi task yêu cầu 1 CPU |
| Partition | Một phần logic của dataset/RDD/DataFrame | Workload synthetic 120k có 2 input partitions và 2 feature partitions trong artifact |
| Task | Một đơn vị xử lý một partition trong một stage attempt | Stage đọc 2 partitions cần 2 task logic; retry có thể có nhiều attempts |
| Stage | Tập task có thể thực hiện trước một dependency/shuffle boundary | Partial aggregate ở đầu vào, rồi aggregate sau exchange |
| Job | Lượt thực thi Spark được kích hoạt khi cần kết quả | Các count/collect/write của main khiến Spark thực thi; không đồng nhất một hàm Python với một job |

Worker capacity nằm ở [Compose:52](../../docker/docker-compose.yml#L52) và
[Compose:63](../../docker/docker-compose.yml#L63); executor request ở
[config:134](../../processing/config.py#L134). Số partition lấy từ
[partitions.json](../benchmarks/20261021/partitions.json), không suy từ tên file.
Một action SQL có thể phát sinh nhiều Spark jobs/stages, nhất là với adaptive
execution/exchanges; không nên hứa `count()` luôn tương ứng đúng một job.

## 2. Driver ở đâu? Master có chạy code cleaning không?

Trong lệnh được lưu trong [commands.txt](../benchmarks/20261021/commands.txt), Python
chạy bằng `docker compose exec spark-master python3 -m ...`. Python driver và
JVM SparkContext nằm ở container đó, cạnh master daemon. CLI tạo session ở
[main:188](../../processing/main.py#L188); `.master(...)` lấy environment Master URL
ở [config:84](../../processing/config.py#L84).

Master phân phối tài nguyên application cho workers; driver lập kế hoạch/lập lịch
task và giao tiếp executor. Worker khởi executor; executor làm data work. Không
có đường “mọi row đi qua master để được clean”. Driver vẫn có việc tuần tự:
header, SHA-256, điều phối, nhận thống kê nhỏ và ghép reports.
[ingestion:69](../../processing/ingestion.py#L69), [main:67](../../processing/main.py#L67).

```text
Master daemon :7077       (quản lý tài nguyên)
        ↓
Worker daemon → Executor (application-specific)
                        ↑ task/result/cache
              Driver + SparkContext (điều phối application)
```

Mô hình thành phần đối chiếu với [Spark 3.5.0 cluster overview](https://github.com/apache/spark/blob/v3.5.0/docs/cluster-overview.md).
Chạy process trong image Spark không đủ để distributed: `local[2]` vẫn xử lý local.
Tests dùng config mặc định local[2] ở [SparkCase:8](../../tests/spark_case.py#L8).

## 3. Một CSV được chia thế nào trong dự án này?

Reader không gọi SQL CSV file datasource. Nó dùng `SparkContext.textFile()` qua
Hadoop TextInputFormat; file splits được đọc thành RDD text records, rồi Python
`mapPartitionsWithIndex` parse từng line. Một file `.csv` không nén có thể chia
thành nhiều splits; split reader xử lý biên dòng, không đơn giản cắt byte rồi cho
CSV parser một nửa dòng. Chỉ partition index 0 bỏ header đầu file.
[read_raw_flows:94](../../processing/ingestion.py#L94),
[_parse_partition:34](../../processing/ingestion.py#L34).

Không lấy `spark.sql.shuffle.partitions` làm số input splits. Cũng không áp dụng
công thức SQL `spark.sql.files.maxPartitionBytes` một cách máy móc cho đường
textFile/RDD này. Số input partitions phụ thuộc Hadoop file splits, filesystem,
cấu hình/minimum gợi ý của reader và số file; `unionByName` ghép các frame file.
Muốn biết số thật, xem [main:124](../../processing/main.py#L124).

Artifact 8 file thật ghi 30 input partitions; artifact một CSV synthetic 20,34 MB
ghi 2. Đây là quan sát của các lần chạy cụ thể, không bảo đảm mọi CSV 20 MB luôn có 2.
[full-real report](../evidence/local/pipeline-eight-real.local.json),
[partition report](../benchmarks/20261021/partitions.json).

## 4. Lazy Evaluation: viết biểu thức chưa có nghĩa đã tính

Ví dụ trong đường chạy hiện tại:

```text
read_raw_flows → transform_flows → validate_flows → clean_classified_flows
    các frame/Column phần lớn dựng lineage/plan
quality_snapshot(...).groupBy(...).agg(...).collect()
    cần kết quả thật → Spark thực thi dữ liệu
```

`withColumn`/`select`/`filter` trả DataFrame mới, không đi qua toàn dataset ngay.
Spark SQL có thể gộp/đẩy projection/filter khi tối ưu plan; không suy mỗi withColumn
là một stage. Driver vẫn đã đọc header/checksum trước action DataFrame vì đó là
I/O imperative trong reader. Như vậy không được nói “mọi dòng code ingestion đều lazy”.
[transform:43](../../processing/transform.py#L43),
[quality_snapshot:55](../../processing/quality_report.py#L55).

Lazy transformations và action được đối chiếu với
[Spark 3.5.0 RDD guide](https://raw.githubusercontent.com/apache/spark/v3.5.0/docs/rdd-programming-guide.md).

## 5. DAG → stage → task và shuffle

DAG (Directed Acyclic Graph) biểu diễn dependencies của việc tính dữ liệu. Với
`transform_flows`, numeric expression chỉ cần row/partition hiện tại. Với `groupBy(label)`,
các row cùng label có thể nằm nhiều executors nên Spark thường phải exchange dữ
liệu theo key trước final aggregate. Đây là shuffle: dữ liệu có thể được serialize,
ghi/đọc disk và truyền giữa executors, tốn hơn local row expressions.
[features:48](../../processing/features.py#L48).

```text
Input partitions [P0] [P1]
       ↓ narrow row parse/cast/flag expressions
Partial group aggregates
       ↓ Exchange theo label  ← shuffle boundary trong physical plan
Shuffle partitions [S0] [S1] [S2] [S3] (ví dụ cấu hình 4)
       ↓ final aggregate
Write output / collect statistics
```

Các điểm có thể shuffle trong source: groupBy file/status/label của QA,
`exceptAll` bảo toàn multiset, group distribution read-back và aggregate bốn cấp.
Global aggregation/count cần tổng hợp kết quả giữa các partitions; không chỉ
`groupBy(label)` mới có exchange. Physical plan/AQE quyết định boundary thực tế,
không vẽ một DAG giả như thể đã được chụp từ Spark UI.
[validation:60](../../processing/validation.py#L60),
[quality_report:55](../../processing/quality_report.py#L55),
[output:90](../../processing/output.py#L90).

## 6. Count, collect và write khác nhau

| API đang dùng | Loại | Kết quả / tác động |
|---|---|---|
| `count()` trên DataFrame | Action | Trả int; cần biết số rows thật |
| `collect()` | Action | Trả list Row về driver; đang dùng cho thống kê nhóm nhỏ |
| `write.parquet()` | Action/I/O | Thực thi upstream và ghi nhiều part-files |
| `groupBy().agg()` | Dựng aggregate DataFrame | Chưa trả kết quả Python cho tới action |
| `F.count('*')` trong agg | Aggregate expression | Không đồng nhất với `df.count()` action |
| `frame.schema`, `frame.dtypes` | Schema/metadata access | Không tương đương count quét toàn rows |

Vị trí cụ thể: [main:94](../../processing/main.py#L94),
[quality_report:20](../../processing/quality_report.py#L20),
[output:81](../../processing/output.py#L81). Không `collect()` toàn raw 2,8 triệu rows;
ngược lại, số lượng groups lớn trong tương lai vẫn có thể khiến QA collect quá lớn.

## 7. Vì sao tính lại? Persist giúp gì và không giúp gì?

Nếu hai actions sử dụng cùng lazy lineage mà chưa persist, Spark có thể đọc và
tính upstream lại cho mỗi action. Main reuse frame cho QA, reconciliation, write,
nên persist ở raw/classified/annotated/features/aggregates và checked projection.
Cache mới được populate khi action cần dữ liệu. Unpersist blocking giải phóng trước
giai đoạn tiếp để giảm áp lực tài nguyên. [main:47](../../processing/main.py#L47),
[main:77](../../processing/main.py#L77), [main:89](../../processing/main.py#L89).

| Cơ chế | Trong repository | Giới hạn |
|---|---|---|
| `cache()` | Tests, ví dụ [test_post:12](../../tests/test_post_validation.py#L12) | Dùng default DataFrame storage level; không phải tự nhiên mọi row nằm RAM |
| `persist(StorageLevel.DISK_ONLY)` | Main mặc định từ config | Cache trên disk executor, vẫn cần heap để tính/materialize batches |
| `MEMORY_AND_DISK` | Được config hỗ trợ; check_minio dùng cố định | Có thể spill disk nhưng vẫn có áp lực bộ nhớ |
| `unpersist(blocking=True)` | Main | Bỏ cache; không xóa CSV/Parquet nguồn/đầu ra |
| Checkpoint | Chưa dùng | Persist không thay thế checkpoint, không tự chặt lineage |

Full-real 1g đã OOM ở các lần thử lịch sử; 2g + DISK_ONLY đã PASS. Không suy DISK_ONLY
có nghĩa Spark không cần RAM. Batch cache size=1000 nhỏ hơn có thể giảm heap mỗi lô;
không tự chứng minh đó là optimum. [hồ sơ OOM/PASS](../reports/pipeline-validation.md),
[Spark 3.5.0 tuning](https://raw.githubusercontent.com/apache/spark/v3.5.0/docs/sql-performance-tuning.md).

## 8. Các API trong yêu cầu có thật sự xuất hiện không?

| API/khái niệm | Cách dùng cụ thể / ví dụ dễ hiểu | Vị trí |
|---|---|---|
| SparkSession | Entry point tạo DataFrame/read/write/SQL config | [config:126](../../processing/config.py#L126) |
| DataFrame | Dataset có schema, partitioned, kế hoạch biến đổi | [ingestion:97](../../processing/ingestion.py#L97) |
| StructType/StructField | Định nghĩa tên, type, nullable của raw fields | [config:45](../../processing/config.py#L45) |
| withColumn | Thêm/thay cột trong frame mới, ví dụ flag rate | [clean:48](../../processing/clean.py#L48) |
| select | Projection giữ core/metadata hoặc evidence | [clean:60](../../processing/clean.py#L60) |
| filter DataFrame | Chọn rows status VALID | [clean:60](../../processing/clean.py#L60) |
| F.filter array | Lọc NULL khỏi array reason; không lọc rows DataFrame | [transform:45](../../processing/transform.py#L45) |
| groupBy / agg | Gom key rồi count/sum/avg | [features:50](../../processing/features.py#L50) |
| cast | Decimal trước sum, Double khi chia; transformation còn dùng SQL try_cast | [features:20](../../processing/features.py#L20), [transform:32](../../processing/transform.py#L32) |
| when / otherwise | CASE expression cho flags/status; không có otherwise mặc định NULL | [validation:185](../../processing/validation.py#L185), [clean:49](../../processing/clean.py#L49) |
| F.coalesce | Chọn biểu thức non-NULL đầu tiên, ở đây defaults Boolean/sum | [validation:85](../../processing/validation.py#L85) |
| unionByName | Ghép rows nhiều file/grouping theo tên cột | [ingestion:98](../../processing/ingestion.py#L98) |
| exceptAll | Multiset difference xét duplicate | [validation:60](../../processing/validation.py#L60) |
| eqNullSafe | Equality có quy tắc NULL=NULL cho invariant | [validation:90](../../processing/validation.py#L90) |
| cache / persist / count / write | Reuse và materialization như các mục trên | [main:47](../../processing/main.py#L47), [output:81](../../processing/output.py#L81) |
| DataFrame repartition / coalesce | **Không dùng trong pipeline hiện tại** | Kiến thức bổ trợ bên dưới |
| spark.sql query strings | Không dùng để chạy toàn aggregate; dùng DataFrame API | [features:30](../../processing/features.py#L30) |

Kiến thức bổ trợ: `repartition(n)` phân phối lại dữ liệu, thường gây shuffle;
DataFrame `coalesce(n)` thường dùng để giảm partitions mà tránh full shuffle,
nhưng có thể làm giảm parallelism. **Khác hoàn toàn** `functions.coalesce()` xử lý
NULL. Không nói dự án đã gom output thành một file bằng coalesce. Định nghĩa API
đối chiếu [PySpark DataFrame 3.5.0](https://github.com/apache/spark/blob/v3.5.0/python/pyspark/sql/dataframe.py).

## 9. Vì sao tăng worker chưa chắc nhanh?

Ví dụ core/task: input chỉ có 2 partitions → stage narrow có tối đa 2 task logic;
executor 2 cores trên một worker đã có thể chạy cả hai cùng lúc. Thêm worker không
tự chia thành 4 input partitions. Shuffle stage có thể nhiều task hơn và dùng thêm
slots, nhưng cũng có network/scheduling/serialization và dùng chung host/disk.
Đây là giải thích khả dĩ, không phải profile chứng minh nguyên nhân từng mili-giây.
[partition observations](../benchmarks/20261021/partitions.json),
[worker results](../benchmarks/20261021/summary.csv).

Benchmark worker measured median 22,766 s (1) và 24,851 s (2): speedup=0,916×.
Không thể dùng resource allocation 4 cores để hứa speedup 2×. Hai workers đều trên
một Docker Desktop host, không hai máy độc lập. Executor 2g là memory setting,
không toàn bộ RSS/JVM/Python overhead; Compose worker declaration cũng không phải
container cgroup hard limit.
[Compose:47](../../docker/docker-compose.yml#L47),
[environment artifact](../benchmarks/20261021/docker-environment.json).

## 10. Bottleneck: quan sát và giả thuyết

| Quan sát có số đo/code | Điều có thể kết luận | Chưa thể kết luận |
|---|---|---|
| Full-real checksum/header+ingestion 88,930 s | Đường ingestion đáng kể, SHA thêm full read | Tất cả 88,930 s là network hay SHA CPU |
| Full-real clean/post/after-QA 133,420 s | Correctness/reconciliation/QA có cost lớn | Cleaning riêng chậm 133 s; timer gộp nhiều thao tác |
| Many groupBy/exceptAll/read-back actions | Có các điểm phát sinh shuffle/I/O | Chính xác số shuffle bytes nếu chưa xem UI/metrics |
| 32 partitions median cao hơn 16 | Config nhiều partitions hơn không tốt hơn ở thí nghiệm này | 16 luôn tối ưu trên mọi size/cluster |
| OOM 1g, PASS 2g | Heap 1g từng không đủ cho workload/config đó | Thêm RAM luôn tăng throughput tuyến tính |

Nguồn: [full-real report](../evidence/local/pipeline-eight-real.local.json),
[code calls](../../processing/main.py#L65), [benchmark](benchmark-explained.md).

## 11. Đọc Spark UI để chứng minh distributed execution

Phải phân biệt ba lớp:

1. **Infrastructure:** `docker compose ps` và Master :8080 thấy 2 ALIVE workers.
   Điều này chỉ chứng minh hạ tầng đăng ký được.
2. **Application resources:** đúng app ID/master `spark://...`, executors/cores/memory
   được cấp. Benchmark đã lưu topology và application_resources;
   [benchmark:128](../../experiments/benchmark.py#L128), [benchmark:145](../../experiments/benchmark.py#L145).
3. **Thực thi data:** application UI **Executors** có completed tasks/input/shuffle
   trên executor ở cả hai worker hosts; **Stages** có task attempts, thời gian,
   executor IDs và metrics. Cần nhìn actual task placement, không chỉ tên worker.

Trong **Jobs**, chọn action ứng với giai đoạn; trong **SQL**, xem plan có Exchange,
InMemoryTableScan hay FileScan; trong **Stages**, xem skew/straggler/spill/GC/shuffle;
trong **Environment**, đối chiếu executor cores/memory và AQE/shuffle config.
Ví dụ field `input_partitions` trong report là số logic, không thay UI task metrics.

Compose chưa publish driver UI :4040 và chưa cấu hình History Server/event log.
Master UI :8080 không thay thế application UI. Driver kết thúc thì UI của app có
thể không còn truy cập; app ID lịch sử không khôi phục task timeline tự động.
Đọc source Compose [dòng 37](../../docker/docker-compose.yml#L37) để thấy các port thật.
Vòng đời UI và REST endpoint được đối chiếu với
[Spark 3.5.0 Monitoring](https://raw.githubusercontent.com/apache/spark/v3.5.0/docs/monitoring.md).

Do chưa có saved per-task timeline trong artifact đã đọc, mức kết luận hiện tại
là **đã có distributed master/resource allocation và cluster runs thành công**;
chưa công bố số task/bytes mỗi executor như số liệu đã đo. Kịch bản giữ driver sống,
đọc REST/UI và kiểm hai hosts được trình bày ở [demo](../defense/defense-demo-script.md).

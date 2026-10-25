# Chạy lại Evaluation & Benchmark

Chạy PowerShell tại root repository, Docker Desktop ở Linux containers. Credentials
đã cấu hình trong `docker/.env`; không đưa chúng vào CLI hoặc báo cáo. Các lệnh dưới
đây chỉ seed **synthetic scaling**, không upload 8 CSV thật. Mỗi lần chạy dùng một
experiment ID mới, kết quả và output không ghi đè batch trước.

## Phương pháp

- Ba input: nhân 12 dòng fixture 1.000 / 5.000 / 10.000 lần, thành 12.000 / 60.000 /
  120.000 records. Manifest ghi số byte và SHA-256 thật; header chỉ xuất hiện một lần.
- Size: cùng hai workers và 4 shuffle partitions. Worker: cùng input 120.000 dòng,
  4 shuffle partitions, lần lượt 1 và 2 workers. Partition: cùng hai workers và input
  120.000 dòng, cấu hình 4 / 8 / 16 / 32 shuffle partitions.
- Mỗi worker 2 cores / 2 GiB; mỗi executor 2 cores / 2 GiB. Tăng worker làm tăng tổng
  tài nguyên được cấp từ 2 cores / 2 GiB lên 4 cores / 4 GiB; đây không phải phép so
  sánh giữ nguyên tổng tài nguyên. Các container vẫn chia sẻ một máy Docker Desktop.
- Mỗi invocation có một warmup toàn pipeline, lưu riêng vào `warmup_runs`, không tính
  vào median. Hai lần lặp được tính điểm; thứ tự size/partition đảo ngược ở lần hai.
  Worker chạy hai invocation riêng. Thứ tự thực thi phải được giữ trong báo cáo.
- AQE mặc định **false** trong benchmark để tránh tự gộp shuffle. Các số 4/8/16/32 là
  `spark.sql.shuffle.partitions`, không phải số partition của mọi stage hay mọi file
  output. Số input/feature partitions thực tế cũng được lưu.
- `DISK_ONLY`, cache batch 1000; cache DataFrame được giải phóng sau mỗi trial.
  Không flush cache của OS, MinIO hoặc JVM/JIT. Không chạy test hoặc job khác song song.
- Timer bao gồm đọc S3A, checksum, xử lý, hậu kiểm, tính quality report, features,
  aggregation, ghi năm Parquet và đọc lại xác minh; không bao gồm startup Spark,
  seed input hoặc xuất JSON/CSV cuối. `stage_duration_seconds` tách các phần này.
- Throughput = input records / thời gian từng trial; bảng lấy median của throughput.
  Speedup = median thời gian baseline / median thời gian cấu hình đang xét.
  Baseline worker = 1, partition = 4. Size khác workload nên **không tính speedup**.
  Summarizer chỉ ghép baseline khi cùng checksum, phạm vi đo, environment và cấu hình
  ngoài trục đang so sánh. Thiếu baseline thì để trống, không suy diễn.
- Input synthetic có độ lặp cao và tỷ lệ lỗi nhân tạo; checksum, cache, nén và shuffle
  chịu ảnh hưởng từ đặc tính này. Không dùng kết quả để đánh giá phát hiện tấn công,
  không coi đó là phép đo CICIDS2017 thật hoặc bằng chứng scaling ở mức GB.

## Chuẩn bị và seed

```powershell
docker compose -f docker/docker-compose.yml up -d --no-build
$benchId = 'scaling-' + (Get-Date -Format yyyyMMdd-HHmmss)
$resultDir = "/tmp/$benchId"
$benchInput = 's3a://bigdata-network/smoke/benchmark-scaling/v1/copies-10000/synthetic.csv'
$common = @('--dataset-kind', 'synthetic', '--synthetic-scaling', '--experiment-id', $benchId,
  '--warmups', '1', '--repeats', '2', '--adaptive', 'false',
  '--master-ui', 'http://spark-master:8080',
  '--output-prefix', "s3a://bigdata-network/smoke/benchmarks/$benchId", '--execute')
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m experiments.seed_scaling --manifest "$resultDir/scaling-manifest.json" --execute
```

Seeder yêu cầu bucket đã tồn tại. Nếu object synthetic đã tồn tại và giống hệt thì
tái sử dụng; khác nội dung sẽ dừng. Manifest local và result JSON phải là tên mới.
Nếu bucket khác mặc định, thay `bigdata-network` trong URI bằng bucket của repository.

## Size và partitions trên hai workers

```powershell
docker compose -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m experiments.benchmark_size --inputs s3a://bigdata-network/smoke/benchmark-scaling/v1/copies-1000/synthetic.csv s3a://bigdata-network/smoke/benchmark-scaling/v1/copies-5000/synthetic.csv $benchInput --expected-workers 2 --results "$resultDir/size.json" @common
docker compose -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m experiments.benchmark_partitions --input $benchInput --partitions 4 8 16 32 --expected-workers 2 --results "$resultDir/partitions.json" @common
```

Kiểm tra `$LASTEXITCODE` sau từng lệnh; chỉ tiếp tục khi bằng 0. Có thể bỏ `--execute`
khỏi `$common` để xem kế hoạch trước. Spark Master API được kiểm tra trước/sau mỗi
trial và app phải nhận đủ số cores dự kiến. Không suy số worker từ executor count.

## Một và hai workers

Trước khi thay topology, kiểm tra Spark Master `http://localhost:8080` không có
application khác đang chạy. Chỉ dừng worker thứ hai của dự án; không xóa container
hay volume. Chờ Master thấy đúng một worker ALIVE trước khi chạy baseline.

```powershell
docker compose -f docker/docker-compose.yml stop spark-worker-2
# Xem trạng thái cho đến khi chỉ còn 1 worker ALIVE:
(Invoke-RestMethod http://localhost:8080/json/).workers | Select-Object id,state,cores,memory
try {
  docker compose -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m experiments.benchmark_workers --input $benchInput --expected-workers 1 --results "$resultDir/workers-1.json" @common
} finally {
  docker compose -f docker/docker-compose.yml start spark-worker-2
}
# Chờ đủ 2 workers ALIVE rồi chạy:
docker compose -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g spark-master python3 -m experiments.benchmark_workers --input $benchInput --expected-workers 2 --results "$resultDir/workers-2.json" @common
```

## Lấy kết quả, CSV và biểu đồ

```powershell
New-Item -ItemType Directory -Force "docs/benchmarks/$benchId" | Out-Null
docker cp "spark-master:$resultDir/." "docs/benchmarks/$benchId/"
python -m experiments.plot_results "docs/benchmarks/$benchId/size.json" "docs/benchmarks/$benchId/partitions.json" "docs/benchmarks/$benchId/workers-1.json" "docs/benchmarks/$benchId/workers-2.json" --csv "docs/benchmarks/$benchId/summary.csv" --runs-csv "docs/benchmarks/$benchId/runs.csv" --png "docs/benchmarks/$benchId/benchmark.png"
```

PNG cần `matplotlib` trên host. Mỗi output bảng/biểu đồ phải là tên mới. JSON chứa cả
manifest nguồn, môi trường Spark/Python/Java, topology, tài nguyên app, stage timings,
quality counts, đường dẫn/report và bằng chứng read-back từng output. `runs.csv` gồm
cả warmup có cờ riêng; `summary.csv` chỉ tổng hợp các trial được tính điểm. Benchmark
FAILED bị từ chối, không đưa các trial dở dang vào biểu đồ hoàn thành.

Audit đọc lại marker/quality/performance JSON đã lưu (không ghi object MinIO):

```powershell
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m experiments.verify_benchmark_outputs "$resultDir/partitions.json" "$resultDir/size.json" "$resultDir/workers-1.json" "$resultDir/workers-2.json" --report "$resultDir/minio-audit.json" --example-report "$resultDir/quality-example.json"
```

Copy lại thư mục results để lấy hai artifact audit. Audit kiểm tra bằng chứng của
những lần Spark read-back trong pipeline; không thay thế bằng một lần đọc mới Parquet.

## Kiểm thử

```powershell
python -m unittest tests.test_benchmark -v
docker compose -f docker/docker-compose.yml exec -T spark-master python3 -m unittest discover -s tests -v
python -m compileall -q processing experiments tests
git -c core.safecrlf=false diff --check
```

Test benchmark chạy được trên host không cần PySpark; các test Spark chạy trong image
dự án. End-to-end MinIO được kiểm chứng ngay trong mỗi trial benchmark, không cần
seed lại dữ liệu thật. Kết quả đã đo nằm trong [báo cáo triển khai](storage-processing-report.md).

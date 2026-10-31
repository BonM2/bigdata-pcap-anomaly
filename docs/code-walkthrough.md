# Đọc mã nguồn và review kỹ thuật

Khảo sát ngày 25/10/2026. `DataFrame` dưới đây là PySpark DataFrame; `Column` là
biểu thức chưa thực thi; các hàm Python phần lớn không có type annotation nên
kiểu trong bảng được diễn giải từ cách sử dụng. Không sửa code trong lần review này.

## 1. Bản đồ lời gọi và entry points

```text
processing.audit_source.main → load_contract → audit_file (Python processes)
storage.upload_minio.main → prepare_upload → sha256_file → Boto3 khi --execute
processing.check_minio.main → create_spark_session → read_raw_flows → clean_flows

processing.main.main → output_plan → create_spark_session → run_pipeline
  ├─ read_raw_flows → validate_header / parse_record / _parse_partition
  ├─ transform_flows → load_contract
  ├─ validate_flows → quality_snapshot(before)
  ├─ clean_classified_flows → CleanResult
  ├─ validate_cleaned_flows → post_clean_summary / schema_validation
  ├─ reconcile_stages → classification_counts / exceptAll
  ├─ quality_snapshot(after)
  ├─ add_features(valid)
  ├─ aggregate_flows(annotated) → add_features(annotated)
  ├─ write_verified_parquet × 5 khi write_outputs=True
  └─ build_quality_report / summary_csv / write_text / final marker

experiments.benchmark_{size,workers,partitions}
  → benchmark.main(dimension) → run_pipeline(write_outputs=True)
experiments.plot_results.main → summarize / measured_runs / chart
experiments.verify_benchmark_outputs.main → Boto3 read stored reports/markers
```

Các cạnh chính: [main imports/calls:14](../processing/main.py#L14),
[main orchestrator:65](../processing/main.py#L65),
[benchmark:45](../experiments/benchmark.py#L45), [clean_flows:29](../processing/clean.py#L29).

CLI chính khác hàm Python: `python -m processing.main` không có flag sẽ chỉ in
PLAN_ONLY; `run_pipeline()` gọi trực tiếp mặc định **thực sự tính toán** nhưng
không ghi storage output. Chế độ `--validate-only` vẫn có thể ghi local report
nếu thêm `--local-report`, nên “read-only” ở đây nói về input/output storage.
[CLI:162](../processing/main.py#L162), [API:34](../processing/main.py#L34).

## 2. `processing/config.py`

| Hàm/class và dòng | Input → return | Logic và liên hệ |
|---|---|---|
| [load_contract:23](../processing/config.py#L23) | path str/Path → dict | JSON utf-8-sig, sort source_index, kiểm 79/22/unique/version/raw String, thêm fields đã sort |
| [raw_schema:45](../processing/config.py#L45) | contract dict hoặc None → StructType | Tạo 79 StructField StringType nullable=True |
| [ProcessingConfig:52](../processing/config.py#L52) | dataclass fields → config frozen | Defaults local[2], executor 1g/2 cores, DISK_ONLY, shuffle=4, UUID batch, bảy output prefixes |
| [from_env:79](../processing/config.py#L79) | Mapping[str,str] hoặc os.environ → config | Parse int, nối bucket-derived URI; không tự đọc file `.env` |
| [validate:110](../processing/config.py#L110) | require_minio bool → None / raise | Kiểm contract, positive resources, storage level, batch characters; khi yêu cầu MinIO kiểm endpoint/credentials |
| [create_spark_session:126](../processing/config.py#L126) | config + enable_minio bool → SparkSession | Builder master/app/SQL/cache/memory/Snappy; cấu hình S3A, optional packages; getOrCreate |

Không có Spark action trong `load_contract`; API Spark thực sự xuất hiện khi tạo
schema/session. Credentials `repr=False` chỉ ẩn khỏi repr dataclass, không phải
mã hóa credentials. `getOrCreate()` có thể trả session đã tồn tại; không dùng nó
để giả định mọi startup config đã được thay động. Lỗi: JSON/schema không đúng,
integer environment parse sai, URI/credentials thiếu, connector JAR không có.

**Đoạn phải hiểu:** `.master(config.spark_master)` quyết định local/distributed,
không phải tên container. **Hỏi code:** “Vì sao full 8 CSV cần `-e
SPARK_EXECUTOR_MEMORY=2g`?” Vì default vẫn 1g; cấu hình full-data thành công override
2g, worker capacity 2g không tự đổi executor request. Xem [config:58](../processing/config.py#L58).

## 3. `processing/audit_source.py`

`audit_file(path, contract) → dict` ở [dòng 17](../processing/audit_source.py#L17)
đọc bytes tuần tự, cập nhật SHA-256 cùng lượt đọc, parse CSV từng physical line,
Counter lỗi/range/example cho 79 cột, distribution label, kiểm duplicate aliases
bằng giá trị cột 34/55 (zero-based). Hard/soft preview chỉ xét core; malformed
được tách trước để tránh đếm hai status. Hàm nested `issue(index, code, token)`
lưu count và một ví dụ gặp đầu tiên, không lưu mọi giá trị lỗi.

`main() → None` ở [dòng 146](../processing/audit_source.py#L146) kiểm đúng tập 8 file,
dispatch Python process pool, ghép report/hash schema+Markdown, ghi JSON. Không
dùng Spark DataFrame/SQL. Sai file set dừng; sai header được report; malformed và
decode replacement có counts. Empty header hoặc parse header hỏng có thể raise.

**Điểm phải hiểu:** `policy_preview` là dự kiến của Python audit, không phải
cleaned output. **Hỏi code:** “Hash có tính header không?” Có, `digest.update`
header rồi mọi raw line ở [audit:33](../processing/audit_source.py#L33).

## 4. `processing/ingestion.py`

| Hàm và dòng | Input → return | Logic/API |
|---|---|---|
| [validate_header:16](../processing/ingestion.py#L16) | line str, contract dict → None/raise | Bỏ BOM đầu line, strict csv, so list exact đủ mọi vị trí |
| [parse_record:24](../processing/ingestion.py#L24) | line str, count int=79 → (list, reason str/None) | Parse strict; syntax lỗi → [] + CSV_SYNTAX; pad/cắt list về count |
| [_parse_partition:34](../processing/ingestion.py#L34) | partition index, iterator[str], source_path/count → generator[tuple] | Bỏ đúng header đầu partition 0; thêm source_file/path/raw/corrupt/reason |
| [read_raw_flows:43](../processing/ingestion.py#L43) | spark, URI str, contract?, checksum bool → (DataFrame, list[dict]) | Hadoop glob/list, header+JVM checksum; textFile/mapPartitionsWithIndex; createDataFrame(schema); unionByName |

Header/checksum loop chạy tại driver qua JVM Hadoop FS; parse row chạy phân tán.
Manifest được `main` thêm record count sau action, reader lúc return chưa biết count.
`checksum=False` bỏ SHA, để digest None, không đổi parser. No CSV, file rỗng hoặc
header sai làm fail cả job; lỗi row giữ evidence cho reject. Một file compressed
không có đuôi `.csv` cũng không được list này chọn.

**Đoạn phải hiểu**, [ingestion:96](../processing/ingestion.py#L96):

```python
rows = spark.sparkContext.textFile(item["source_path"]).mapPartitionsWithIndex(parser)
frames.append(spark.createDataFrame(rows, schema))
```

Không diễn giải thành `spark.read.csv`. **Hỏi code:** “Header thứ hai lặp giữa file
có tự bị bỏ không?” Không; chỉ first physical line bị bỏ, header lặp giữa file là data.

## 5. `processing/check_minio.py`

[main:15](../processing/check_minio.py#L15) nhận args report Path/skip-checksum bool,
config từ environment; return None, in JSON và tùy chọn ghi local file. Nó gọi
reader, raw persist MEMORY_AND_DISK, `groupBy(source_path).agg(count,sum).collect()`,
`clean_flows`, `CleanResult.counts`, đối soát input, lấy cleaned schema.

Lỗi non-S3A URI bị chặn trước session; lỗi reader/connector/Spark propagate;
input zero bị báo lỗi; finally giải phóng cache/stop. Không có post-cleaning gate,
features, output Parquet ở entry point này. Local report dùng write_text có thể
ghi đè nếu dùng lại tên; khác main CLI kiểm tên mới. Đây là smoke chứ không
full storage acceptance. **Hỏi code:** “Vì sao checksum có thể tốn thời gian?”
Vì thêm full read; source ở [check:28](../processing/check_minio.py#L28).

## 6. `processing/transform.py`

[transform_flows:8](../processing/transform.py#L8): input raw DataFrame, config?,
contract? → DataFrame có typed 22 core + source/evidence + metadata/cast failures.
`select` dựng projection; `F.trim` numeric, `F.expr(try_cast(...))`, regex Int/Long,
`F.when`, struct all raw, `F.filter(array, lambda)` bỏ NULL khỏi array lỗi.
Đây là built-in Column/higher-order expression, không phải Python row UDF.

Label string đi thẳng `.alias(name)`; `F.current_timestamp()` là biểu thức thời
gian query/process, không timestamp gói mạng hay một source row ID. Thiếu canonical
name, trùng cột hoặc thiếu source_file làm ValueError. Bad numeric token cho NULL
và failure field, không làm mất row. `try_cast` bảo vệ ANSI overflow; regex tránh
truncation. **Hỏi code:** “`1e2` có được không?” Với Double có thể; Int/Long bị regex
loại. [transform:30](../processing/transform.py#L30).

## 7. `processing/validation.py`

| Hàm và dòng | Input → return | API/thuật toán |
|---|---|---|
| [schema_validation:11](../processing/validation.py#L11) | frame, contract?, cleaned/exact bool → dict PASS/FAIL/errors | Đọc dtypes; kiểm core + metadata + flags; exact chặn cột dư, không dựa nullable hints |
| [classification_counts:34](../processing/validation.py#L34) | frame có quality_status → dict counts | groupBy/count/collect; từ chối status lạ hoặc NULL |
| [reconcile_stages:43](../processing/validation.py#L43) | before/after frames, expected_input_records int → dict | So counts, select raw+prior status, exceptAll hai chiều, group transitions |
| [validate_cleaned_flows:69](../processing/validation.py#L69) | annotated có raw/status_before/cast failures → DataFrame | Schema gate, eqNullSafe label/cờ/status, raw-based rate/window check, valid-domain checks; thêm post_clean_errors |
| [post_clean_summary:130](../processing/validation.py#L130) | checked frame → dict | explode errors/groupBy/count, count bad rows, classification counts; là nhiều action |
| [validate_flows:138](../processing/validation.py#L138) | typed frame, contract? → DataFrame | Column rules/hard expression, reason array, status precedence; chưa có action riêng |

`rule()` nội bộ dùng `F.coalesce(condition, False)` để SQL UNKNOWN không làm
predicate không rõ ràng. `eqNullSafe` cho phép so hai NULL bằng nhau khi kiểm raw
label/cờ/value; không dùng `col1 == col2` rồi hy vọng NULL cho True.

**Đoạn phải hiểu**, [validation:185](../processing/validation.py#L185):

```python
typed.withColumn("quality_status", F.when(malformed, "REJECTED")
                 .when(hard, "QUARANTINED").otherwise("VALID"))
```

Không cộng các reason codes thành row count vì một row có nhiều reason. Errors
ValueError gồm schema/required evidence/unknown status/loss/multiset/status transition.
**Hỏi code:** “Hậu kiểm FAIL thế nào?” Có schema fail raise ngay, hoặc post error
array khác rỗng → summary FAIL → main raise; xem [main:85](../processing/main.py#L85).

## 8. `processing/clean.py`

`CleanResult` dataclass ở [dòng 13](../processing/clean.py#L13) chứa bốn frame:
annotated là toàn bộ, ba nhánh loại trừ nhau. `.counts() → dict` ở dòng 19 gọi
groupBy/count/collect; nó không từ chối unknown status chặt như `classification_counts`.
`clean_flows(raw, config?, contract?) → CleanResult` ở dòng 29 gọi transform/validate,
còn `clean_classified_flows(classified, contract?) → CleanResult` ở dòng 41 tái dùng
classified. Main chọn API thứ hai.

Rate invalid boolean xét NULL/NaN/abs==Inf/negative; `when(~flag, value)` không
otherwise tạo NULL khi invalid. Window flag dùng `F.coalesce(value==-1,False)`.
`filter` chia nhánh, valid `select` 34 cột. Thao tác vẫn lazy; không in-place đổi
CSV hay DataFrame nguồn. **Hỏi code:** “Annotating xong có bỏ raw ở mọi nhánh?”
Không, chỉ valid projection bỏ evidence. [clean:58](../processing/clean.py#L58).

## 9. `processing/features.py`

`add_features(frame) → DataFrame` ở [dòng 9](../processing/features.py#L9) yêu cầu
bốn source field kiểu bigint. Helper `total(left,right) → Column` ở dòng 16 chỉ
cộng hai giá trị non-null, nonnegative, chuyển Decimal trước cộng rồi về Decimal(20,0).
`withColumn` thêm total_packets/total_bytes và hai ratios khi total_packets>0.
Missing/wrong type raise; bad values → NULL. Không dùng rate để tái tính counts.

`aggregate_flows(annotated) → DataFrame` ở [dòng 30](../processing/features.py#L30)
gọi add_features lần nữa trên all rows. Danh sách measures có count, status sums,
VALID-gated total sums và avg. Lặp bốn grouping, thêm dimension NULL khi roll-up,
valid/error ratios, unionByName. Không có action ở hàm; main count mới materialize.
**Hỏi code:** “Tổng flow_count mọi row aggregate có bằng input không?” Không,
cùng input được đếm ở nhiều cấp; chỉ lấy đúng `grouping='batch'` để so input.

## 10. `processing/quality_report.py`

| Hàm và dòng | Input → return | Điều phải hiểu |
|---|---|---|
| [quality_snapshot:13](../processing/quality_report.py#L13) | frame, contract?, cleaned bool → Python dict | groupBy source_file/agg nhiều count predicates rồi collect; labels group/collect riêng |
| Nested count:23 / unpack:59 | tên+Column → append expression; values dict → metrics dict | Count cells theo alias; tổng batch bằng tổng theo file, parsed=input-rejected |
| [build_quality_report:77](../processing/quality_report.py#L77) | config/manifest/before/after/post/reconcile/performance/outputs → dict | Chỉ ghép kết quả đã đo, trạng thái VERIFIED, không tự chạy Spark action |
| [summary_csv:97](../processing/quality_report.py#L97) | report dict → CSV str | Python csv, batch+per-file ở hai giai đoạn |
| [json_text:109](../processing/quality_report.py#L109) | JSON-serializable object → str | allow_nan=False, không xuất NaN/Infinity JSON không chuẩn |

Historical cast evidence không biến mất sau clean. `quality_snapshot` bỏ 57 non-core
fields khỏi QA typed, khác source audit. Số file/label groups lớn có thể làm collect
về driver lớn; hiện dataset có ít file/label. Missing columns hoặc object không JSON
serializable làm lỗi. **Hỏi code:** “NULL tăng nghĩa chất lượng giảm?” Không nhất
thiết; [theo record B/C](data-quality-explained.md) cho thấy NULL có policy và flag.

## 11. `processing/output.py` — file phải đọc thêm

| Hàm và dòng | Input → return | Logic |
|---|---|---|
| [_location:11](../processing/output.py#L11) | path str, pattern bool → tuple normalized location | Chặn scheme lạ, credentials/query, escapes/traversal; thu input glob về prefix để so overlap |
| [_overlaps:36](../processing/output.py#L36) | hai locations → bool | Cùng scheme/authority, bằng hoặc ancestor/descendant |
| [output_plan:43](../processing/output.py#L43) | config → dict[path_name, URI] | Validate config không buộc MinIO, cấm raw/root/overlap; thêm batch ID |
| [hadoop_path:58](../processing/output.py#L58) | spark, uri → (filesystem, JVM Path) | getFileSystem theo scheme |
| [preflight_outputs:63](../processing/output.py#L63) | spark, paths → None/raise | Từ chối batch output có sẵn |
| [write_text:70](../processing/output.py#L70) | spark, uri, str → None | Hadoop create overwrite=False, UTF-8 writer, close finally |
| [write_verified_parquet:79](../processing/output.py#L79) | frame,path,expected_count?,verify_labels bool → evidence dict | Write Snappy/errorifexists; reread dtypes/count; optional group distribution exceptAll |

Read-back schema dùng dtypes, không so nullability hints. Giữ full schema JSON trong
evidence để review. Không `partitionBy`, không compaction, không so numeric row
content đầy đủ. **Hỏi code:** “`compression='snappy'` trong JSON đủ chứng minh codec?”
Đó là option/evidence khai báo; test footer tại [test_pipeline:57](../tests/test_pipeline.py#L57)
là bằng chứng physical codec bổ sung.

## 12. `processing/main.py`

`local_output_config(config, root:Path) → config mới` ở [dòng 27](../processing/main.py#L27)
đổi bảy output prefixes thành file URI. `run_pipeline(spark, config,
write_outputs=False) → report dict` ở [dòng 34](../processing/main.py#L34) là flow
thực sự: preflight/started nếu ghi → ingest/cache/count → typed/classified/before QA →
clean/cache/post schema+value gate → reconcile/after QA → features/agg/count →
5 writes/read-back nếu ghi → measure performance → JSON/CSV/final marker.

Nested `persist(frame)` chọn StorageLevel theo config; `elapsed(stage,tick)` đo
perf_counter và log duration. Persist lazy; các group/count/collect phía sau mới
materialize. Sau mỗi ranh giới, code unpersist blocking; cuối hàm cleanup reverse
list, kể cả exception. Persist không phải checkpoint và không tự cắt lineage DAG.
[main:47](../processing/main.py#L47), [main:154](../processing/main.py#L154).

`main() → None` ở [dòng 162](../processing/main.py#L162) parse CLI, tạo session chỉ
khi execute/validate-only; local cần local input/output; optional local report phải
tên mới; finally stop. `--execute` là quyền ghi batch, không tự upload input.

**Đoạn phải hiểu:** [main:136](../processing/main.py#L136) ghi final marker sau các
report; [main:143](../processing/main.py#L143) best-effort failed marker rồi raise.
**Hỏi code:** “Fail ở output thứ ba thì sao?” Hai output trước có thể đã tồn tại,
không rollback; consumer chỉ chọn batch có final `_SUCCESS.json`.

## 13. `storage/minio_client.py` và `storage/upload_minio.py`

`minio_client.py` có 1 byte và không có hàm/class. Không có singleton hay shared
client được gọi từ file này. Uploader tự tạo Boto3 client.

| Hàm uploader | Input → return | Logic |
|---|---|---|
| [sha256_file:15](../storage/upload_minio.py#L15) | path → hex str | Đọc từng block 1 MiB |
| [prepare_upload:23](../storage/upload_minio.py#L23) | source_dir, audit_path, config → list[dict] | Kiểm structural ready, exact file set/hash/size/contract, tạo raw keys |
| [main:49](../storage/upload_minio.py#L49) | CLI args → None | Print plan; --execute mới tạo client, HEAD preflight, upload_file pending, HEAD verify |

Không dùng Spark. Bucket phải có sẵn (`head_bucket`); không tự tạo bucket như
synthetic seeder. Lỗi audit/hash/remote mismatch/mạng dừng; không transaction
all-eight. **Hỏi code:** “Có chống concurrent writer tuyệt đối không?” Không,
đây là single-writer policy, preflight và write tách biệt.

## 14. Ba benchmark wrappers và runner chung

Các file tồn tại và mỗi file chỉ gọi main import từ `experiments.benchmark`:

| Wrapper | Hàm thực sự gọi | Không được suy nhầm |
|---|---|---|
| [benchmark_size.py:5](../experiments/benchmark_size.py#L5) | `main('size')` | Không tự sinh 1/3/5 GB, nhận --inputs |
| [benchmark_workers.py:5](../experiments/benchmark_workers.py#L5) | `main('workers')` | Không tự đổi topology Docker |
| [benchmark_partitions.py:5](../experiments/benchmark_partitions.py#L5) | `main('partitions')` | Đổi shuffle config, không repartition input CSV |

Runner [main:45](../experiments/benchmark.py#L45) parse URI/partitions/repeats/warmup,
new result path, dataset_kind, output prefix, topology arguments. Default PLAN_ONLY;
execute reserve file, create một session, set AQE false mặc định, loop full pipeline,
verify topology/core allocation, append warmup riêng hoặc measured runs, write COMPLETE.
Exception ghi FAILED/error class và stop Spark. Lỗi startup vẫn có result status FAILED.

Các helpers: [master_topology:16](../experiments/benchmark.py#L16) đọc Master JSON và
lọc ALIVE/sort ID; [trial_order:26](../experiments/benchmark.py#L26) đảo thứ tự ở even
repeat; [environment:33](../experiments/benchmark.py#L33) ghi Python/platform/Spark/Java/
heap/cgroup limits. Không lưu toàn environment chứa secrets.

**Hỏi code:** “Master nói 4 cores đã đủ chứng minh hai executors đều chạy task chưa?”
Chưa, resource allocation là bằng chứng khác task execution. Cần Executors/Stages UI
hoặc task event log. Xem [Spark explained](spark-distributed-explained.md).

## 15. Những helper và test liên quan

- [seed_scaling.scaled_payload:11](../experiments/seed_scaling.py#L11) nhận bytes fixture
  và copies int → bytes một header + lặp 12 rows; main chỉ ghi prefix smoke được phép,
  đếm bytes/hash, kiểm existing content. Giữ payload của các size trong RAM driver
  nên chưa phù hợp sinh tải GB; hiện cao nhất khoảng 20 MB/file.
- [plot_results.summarize:12](../experiments/plot_results.py#L12) đọc COMPLETE JSON,
  group đúng workload/config/environment, median/min/max/throughput; matched baseline
  mới có speedup. `chart()` vẽ, không thực thi Spark. Trial timestamps trong helper
  unit test là test fixture, không phải số benchmark.
- [verify_benchmark_outputs.main:11](../experiments/verify_benchmark_outputs.py#L11)
  dùng Boto3 đọc lại final marker/quality/performance, so với JSON benchmark;
  không chạy Spark count toàn bộ Parquet lần mới.
- [tests.spark_case:8](../tests/spark_case.py#L8) tạo Spark local[2], bật ANSI;
  36 tests không tương đương 36 cluster distributed benchmarks. Các test inject
  fail chủ động cần exception và absence success marker mới PASS.
- `processing/__init__.py`, `tests/__init__.py`, extraction helpers không thêm logic
  orchestration. `docs/examine.md` phản ánh mốc cũ 12 tests, không phải snapshot mới nhất.

## 16. Findings cần biết khi bảo vệ — chưa sửa

Đây là review tĩnh và đối chiếu artifact, không phải phát hiện qua một vòng chạy mới.

| Mức | Finding / tác động | Source | Hướng cải thiện đề xuất |
|---|---|---|---|
| Cao về minh chứng | Full real input mới chạy local read-only; synthetic MinIO không thay full real E2E | [report thật](pipeline-eight-real.local.json), [audit](benchmarks/20261021/minio-audit.json) | Thực nghiệm real write/read-back riêng khi có quyền và tài nguyên |
| Cao về diễn giải | Two workers/resources allocated chưa chứng minh mỗi executor làm bao nhiêu task | [benchmark:145](../experiments/benchmark.py#L145) | Lưu Stages/Executors UI hoặc event logs; phân biệt loại bằng chứng |
| Vừa về invariant | Hậu kiểm non-rate VALID kiểm type/domain, chưa so mọi numeric value trước/sau | [validation:98](../processing/validation.py#L98) | Thêm invariant non-rate immutability/test khi chủ động nâng policy |
| Vừa về ranh giới count | Expected input lấy từ raw_df, chưa so với count audit độc lập của file | [main:67](../processing/main.py#L67) | Bảo toàn được kiểm từ raw_df trở đi; nếu cần, đối chiếu thêm audit manifest count |
| Vừa về integrity | Read-back kiểm schema/count/distribution, chưa full row numeric equality | [output:83](../processing/output.py#L83) | Nếu cần assurance cao, so multiset/content digest phù hợp cost |
| Vừa về commit | Preflight/write không atomic; đa output không transaction; batch mới có thể lặp dữ liệu | [output:63](../processing/output.py#L63), [main:99](../processing/main.py#L99) | Single writer, manifest consumer, thiết kế idempotence/transaction khi scope cần |
| Vừa về hiệu năng | Checksum/header tuần tự driver, 4 grouping, nhiều QA actions/shuffles | [ingestion:69](../processing/ingestion.py#L69), [features:48](../processing/features.py#L48) | Profile trước tối ưu; giữ nguyên correctness gates khi giảm repeated work |
| Vừa về storage | Chưa partitionBy/compaction; aggregate nhỏ có thể sinh small files | [output:81](../processing/output.py#L81) | Đo part-file size/count, cân nhắc số output partition theo bảng |
| Vừa về contract | JSON nói PERMISSIVE/quarantine malformed; code strict/reject; Markdown trim header nhưng code exact | [raw JSON](../data/schemas/raw_flow_schema.json), [ingestion:16](../processing/ingestion.py#L16) | Version và thống nhất đặc tả ở đợt sửa riêng |
| Vừa về tái lập | pipeline_version mặc định hằng `0.1.0`, chưa buộc Git SHA; image tags chưa pin digest | [config:14](../processing/config.py#L14), [Dockerfile:1](../docker/spark/Dockerfile#L1) | Ghi commit/dependency/image digest cho thí nghiệm mới |
| Vừa về resources | Executor default 1g khác lần full-data PASS 2g; DISK_ONLY vẫn cần heap để tạo batch | [config:58](../processing/config.py#L58), [OOM hồ sơ](pipeline-validation.md) | Công bố đúng override, không suy worker memory là executor memory |
| Vừa về vận hành | check_minio cố định MEMORY_AND_DISK; optional report có thể overwrite | [check:29](../processing/check_minio.py#L29), [check:59](../processing/check_minio.py#L59) | Dùng input nhỏ/tên report mới khi demo; sau này thống nhất config/CLI |
| Tài liệu | Hai PNG và report_draft rỗng; minio_client/extraction placeholder; không thấy rubric gốc | [tổng quan](system-overview-explained.md) | Dùng Mermaid hiện có, hoàn thiện báo cáo chính và đối chiếu đề lớp |

Không cần che các giới hạn để bảo vệ. Câu trả lời tốt phải chỉ ra code bảo đảm điều
gì, không bảo đảm điều gì, bằng chứng nào đang có và bước kiểm chứng tiếp theo.

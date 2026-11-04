# Kiểm chứng Storage → Processing

> Hồ sơ lịch sử; không coi kết quả dưới đây là lần chạy mới. Đường dẫn bằng chứng
> đã được cập nhật theo [danh mục lưu trữ](../evidence/README.md). Dùng
> [README hiện hành](../../README.md) để xem trạng thái mới nhất và lệnh chạy.

Đây là hồ sơ kiểm chứng ngày 20/10/2026. Thực nghiệm bổ sung ngày **21/10/2026**
(size, 1/2 workers, 4/8/16/32 partitions và bộ test mở rộng) được ghi riêng tại
[storage-processing-report.md](storage-processing-report.md); giữ các số đo cũ
ở đây để không trộn workload/cấu hình hoặc che các lần thử thất bại.

Ngày tổng hợp: 20/10/2026 (Asia/Bangkok). Số liệu dưới đây được lấy từ các job
thực thi, không thay bằng số liệu minh họa. JSON/log `*.local.*` được giữ local
theo `.gitignore`; bản Markdown này lưu kết luận để review cùng source code.

## Kiểm tra ban đầu và phần tái sử dụng

- Đã đọc Data Contract v2.3.0, schema 79 trường String/22 trường lõi, cả 8 tên
  file, quy tắc duplicate header, rate/window/label và cấu hình Compose.
- Tái sử dụng `config`, `audit_source`, `ingestion`, `transform`, `validate_flows`,
  chính sách cleaning và uploader. Không sửa 8 CSV gốc hoặc Data Contract.
- `features.py`, `quality_report.py`, `main.py`, các benchmark script ban đầu
  rỗng; đây là các phần triển khai mới.
- `clean_classified_flows()` tách từ cleaning cũ để orchestration không cast và
  validate hai lần; `clean_flows()` giữ API và kết quả cũ.
- Windows host Python 3.13.2 chưa có PySpark: lần test đầu 4 test đạt, 3 module
  Spark không import được. Trong image dự án (Spark 3.5.0/Python 3.8), **12 test
  cũ đều đạt**, 25,483 giây.
- Compose kiểm tra hợp lệ; Spark Master + hai Workers + MinIO khởi động được.
  Không in hoặc hardcode credentials; Spark dùng credentials từ môi trường.
- `storage.upload_minio` chạy **DRY_RUN** thành công để đối chiếu SHA-256 hiện
  tại của đủ 8 CSV, schema và Contract với audit đã có; không upload object.
  Bằng chứng: `docs/evidence/local/source-integrity-20261021.local.json`.

## Test và lỗi được phát hiện

Bộ mở rộng đầu tiên phát hiện hết Java heap khi hậu kiểm một chuỗi Spark plan
dài chưa materialize. Test đã được điều chỉnh để cache từng giai đoạn giống
pipeline; **26 test đạt trong 79,208 giây**. Log:
`docs/evidence/local/pipeline-tests-rerun.local.log`.

Sau khi bổ sung test output rỗng, metadata, cấu hình cache và benchmark,
**bộ cuối cùng có 29/29 test đạt trong 93,481 giây**. Log:
`docs/evidence/local/pipeline-tests-verified.local.log`. Test output rỗng cũng materialize
annotated input trước khi ghi ba nhánh, như pipeline thực tế; lần thử chưa
materialize bị OOM được lưu riêng ở `docs/evidence/local/pipeline-tests-final.local.log`.
Các lỗi ghi và count mismatch được cố ý inject trong test phải trả lỗi và
không tạo marker hoàn thành. `compileall` và `git diff --check` đạt.

Kiểm tra CSV thật local `[2]` với heap mặc định 1 GB còn gặp thiếu bộ nhớ khi
cache raw evidence có 79 cột. Pipeline giảm cache batch xuống 1000, cache hậu
kiểm chỉ giữ cột trạng thái/lỗi, giải phóng cache đồng bộ sau khi dùng. CSV thật
đã chạy thành công trên cluster hai workers, mỗi executor 2 cores/1 GB. Không
coi các lần hết bộ nhớ là kết quả PASS; log lỗi vẫn được giữ local.

## End-to-end synthetic qua MinIO

Nguồn `s3a://bigdata-network/smoke/cicids2017/synthetic_flow.csv`: 12 dòng,
3.420 bytes. Batch `d8d60119a64e4d4aabe4de5833a51faa`, application
`app-20261019160657-0000`. Spark chạy qua `spark://spark-master:7077`.

| Chỉ số | Trước cleaning | Sau cleaning |
|---|---:|---:|
| Input records | 12 | 12 |
| VALID | 4 | 4 |
| QUARANTINED | 6 | 6 |
| REJECTED | 2 | 2 |
| NULL cells | 2 | 5 |
| NaN cells | 1 | 0 |
| Infinity cells | 1 | 0 |
| Invalid range cells | 4 | 4 |
| Lịch sử lỗi cast | 1 | 1 |

- Hậu kiểm PASS; không có dòng VALID vi phạm. Lỗi miền còn lại thuộc output
  quarantine/reject và được giữ để truy vết.
- Cả 5 output Parquet Snappy ghi vào batch riêng dưới `smoke/pipeline/`, đọc lại
  được; schema, count và phân bố nhãn của các bảng flow khớp.
- Cleaned giữ 34 cột (22 lõi + 12 metadata); features có prefix riêng.
- Thời gian đo: **37,131 giây**, throughput **0,323 flow/s**. Đây là workload cực
  nhỏ, chủ yếu đo overhead; không dùng để kết luận khả năng mở rộng.
- Có `quality.json`, `summary.csv`, `performance.json`, marker hoàn thành cuối.
  Bản báo cáo lấy về: `docs/evidence/local/pipeline-smoke.local.json`; log:
  `docs/evidence/local/pipeline-smoke.local.log`.

**Xác minh lại với code cuối ngày 20/10/2026:** batch
`c53a54981e4b46b99c7093030c2faa66`, app `app-20261020041357-0004`,
storage level DISK_ONLY, **32,587 giây**. Vẫn 12 = 4 VALID + 6 QUARANTINED +
2 REJECTED; hậu kiểm PASS và cả 5 output Parquet read-back PASS. Bằng chứng:
`docs/evidence/local/pipeline-smoke-final.local.json`, `docs/evidence/local/pipeline-smoke-final.local.log`.

## Một CSV thật — chỉ đọc trên cluster

File `Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv`, 52.023.263 bytes,
đọc từ `/workspace/data/raw/cicids2017/` được mount read-only ở driver/workers.
Batch `d96c95ae1f4b490a86503e2d527efd70`, app `app-20261020035145-0000`.
Trạng thái **VALIDATED_NO_OUTPUT**; không upload input, không xuất Parquet thật.

| Chỉ số | Trước cleaning | Sau cleaning |
|---|---:|---:|
| Input records | 170.366 | 170.366 |
| VALID | 170.355 | 170.355 |
| QUARANTINED | 11 | 11 |
| REJECTED | 0 | 0 |
| NULL cells | 0 | 184.575 |
| NaN cells | 20 | 0 |
| Infinity cells | 250 | 0 |
| Invalid range cells | 43 | 22 |
| Lịch sử lỗi cast | 0 | 0 |

NULL tăng do rate lỗi và sentinel window chuyển NULL theo Contract; không phải
số record bị mất. Các nhóm lỗi đếm cell, có thể giao nhau. Hậu kiểm PASS, nhãn
và multiset raw evidence bảo toàn, không có chuyển trạng thái ngoài chính sách.
Tính được 170.355 dòng feature và 10 dòng aggregate trên bốn loại grouping.

Thời gian **62,293 giây**, **2.734,922 flow/s**: bao gồm đọc/checksum, cast,
validation, cleaning, đối soát, tính QA, features/aggregation; không có ghi/đọc
Parquet và không bao gồm khởi tạo Spark hoặc ghi JSON cuối. Không so trực tiếp
với tốc độ smoke vì khác workload/phạm vi thời gian. Shuffle partitions = 4,
AQE bật, cache batch = 1000. Report để worker_count null khi chưa thu bằng chứng
Master API; không suy worker count từ số executor.

Báo cáo: `docs/evidence/local/pipeline-one-real.local.json`. Log thành công:
`docs/evidence/local/pipeline-one-real-cluster.local.log`. Log lỗi local được lưu riêng.

## Đủ 8 CSV thật — chỉ đọc trên cluster

Đã hoàn thành ngày 20/10/2026 với batch `8db0cd669fa5487eb8e2589c018c3d3d`,
app `app-20261020040330-0003`. Đọc 884.645.759 bytes từ mount local read-only;
trạng thái **VALIDATED_NO_OUTPUT**. Không upload hoặc ghi Parquet dữ liệu thật.

| Chỉ số | Trước cleaning | Sau cleaning |
|---|---:|---:|
| Input records | 2.830.743 | 2.830.743 |
| VALID | 2.830.628 | 2.830.628 |
| QUARANTINED | 115 | 115 |
| REJECTED | 0 | 0 |
| NULL cells | 0 | 2.448.675 |
| NaN cells | 1.358 | 0 |
| Infinity cells | 4.376 | 0 |
| Invalid range cells | 430 | 230 |
| Lịch sử lỗi cast | 0 | 0 |
| Window sentinel đã ghi nhận | 2.442.741 | 2.442.741 |

Hậu kiểm và schema PASS, **0 record có lỗi hậu kiểm**. Còn 230 cell lỗi miền
trong các bản ghi cách ly; chúng không nằm trong cleaned VALID. Multiset nguồn
và nhãn được bảo toàn. Không có chuyển trạng thái ngoài chính sách. Tính được
2.830.628 dòng feature và 46 dòng aggregate.

| Source file (tên rút gọn) | Input | VALID | QUARANTINED | REJECTED |
|---|---:|---:|---:|---:|
| Monday | 529.918 | 529.903 | 15 | 0 |
| Tuesday | 445.909 | 445.892 | 17 | 0 |
| Wednesday | 692.703 | 692.682 | 21 | 0 |
| Thursday Morning WebAttacks | 170.366 | 170.355 | 11 | 0 |
| Thursday Afternoon Infilteration | 288.602 | 288.598 | 4 | 0 |
| Friday Morning | 191.033 | 191.024 | 9 | 0 |
| Friday Afternoon PortScan | 286.467 | 286.431 | 36 | 0 |
| Friday Afternoon DDos | 225.745 | 225.743 | 2 | 0 |

Thời gian **315,380 giây**, throughput **8.975,671 flow/s**, cùng định nghĩa
phạm vi chỉ đọc như job một CSV. Cấu hình: executor **2 cores / 2 GB**, shuffle
partitions 4, input partitions 30, AQE bật, cache batch 1000, **DISK_ONLY**.

| Giai đoạn được đo | Giây |
|---|---:|
| Ingestion + SHA-256 | 88,930 |
| Transformation + validation + QA trước | 68,065 |
| Cleaning + hậu kiểm + đối soát + QA sau | 133,420 |
| Features + aggregation | 24,947 |

Các lần thử toàn bộ input với executor 1 GB đã gặp OOM cả khi dùng
MEMORY_AND_DISK và DISK_ONLY, được dừng rồi chạy lại bằng batch mới. **DISK_ONLY
không thay thế yêu cầu heap đủ lớn để tạo các lô dữ liệu cache.** Cấu hình 2 GB
phù hợp giới hạn 2 GB/worker trong Compose và đã hoàn thành không có OOM trong
log lần chạy thành công. Không so tốc độ một CSV/8 CSV như benchmark size có
kiểm soát vì khác cấu hình bộ nhớ/cache và chưa lặp lại nhiều lần.

Báo cáo: `docs/evidence/local/pipeline-eight-real.local.json`. Log thành công:
`docs/evidence/local/pipeline-eight-real-2g.local.log`. Các log thử 1 GB được giữ riêng.
`docs/evidence/local/pipeline-final-topology.local.json` lấy từ Spark Master xác nhận app trên
ở trạng thái **FINISHED**, 4 cores, memory per executor 2048 MiB; hai workers
ALIVE, mỗi worker 2 cores/2048 MiB.

## Benchmark partition trên synthetic fixture

Đã thực thi đủ bốn trial local `[2]`, cùng input 12 dòng/3.420 bytes, ghi và
đọc lại đầy đủ output vào `/tmp/benchmark-output-20261020`. Mỗi trial có batch
riêng. Không dùng dữ liệu nhân bản hoặc số đo giả.

| Shuffle partitions | Lần 1 (giây) | Lần 2 (giây) | Median (giây) | Median throughput (flow/s) |
|---|---:|---:|---:|---:|
| 2 | 23,561 | 11,823 | 17,692 | 0,762 |
| 4 | 11,227 | 11,023 | 11,125 | 1,079 |

Thứ tự chạy: 2, 2, 4, 4 trong cùng Spark session; không bỏ warmup. Lần đầu có
overhead rõ rệt, nên **không kết luận 4 partitions nhanh hơn về khả năng xử lý
dữ liệu lớn** từ bảng này. AQE bật nên shuffle partitions là cấu hình ban đầu,
không bảo đảm số task cuối. Throughput median tính từ từng trial, không phải
lấy input chia cho median duration. Thời gian bao gồm ghi và read-back, khác
với job CSV thật chỉ đọc.

Các trial này chạy với `MEMORY_AND_DISK`, trước khi mặc định pipeline được đổi
sang `DISK_ONLY` để phục vụ input lớn. Không trộn chúng với trial có storage
level khác khi so sánh. Summarizer mới tách nhóm cả theo storage level, cache
batch size và AQE; báo cáo cũ không có storage_level được đánh dấu UNRECORDED.

Bằng chứng: `docs/evidence/local/benchmark-partitions.local.json`,
`docs/evidence/local/benchmark-partitions.local.log`, `docs/evidence/local/benchmark-summary.local.csv`.
Runner size và worker đã có CLI, nhưng chưa có phép so sánh size hoặc 1/2 workers
đủ lặp để kết luận hiệu năng. Không tự điền các kết quả chưa đo.

## Giới hạn và tái lập

Lệnh CLI, chính sách output, định nghĩa metric và benchmark nằm trong
[processing.md](../processing.md). Chỉ sử dụng batch có marker `_SUCCESS.json` tại
prefix quality report. Một `_SUCCESS` bên trong Parquet không xác nhận cả job.

Chưa có kết quả ghi Parquet của dữ liệu thật lên MinIO. Việc upload CSV thật
hoặc chạy `--execute` trên dữ liệu thật cần quyết định riêng của người dùng.
Không dùng kết quả synthetic để thay thế thử nghiệm đó. Không triển khai ML,
time-window/IP aggregation hoặc cảnh báo.

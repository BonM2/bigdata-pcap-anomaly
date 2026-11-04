# Danh mục bằng chứng thực nghiệm

Các artifact có số đo phục vụ báo cáo được giữ nguyên khi sắp xếp tài liệu ngày
02/11/2026. Di chuyển file không phải một lần chạy lại pipeline và không thay đổi
kết quả PASS/FAIL lịch sử.

## Nơi lưu

| Thư mục | Nội dung | Có trong Git |
|---|---|---|
| [../benchmarks/20261021/](../benchmarks/20261021/) | Trial JSON, manifest, CSV, biểu đồ, audit MinIO và tổng hợp tests | Có |
| [../reports/](../reports/) | Hồ sơ audit nguồn, kiểm chứng pipeline và báo cáo triển khai | Có |
| `local/` | Log chi tiết, báo cáo CSV thật, các lần thử thành công/thất bại | Không |

`local/` nằm trong `.gitignore`. Khi clone mới, tạo thư mục trước khi copy báo cáo:

```powershell
New-Item -ItemType Directory -Force docs/evidence/local | Out-Null
```

Các link local bên dưới chỉ mở được trên máy đang giữ bằng chứng. Không nhận một
kết quả là đã chạy lại khi chỉ đọc artifact cũ. Khi chuyển máy để bảo vệ, sao chép
bằng chứng cần dùng riêng; không đưa dataset lớn hoặc secrets vào Git.

## Vai trò các file local được giữ lại

| File | Vai trò |
|---|---|
| [source-audit.local.json](local/source-audit.local.json) | Audit chi tiết 8 CSV: header, số dòng, giá trị, nhãn và hash |
| [source-integrity-20261021.local.json](local/source-integrity-20261021.local.json) | Kết quả uploader DRY_RUN, đối chiếu tính toàn vẹn nguồn |
| [minio-smoke.local.json](local/minio-smoke.local.json) | Bằng chứng đọc fixture qua S3A và phân loại 12 records |
| [pipeline-smoke-final.local.json](local/pipeline-smoke-final.local.json) | Report synthetic E2E của code cuối ngày 20/10 |
| [pipeline-smoke-final.local.log](local/pipeline-smoke-final.local.log) | Log của lần smoke cuối |
| [pipeline-smoke.local.json](local/pipeline-smoke.local.json) | Report của lần smoke trước, khác batch/thời gian |
| [pipeline-smoke.local.log](local/pipeline-smoke.local.log) | Log đi kèm lần smoke trước |
| [pipeline-one-real.local.json](local/pipeline-one-real.local.json) | Report một CSV thật, VALIDATED_NO_OUTPUT |
| [pipeline-one-real-cluster.local.log](local/pipeline-one-real-cluster.local.log) | Log lần một CSV thành công trên cluster |
| [pipeline-one-real.local.log](local/pipeline-one-real.local.log) | Log lần thử trước trên một CSV, giữ để đối chiếu lỗi |
| [pipeline-one-real-rerun.local.log](local/pipeline-one-real-rerun.local.log) | Log lần thử lại một CSV trước cấu hình thành công |
| [pipeline-eight-real.local.json](local/pipeline-eight-real.local.json) | Report đủ 2.830.743 records thật, chưa ghi output |
| [pipeline-eight-real-2g.local.log](local/pipeline-eight-real-2g.local.log) | Log thành công với executor 2g |
| [pipeline-eight-real.local.log](local/pipeline-eight-real.local.log) | Log lần thử full-data trước khi tăng heap |
| [pipeline-eight-real-disk.local.log](local/pipeline-eight-real-disk.local.log) | Log lần thử DISK_ONLY với heap chưa đủ |
| [pipeline-cluster-topology.local.json](local/pipeline-cluster-topology.local.json) | Snapshot tài nguyên cluster trong quá trình kiểm chứng |
| [pipeline-final-topology.local.json](local/pipeline-final-topology.local.json) | Snapshot app full-data FINISHED và tài nguyên Workers |
| [pipeline-tests-20261021.local.log](local/pipeline-tests-20261021.local.log) | Log 36/36 tests PASS; SHA-256 nằm trong test-results.json |
| [pipeline-tests-verified.local.log](local/pipeline-tests-verified.local.log) | Log bộ 29 tests của đợt 20/10 |
| [pipeline-tests-rerun.local.log](local/pipeline-tests-rerun.local.log) | Log bộ test trung gian sau điều chỉnh cache |
| [pipeline-tests-final.local.log](local/pipeline-tests-final.local.log) | Log lần thử có lỗi OOM; tên final không có nghĩa PASS |
| [pipeline-tests.local.log](local/pipeline-tests.local.log) | Log lần chạy bộ test trước các điều chỉnh |
| [benchmark-size-20261021.local.log](local/benchmark-size-20261021.local.log) | Log benchmark 12k/60k/120k |
| [benchmark-partitions-20261021.local.log](local/benchmark-partitions-20261021.local.log) | Log benchmark 4/8/16/32 shuffle partitions |
| [benchmark-workers-1-20261021.local.log](local/benchmark-workers-1-20261021.local.log) | Log benchmark 1 Worker |
| [benchmark-workers-2-20261021.local.log](local/benchmark-workers-2-20261021.local.log) | Log benchmark 2 Workers |
| [benchmark-topology-restored.local.log](local/benchmark-topology-restored.local.log) | Bằng chứng khôi phục Worker 2 sau thí nghiệm |
| [benchmark-partitions.local.json](local/benchmark-partitions.local.json) | Phép đo cũ trên fixture 12 dòng; không trộn với đợt 120k |
| [benchmark-partitions.local.log](local/benchmark-partitions.local.log) | Log của phép đo partition cũ |
| [benchmark-summary.local.csv](local/benchmark-summary.local.csv) | Bảng tổng hợp phép đo partition cũ |

## Quy tắc sử dụng sau khi sắp xếp

- Đường dẫn cũ `docs/<tên>.local.*` hiện tương ứng `docs/evidence/local/<tên>.local.*`.
  [commands.txt](../benchmarks/20261021/commands.txt) giữ nguyên lệnh lịch sử, nên các
  đường dẫn local trong đó phản ánh vị trí tại thời điểm chạy. Dùng README chính
  và hướng dẫn hiện hành để chạy mới.
- Trường `log` trong [test-results.json](../benchmarks/20261021/test-results.json)
  đã đổi sang vị trí hiện tại; số test, thời gian, trạng thái và SHA-256 không đổi.
- CLI audit/uploader vẫn có mặc định cũ trong code. Để giữ file mới trong thư mục
  này, truyền `--report` và `--audit-report` tường minh theo [processing.md](../processing.md).
- Dùng tên mới cho lần chạy mới; không ghi đè bằng chứng đã dùng trong báo cáo.
  Không xóa log lỗi chỉ vì đã có lần chạy thành công.

Đã loại bỏ hai PNG placeholder 1 byte, bản nháp báo cáo 1 byte và checklist
`examine.md` lỗi thời. `source-integrity.local.json` trùng SHA-256 hoàn toàn với
`source-integrity-20261021.local.json`, nên chỉ giữ bản có ngày. Các test Python
được giữ đủ 36 cases; chỉ cache `tests/__pycache__/` được dọn.

## Kiểm tra sau khi sắp xếp — 02/11/2026

Đã chạy:

```powershell
python -B -m unittest tests.test_config tests.test_audit tests.test_output tests.test_benchmark -v
```

Kết quả: **12 PASS, 0 FAIL, 0 ERROR, 0 SKIPPED**, 0,100 giây. Đây là các tests
không tạo Spark session; không thay thế kết quả suite 36 tests ngày 21/10.
Không chạy lại Spark jobs hoặc benchmark trong đợt dọn tài liệu.

Đã đối chiếu SHA-256: 30 file bằng chứng local giữ nguyên nội dung; mã nguồn,
test Python, Data Contract và Docker config không thay đổi. Các artifact benchmark
giữ nguyên byte, ngoại trừ cập nhật đường dẫn log trong `test-results.json`.
Các liên kết tương đối và cú pháp PowerShell trong hướng dẫn đã được kiểm tra.

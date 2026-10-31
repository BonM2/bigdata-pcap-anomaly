# Data Contract, Cleaning và theo dõi từng record

Đọc source ngày 25/10/2026. Các record dưới là **fixture/test tổng hợp**, không
phải quan sát traffic thật. Giá trị trước/sau được suy ra từ fixture và các hàm
đã đọc, có test lịch sử kiểm chứng; không phát sinh một job mới trong đợt phân tích.

## 1. Ba việc thường bị nhầm

| Việc | Câu hỏi trả lời | Có thay giá trị không? | Source |
|---|---|---|---|
| Source Audit / Profiling | File có bao nhiêu row, lỗi, kiểu, phân bố? | Không sửa file; tạo thống kê/preview | [audit_file:17](../processing/audit_source.py#L17) |
| Validation | Row vi phạm rule nào, thuộc nhánh gì? | Thêm status/reason, chưa sửa numeric value | [validate_flows:138](../processing/validation.py#L138) |
| Cleaning | Lỗi nào được phép đổi thành dạng sử dụng được? | Chỉ rate invalid/window sentinel theo policy | [clean_classified_flows:41](../processing/clean.py#L41) |
| Post-validation | Việc clean/split có giữ invariant không? | Thêm error array/summary, không đổi status | [validate_cleaned_flows:69](../processing/validation.py#L69) |

Audit cho preview bằng Python không thay thế chạy Spark. Một record `VALID` có
thể mang nhãn tấn công; record `QUARANTINED` có thể mang `BENIGN`. Hai trục này
khác nhau: [validation:177](../processing/validation.py#L177).

## 2. Raw schema và cleaned schema

Schema JSON có `version=1.0.0` cho mapping raw, tham chiếu **contract 2.3.0**;
`SCHEMA_VERSION` xuất vào metadata là `v2.3.0`. Không đồng nhất hai số version.
[JSON raw](../data/schemas/raw_flow_schema.json), [config:13](../processing/config.py#L13).

| Tầng | Trường/kiểu | Cần hiểu |
|---|---|---|
| CSV bytes | 79 tokens theo vị trí header | Nguồn bất biến, có cả header trùng |
| Raw DataFrame | 79 StringType + 5 source/evidence String fields | Nullable=True là schema parser, chưa phải phê duyệt missing |
| Typed/annotated | 22 core typed + metadata/evidence/quality | `_raw_values` giữ cả 79 tokens; `_raw_record` giữ physical text |
| Valid cleaned | 22 core + 12 metadata = **34 cột** | Không mang raw struct/text trong output này |
| Features | 34 + 4 = **38 cột** | Bốn feature không thay core contract |
| Quarantine/reject | Core typed + evidence + quality/status_before/flags | Cần để giải thích lỗi và truy lại nguồn |

Nguồn: [raw_schema:45](../processing/config.py#L45),
[ingestion schema:89](../processing/ingestion.py#L89),
[transform select:43](../processing/transform.py#L43),
[clean select:53](../processing/clean.py#L53), [features:22](../processing/features.py#L22).

22 core chia theo kiểu đích:

| Kiểu Spark | Các trường |
|---|---|
| Int (6) | `destination_port`, `fin_flag_count`, `syn_flag_count`, `rst_flag_count`, `psh_flag_count`, `ack_flag_count` |
| Long (7) | `flow_duration`, `total_fwd_packets`, `total_backward_packets`, `total_length_of_fwd_packets`, `total_length_of_bwd_packets`, `init_win_bytes_forward`, `init_win_bytes_backward` |
| Double (8) | `flow_bytes_s`, `flow_packets_s`, `flow_iat_mean`, `flow_iat_std`, `min_packet_length`, `max_packet_length`, `packet_length_mean`, `packet_length_std` |
| String (1) | `label` |

Đơn vị: duration/IAT là µs, rate là byte/s hoặc packet/s; bytes tổng ở đây là payload
bytes theo contract, không wire bytes. `fwd` không luôn có nghĩa client→server.
[Contract:77](../data/schemas/feature_dictionary.md#L77),
[Contract:118](../data/schemas/feature_dictionary.md#L118).

## 3. Ép kiểu không chỉ là gọi cast

`transform_flows()` dùng numeric trim + `try_cast`; Int/Long phải khớp
`^[+-]?[0-9]+$`. Vì vậy port `' 443 '` → 443; packet count `'1.5'` hoặc `'1e2'`
→ NULL và có cast_failure; Double `'1.25e2'` → 125.0. Empty string → NULL nhưng
được báo EMPTY, không thêm cast failure. Label được select nguyên chuỗi.
[transform:30](../processing/transform.py#L30),
[test_transform:24](../tests/test_transform.py#L24).

Giữ raw giúp phân biệt NULL sinh từ empty, cast lỗi, sentinel hay lỗi rate; nếu chỉ
nhìn numeric NULL sẽ mất nguyên nhân. Lineage bắt buộc ở mức batch/file/hash,
không có record_id hay source_row_number ổn định. `VALID` output bỏ raw struct,
nên truy vết qua batch + source_file + manifest SHA về nguồn, chưa đủ định danh
duy nhất từng row trùng: [clean:60](../processing/clean.py#L60),
[manifest:86](../processing/ingestion.py#L86).

## 4. NULL, NaN, Infinity và sentinel

| Giá trị | Nghĩa kỹ thuật | Policy thực tế |
|---|---|---|
| NULL | Không có giá trị sử dụng được trong ô typed | Ở rate có thể clean và gắn flag; non-rate bắt buộc thường hard error |
| Empty `''`/whitespace | Có token text nhưng rỗng sau trim | Reason EMPTY; label rỗng là INVALID_LABEL |
| NaN | Giá trị floating-point “not a number”, khác SQL NULL | Rate → NULL+flag; IAT/packet size → quarantine |
| ±Infinity | Giá trị floating-point không hữu hạn | Cùng phân biệt rate/non-rate như trên |
| Rate âm | Cast thành công nhưng không hợp lệ theo contract | NULL+invalid flag, không đổi thành 0 |
| Window `-1` | Sentinel missing theo bộ nguồn/contract này | NULL+missing flag; không phải window đo được bằng 0 |
| Window `-2` | Giá trị âm khác sentinel được cho phép | Quarantine, giữ value/evidence |

Xem [validate_flows:160](../processing/validation.py#L160),
[clean:45](../processing/clean.py#L45), [Contract:304](../data/schemas/feature_dictionary.md#L304).
Nguyên tắc lý thuyết: missing/error khác đo bằng 0. Policy cụ thể: hai rate được
phép missing còn các trường kia không tự động được miễn. Sentinel -1 là quy ước
nguồn đã được contract chấp nhận, không suy mọi extractor hoặc mọi TCP stack dùng cùng quy ước.

Infinity **có thể** xuất hiện khi tính rate với duration bằng 0, nhưng code không
xác minh nguyên nhân đó cho từng row. Không nói “mọi Infinity đều do 0 duration”.
Rate lớn nhưng hữu hạn, không âm vẫn giữ; chưa có threshold “outlier traffic”
để xóa row. Không nên loại các hành vi hiếm chỉ vì có thể liên quan tấn công.
[validation:164](../processing/validation.py#L164).

## 5. Quy tắc ba trạng thái

```text
Có _corrupt_record hoặc _malformed_reason ? → REJECTED
Nếu không, có hard error ?                  → QUARANTINED
Còn lại                                   → VALID (có thể có soft reasons)
```

Ưu tiên này được dựng bằng `when(malformed).when(hard).otherwise(VALID)` tại
[validation:184](../processing/validation.py#L184). Cleaning không đổi status.
Reason của row reject vẫn có thể gồm hard error khác; không đếm nó thêm vào quarantine.

## 6. Record A: flow hợp lệ — fixture dữ liệu số 1

Nguồn [sample_flow.csv dòng 2](../data/sample/sample_flow.csv#L2), header ở dòng 1.
Tất cả numeric token bằng `'0'`, label `SYNTHETIC_TEST`. Đây là ví dụ hợp lệ theo
policy, không khẳng định đây là flow mạng có ý nghĩa trong thực tế.

| Stage | Trước → sau | Hàm/quy tắc | Status | Nơi chuyển tiếp |
|---|---|---|---|---|
| Raw | 79 text tokens, label gốc | Nguồn fixture | Chưa gán | CSV input |
| Ingestion | Text → 79 tên canonical + source/evidence | [parse_record:24](../processing/ingestion.py#L24), width=79 | Chưa gán | Raw df |
| Transform | `'0'` → 0/0.0; label không đổi | [transform_flows:8](../processing/transform.py#L8), `cast_failure_fields=[]` | Chưa gán | Typed df |
| Validate | Values giữ nguyên; reasons=[] | [validate_flows:138](../processing/validation.py#L138) | VALID | Classified |
| Clean | Rate/window 0 giữ 0; 4 cờ=false | [clean:41](../processing/clean.py#L41) | VALID→VALID | Valid 34 cột; annotated có evidence |
| Post-check | Label/cờ/range đúng; errors=[] | [validate_cleaned_flows:69](../processing/validation.py#L69) | VALID | Qua gate main |
| Features | packets=0, bytes=0, fwd/bwd ratio=NULL | [add_features:9](../processing/features.py#L9), mẫu số 0 | VALID | Features 38 cột |
| Storage | Giá trị typed được materialize | [output:79](../processing/output.py#L79) | VALID | `cleaned/<batch>` và `features/<batch>` trong benchmark smoke |

Ratio NULL ở derived columns không trái quy tắc NULL của 22 core: đó là schema
processed riêng. E2E 12 dòng và output schema được test tại
[test_pipeline:18](../tests/test_pipeline.py#L18).

## 7. Record B: Infinity ở rate — fixture dữ liệu số 2

Nguồn [sample_flow.csv dòng 3](../data/sample/sample_flow.csv#L3): như A nhưng
`flow_bytes_s='Infinity'`.

| Stage | Trước → sau | Hàm/quy tắc | Status | Nơi chuyển tiếp |
|---|---|---|---|---|
| Raw | Token `'Infinity'` | CSV bytes giữ nguyên | Chưa gán | Input |
| Ingestion | Token String `'Infinity'`, raw text giữ | [ingestion:24](../processing/ingestion.py#L24) | Chưa gán | Raw df |
| Transform | String → Double +∞; không phải cast failure | [transform:32](../processing/transform.py#L32) | Chưa gán | Typed + `_raw_values` |
| Validate | +∞ giữ nguyên; reason `INFINITY:flow_bytes_s` | [validation:166](../processing/validation.py#L166), rate là soft | VALID | Classified |
| Clean | +∞ → NULL; `flow_bytes_s_invalid=true` | [clean:45](../processing/clean.py#L45) | VALID→VALID | Valid; annotated vẫn raw token |
| Post-check | NULL được phép vì flag/raw phép clean khớp | [validation:104](../processing/validation.py#L104) | VALID | errors=[] |
| Features | Nguồn packet/byte counts đều 0 → totals 0, ratios NULL | [features:10](../processing/features.py#L10), không lấy rate làm nguồn total | VALID | Features |
| Storage | NULL + flag + reason + label gốc | [clean:53](../processing/clean.py#L53), [output:79](../processing/output.py#L79) | VALID | Cleaned/features; raw nguồn không đổi |

Điểm dễ bị hỏi: chuyển Infinity thành NULL làm NULL count tăng nhưng không làm mất
record. Giải pháp 0-fill sẽ biến lỗi đo thành tốc độ 0 giả. Test lỗi mềm bao gồm
Infinity tại [test_clean:6](../tests/test_clean.py#L6).

## 8. Record C: sentinel window -1 — fixture dữ liệu số 3

Nguồn [sample_flow.csv dòng 4](../data/sample/sample_flow.csv#L4), chỉ khác
`init_win_bytes_forward='-1'`.

| Stage | Trước → sau | Hàm/quy tắc | Status | Nơi chuyển tiếp |
|---|---|---|---|---|
| Raw | Token `'-1'` | Không sửa bytes | Chưa gán | Input |
| Ingestion | String `'-1'`, có evidence | [ingestion:34](../processing/ingestion.py#L34) | Chưa gán | Raw df |
| Transform | `'-1'` → Long -1, cast hợp lệ | [transform:33](../processing/transform.py#L33) | Chưa gán | Typed |
| Validate | reason `SENTINEL:init_win_bytes_forward`, không hard | [validation:167](../processing/validation.py#L167) | VALID | Classified |
| Clean | -1 → NULL; `init_win_fwd_missing=true` | [clean:50](../processing/clean.py#L50) | VALID→VALID | Valid + annotated |
| Post-check | Flag true phải khớp original đúng -1; value phải NULL | [validation:111](../processing/validation.py#L111) | VALID | errors=[] |
| Features | Không dùng window để tính 4 feature | [features:10](../processing/features.py#L10) | VALID | Features |
| Storage | Window NULL, missing flag true, reason còn | [clean:53](../processing/clean.py#L53) | VALID | Cleaned/features |

Nếu thay -1 bằng -2, rule hard kích hoạt, flag sentinel=false, value -2 giữ ở
quarantine. Không được nói “mọi window âm đều chuyển NULL”.
[test_clean:28](../tests/test_clean.py#L28).

## 9. Record D: duration âm — fixture dữ liệu số 5

Nguồn [sample_flow.csv dòng 6](../data/sample/sample_flow.csv#L6),
`flow_duration='-1'`.

| Stage | Trước → sau | Hàm/quy tắc | Status | Nơi chuyển tiếp |
|---|---|---|---|---|
| Raw | `'-1'` ở duration | Text nguồn | Chưa gán | Input |
| Ingestion | 79 field parse được | [parse_record:24](../processing/ingestion.py#L24) | Chưa gán | Raw df |
| Transform | `'-1'` → Long -1 | [transform:32](../processing/transform.py#L32) | Chưa gán | Typed |
| Validate | reason `NEGATIVE:flow_duration`, hard=true | [validation:171](../processing/validation.py#L171) | QUARANTINED | Classified |
| Clean | Duration -1 vẫn -1; không ép thành 0 | [clean:41](../processing/clean.py#L41) chỉ đổi rate/window | Q→Q | Quarantined giữ evidence |
| Post-check | Hard range check gated bởi VALID; label/cờ/status vẫn kiểm | [validation:87](../processing/validation.py#L87), [validation:119](../processing/validation.py#L119) | QUARANTINED | errors=[] có thể đúng |
| Features/Aggregate | Không vào file features; vẫn đóng góp Q count/error ratio | [main:92](../processing/main.py#L92), [features:37](../processing/features.py#L37) | QUARANTINED | Không cộng duration/volume lỗi vào VALID measures |
| Storage | -1 + raw text/struct + reason + before-status | [clean:61](../processing/clean.py#L61) | QUARANTINED | `quarantine/<batch>` |

Post-validation PASS ở đây chứng minh row lỗi đã được giữ đúng nhánh, không chứng
minh -1 đã trở thành duration hợp lệ. Lỗi còn ở quarantine được báo cáo riêng.

## 10. Record E: dòng 78 fields — fixture dữ liệu số 11

Nguồn [sample_flow.csv dòng 12](../data/sample/sample_flow.csv#L12). Không có token
label cuối; ví dụ này minh họa lỗi cấu trúc chứ không chỉ missing một numeric cell.

| Stage | Trước → sau | Hàm/quy tắc | Status | Nơi chuyển tiếp |
|---|---|---|---|---|
| Raw | 78 fields | Physical line không đủ width | Chưa gán | Input |
| Ingestion | Pad thành 79 parsed values; label=NULL; giữ toàn text | [parse_record:24](../processing/ingestion.py#L24), reason `COLUMN_COUNT:78:EXPECTED:79` | Chưa gán | Raw + `_corrupt_record` |
| Transform | Numeric hợp lệ được cast; label=NULL giữ | [transform:43](../processing/transform.py#L43) | Chưa gán | Typed + evidence |
| Validate | `MALFORMED_CSV`, `INVALID_LABEL`; malformed ưu tiên | [validation:148](../processing/validation.py#L148), [validation:184](../processing/validation.py#L184) | REJECTED | Classified |
| Clean | Không sửa thành row VALID | [clean:62](../processing/clean.py#L62) | R→R | Rejected |
| Post-check | NULL label raw/typed bằng nhau theo eqNullSafe; không VALID nên không áp rule valid-label | [validation:90](../processing/validation.py#L90) | REJECTED | Invariant có thể PASS |
| Features/Aggregate | Không file features; đếm REJECTED và label NULL trong thống kê | [main:92](../processing/main.py#L92), [features:48](../processing/features.py#L48) | REJECTED | Aggregate quality counts |
| Storage | Text, reason, source/batch, parsed evidence giữ | [output write loop:99](../processing/main.py#L99) | REJECTED | `rejected/<batch>` |

Một CSV có multiline quoted fields nằm ngoài định dạng ingest được hỗ trợ;
không tuyên bố parser phục hồi được logical multiline record. Source bytes vẫn là
bằng chứng đầy đủ; text RDD không giữ nguyên byte sequence/line-ending như file gốc.
[ingestion docstring:1](../processing/ingestion.py#L1).

## 11. Ví dụ feature không bằng 0, lấy từ unit test

Đây là test input, khác các row zero của fixture CSV:

| Đại lượng | Trước | Sau `add_features()` |
|---|---:|---:|
| forward/backward packets | 3 / 1 | `total_packets=4` |
| forward/backward payload bytes | 100 / 25 | `total_bytes=125` |
| tỷ lệ hướng | Chưa có | `fwd_packet_ratio=0.75`, `bwd_packet_ratio=0.25` |

Nguồn: [test_features:11](../tests/test_features.py#L11),
[add_features:16](../processing/features.py#L16). Tổng giữ Decimal(20,0), tỷ lệ Double.

## 12. Không mất bản ghi ngoài chính sách được chứng minh thế nào?

Ba tầng kiểm tra ở [reconcile_stages:43](../processing/validation.py#L43):

1. Count before/after đều bằng input ingestion. Mỗi status thuộc tập ba giá trị.
2. `input = valid + quarantined + rejected` ở từng giai đoạn, không cộng
   `quality_status_before` như một tập bản ghi mới.
3. Multiset `(source evidence, prior status)` bằng nhau hai chiều qua `exceptAll`;
   transitions hiện chỉ cho V→V, Q→Q, R→R.

Nếu có A,A,B trước và A,A,A sau, count vẫn 3 nhưng multiset khác. Test bắt đúng
trường hợp này ở [test_post_validation:62](../tests/test_post_validation.py#L62).
Không có `dropDuplicates()` vì không có khóa ổn định để phân biệt duplicate thực
với hai quan sát giống nhau.

## 13. Đọc Data Quality Report đúng cách

`quality_snapshot()` đo **typed cells** của 22 core trên toàn annotated, theo field
và file. “Before cleaning” là sau cast/classification, không phải byte-level raw
trước transformation. Label distribution nhóm `source_file,label,quality_status`.
`parsed_records = input - rejected`. Xem [quality_snapshot:13](../processing/quality_report.py#L13).

| Chỉ số | Diễn giải đúng |
|---|---|
| NULL count | Tổng ô NULL, gồm missing/cast fail và NULL hợp lệ sau clean |
| NaN / Infinity | Số ô float tương ứng; không phải ba tập record rời nhau |
| Invalid range | Số ô vi phạm ít nhất một range rule; có thể giao với metric khác |
| Cast errors | Lịch sử nguồn từ `cast_failure_fields`, không bị xóa sau clean |
| Sentinel count | Trước đếm -1; sau đếm missing flag, giữ lịch sử |
| Records with reasons | Bao gồm VALID có soft issues, không đồng nghĩa quarantine+reject |
| Post-clean errors | Vi phạm invariant được hậu kiểm phát hiện, khác số lỗi giá trị giữ ở nhánh lỗi |
| Duration / throughput | Thời gian thật và records/duration, phải đọc timing_scope |

Nguồn: [quality_report:28](../processing/quality_report.py#L28),
[quality_report:59](../processing/quality_report.py#L59),
[build_quality_report:77](../processing/quality_report.py#L77),
[main performance:110](../processing/main.py#L110).

Ví dụ **số đo thật trên dữ liệu synthetic 120k** từ
[quality-example.json](benchmarks/20261021/quality-example.json):

| Chỉ số | Before | After |
|---|---:|---:|
| Input / V / Q / R | 120.000 / 40.000 / 60.000 / 20.000 | Không đổi |
| NULL cells | 20.000 | 50.000 |
| NaN cells | 10.000 | 0 |
| Infinity cells | 10.000 | 0 |
| Invalid-range cells | 40.000 | 40.000 |
| Historical cast-error cells | 10.000 | 10.000 |
| Post errors | Không áp ở before | 0, PASS |

Kết quả **8 CSV thật**, chỉ xử lý local read-only, từ
[pipeline-eight-real.local.json](pipeline-eight-real.local.json):

| Chỉ số | Before | After |
|---|---:|---:|
| Input / V / Q / R | 2.830.743 / 2.830.628 / 115 / 0 | Không đổi |
| NULL cells | 0 | 2.448.675 |
| NaN cells | 1.358 | 0 |
| Infinity cells | 4.376 | 0 |
| Invalid-range cells | 430 | 230 |
| Cast errors | 0 | 0 |

230 ô còn lại là duration và flow_iat_mean âm trong 115 quarantine rows. Chất lượng
sử dụng tốt hơn vì valid output có schema/flags rõ ràng, không NaN/Inf trái policy,
nhãn/record bảo toàn và lỗi cứng tách riêng. Không suy “độ chính xác phát hiện tấn
công tăng” hoặc “tất cả nguồn đã hết lỗi”. Full real report có `outputs={}`, không
chứng minh đã ghi Parquet thật lên MinIO.

## 14. Contract được kiểm và còn lệch ở đâu?

| Nội dung | Kiểm ở đâu / giới hạn |
|---|---|
| 79 field, 22 core, version, raw String | [load_contract:23](../processing/config.py#L23) |
| Header/order | [validate_header:16](../processing/ingestion.py#L16), so exact, nghiêm hơn câu “header đã trim” trong Markdown mục 6.1 |
| Kiểu/missing/range | [transform:8](../processing/transform.py#L8), [validate_flows:138](../processing/validation.py#L138) |
| Cleaned đúng số cột/type | [schema_validation:11](../processing/validation.py#L11), main gọi `exact=True` cho valid |
| Rate/window/label/status | [validate_cleaned_flows:69](../processing/validation.py#L69) |
| Source conservation | [reconcile_stages:43](../processing/validation.py#L43) |
| Storage/read-back | [output:79](../processing/output.py#L79) |

JSON `spark_csv_options` mô tả PERMISSIVE, nhưng reader hiện tại dùng text RDD/strict
Python CSV; JSON malformed note nói “quarantine” trong khi code và Markdown phân
loại malformed thành REJECTED. Các mục metadata này không được reader tự áp dụng.
Contract đề xuất partition theo source ở mục 10; code chưa làm. Đây là các điểm
cần thống nhất tài liệu trong một đợt chỉnh sửa riêng, không tự sửa contract khi review.

Giới hạn thêm: hậu kiểm kiểm miền non-rate ở VALID, nhưng chưa đối chiếu tất cả
numeric non-rate trước/sau. Một sửa đổi nhầm từ số dương này sang số dương khác
có thể qua các rule hiện tại nếu raw evidence không đổi. Đây là phân tích tĩnh từ
[validation:98](../processing/validation.py#L98), không phải test mới đã chạy.

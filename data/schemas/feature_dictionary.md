# DATA CONTRACT

## Network Flow Feature Dictionary — Storage → Processing

**Mục đích:** Chuẩn hóa **schema, ngữ nghĩa, chất lượng và đầu ra xử lý** của dữ liệu network flow từ **8 tệp CSV CICIDS2017**. Contract là thỏa thuận đầu vào/đầu ra giữa **Storage (MinIO/Data Lake)** và **Processing (Apache Spark/PySpark)** trong **đồ án học phần Nhập môn Dữ liệu lớn**.

| Thuộc tính | Quy định |
|---|---|
| **Tên tài liệu** | Network Flow Feature Dictionary — Storage → Processing |
| **Phiên bản** | **v2.3.0** |
| **Phạm vi học phần** | **Storage → Processing** (không bao gồm Analytics, Machine Learning hay Decision/Action) |
| **Nguồn dữ liệu** | 8 CSV dạng `*.pcap_ISCX.csv` của CICIDS2017, đã được trích xuất flow từ PCAP |
| **Phạm vi lưu lượng** | Dữ liệu CICIDS2017 phục vụ bài toán lưu lượng IPv4; các CSV **không có trường `ip_version`** để xác minh từng flow |
| **Storage đầu vào** | MinIO (giao diện tương thích S3), đối tượng raw CSV không chỉnh sửa |
| **Processing** | Apache Spark / PySpark, **batch ETL** và Spark SQL |
| **Storage đầu ra** | **Apache Parquet**, nén **Snappy**, trên MinIO |
| **Quy ước tên cột** | Chữ thường, `snake_case`, ánh xạ tường minh theo header/vị trí gốc |
| **Nhãn gốc** | `label` (giữ nguyên dữ liệu từ cột `Label`; không sử dụng để sửa feature) |
| **Schema chuẩn lõi** | **22 trường:** 21 feature mạng + 1 nhãn `label` |
| **Trường kỹ thuật** | Metadata, trạng thái chất lượng và mã lỗi (ngoài 22 trường lõi) |
| **Đầu ra tối thiểu** | Cleaned Parquet, quarantine/reject log, bảng tổng hợp, Data Quality Report, thời gian xử lý và log Spark |
| **Tình trạng** | Đặc tả cần kiểm chứng bằng job và dữ liệu thực tế; **không phải xác nhận pipeline đã chạy** |

---

# 1. Mục đích và phạm vi

## 1.1. Quy trình áp dụng

```text
                  [PHẦN ĐẦU VÀO — STORAGE]
MinIO raw/cicids2017/ (8 tệp CSV, read-only)
                  |
                  | S3A / Spark read CSV
                  v
             [PROCESSING — SPARK]
1. Source audit / validate source headers (79 cột)
                  |
2. Standardization (positional mapping → snake_case → cast)
                  |
3. Data Quality (validate → clean / quarantine / reject)
                  |
4. Transformation (derived metrics có định nghĩa)
                  |
5. Aggregation (theo source_file và label)
                  |
6. Write Parquet/Snappy + technical reports
                  |
                  v
              [ĐẦU RA — MINIO]
cleaned/flows/       processed/aggregates/
quality/quarantine/  reports/data_quality/
reports/performance/
```

**Điểm kết thúc:** dữ liệu sau xử lý có schema xác định, có thể đọc lại bằng Spark; số record và sai lỗi được đối chiếu; thời gian chạy đo được.

Contract bảo đảm: thống nhất ngữ nghĩa các trường, không ghi đè raw, truy vết về tệp nguồn, kiểm soát lỗi trước/sau và có bằng chứng thực thi theo yêu cầu đồ án.

## 1.2. Nguồn dữ liệu đã cung cấp

| Tệp nguồn CICIDS2017 | Số flow theo kiểm toán |
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
---
f
# 2. Quy ước dữ liệu

## 2.1. Hướng flow

- **Forward (`fwd`)**: hướng xuất phát do bộ trích xuất flow xác định.
- **Backward (`bwd`)**: hướng ngược lại trong flow tương ứng.

Không đồng nhất cứng nhắc `fwd = client → server` hay `bwd = server → client`.

## 2.2. Đơn vị đo

| Nhóm dữ liệu | Đơn vị |
|---|---|
| `flow_duration`, `flow_iat_mean`, `flow_iat_std` | microsecond (µs) |
| Packet lengths / tổng byte payload | byte |
| `flow_bytes_s` | byte/second |
| `flow_packets_s` | packet/second |
| Packet count | count (số gói) |
| `destination_port` | số cổng, 0–65535 |
| `init_win_bytes_forward`, `init_win_bytes_backward` | byte khi hợp lệ; `-1` biểu diễn giá trị không sẵn có trong bộ nguồn |
| TCP flag columns trong CSV cung cấp | **0 hoặc 1** theo dữ liệu nguồn; **không suy ra số packet thực tế mang flag** |

## 2.3. Quy ước giá trị

- Giữ nguyên tệp CSV nguồn; mọi sửa đổi chỉ xảy ra ở các tầng đầu ra.
- Các trường đếm/kích thước/thời lượng không được âm, ngoại trừ sentinel được xác nhận ở `init_win_bytes_*`.
- `total_backward_packets = 0` có thể hợp lệ và **không được tự động loại**.
- Không tự động thay `NaN`, `Infinity`, `-Infinity` bằng 0.
- Phân biệt **lỗi chất lượng dữ liệu** với nhãn mạng `ATTACK`.
- `label` là nhãn nguồn; không sửa hay phân nhóm ngầm trong 22 trường lõi.

## 2.4. Header trùng

Trong cả 8 CSV, tên `Fwd Header Length` xuất hiện **hai lần ở vị trí cột 35 và 56 (đánh số từ 1)**. Parser có thể tự thêm hậu tố `.1`, nhưng đó **không phải tên trong CSV gốc**.

**Quy tắc đọc bắt buộc:** xác nhận đủ 79 cột và signature header trước khi xử lý; ánh xạ cột theo **vị trí gốc + tên dự kiến** (hoặc schema tường minh 79 cột tên duy nhất). Không sử dụng thao tác `toDF(snake_case(...))` trực tiếp trên header có tên lặp.

---

# 3. Data Schema

**Canonical cleaned schema lõi = 21 network-flow features + 1 nhãn `label`**; metadata và cờ chất lượng nằm ngoài số đếm này. Bộ 21 feature giữ tương thích với tài liệu mẫu của anh; không bắt buộc đưa tất cả 79 cột vào cleaned.

## 3.1. Flow & Traffic Volume

| Tên chuẩn hóa | Cột CICFlowMeter | Spark SQL type | Đơn vị | Ý nghĩa |
|---|---|---|---|---|
| `destination_port` | `Destination Port` | Integer | 0–65535 | Cổng đích |
| `flow_duration` | `Flow Duration` | Long | µs | Thời lượng của flow |
| `total_fwd_packets` | `Total Fwd Packets` | Long | gói | Số gói hướng forward |
| `total_backward_packets` | `Total Backward Packets` | Long | gói | Số gói hướng backward |
| `total_length_of_fwd_packets` | `Total Length of Fwd Packets` | Long | byte | Tổng độ dài payload packet forward theo định nghĩa nguồn |
| `total_length_of_bwd_packets` | `Total Length of Bwd Packets` | Long | byte | Tổng độ dài payload packet backward theo định nghĩa nguồn |
| `flow_bytes_s` | `Flow Bytes/s` | Double nullable | byte/s | Tốc độ byte của flow; có thể thiếu sau chuẩn hóa |
| `flow_packets_s` | `Flow Packets/s` | Double nullable | gói/s | Tốc độ packet của flow; có thể thiếu sau chuẩn hóa |

## 3.2. Inter-Arrival Time (IAT)

| Tên chuẩn hóa | Cột CICFlowMeter | Spark SQL type | Đơn vị | Ý nghĩa |
|---|---|---|---|---|
| `flow_iat_mean` | `Flow IAT Mean` | Double | µs | IAT trung bình giữa các packet trong flow |
| `flow_iat_std` | `Flow IAT Std` | Double | µs | Độ lệch chuẩn IAT |

## 3.3. TCP Flags (giữ để kiểm toán, không diễn giải như packet count)

| Tên chuẩn hóa | Cột CICFlowMeter | Spark SQL type | Ngữ nghĩa áp dụng |
|---|---|---|---|
| `syn_flag_count` | `SYN Flag Count` | Integer | Chỉ báo 0/1 ghi trong dữ liệu nguồn |
| `ack_flag_count` | `ACK Flag Count` | Integer | Chỉ báo 0/1 ghi trong dữ liệu nguồn |
| `rst_flag_count` | `RST Flag Count` | Integer | Chỉ báo 0/1 ghi trong dữ liệu nguồn |
| `fin_flag_count` | `FIN Flag Count` | Integer | Chỉ báo 0/1 ghi trong dữ liệu nguồn |
| `psh_flag_count` | `PSH Flag Count` | Integer | Chỉ báo 0/1 ghi trong dữ liệu nguồn |

**Giới hạn:** Bộ CSV đã kiểm toán có các cột này dạng 0/1. Không lấy tổng giá trị rồi công bố là **số SYN/ACK packet thực tế**. Không sử dụng cột này để kết luận kỹ thuật về đếm flag theo gói.

## 3.4. Packet Size Statistics

| Tên chuẩn hóa | Cột CICFlowMeter | Spark SQL type | Đơn vị | Ý nghĩa |
|---|---|---|---|---|
| `packet_length_mean` | `Packet Length Mean` | Double | byte | Trung bình độ dài packet |
| `packet_length_std` | `Packet Length Std` | Double | byte | Độ lệch chuẩn độ dài packet |
| `min_packet_length` | `Min Packet Length` | Double | byte | Độ dài packet nhỏ nhất |
| `max_packet_length` | `Max Packet Length` | Double | byte | Độ dài packet lớn nhất |

## 3.5. TCP Initial Window

| Tên chuẩn hóa | Cột CICFlowMeter | Spark SQL type | Đơn vị | Ý nghĩa |
|---|---|---|---|---|
| `init_win_bytes_forward` | `Init_Win_bytes_forward` | Long nullable | byte | Initial window hướng forward; `-1` là sentinel nguồn |
| `init_win_bytes_backward` | `Init_Win_bytes_backward` | Long nullable | byte | Initial window hướng backward; `-1` là sentinel nguồn |

Ở cleaned, `-1` được đổi thành `NULL` **kèm cờ chất lượng**, không đổi thành 0. Giá trị âm khác `-1` cần cách ly/xác minh.

## 3.6. Ground Truth

| Tên chuẩn hóa | Cột CSV | Spark SQL type | Ý nghĩa |
|---|---|---|---|
| `label` | `Label` | String | Giữ nhãn nguồn từ CSV (không phân loại nhị phân trong contract này) |

Giữ `label` nguyên bản sau giải mã; nếu phục vụ tổng hợp cần chuẩn hóa khoảng trắng, tạo trường dẫn xuất **`label_group`** hoặc bảng mapping riêng thay vì ghi đè `label`.

## 3.7. Technical Metadata (ngoài schema 22 trường)

| Trường | Kiểu | Quy tắc |
|---|---|---|
| `source_file` | String | Tên file CSV đầu vào từ MinIO |
| `source_dataset` | String | `CICIDS2017` |
| `batch_id` | String | Định danh lần chạy ingest/ETL |
| `pipeline_version` | String | Phiên bản mã xử lý hoặc Git commit |
| `schema_version` | String | `v2.3.0` |
| `processed_at` | Timestamp | Thời gian job xử lý; **không phải event_time** |
| `quality_status` | String | `VALID` / `QUARANTINED` / `REJECTED` |
| `quality_reason_codes` | Array<String> | Danh mục mã lỗi áp dụng |
| `flow_bytes_s_invalid` | Boolean | Dữ liệu rate thiếu/không hữu hạn/âm |
| `flow_packets_s_invalid` | Boolean | Dữ liệu rate thiếu/không hữu hạn/âm |
| `init_win_fwd_missing`, `init_win_bwd_missing` | Boolean | Sentinel `-1` ở initial-window |

**Quy ước batch-level lineage:** bắt buộc lưu SHA-256 của tệp và đếm dòng theo tệp. Không yêu cầu `source_row_number`/`record_id` toàn cục khi chưa có cách đánh số ổn định trên input đã phân tán; **không dùng `monotonically_increasing_id()` làm số thứ tự bản ghi gốc**.

---

# 4. Feature Classification

| Nhóm | Các trường | Phép xử lý phù hợp trong giai đoạn này |
|---|---|---|
| **Flow / Volume** | `flow_duration`, `total_fwd_packets`, `total_backward_packets`, `total_length_of_fwd_packets`, `total_length_of_bwd_packets` | Kiểm tra miền giá trị; tổng hợp min/avg/max/sum phù hợp |
| **Traffic Rate** | `flow_bytes_s`, `flow_packets_s` | Kiểm tra hữu hạn; thống kê NULL; avg/percentile trên tập hợp lệ |
| **Timing** | `flow_iat_mean`, `flow_iat_std` | Chuẩn hóa kiểu, thống kê phân bố |
| **TCP Behavior (source flags)** | `syn_flag_count`, `ack_flag_count`, `rst_flag_count`, `fin_flag_count`, `psh_flag_count` | Kiểm tra 0/1; không tổng hợp dưới nghĩa số packet thực tế |
| **Packet Size** | `packet_length_mean`, `packet_length_std`, `min_packet_length`, `max_packet_length` | Kiểm tra miền và thống kê |
| **TCP Initial Window** | `init_win_bytes_forward`, `init_win_bytes_backward` | Xử lý sentinel `-1`; đếm tỷ lệ thiếu |
| **Service Context** | `destination_port` | Thống kê tần suất cổng đích; không suy ra host nguồn |
| **Ground Truth** | `label` | Thống kê phân bố nhãn, theo file nguồn |

---

# 5. Processing Interpretation

**Mục tiêu xử lý không phải dự đoán.** Bốn nhóm thao tác của Storage → Processing:

```text
Raw CSV
  ↓ Data Standardization
Canonical schema (22 trường + metadata)
  ↓ Data Cleaning / Validation
Valid clean flows + Quarantine + Reject
  ↓ Feature Transformation
Well-defined derived statistics (không tự bịa trường thời gian/IP)
  ↓ Aggregation (Spark SQL / DataFrame)
Source-file statistics + Label statistics
  ↓ Materialization
Cleaned Parquet + Aggregated Parquet + QA report
```

## 5.1. Transformation được phép

Nếu cần minh họa feature transformation, có thể tạo trường **dẫn xuất** (không thay thế cột gốc):

| Trường dẫn xuất | Định nghĩa | Điều kiện |
|---|---|---|
| `total_packets` | `total_fwd_packets + total_backward_packets` | Hai trường đầu vào hợp lệ |
| `total_payload_bytes` | `total_length_of_fwd_packets + total_length_of_bwd_packets` | Hai trường hợp lệ; không gọi là toàn bộ wire bytes |
| `fwd_packet_ratio` | `total_fwd_packets / total_packets` | Chỉ tính khi `total_packets > 0` |
| `has_backward_traffic` | `total_backward_packets > 0` | Chỉ tính khi trường nguồn hợp lệ |

Các trường dẫn xuất thuộc **processed layer**, không tính vào 22 trường canonical cleaned.

## 5.2. Aggregation phù hợp với CSV hiện có

- **Theo `source_file`:** `flow_count`, `valid_count`, `quarantine_count`, tỷ lệ lỗi, thời lượng trung bình trên dòng hợp lệ.
- **Theo `label`:** đếm số flow theo nhãn gốc và nguồn dữ liệu; không viết lại nhãn.
- **Theo `source_file + label`:** `flow_count`, tổng packet, tổng payload bytes, thống kê tốc độ hữu hạn.
- **Theo `destination_port`:** số flow theo cổng đích (nếu cần một phép tổng hợp bổ sung).

Không tính `unique destination ports per source IP`, `flows per event-time window`, hay `SYN packet rate`: dữ liệu nguồn hiện tại không hỗ trợ các kết quả này một cách đáng tin cậy.

---

# 6. Data Quality Rules

## 6.1. Column Name Validation

- Phải nhận dạng chính xác **79 cột nguồn** theo vị trí, so sánh header đã trim với danh sách kỳ vọng.
- Ánh xạ tường minh từng trường canonical (không tự đổi tên tất cả rồi chấp nhận cột trùng).
- Dữ liệu cleaned phải có **đúng 22 cột canonical** và các metadata/cờ chất lượng đã công bố (tách số lượng này khỏi metadata).
- Không có duplicate canonical name.

**Ví dụ:** `" Flow Duration"` → `flow_duration`; `" Flow Bytes/s"` → `flow_bytes_s`.

## 6.2. Type Validation

| Nhóm dữ liệu | Kiểu Spark |
|---|---|
| `destination_port`, TCP source flags | Integer |
| Thời lượng, packet count, total length, initial window | Long |
| Rate, IAT, packet size | Double |
| `label`, `source_file`, `batch_id` | String |
| `processed_at` | Timestamp |
| Các cờ chất lượng | Boolean / Array<String> |

Dùng schema tường minh; lỗi cast được ghi nhận. Không dùng `inferSchema` làm hợp đồng đầu ra.

## 6.3. Range Validation

| Kiểm tra | Chính sách |
|---|---|
| `destination_port` ngoài [0,65535] | QUARANTINED |
| `flow_duration < 0` | QUARANTINED, không ép thành 0 |
| Packet count / length âm | QUARANTINED |
| IAT / packet size có giá trị âm hoặc không hữu hạn | QUARANTINED hoặc policy riêng có ghi lý do |
| `flow_bytes_s` / `flow_packets_s` null, NaN, ±Infinity hoặc âm | Đánh dấu invalid; chuyển NULL ở cleaned; giữ lại row nếu các trường bắt buộc khác hợp lệ |
| `init_win_bytes_* = -1` | Chuyển NULL + flag; không coi là packet/window bằng 0 |
| Initial-window âm khác `-1` | QUARANTINED |
| TCP flags ngoài 0/1 | Flag bất thường; xử lý theo policy QA, không cộng thành đếm packet |
| `label` rỗng / không parse được | QUARANTINED; không tự gán `BENIGN` hay `ATTACK` |

## 6.4. Phân loại trạng thái bản ghi

```text
Input row
  ├─ REJECTED: malformed CSV / không thể khôi phục cấu trúc hàng
  ├─ QUARANTINED: parse được nhưng vi phạm điều kiện cứng
  └─ VALID: đạt chuẩn hoặc lỗi mềm đã xử lý và ghi nhận
```

Ba trạng thái **loại trừ nhau**. Mỗi hàng chỉ được tính vào đúng một trạng thái cuối. Record lỗi mềm vẫn là `VALID` nếu chính sách cho phép làm sạch; phải ghi lại flags và mã lỗi. `REJECTED` phải có bằng chứng/đếm được, không được bỏ qua âm thầm bằng chế độ parser tự loại dữ liệu.

---

# 7. Null, NaN và Infinite Values

Các loại lỗi cần phân biệt: `NULL`, chuỗi rỗng, `NaN`, `+Infinity`, `-Infinity`, lỗi cast, giá trị âm không hợp lệ, sentinel `-1`.

| Trường hợp | Raw | Cleaned | Data Quality Report |
|---|---|---|---|
| Rate null/NaN | Giữ text gốc | NULL + flag | Count theo trường/file |
| Rate ±Infinity | Giữ text gốc | NULL + flag | Count theo trường/file |
| Rate âm | Giữ text gốc | NULL + flag | Count theo trường/file |
| Duration âm | Giữ text gốc | Quarantine | Count + ví dụ có kiểm soát |
| TCP window `-1` | Giữ `-1` | NULL + flag | Count theo hướng |
| Invalid label | Giữ raw | Quarantine | Count và giá trị gặp phải |

**Số liệu audit của 8 tệp** (đếm giá trị theo từng cột, **không phải số record lỗi duy nhất**):

| Hiện tượng | Số lần quan sát |
|---|---:|
| `Flow Bytes/s` null/NaN | 1.358 |
| `Flow Bytes/s` vô cực | 1.509 |
| `Flow Bytes/s` âm | 85 |
| `Flow Packets/s` vô cực | 2.867 |
| `Flow Packets/s` âm | 115 |
| `Flow Duration` âm | 115 |
| `Init_Win_bytes_forward` âm | 1.001.189 |
| `Init_Win_bytes_backward` âm | 1.441.552 |

Hai số âm ở initial window là **số giá trị âm được kiểm toán**; cần xác minh khi chạy pipeline rằng từng trường hợp âm là `-1` trước khi áp dụng sentinel policy. Các nhóm lỗi có thể cùng xuất hiện trên một record.

Không tự động loại toàn bộ flow mang rate null/Infinity; nếu việc đổi sang NULL được chấp thuận, row vẫn có thể xuất ra cleaned và những phép aggregation phải dùng chính sách NULL tường minh.

---

# 8. Label Governance

`label` là nhãn gốc và **không bị ghi đè** trong cleaned.

```text
CSV Label
   ↓ (preserve as-is)
label
   ├──→ Data Quality / phân bố giá trị gốc
   └──→ Optional, versioned mapping
           label_group (chỉ nếu cần thống kê gộp)
```

Nguyên tắc:

- Không tự động thay nhãn lạ bằng `BENIGN` hoặc nhãn tấn công.
- Không mặc định gộp các nhãn thành bài toán binary classification trong giai đoạn Storage → Processing.
- Nếu cần chuẩn hóa hiển thị, dùng mapping tường minh, có version, lưu cả chuỗi gốc.
- Một số nhãn Web Attack trong CSV quan sát có ký tự thay thế **`�`**; **giữ chuỗi gốc** và công bố vấn đề encoding thay vì ngầm sửa sai.
- Báo cáo phân bố các nhãn **trước và sau cleaning**, đặc biệt số row bị quarantine theo nhãn.

---

# 9. Storage Contract

## 9.1. File Format

```text
Source input : CSV (raw, immutable)
Output flows: Apache Parquet + Snappy
Output stats: Apache Parquet + Snappy (hoặc JSON/CSV cho báo cáo nhỏ)
Reports     : JSON / CSV / log (có batch_id)
```

Lý do: Parquet lưu theo cột; Spark đọc/ghi hiệu quả các trường thống kê; Snappy hỗ trợ nén/giải nén phù hợp với workload batch.

## 9.2. Data Layers (bucket/prefix đề xuất)

```text
MinIO: bigdata-network/
│
├── raw/
│   └── cicids2017/
│       └── [8 source CSV + manifest/checksum]
│
├── cleaned/
│   └── flows/                  [Parquet + Snappy, 22 fields + metadata]
│
├── processed/
│   ├── derived_features/      [Parquet + Snappy, optional]
│   └── aggregates/            [Parquet + Snappy]
│
├── quality/
│   ├── quarantine/            [record lỗi có thể đọc và giải thích]
│   └── rejected/              [record lỗi cấu trúc, nếu có]
│
└── reports/
    ├── data_quality/          [báo cáo theo batch]
    └── performance/           [thời gian, throughput, cấu hình Spark]
```

MinIO **không tự xử lý dữ liệu**: Spark đọc từ prefix `raw/` và ghi sang prefix mới. Không ghi đè raw; `overwrite` chỉ được phép ở prefix output của batch đang chạy theo chính sách rõ ràng.

## 9.3. Output Schema Contract

| Output | Bắt buộc | Nội dung tối thiểu |
|---|---|---|
| `cleaned/flows/` | Có | 22 trường chuẩn + `source_file`, `batch_id`, `schema_version`, quality flags/status |
| `processed/aggregates/` | Có | `source_file`, `label`, `flow_count`, `total_packets_sum` (nếu đủ nguồn hợp lệ), `avg_flow_duration`, `batch_id` |
| `quality/quarantine/` | Có chính sách | Record cách ly + cột gốc cần truy vết + `quality_reason_codes` |
| `quality/rejected/` | Có chính sách | Dòng hỏng/đếm lỗi cấu trúc + nguồn; không tự động mất dòng |
| `reports/data_quality/` | Có | Số bản ghi, lỗi, chất lượng theo file và tổng |
| `reports/performance/` | Có | Processing time, throughput, cấu hình test |

`total_packets_sum` phải được tính từ các packet count hợp lệ; báo cáo cần giải thích rõ liệu thống kê chỉ tính hàng `VALID`.

---

# 10. Partitioning Strategy

Partitioning là cấu hình của Storage → Processing, **không phải feature của mạng**.

- Khuyến nghị **partition cleaned theo `source_file` hoặc `source_subset` có mã ngắn** khi truy vấn thường lọc theo file; đánh giá file count/size thực tế trước khi chốt.
- `processed/aggregates/` thường nhỏ: tránh tạo nhiều Parquet quá nhỏ; có thể gom số file đầu ra.
- **Không mặc định partition theo `label`** do các lớp chênh lệch lớn và dễ tạo file nhỏ/skew.
- **Không partition theo `event_date`** vì CSV không có timestamp sự kiện; `processed_at` là thời gian job chạy.
- Số Spark shuffle partitions và output partitions là **tham số thí nghiệm**, không coi một số cố định là tối ưu.

Ví dụ đường dẫn (chỉ là quy ước đề xuất):

```text
cleaned/flows/source_subset=tuesday/part-*.snappy.parquet
processed/aggregates/batch_id=<job-id>/part-*.snappy.parquet
```

---

# 11. Data Lineage & Traceability

Mỗi batch phải có metadata:

| Trường | Ý nghĩa |
|---|---|
| `batch_id` | Định danh job chạy |
| `source_dataset` | `CICIDS2017` |
| `source_file` | File nguồn đầu vào |
| `source_sha256` | Hash kiểm tra tính toàn vẹn từng file |
| `schema_version` | Phiên bản hợp đồng |
| `pipeline_version` | Phiên bản job/chương trình |
| `processing_started_at`, `processing_finished_at` | Mốc thời gian đo xử lý |
| `input_record_count` | Số record nguồn |
| `valid_record_count` | Số record đi vào cleaned |
| `quarantined_record_count` | Số record cách ly |
| `rejected_record_count` | Số record lỗi cấu trúc |
| `quality_report_path` | Đường dẫn báo cáo |
| `output_path`, `output_record_count` | Đường dẫn và số record đầu ra |

```text
Processed aggregate / Cleaned record
          ↓ batch_id + source_file + schema_version
Spark ETL job / execution log
          ↓ source_file + sha256
Original CSV in MinIO raw/
```

**Mức bắt buộc:** truy vết theo batch/file. Nếu muốn truy vết chính xác từng dòng, cần cơ chế source row number ổn định được triển khai và kiểm chứng riêng; không tuyên bố có sẵn từ Spark CSV reader.

---

# 12. Data Quality Report

Mỗi lần xử lý phải phát sinh báo cáo theo **từng tệp và toàn batch**, có ít nhất:

```text
batch_id / schema_version / pipeline_version
source_file / input_bytes / input_records
parsed_records / valid_records / quarantined_records / rejected_records
null_count_by_field / nan_count_by_field / infinity_count_by_field
negative_invalid_count_by_field / sentinel_count_by_field
cast_failure_count / invalid_label_count / duplicate_header_check
label_distribution_before / label_distribution_after
cleaned_output_records / aggregate_output_records
processing_duration_seconds / throughput_flows_per_second
spark_app_id / worker_count / cores / memory / shuffle_partitions
status (PASS/FAIL) / report_timestamp
```

**Đối chiếu bắt buộc:**

```text
input_record_count = valid_record_count
                   + quarantined_record_count
                   + rejected_record_count
```

Điều kiện này đúng khi một input row được gán chính xác một trạng thái; cần báo cáo riêng parser corrupt records mà Spark có thể tự xử lý/skip, tuyệt đối tránh im lặng bỏ dòng.

**Lưu ý về benchmark:** `throughput_flows_per_second = input_records / processing_duration_seconds` và mô tả rõ thời gian có bao gồm đọc, làm sạch, aggregation, ghi output, khởi tạo Spark hay không. Dữ liệu demo/benchmark phải đo **từ lần chạy thực tế**, không điền số mẫu như số liệu đo.

---

# 13. Acceptance Criteria

Giai đoạn **Storage → Processing** chỉ được xem là **hoàn thành để nghiệm thu** khi thỏa đồng thời các nhóm sau.

## 13.1. Schema

- [ ] Kiểm tra được 8 header CSV có 79 trường; xử lý tên `Fwd Header Length` lặp có kiểm soát.
- [ ] Cleaned có 21 feature chuẩn + 1 `label` và metadata/cờ chất lượng đã khai báo.
- [ ] Tên canonical dùng `snake_case`, kiểu Spark đúng contract, không trùng cột.

## 13.2. Data Quality

- [ ] Có chính sách và báo cáo cho Null, NaN, Infinity, duration âm, sentinel `-1`, lỗi cast và nhãn lỗi encoding.
- [ ] Không tự động sửa raw hoặc đổi nhãn gốc.
- [ ] `input = valid + quarantined + rejected` được chứng minh bằng số liệu chạy thực.
- [ ] Có báo cáo phân bố nhãn và lỗi theo **từng file** trước/sau xử lý.

## 13.3. Storage / Processing

- [ ] Spark đọc trực tiếp từ MinIO/S3A, xử lý rồi ghi được **Parquet + Snappy** vào prefix đầu ra.
- [ ] Có job chuẩn hóa, làm sạch và tổng hợp bằng Spark DataFrame/Spark SQL.
- [ ] Đọc lại Parquet, kiểm tra schema và số bản ghi đúng với report.
- [ ] Có các output bắt buộc trong mục 9.3 và đường dẫn trong manifest.

## 13.4. Thực nghiệm & khả năng mở rộng

- [ ] Có log Spark và **thời gian xử lý** đo thực tế.
- [ ] Benchmark ít nhất theo **kích thước đầu vào**, **partition** và **worker** (nếu môi trường cho phép worker phân tán); công bố cấu hình tương ứng.
- [ ] Có thống kê throughput, so sánh và giải thích các trường hợp tăng/giảm hiệu năng.
- [ ] Nếu dùng dữ liệu nhân bản để tạo tải, phải đánh dấu **synthetic benchmark load**, không xem là dữ liệu mới dùng phân tích an ninh.

## 13.5. Demo & tái lập

- [ ] Có README lệnh khởi chạy Docker, kết nối MinIO, chạy Spark job và xem output.
- [ ] Có Data Contract, sơ đồ data-flow/architecture, log, kết quả trước/sau, báo cáo/biểu đồ.
- [ ] Nộp source code, cấu hình và dữ liệu mẫu/đường dẫn nguồn theo yêu cầu giảng viên.

---

# 14. Contract Boundary

```text
IN SCOPE:
  MinIO raw CSV
     ↓
  Schema & Type Standardization
     ↓
  Data Quality / Cleaning / Quarantine
     ↓
  Feature Transformation / Aggregation
     ↓
  Parquet Output + Reports + Benchmarks

OUT OF SCOPE (KHÔNG PHẢI NGHIỆM THU CONTRACT NÀY):
  PCAP extraction / packet capture
  Model training / classification / attack prediction
  ML accuracy / Precision / Recall / F1
  Streaming / Kafka / real-time alerting
  SOC dashboard / Incident Response
  NCKH-specific experimentation
```

**Ranh giới bắt đầu:** dữ liệu flow CSV được lưu và đọc từ MinIO. **Ranh giới kết thúc:** Spark hoàn tất chuẩn hóa–làm sạch–tổng hợp và ghi lại dữ liệu cùng báo cáo kỹ thuật.

---

# 15. Downstream Usage

Đầu ra có thể được các thành phần khác sử dụng, nhưng contract này **không định nghĩa cách thực hiện downstream**.

```text
Cleaned Network Flows (Parquet)
            |
            ├──→ Spark SQL / descriptive statistics
            |
            ├──→ Aggregated datasets / quality dashboard
            |
            └──→ Other consumers (independent contracts)
```

Thực tế trong đồ án này, **đầu ra đã xử lý** được chứng minh bằng truy vấn đọc lại Parquet và báo cáo chất lượng/hiệu năng, không bắt buộc có mô hình dự đoán.

---

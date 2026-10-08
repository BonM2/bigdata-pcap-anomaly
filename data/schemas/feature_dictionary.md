# DATA CONTRACT

## Network Flow Feature Dictionary

**Mục đích:** Chuẩn hóa schema và ngữ nghĩa của các đặc trưng luồng mạng được trích xuất từ PCAP, làm đầu vào thống nhất cho tầng **Storage → Processing → Security Analysis** trên Apache Spark.

| Thuộc tính                  | Quy định                                                        |
| --------------------------- | --------------------------------------------------------------- |
| **Tên tài liệu**            | Network Flow Feature Dictionary                                 |
| **Phiên bản**               | v1.0                                                            |
| **Nguồn dữ liệu**           | PCAP thuộc CICIDS2017, được trích xuất bằng CICFlowMeter        |
| **Dạng dữ liệu trung gian** | Network Flow                                                    |
| **Định dạng lưu trữ**       | Apache Parquet                                                  |
| **Compression**             | Snappy                                                          |
| **Quy ước tên cột**         | `snake_case`, chữ thường, không khoảng trắng                    |
| **Đối tượng sử dụng**       | Apache Spark, Data Engineering, SOC Analysis, Anomaly Detection |
| **Ground Truth**            | `label`                                                         |
| **Số trường chuẩn hóa**     | 22 trường: 21 feature + 1 ground-truth label                    |

---

# 1. Mục đích và phạm vi

Data Contract này định nghĩa tập đặc trưng mạng được sử dụng thống nhất giữa các tầng của pipeline xử lý dữ liệu:

```text
1. Dataset / PCAP
   ↓
2. Flow Extraction
   CICFlowMeter: Packet → Flow
   ↓
3. Data Standardization
   Chuẩn hóa theo Data Contract
   ↓
4. Storage
   Apache Parquet
   ↓
5. Processing & Feature Engineering
   Apache Spark
   ↓
6. Security Analysis
   Feature → Aggregation → Indicator → Detection Rule
   ↓
7. Detection & Alerting
   Phát hiện hoạt động bất thường → Sinh cảnh báo
   ↓
8. Evaluation
   Đánh giá  Time / Throughput / Speedup / Scaling

```

Tài liệu nhằm đảm bảo:

* Các thành viên sử dụng cùng một tên trường và kiểu dữ liệu.
* Ngữ nghĩa của từng feature được hiểu thống nhất.
* Dữ liệu có thể kiểm tra và truy vết giữa các bước xử lý.
* Dataset sau khi chuẩn hóa có thể được sử dụng nhất quán trên Spark.
* `label` được bảo toàn như ground truth và không bị sử dụng nhầm như một feature đầu vào.

---

# 2. Quy ước dữ liệu

## 2.1. Quy ước hướng của Flow

Trong CICFlowMeter:

* **Forward (`fwd`)**: chiều được xác định là hướng chính của flow, từ source đến destination.
* **Backward (`bwd`)**: chiều ngược lại của flow.

> Không mặc định `forward = client → server` và `backward = server → client`. Việc xác định client/server phụ thuộc ngữ cảnh của phiên giao tiếp.

---

## 2.2. Đơn vị đo

| Loại dữ liệu        | Đơn vị             |
| ------------------- | ------------------ |
| `flow_duration`     | microsecond (`µs`) |
| `flow_iat_mean`     | microsecond (`µs`) |
| `flow_iat_std`      | microsecond (`µs`) |
| Packet length       | byte               |
| Total packet length | byte               |
| `flow_bytes_s`      | byte/second        |
| `flow_packets_s`    | packet/second      |
| Packet / flag count | count              |

---

## 2.3. Quy ước giá trị

* Các trường đếm (`count`) không được âm.
* Các trường thời lượng không được âm.
* Các trường kích thước không được âm.
* Các trường tốc độ không được âm.
* `label` phải giữ nguyên giá trị gốc từ dataset.
* `label` là **ground truth**, không phải feature đầu vào.
* Giá trị `NaN`, `Infinity` và `-Infinity` phải được xử lý trước khi ghi vào tầng dữ liệu đã chuẩn hóa.
* Không tự ý thay đổi hoặc gộp nhãn nếu chưa có bảng mapping được phê duyệt.

---

# 3. Data Schema

## 3.1. Flow & Traffic Volume

| Tên đã được chuẩn hóa            | Cột CICFlowMeter           | Kiểu dữ liệu          | Đơn vị     | Mô tả                                                                                              |
| ----------------------------- | ----------------------------- | ------------- | -------- | -------------------------------------------------------------------------------------------------------- |
| `destination_port`            | `Destination Port`            | `Integer` | 0–65535  | Cổng đích của flow. Hữu ích cho phân tích dịch vụ và nhận diện hành vi quét cổng.                        |
| `flow_duration`               | `Flow Duration`               | `Long`    | µs       | Tổng thời lượng của flow.                                                                                |
| `total_fwd_packets`           | `Total Fwd Packets`           | `Long`    | gói    | Tổng số packet theo hướng forward.                                                                       |
| `total_backward_packets`      | `Total Backward Packets`      | `Long`    | gói    | Tổng số packet theo hướng backward. Giá trị bằng 0 có thể xuất hiện ở các flow không nhận được phản hồi. |
| `total_length_of_fwd_packets` | `Total Length of Fwd Packets` | `Long`    | byte     | Tổng kích thước packet theo hướng forward.                                                               |
| `total_length_of_bwd_packets` | `Total Length of Bwd Packets` | `Long`    | byte     | Tổng kích thước packet theo hướng backward.                                                              |
| `flow_bytes_s`                | `Flow Bytes/s`                | `Double`  | byte/s   | Tốc độ truyền dữ liệu của flow.                                                                          |
| `flow_packets_s`              | `Flow Packets/s`              | `Double`  | packet/s | Tốc độ packet của flow; đặc biệt hữu ích cho phân tích flood/DoS.                                        |

---

## 3.2. Inter-Arrival Time (IAT)

| Tên đã được chuẩn hóa | Cột CICFlowMeter | Kiểu dữ liệu         | Đơn vị | Mô tả                                             |
| ----------------- | ------------------- | ------------ | ---- | ------------------------------------------------------- |
| `flow_iat_mean`   | `Flow IAT Mean`     | `Double` | µs   | Khoảng thời gian trung bình giữa các packet trong flow. |
| `flow_iat_std`    | `Flow IAT Std`      | `Double` | µs   | Độ lệch chuẩn của khoảng thời gian giữa các packet.     |

**Security relevance:**

IAT không nên được dùng như một dấu hiệu độc lập. Giá trị IAT cần được kết hợp với packet rate, packet count, flow duration và các đặc trưng khác để nhận diện hành vi bất thường.

---

## 3.3. TCP Flags

| Tên đã được chuẩn hóa | Cột CICFlowMeter | Kiểu dữ liệu          | Đơn vị  | Mô tả                     |
| ----------------- | ------------------- | ------------- | ----- | ------------------------------- |
| `syn_flag_count`  | `SYN Flag Count`    | `Integer` | gói | Số packet trong flow có cờ SYN. |
| `ack_flag_count`  | `ACK Flag Count`    | `Integer` | gói | Số packet trong flow có cờ ACK. |
| `rst_flag_count`  | `RST Flag Count`    | `Integer` | gói | Số packet trong flow có cờ RST. |
| `fin_flag_count`  | `FIN Flag Count`    | `Integer` | gói | Số packet trong flow có cờ FIN. |
| `psh_flag_count`  | `PSH Flag Count`    | `Integer` | gói | Số packet trong flow có cờ PSH. |

**Security relevance:**

Các TCP flag chỉ có ý nghĩa khi được phân tích theo **tổ hợp và tỷ lệ**, không nên kết luận tấn công chỉ dựa trên một flag.

Ví dụ:

```text
High SYN
+ Low/Zero ACK
+ Short Flow Duration
+ High Flow Rate
            ↓
Potential SYN-based anomaly
```

---

# 3.4. Packet Size Statistics

| Tên đã được chuẩn hóa   | Cột CICFlowMeter | Kiểu dữ liệu         | Đơn vị | Mô tả                              |
| -------------------- | -------------------- | ------------ | ---- | ---------------------------------------- |
| `packet_length_mean` | `Packet Length Mean` | `Double` | byte | Kích thước packet trung bình trong flow. |
| `packet_length_std`  | `Packet Length Std`  | `Double` | byte | Độ lệch chuẩn kích thước packet.         |
| `min_packet_length`  | `Min Packet Length`  | `Double` | byte | Kích thước packet nhỏ nhất.              |
| `max_packet_length`  | `Max Packet Length`  | `Double` | byte | Kích thước packet lớn nhất.              |

**Security relevance:**

Các thống kê về packet size có thể hỗ trợ phân biệt các kiểu traffic có đặc trưng kích thước bất thường hoặc tương đối đồng nhất.

Không nên xem một giá trị `packet_length_mean` hoặc `packet_length_std` đơn lẻ là bằng chứng trực tiếp của một cuộc tấn công.

---

# 3.5. TCP Initial Window

| Tên đã được chuẩn hóa     | Cột CICFlowMeter       | Kiểu dữ liệu       | Đơn vị | Mô tả                               |
| ------------------------- | ------------------------- | ---------- | ---- | ----------------------------------------- |
| `init_win_bytes_forward`  | `Init_Win_bytes_forward`  | `Long` | byte | TCP initial window size ở hướng forward.  |
| `init_win_bytes_backward` | `Init_Win_bytes_backward` | `Long` | byte | TCP initial window size ở hướng backward. |

Các trường này có thể hỗ trợ **TCP fingerprinting** hoặc phân tích đặc điểm của hệ điều hành/công cụ mạng, nhưng không nên được sử dụng như một dấu hiệu độc lập để kết luận nguồn traffic là Nmap hoặc một hệ điều hành cụ thể.

---

# 3.6. Ground Truth

| Tên đã được chuẩn hóa | Cột CICFlowMeter | Kiểu dữ liệu         | Mô tả                                                                 |
| ----------------- | ------------------- | ------------ | --------------------------------------------------------------------------- |
| `label`           | `Label`             | `String` | Nhãn ground truth của flow, ví dụ `BENIGN`, `PortScan`, `DoS Hulk`, `DDoS`. |

`label` được sử dụng cho:

* đánh giá detection;
* phân tích phân bố dữ liệu;
* supervised learning nếu được sử dụng;
* đối chiếu kết quả detection với ground truth.

`label` **không được sử dụng làm feature đầu vào** cho bài toán unsupervised anomaly detection.

---

# 4. Feature Classification

Để thuận tiện cho tầng Security Analysis, 21 feature được chia thành các nhóm sau:

| Nhóm                   | Các đặc trưng chính                                                                                                                |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| **Flow / Volume**      | `flow_duration`, `total_fwd_packets`, `total_backward_packets`, `total_length_of_fwd_packets`, `total_length_of_bwd_packets` |
| **Traffic Rate**       | `flow_bytes_s`, `flow_packets_s`                                                                                             |
| **Timing**             | `flow_iat_mean`, `flow_iat_std`                                                                                              |
| **TCP Behavior**       | `syn_flag_count`, `ack_flag_count`, `rst_flag_count`, `fin_flag_count`, `psh_flag_count`                                     |
| **Packet Size**        | `packet_length_mean`, `packet_length_std`, `min_packet_length`, `max_packet_length`                                          |
| **TCP Fingerprinting** | `init_win_bytes_forward`, `init_win_bytes_backward`                                                                          |
| **Service Context**    | `destination_port`                                                                                                           |
| **Ground Truth**       | `label`                                                                                                                      |

---

# 5. Security Interpretation

Feature không đồng nghĩa với Indicator.

```text
Raw Feature
     ↓
Aggregation / Ratio / Time Window
     ↓
Behavioral Pattern
     ↓
Security Indicator
     ↓
Detection Rule
     ↓
Alert
```

Ví dụ:

### Port Scan

```text
destination_port
syn_flag_count
rst_flag_count
flow_duration
```

sau khi aggregation:

```text
High number of unique destination ports
+ High SYN rate
+ Short time window
            ↓
Port Scan Indicator
```

### DoS / Flood

```text
flow_packets_s
flow_bytes_s
total_fwd_packets
flow_iat_mean
```

kết hợp theo time window:

```text
Very high packet rate
+ Very low IAT
+ Abnormally high traffic volume
            ↓
Potential DoS/Flood Indicator
```

Do đó, Data Contract này định nghĩa **feature layer**, không định nghĩa toàn bộ detection logic.

---

# 6. Data Quality Rules

## 6.1. Column Name Validation

Tên cột đầu ra phải:

* sử dụng `snake_case`;
* viết thường;
* không chứa khoảng trắng;
* không chứa ký tự đặc biệt ngoài `_`;
* ánh xạ được rõ ràng về cột gốc của CICFlowMeter.

Ví dụ:

```text
Destination Port
        ↓
destination_port
```

Có thể thực hiện chuẩn hóa trong Spark:

```python
import re

def to_snake_case(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")

df = raw_df.toDF(
    *[to_snake_case(c) for c in raw_df.columns]
)
```

---

## 6.2. Type Validation

Trước khi ghi Parquet, các trường phải được ép về schema chuẩn:

```text
destination_port          → Integer
flow_duration             → Long
packet counts             → Long / Integer
traffic rates             → Double
packet-length statistics  → Double
initial window            → Long
label                     → String
```

---

## 6.3. Range Validation

Các điều kiện tối thiểu:

```text
destination_port ∈ [0, 65535]

flow_duration >= 0

packet counts >= 0

packet lengths >= 0

flow_bytes_s >= 0

flow_packets_s >= 0

flow_iat_mean >= 0

flow_iat_std >= 0

TCP flag counts >= 0

initial window >= 0
```

Bản ghi không thỏa điều kiện phải được:

```text
Rejected
   hoặc
Quarantined
   hoặc
Corrected
```

theo chính sách Data Quality của pipeline.

Không tự động sửa dữ liệu gốc nếu chưa xác định nguyên nhân.

---

# 7. Null, NaN và Infinite Values

Đối với các trường số thực:

```text
NULL
NaN
+Infinity
-Infinity
```

phải được kiểm tra trước khi ghi vào tầng processed.

Chính sách xử lý phải được xác định rõ cho từng pipeline:

```text
Invalid value
      ↓
Validate
      ↓
┌───────────────┐
│ Valid         │ → Continue
├───────────────┤
│ Missing       │ → Impute / Preserve / Reject
├───────────────┤
│ Invalid       │ → Quarantine / Reject
└───────────────┘
```

Việc loại bỏ bản ghi phải được ghi nhận trong Data Quality Report.

---

# 8. Label Governance

`label` là trường ground truth và phải được bảo toàn trong quá trình ETL.

### Nguyên tắc

* Không tự ý đổi tên hoặc thay đổi giá trị nhãn gốc.
* Nếu cần nhóm các label thành category lớn hơn, phải tạo **mapping table riêng**.
* Không ghi đè label gốc bằng label đã mapping.
* Phải lưu được mối quan hệ:

```text
Original Label
      ↓
Mapped Label
```

Ví dụ:

```text
DoS Hulk
DoS GoldenEye
DoS Slowloris
DoS Slowhttptest
        ↓
       DoS
```

Label gốc vẫn phải được giữ lại nếu cần phục vụ truy vết và đánh giá.

---

# 9. Storage Contract

## 9.1. File Format

Dữ liệu chuẩn hóa phải được lưu dưới dạng:

```text
Apache Parquet
Compression: Snappy
```

Lý do sử dụng:

* columnar storage;
* phù hợp với Apache Spark;
* hỗ trợ predicate pushdown;
* giảm I/O khi chỉ truy vấn một số feature;
* phù hợp với workload phân tích dữ liệu lớn.

---

## 9.2. Data Layers

Khuyến nghị tổ chức:

```text
MinIO
│
├── raw/
│   └── CICIDS2017 PCAP / source data
│
├── cleaned/
│   └── standardized network flow
│
└── processed/
    ├── security_features/
    ├── window_aggregates/
    └── detection_results/
```

### `raw/`

Dữ liệu nguồn, không chỉnh sửa.

### `cleaned/`

Dữ liệu flow sau:

* chuẩn hóa schema;
* chuẩn hóa tên cột;
* type casting;
* data quality validation.

### `processed/`

Dữ liệu phục vụ:

* feature engineering;
* aggregation;
* behavioral indicators;
* detection;
* evaluation.

---

# 10. Partitioning Strategy

Partitioning không phải là một phần của **feature schema**, mà là quyết định ở tầng storage dựa trên workload truy vấn.

Không nên mặc định partition toàn bộ dataset theo `label` chỉ vì đây là trường phân loại. Dataset IDS thường có phân bố label rất mất cân bằng, dễ dẫn tới partition không đồng đều hoặc tạo nhiều file nhỏ.

Khuyến nghị:

```text
Primary consideration:
    processing date / batch / time window

Optional:
    protocol
    dataset subset

Use label only when:
    query workload thực sự cần lọc theo label
```

Trong trường hợp pipeline hiện tại chưa có trường thời gian hoặc metadata phù hợp, partition có thể được quyết định riêng ở tầng Storage và ghi rõ trong configuration của pipeline.

---

# 11. Data Lineage & Traceability

Mỗi lần xử lý dataset phải có metadata tối thiểu:

| Metadata                | Mô tả                |
| ----------------------- | -------------------------- |
| `source_dataset`        | Tên dataset nguồn          |
| `source_file`           | File PCAP / source file    |
| `processing_date`       | Thời điểm xử lý            |
| `pipeline_version`      | Phiên bản pipeline         |
| `schema_version`        | Phiên bản Data Contract    |
| `input_record_count`    | Số bản ghi đầu vào         |
| `valid_record_count`    | Số bản ghi hợp lệ          |
| `rejected_record_count` | Số bản ghi bị loại         |
| `quality_report`        | Báo cáo chất lượng dữ liệu |

Mục tiêu:

```text
Processed Record
      ↓
Pipeline Version
      ↓
Source Dataset
      ↓
Original Source
```

để đảm bảo khả năng truy xuất nguồn gốc.

---

# 12. Data Quality Report

Mỗi batch xử lý phải tạo báo cáo tối thiểu gồm:

```text
Input Records
Valid Records
Rejected Records
Null Count
NaN Count
Infinite Count
Invalid Range Count
Label Distribution
Schema Validation Status
Processing Duration
```

Ví dụ:

```text
Dataset: CICIDS2017
Batch: 2026-10-08

Input records       : 1,250,000
Valid records       : 1,247,321
Rejected records    : 2,679

Null values         : 0
NaN values          : 0
Infinite values     : 0
Invalid ranges      : 2,679

Schema validation   : PASS
Label validation    : PASS
```

---

# 13. Acceptance Criteria

Dataset được xem là **Ready for Processing** khi đáp ứng toàn bộ các điều kiện:

### Schema

* Đủ 22 trường theo Data Contract.
* Tên cột sử dụng `snake_case`.
* Không có khoảng trắng đầu/cuối.
* Không có duplicate column.
* Data type đúng schema.

### Data Quality

* Không tồn tại giá trị âm tại các trường không cho phép.
* Không tồn tại `NaN` hoặc `Infinity` chưa được xử lý.
* Null values được xử lý theo policy.
* `destination_port` nằm trong miền hợp lệ.
* `label` không bị thay đổi ngoài mapping được phê duyệt.

### Storage

* Định dạng Parquet.
* Compression = Snappy.
* Partition strategy được ghi nhận.
* Có metadata và Data Quality Report.

### Traceability

* Xác định được dataset nguồn.
* Xác định được phiên bản schema.
* Xác định được phiên bản pipeline.
* Có thống kê số lượng record trước và sau xử lý.

---

# 14. Contract Boundary

Data Contract này chịu trách nhiệm định nghĩa:

```text
PCAP-derived Flow
        ↓
    Data Schema
        ↓
 Feature Semantics
        ↓
 Data Quality
        ↓
 Storage Contract
```

Data Contract **không chịu trách nhiệm trực tiếp** cho:

* Detection Rule;
* Security Alert;
* Machine Learning Model;
* SOC dashboard;
* Incident Response.

Các thành phần trên sử dụng dữ liệu từ Contract này làm đầu vào.

---

# 15. Downstream Usage

Feature layer sau khi đạt chuẩn có thể được sử dụng bởi:

```text
                     ┌── Rule-based Detection
                     │
Processed Features ──┼── Anomaly Detection
                     │
                     ├── Statistical Analysis
                     │
                     └── Machine Learning
```

Ví dụ:

```text
Flow Features
     ↓
Time-window Aggregation
     ↓
Behavioral Indicators
     ↓
Detection Rules
     ↓
Alert
     ↓
Time / Throughput / Speedup / Scaling
```

Đây là ranh giới giữa **Data Engineering Layer** và **Security Detection Layer** của hệ thống.

---

# 16. Versioning

Mọi thay đổi ảnh hưởng tới schema hoặc semantics phải tăng phiên bản Data Contract.

```text
v1.0
  ↓
Initial standardized schema

v1.1
  ↓
Non-breaking change
  (documentation / validation refinement)

v2.0
  ↓
Breaking change
  (rename/remove/change type/add mandatory field)
```

Các pipeline sử dụng Data Contract phải ghi nhận `schema_version` tương ứng trong metadata.

---
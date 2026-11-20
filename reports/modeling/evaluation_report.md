# Báo cáo Huấn luyện và Đánh giá Mô hình Nhận diện Bất thường Mạng (CICIDS2017)

> Thực nghiệm tuân thủ nghiêm ngặt Data Contract v2.3.0: chỉ sử dụng dữ liệu sạch `VALID` (Parquet + Snappy) từ 8 CSV CICIDS2017 thật; không dùng dữ liệu nhân bản synthetic; không trộn `QUARANTINED`/`REJECTED`; loại trùng trước khi chia tập; `Imputer` chỉ `fit` trên Train; chọn ngưỡng `REVIEW` hoàn toàn trên Validation và đánh giá công bằng trên cùng tập Test.

## 1. Nguồn Dữ liệu Sạch & Kiểm soát Chống Rò rỉ (Data Leakage)

| Hạng mục | Giá trị thực đo |
|---|---:|
| Đường dẫn Parquet sạch (`VALID`) | `s3a://bigdata-network/cleaned/flows/cicids2017_valid_v1` |
| Batch ID / Completion Marker | `cicids2017_valid_v1` (`COMPLETE`) |
| ETL Input / VALID / QUARANTINED / REJECTED | 2,830,743 / 2,830,628 / 115 / 0 |
| Tổng dòng `VALID` đọc vào | 2,830,628 |
| Bản ghi trùng lặp bị loại trước chia tập (`.dropDuplicates`) | 363,980 (12.8586%) |
| Tổng dòng `VALID` duy nhất sau loại trùng | **2,466,648** |
| Số lớp gốc / Nhãn nhị phân | 15 lớp (`BENIGN` = 0.0; 14 lớp `ATTACK` = 1.0) |
| Tỷ lệ `ATTACK` / `BENIGN` sau loại trùng | 425,712 (17.2587%) / 2,040,936 |
| Nhóm 21 đặc trưng trùng nhau nhưng khác nhãn | 769 |
| Kiểm tra giao nhau Train∩Val / Train∩Test / Val∩Test | **0 / 0 / 0** (PASS) |

### Phân bổ Stratified Split 70% / 15% / 15% (theo `label` gốc, `seed=42`)

| Tập dữ liệu | Tổng mẫu | Tỷ lệ | BENIGN (0.0) | ATTACK (1.0) | Tỷ lệ ATTACK |
|---|---:|---:|---:|---:|---:|
| **Train** | 1,726,537 | 69.9953% | 1,428,442 | 298,095 | 17.2655% |
| **Validation** | 369,948 | 14.9980% | 306,297 | 63,651 | 17.2054% |
| **Test** | 370,163 | 15.0067% | 306,197 | 63,966 | 17.2805% |

### Thống kê Cờ Chất lượng trên Tập `VALID` sau Loại trùng

| Cờ chất lượng (Veracity Flag) | Số dòng bật cờ | Tỷ lệ trên tập sau loại trùng | Vai trò trong Quality-Aware |
|---|---:|---:|---|
| `flow_bytes_s_invalid` | 1,556 | 0.0631% | **Điều kiện (a) chính** (lỗi tốc độ) |
| `flow_packets_s_invalid` | 1,556 | 0.0631% | **Điều kiện (a) chính** (lỗi tốc độ) |
| Hợp 2 cờ lỗi tốc độ (`OR`) | **1,556** | **0.0631%** | **Chế độ Quality-Aware chính** |
| `init_win_fwd_missing` | 856,846 | 34.7373% | Phân tích độ nhạy (sentinel `-1`) |
| `init_win_bwd_missing` | 1,161,817 | 47.1010% | Phân tích độ nhạy (sentinel `-1`) |
| Hợp cả 4 cờ feature (`OR`) | 1,162,659 | 47.1352% | Bảng phân tích độ nhạy (Mục 5) |

> **Lưu ý tránh Label Leakage:** Mã `LABEL_ENCODING_REPLACEMENT` trong `quality_reason_codes` chỉ phát sinh từ chuỗi `label` của 3 lớp Web Attack, nên được **loại trừ hoàn toàn** khỏi quy tắc `REVIEW`.

## 2. Cấu hình Mô hình Đối chứng (Baseline Random Forest)

- **Thuật toán:** `PySpark MLlib RandomForestClassifier` (PySpark MLlib)
- **Đặc trưng đầu vào:** Đúng **21 cột số lõi** từ `cleaned/flows/` (tuyệt đối không đưa 4 cờ chất lượng hay metadata vào Baseline).
- **Xử lý giá trị thiếu:** `Imputer(strategy="median")` chỉ `fit` trên tập **Train**, sau đó `transform` sang Validation và Test.
  - Giá trị median học từ Train: `flow_bytes_s` = 3,427.0493, `flow_packets_s` = 65.7592, `init_win_bytes_forward` = 1,024.0000, `init_win_bytes_backward` = 235.0000
- **Siêu tham số:** `numTrees=50`, `maxDepth=10`, `maxBins=32`, `featureSubsetStrategy="sqrt"`, `impurity="gini"`, `maxMemoryInMB=256`, `seed=42`.
- **Cấu hình đầy đủ lưu tại:** `reports/modeling/baseline_config.json`.

### Top 10 Đặc trưng Quan trọng nhất (Baseline Random Forest)

| Hạng | Tên đặc trưng | Importance |
|---:|---|---:|
| 1 | `packet_length_std` | 0.182888 |
| 2 | `total_length_of_bwd_packets` | 0.092726 |
| 3 | `packet_length_mean` | 0.086394 |
| 4 | `max_packet_length` | 0.085649 |
| 5 | `destination_port` | 0.074539 |
| 6 | `init_win_bytes_backward` | 0.071205 |
| 7 | `total_length_of_fwd_packets` | 0.065707 |
| 8 | `flow_iat_std` | 0.060931 |
| 9 | `total_fwd_packets` | 0.055827 |
| 10 | `init_win_bytes_forward` | 0.041890 |

## 3. Chọn Ngưỡng Bất định `[tau_low, tau_high]` trên Tập Validation

Quy tắc chọn ngưỡng chỉ sử dụng tập **Validation** (tuyệt đối không dùng tập Test). Mục tiêu: tối đa hóa `balance_score = AUTO_F1 - 0.05 * Review_rate` trong ngân sách vận hành `Review rate <= 5.0000%` với điều kiện (a) dùng 2 cờ lỗi tốc độ (`flow_bytes_s_invalid OR flow_packets_s_invalid`).

- **Baseline trên Validation (100% mẫu):** Precision = 99.7267%, Recall = 98.0220%, F1 (ATTACK) = 98.8670%, Macro-F1 = 99.3170%, FPR = 0.0558%.

| Khoảng bất định | Coverage | Review rate | Attack→Review | AUTO Precision | AUTO Recall | AUTO F1 | AUTO Macro-F1 | AUTO FPR | Lỗi Baseline bị chặn | Balance Score |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| [0.45, 0.55] | 99.9081% | 0.0919% | 0.1995% | 99.7344% | 98.1267% | 98.9240% | 99.3514% | 0.0542% | 5.1748% | 0.989194 |
| [0.40, 0.60] | 99.8524% | 0.1476% | 0.4870% | 99.7471% | 98.3723% | 99.0549% | 99.4305% | 0.0516% | 16.8531% | 0.990475 |
| [0.35, 0.65] | 99.8089% | 0.1911% | 0.7227% | 99.7467% | 98.4586% | 99.0985% | 99.4569% | 0.0516% | 20.8392% | 0.990889 |
| [0.30, 0.70] | 99.6745% | 0.3255% | 1.4470% | 99.7484% | 98.5780% | 99.1597% | 99.4944% | 0.0510% | 26.7133% | 0.991435 |
| [0.25, 0.75] | 99.6170% | 0.3830% | 1.6810% | 99.7479% | 98.6338% | 99.1877% | 99.5113% | 0.0510% | 29.3007% | 0.991686 |
| **[0.20, 0.80]** *(Chọn)* | 99.5207% | 0.4793% | 2.0455% | 99.7472% | 98.7361% | 99.2391% | 99.5425% | 0.0510% | 33.9860% | 0.992152 |

**Ngưỡng chốt từ Validation:** `[tau_low, tau_high] = [0.20, 0.80]` (Validation AUTO F1 = 99.2391%, Coverage = 99.5207%, Review rate = 0.4793%).

## 4. Đánh giá So sánh Công bằng trên Cùng Tập Test

Cả hai chế độ đều suy luận từ **cùng một mô hình Baseline Random Forest** và trên **cùng tập Test** (370,163 mẫu: 63,966 ATTACK, 306,197 BENIGN).

| Chỉ số đánh giá | Baseline (100% Test, p ≥ 0.5) | Quality-Aware (AUTO sau tách REVIEW) | Chênh lệch (QA − Baseline) |
|---|---:|---:|---:|
| Số mẫu đánh giá tự động | 370,163 | 368,444 | -1,719 |
| **TP** (ATTACK → ATTACK) | 62,691 | 61,919 | -772 |
| **FP** (BENIGN → ATTACK) | 171 | 158 | -13 |
| **FN** (ATTACK → BENIGN) | 1,275 | 800 | -475 |
| **TN** (BENIGN → BENIGN) | 306,026 | 305,567 | -459 |
| **Precision (ATTACK)** | 99.7280% | 99.7455% | +0.0175% |
| **Recall (ATTACK)** | 98.0068% | 98.7245% | +0.7177% |
| **F1 (ATTACK)** | 98.8599% | 99.2323% | +0.3725% |
| **Macro-F1** | 99.3121% | 99.5379% | +0.2258% |
| **Accuracy** | 99.6094% | 99.7400% | +0.1306% |
| **FPR (False Positive Rate)** | 0.0558% | 0.0517% | -0.0042% |
| **Coverage** | 100.0000% | **99.5356%** | — |
| **Review rate** | 0.0000% | **0.4644%** | — |
| **Attack to Review rate** | 0.0000% | **1.9495%** | — |

### Phân tích Hiệu quả Chặn Lỗi & Nguyên nhân Kích hoạt `REVIEW` (Test)

- Tổng mẫu chuyển sang `REVIEW`: **1,719** (0.4644% tập Test), gồm 1,247 `ATTACK` và 472 `BENIGN`.
- Nguyên nhân kích hoạt `REVIEW`: chỉ do cờ chất lượng = **247**, chỉ do vùng bất định `[0.20, 0.80]` = **1,471**, kích hoạt đồng thời cả hai = **1**.
- **Chặn lỗi của Baseline:** Quality-Aware đã cô lập được **488 / 1,446** tổng lỗi của Baseline (**33.7483%**), bao gồm **13 / 171 FP** (7.6023%) và **475 / 1,275 FN** (37.2549%).

## 5. Phân tích Độ nhạy: So sánh Chế độ 2 Cờ Rate vs 4 Cờ Feature (Gồm `init_win_*_missing`)

Để minh bạch ảnh hưởng của chính sách cờ Veracity, bảng dưới đối chiếu chế độ chính (2 cờ lỗi rate: `flow_bytes_s_invalid`, `flow_packets_s_invalid`) với biến thể bật đủ cả 4 cờ feature (thêm `init_win_fwd_missing`, `init_win_bwd_missing`) trên cùng tập Test và cùng khoảng bất định `[0.20, 0.80]`:

| Chế độ Quality-Aware (trên Test) | Coverage | Review rate | Attack→Review | AUTO Precision | AUTO Recall | AUTO F1 | AUTO Macro-F1 | AUTO FPR | Chặn lỗi Baseline |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Chính: 2 cờ Rate** + `[0.20, 0.80]` | 99.5356% | 0.4644% | 1.9495% | 99.7455% | 98.7245% | 99.2323% | 99.5379% | 0.0517% | 33.7483% |
| Độ nhạy: **4 cờ Feature** + `[0.20, 0.80]` | 52.7227% | 47.2773% | 15.8647% | 99.7226% | 98.8684% | 99.2937% | 99.5132% | 0.1047% | 47.6487% |

> **Nhận xét kỹ thuật:** Cờ `init_win_fwd_missing` và `init_win_bwd_missing` (sentinel `-1`) xuất hiện ở 47.1352% dữ liệu `VALID` (bản chất là đặc trưng giao thức của lưu lượng UDP/ICMP hoặc TCP thiếu window, không hẳn là lỗi đo đạc nghiêm trọng). Nếu dùng chúng làm trigger cứng đẩy sang `REVIEW`, `Review rate` vọt lên 47.2773% (quá tải cho SOC/con người), trong khi dùng 2 cờ lỗi tốc độ kết hợp vùng bất định giữ `Coverage` đạt 99.5356% nhưng vẫn chặn được 33.7483% lỗi của Baseline.

## 6. Phân tích Chi tiết theo Từng Lớp Gốc trên Tập Test

| Nhãn gốc (`label`) | Lớp nhị phân | Tổng Test | Baseline đúng | Baseline Acc/Recall | Chuyển REVIEW | Tỷ lệ REVIEW | AUTO đúng / AUTO tổng | AUTO Acc/Recall |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `BENIGN` | BENIGN | 306,197 | 306,026 | 99.9442% | 472 | 0.1541% | 305,567 / 305,725 | 99.9483% |
| `DoS Hulk` | ATTACK | 26,195 | 25,826 | 98.5913% | 277 | 1.0575% | 25,749 / 25,918 | 99.3479% |
| `DDoS` | ATTACK | 19,161 | 19,150 | 99.9426% | 24 | 0.1253% | 19,132 / 19,137 | 99.9739% |
| `PortScan` | ATTACK | 13,518 | 13,505 | 99.9038% | 180 | 1.3316% | 13,331 / 13,338 | 99.9475% |
| `DoS GoldenEye` | ATTACK | 1,499 | 1,353 | 90.2602% | 192 | 12.8085% | 1,207 / 1,307 | 92.3489% |
| `FTP-Patator` | ATTACK | 963 | 962 | 99.8962% | 12 | 1.2461% | 950 / 951 | 99.8948% |
| `DoS slowloris` | ATTACK | 771 | 712 | 92.3476% | 329 | 42.6719% | 438 / 442 | 99.0950% |
| `DoS Slowhttptest` | ATTACK | 770 | 738 | 95.8442% | 87 | 11.2987% | 679 / 683 | 99.4143% |
| `SSH-Patator` | ATTACK | 461 | 438 | 95.0108% | 29 | 6.2907% | 428 / 432 | 99.0741% |
| `Bot` | ATTACK | 286 | 6 | 2.0979% | 105 | 36.7133% | 4 / 181 | 2.2099% |
| `Web Attack � Brute Force` | ATTACK | 219 | 0 | 0.0000% | 10 | 4.5662% | 0 / 209 | 0.0000% |
| `Web Attack � XSS` | ATTACK | 113 | 0 | 0.0000% | 0 | 0.0000% | 0 / 113 | 0.0000% |
| `Infiltration` | ATTACK | 6 | 0 | 0.0000% | 2 | 33.3333% | 0 / 4 | 0.0000% |
| `Web Attack � Sql Injection` | ATTACK | 3 | 0 | 0.0000% | 0 | 0.0000% | 0 / 3 | 0.0000% |
| `Heartbleed` | ATTACK | 1 | 1 | 100.0000% | 0 | 0.0000% | 1 / 1 | 100.0000% |

## 7. Thời gian Thực thi & Tài nguyên Spark

- **Môi trường Spark:** Spark `3.5.0`, Master `spark://spark-master:7077`, Executor `2 cores / 2g`, Shuffle partitions = `32`, Storage level = `DISK_ONLY`.

| Giai đoạn | Thời gian (giây) |
|---|---:|
| `load_and_validate_valid_parquet` | 25.067 |
| `deduplication_and_labeling` | 61.151 |
| `stratified_split_and_leakage_check` | 86.402 |
| `baseline_rf_training` | 91.930 |
| `validation_inference` | 6.628 |
| `validation_threshold_selection` | 5.955 |
| `test_inference` | 4.937 |
| `test_evaluation` | 3.339 |
| `test_example_extraction` | 4.210 |
| `total_pipeline_seconds` | 289.618 |

## 8. Minh chứng Mẫu Thực tế từ Tập Test

Dữ liệu đầy đủ 21 đặc trưng + 4 cờ chất lượng + `quality_reason_codes` của toàn bộ mẫu minh chứng được lưu tại `reports/modeling/test_examples.csv` và `reports/modeling/test_examples.json`.

### 8.1. Ví dụ False Positive (BENIGN bị Baseline đoán nhầm thành ATTACK)

| # | `label` gốc | `p_attack` | Baseline | QA Decision | Lý do REVIEW | Cờ chất lượng bật | `dst_port` | `flow_dur` (µs) | `fwd/bwd_pkts` | `flow_bytes_s` | `pkt_len_mean` | `init_win_fwd/bwd` |
|---:|---|---:|---|---|---|---|---:|---:|---|---:|---:|---|
| 1 | `BENIGN` | 0.9868 | ATTACK | **ATTACK** | `NONE_AUTO` | Không | 2,111 | 175 | 1/1 | 45,714.29 | 3.33 | 1,024/0 |
| 2 | `BENIGN` | 0.7981 | ATTACK | **REVIEW** | `UNCERTAINTY_BAND` | `init_win_bwd_missing` | 80 | 110,207,150 | 3/0 | 0.00 | 0.00 | 229/`NULL` |
| 3 | `BENIGN` | 0.9868 | ATTACK | **ATTACK** | `NONE_AUTO` | Không | 3,551 | 181 | 1/1 | 44,198.90 | 3.33 | 1,024/0 |
| 4 | `BENIGN` | 0.7949 | ATTACK | **REVIEW** | `UNCERTAINTY_BAND` | `init_win_bwd_missing` | 80 | 13,087,981 | 2/0 | 0.00 | 0.00 | 843/`NULL` |
| 5 | `BENIGN` | 0.9868 | ATTACK | **ATTACK** | `NONE_AUTO` | Không | 34,573 | 74 | 1/1 | 108,108.11 | 3.33 | 1,024/0 |

### 8.2. Ví dụ False Negative (ATTACK bị Baseline đoán nhầm thành BENIGN)

| # | `label` gốc | `p_attack` | Baseline | QA Decision | Lý do REVIEW | Cờ chất lượng bật | `dst_port` | `flow_dur` (µs) | `fwd/bwd_pkts` | `flow_bytes_s` | `pkt_len_mean` | `init_win_fwd/bwd` |
|---:|---|---:|---|---|---|---|---:|---:|---|---:|---:|---|
| 1 | `PortScan` | 0.0007 | BENIGN | **BENIGN** | `NONE_AUTO` | `init_win_fwd_missing`, `init_win_bwd_missing` | 123 | 161 | 1/1 | 596,273.29 | 48.00 | `NULL`/`NULL` |
| 2 | `Bot` | 0.0041 | BENIGN | **BENIGN** | `NONE_AUTO` | Không | 53,880 | 62 | 1/1 | 193,548.39 | 6.00 | 1,255/256 |
| 3 | `DoS slowloris` | 0.0066 | BENIGN | **BENIGN** | `NONE_AUTO` | Không | 80 | 5,240,612 | 9/10 | 802.39 | 210.25 | 29,200/260 |
| 4 | `Web Attack � XSS` | 0.0074 | BENIGN | **BENIGN** | `NONE_AUTO` | Không | 80 | 6,553,104 | 7/5 | 487.86 | 245.92 | 29,200/243 |
| 5 | `Web Attack � Sql Injection` | 0.0226 | BENIGN | **BENIGN** | `NONE_AUTO` | Không | 80 | 5,009,041 | 4/4 | 210.22 | 117.00 | 29,200/235 |

### 8.3. Ví dụ Mẫu bị đẩy sang `REVIEW` (Quality-Aware)

| # | `label` gốc | `p_attack` | Baseline | QA Decision | Lý do REVIEW | Cờ chất lượng bật | `dst_port` | `flow_dur` (µs) | `fwd/bwd_pkts` | `flow_bytes_s` | `pkt_len_mean` | `init_win_fwd/bwd` |
|---:|---|---:|---|---|---|---|---:|---:|---|---:|---:|---|
| 1 | `BENIGN` | 0.4998 | BENIGN | **REVIEW** | `UNCERTAINTY_BAND` | `init_win_bwd_missing` | 80 | 6,000,277 | 7/0 | 7.00 | 6.00 | 256/`NULL` |
| 2 | `FTP-Patator` | 0.7236 | ATTACK | **REVIEW** | `QUALITY_FLAG_AND_UNCERTAINTY` | `flow_bytes_s_invalid`, `flow_packets_s_invalid`, `init_win_bwd_missing` | 21 | 0 | 2/0 | `NULL` | 9.33 | 229/`NULL` |
| 3 | `BENIGN` | 0.1097 | BENIGN | **REVIEW** | `QUALITY_FLAG` | `flow_bytes_s_invalid`, `flow_packets_s_invalid` | 21 | 0 | 1/1 | `NULL` | 0.00 | 229/227 |
| 4 | `BENIGN` | 0.4997 | BENIGN | **REVIEW** | `UNCERTAINTY_BAND` | `init_win_bwd_missing` | 80 | 8,049,510 | 9/0 | 6.71 | 6.00 | 256/`NULL` |
| 5 | `BENIGN` | 0.0935 | BENIGN | **REVIEW** | `QUALITY_FLAG` | `flow_bytes_s_invalid`, `flow_packets_s_invalid` | 22 | 0 | 1/1 | `NULL` | 0.00 | 256/245 |


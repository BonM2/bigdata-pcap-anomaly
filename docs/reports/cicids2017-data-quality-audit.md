# Báo cáo kiểm tra chất lượng dữ liệu CICIDS2017

> Tạo lúc `2026-11-24T11:57:26.518595+00:00` từ 8 CSV cục bộ. Đây là kiểm toán chỉ đọc và mô phỏng chính sách contract; nguồn CSV không bị sửa hay xóa.

## Kết luận điều hành

- Đã đọc **2,830,743** bản ghi trong **8 file**, tổng dung lượng **884,645,759 byte**. Cả 8 header khớp chính xác contract 79 cột; không có dòng sai số cột hoặc lỗi cú pháp CSV.
- Phân loại theo contract: **2,830,628 VALID (99.9959%)**, **115 QUARANTINED (0.0041%)**, **0 REJECTED (0.0000%)**. Đối soát đạt: `2,830,743 = 2,830,628 + 115 + 0`.
- Có **1,444,702** dòng VALID mang ít nhất một lỗi mềm; **1,442,539** dòng VALID có ít nhất một giá trị được đổi thành NULL.
- Nếu giữ VALID và impute đúng quy trình thì giữ 99.9959% dữ liệu. Nếu `dropna()` toàn bộ 21 feature thì chỉ còn **1,388,089** dòng (49.0362%).
- Kết quả này không chứng minh nhãn ground truth là đúng. Không có row bị tự động xóa trùng.

## 1. Nguồn, checksum, số dòng và schema

Nguồn được khai báo là [CIC-IDS2017 của Canadian Institute for Cybersecurity, UNB](https://www.unb.ca/cic/datasets/ids-2017.html). Trang chính thức mô tả PCAP, labeled flows và MachineLearningCSV; repository không có checksum do nhà phát hành ký hoặc manifest tải xuống, nên SHA-256 dưới đây chỉ xác minh rằng các file cục bộ không đổi giữa các lần kiểm tra; nó không xác nhận nguồn gốc tuyệt đối hoặc độ đúng của nhãn.

| File | Dòng | Byte | SHA-256 | Header 79 cột |
|---|---|---|---|---|
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | 225,745 | 77,123,859 | 6ff1580f5f81c0ae28a26f7631721018577f5f7c5e0feac28b795fcfe7b411ee | PASS |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | 286,467 | 76,906,168 | ca1824c51bfbb7b3c72290a11be04366ba8815878c6a1cc5c44cb1cee269e99b | PASS |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | 191,033 | 58,316,725 | 53a41c24d570ea83b7ac55b2e94df94e7a8216aeb80a2af0246b6bc8bb543000 | PASS |
| Monday-WorkingHours.pcap_ISCX.csv | 529,918 | 176,927,918 | 852c4beb34eda186f32561fa79df7a0747e92e1a6535b01270820dd9ffe17f34 | PASS |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | 288,602 | 83,102,436 | 6bcda3857c2504676034e3ea57762d38393cc734cb377a726bd5cb153961b1b5 | PASS |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | 170,366 | 52,023,263 | d67066211fb1689c78406f1506f4c44704ecb92088353d5c96d96d6474eb819d | PASS |
| Tuesday-WorkingHours.pcap_ISCX.csv | 445,909 | 135,078,995 | 52b8692ae8c7d2ed04671fe2b98335693c0a92c7ab157d8c8b534d6523080851 | PASS |
| Wednesday-workingHours.pcap_ISCX.csv | 692,703 | 225,166,395 | 893c27dc968bf7a8adef1689f90be55ca4a4dc3088fb63d6ff247ac56856df2a | PASS |

- Raw ingestion: 79 vị trí được đọc dạng chuỗi để tránh suy luận kiểu làm mất dữ liệu. Cleaned core: 22 trường = 6 integer, 7 long, 8 double và 1 string.
- Tên canonical sau mapping là duy nhất. Header nguồn có một tên trùng: `Fwd Header Length` tại vị trí 35 và 56 (đếm từ 1). Contract giữ thành `fwd_header_length` và `fwd_header_length_repeat_2`.
- Số dòng hai cột trùng tên có giá trị khác nhau: **0**. Dù bằng nhau trong bộ này, không được tự bỏ một cột vì chưa có bằng chứng ngữ nghĩa rằng chúng luôn tương đương.
- SHA-256 schema: `eb52b4a524f9478bcf56919ae8477028027c8f2c97c7145c96b612917f0e953b`; SHA-256 data contract: `ae77a691252d42edb4a92c95df55af70154e0b27b6fcc0109bac29983190277e`.

## 2. Lỗi quan sát được và quy tắc xử lý

Số dưới đây là **số ô lỗi theo cột**. Chúng có thể chồng lấp trên cùng một bản ghi và không được cộng để suy ra số dòng lỗi duy nhất.

| Loại lỗi | Cột | Trước xử lý | Cách xử lý | Mã lý do/cờ | Sau xử lý VALID |
|---|---|---|---|---|---|
| nan | flow_bytes_s | 1,358 (0.0480%) | Đổi NULL + `flow_bytes_s_invalid=true`; vẫn VALID nếu không có lỗi cứng | NAN:flow_bytes_s | 0 NaN trong cleaned VALID |
| positive_infinity | flow_bytes_s | 1,509 (0.0533%) | Đổi NULL + flag | INFINITY:flow_bytes_s | 0 Infinity trong cleaned VALID |
| negative | flow_bytes_s | 85 (0.0030%) | Đổi NULL + flag | NEGATIVE:flow_bytes_s | 0 số âm trong cleaned VALID |
| positive_infinity | flow_packets_s | 2,867 (0.1013%) | Đổi NULL + `flow_packets_s_invalid=true` | INFINITY:flow_packets_s | 0 Infinity trong cleaned VALID |
| negative | flow_packets_s | 115 (0.0041%) | Đổi NULL + flag | NEGATIVE:flow_packets_s | 0 số âm trong cleaned VALID |
| negative | flow_duration | 115 (0.0041%) | QUARANTINED; không ép 0 | NEGATIVE:flow_duration | Không có trong cleaned VALID |
| negative | flow_iat_mean | 115 (0.0041%) | QUARANTINED | NEGATIVE:flow_iat_mean | Không có trong cleaned VALID |
| sentinel_minus_one | init_win_bytes_forward | 1,001,189 (35.3684%) | Đổi NULL + `init_win_fwd_missing=true` | SENTINEL:init_win_bytes_forward | 0 sentinel trong cleaned VALID |
| sentinel_minus_one | init_win_bytes_backward | 1,441,552 (50.9249%) | Đổi NULL + `init_win_bwd_missing=true` | SENTINEL:init_win_bytes_backward | 0 sentinel trong cleaned VALID |
| encoding_replacement | label | 2,180 (0.0770%) | Giữ nguyên nhãn raw + reason code; không đoán ký tự | LABEL_ENCODING_REPLACEMENT | Vẫn giữ để truy vết |

Ngoài bảng trên, số lỗi nonzero khác trong 22 trường lõi: **0 loại** (không có). Vì vậy audit không phát hiện ô rỗng, lỗi ép kiểu, Infinity âm, destination port ngoài miền, TCP flag ngoài 0/1, initial-window âm khác -1 hoặc label rỗng trong 22 trường lõi.

Phân biệt đơn vị đếm:

- Tổng lượt ô được đổi NULL: **2,448,675 ô**.
- Dòng VALID có ít nhất một NULL sau làm sạch: **1,442,539 dòng duy nhất**.
- Dòng VALID có ít nhất một lỗi mềm: **1,444,702 dòng duy nhất**.
- Dòng có lỗi cứng: **115 dòng duy nhất**; các lỗi có thể cùng xuất hiện trên một dòng.

## 3. VALID / QUARANTINED / REJECTED theo file

| File | Input | VALID | QUARANTINED | REJECTED | Dòng bị tác động | VALID có NULL cleaned |
|---|---|---|---|---|---|---|
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | 225,745 | 225,743 | 2 | 0 | 88,310 | 88,308 |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | 286,467 | 286,431 | 36 | 0 | 77,296 | 77,260 |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | 191,033 | 191,024 | 9 | 0 | 118,488 | 118,479 |
| Monday-WorkingHours.pcap_ISCX.csv | 529,918 | 529,903 | 15 | 0 | 299,322 | 299,307 |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | 288,602 | 288,598 | 4 | 0 | 164,384 | 164,380 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | 170,366 | 170,355 | 11 | 0 | 104,618 | 102,444 |
| Tuesday-WorkingHours.pcap_ISCX.csv | 445,909 | 445,892 | 17 | 0 | 256,708 | 256,691 |
| Wednesday-workingHours.pcap_ISCX.csv | 692,703 | 692,682 | 21 | 0 | 335,691 | 335,670 |
| TOÀN BỘ | 2,830,743 | 2,830,628 | 115 | 0 | 1,444,817 | 1,442,539 |

`REJECTED = 0` vì audit không gặp lỗi cú pháp hoặc sai số cột. Điều này không có nghĩa REJECTED bị vô hiệu; parser vẫn có nhánh `MALFORMED_CSV` cho dữ liệu hỏng trong lần chạy khác.

## 4. Phân bố nhãn, mất cân bằng và tác động theo nhãn

| Nhãn raw | Trước | VALID sau | Cách ly | Dòng bị tác động | VALID có NULL cleaned | Giữ lại |
|---|---|---|---|---|---|---|
| BENIGN | 2,273,097 | 2,272,982 | 115 | 1,319,944 | 1,319,829 | 99.9949% |
| DoS Hulk | 231,073 | 231,073 | 0 | 67,060 | 67,060 | 100.0000% |
| PortScan | 158,930 | 158,930 | 0 | 190 | 190 | 100.0000% |
| DDoS | 128,027 | 128,027 | 0 | 46,530 | 46,530 | 100.0000% |
| DoS GoldenEye | 10,293 | 10,293 | 0 | 2,579 | 2,579 | 100.0000% |
| FTP-Patator | 7,938 | 7,938 | 0 | 1,495 | 1,495 | 100.0000% |
| SSH-Patator | 5,897 | 5,897 | 0 | 12 | 12 | 100.0000% |
| DoS slowloris | 5,796 | 5,796 | 0 | 1,642 | 1,642 | 100.0000% |
| DoS Slowhttptest | 5,499 | 5,499 | 0 | 3,172 | 3,172 | 100.0000% |
| Bot | 1,966 | 1,966 | 0 | 10 | 10 | 100.0000% |
| Web Attack � Brute Force | 1,507 | 1,507 | 0 | 1,507 | 0 | 100.0000% |
| Web Attack � XSS | 652 | 652 | 0 | 652 | 17 | 100.0000% |
| Infiltration | 36 | 36 | 0 | 3 | 3 | 100.0000% |
| Web Attack � Sql Injection | 21 | 21 | 0 | 21 | 0 | 100.0000% |
| Heartbleed | 11 | 11 | 0 | 0 | 0 | 100.0000% |

- `BENIGN`: **2,273,097** (80.30%). Tổng nhãn khác BENIGN: **557,646** (19.70%).
- Lớp lớn nhất/nhỏ nhất: `BENIGN` 2,273,097 so với `Heartbleed` 11; tỷ lệ xấp xỉ **206,645.2:1**. Accuracy tổng thể sẽ dễ gây hiểu lầm; nên báo cáo macro-F1, per-class recall và confusion matrix.
- Nhãn chứa ký tự thay thế `�` được giữ nguyên. Không khẳng định sửa thành dấu gạch nối hay ký tự khác nếu chưa có bằng chứng từ nguồn tương ứng.

### Phân bố trước/sau theo từng file và nhãn

| File | Nhãn | Trước | VALID sau | Cách ly | Bị tác động |
|---|---|---|---|---|---|
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | BENIGN | 97,718 | 97,716 | 2 | 41,780 |
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | DDoS | 128,027 | 128,027 | 0 | 46,530 |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | BENIGN | 127,537 | 127,501 | 36 | 77,106 |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | PortScan | 158,930 | 158,930 | 0 | 190 |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | BENIGN | 189,067 | 189,058 | 9 | 118,478 |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | Bot | 1,966 | 1,966 | 0 | 10 |
| Monday-WorkingHours.pcap_ISCX.csv | BENIGN | 529,918 | 529,903 | 15 | 299,322 |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | BENIGN | 288,566 | 288,562 | 4 | 164,381 |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | Infiltration | 36 | 36 | 0 | 3 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | BENIGN | 168,186 | 168,175 | 11 | 102,438 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | Web Attack � Brute Force | 1,507 | 1,507 | 0 | 1,507 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | Web Attack � Sql Injection | 21 | 21 | 0 | 21 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | Web Attack � XSS | 652 | 652 | 0 | 652 |
| Tuesday-WorkingHours.pcap_ISCX.csv | BENIGN | 432,074 | 432,057 | 17 | 255,201 |
| Tuesday-WorkingHours.pcap_ISCX.csv | FTP-Patator | 7,938 | 7,938 | 0 | 1,495 |
| Tuesday-WorkingHours.pcap_ISCX.csv | SSH-Patator | 5,897 | 5,897 | 0 | 12 |
| Wednesday-workingHours.pcap_ISCX.csv | BENIGN | 440,031 | 440,010 | 21 | 261,238 |
| Wednesday-workingHours.pcap_ISCX.csv | DoS GoldenEye | 10,293 | 10,293 | 0 | 2,579 |
| Wednesday-workingHours.pcap_ISCX.csv | DoS Hulk | 231,073 | 231,073 | 0 | 67,060 |
| Wednesday-workingHours.pcap_ISCX.csv | DoS Slowhttptest | 5,499 | 5,499 | 0 | 3,172 |
| Wednesday-workingHours.pcap_ISCX.csv | DoS slowloris | 5,796 | 5,796 | 0 | 1,642 |
| Wednesday-workingHours.pcap_ISCX.csv | Heartbleed | 11 | 11 | 0 | 0 |

## 5. Khảo sát bản ghi trùng

Tiêu chí: **byte-identical physical CSV record, excluding CR/LF; BLAKE2b-128 fingerprint**. Đây là khảo sát bằng fingerprint, không phải khóa định danh flow. Không xóa trùng vì CSV không có source/destination IP, timestamp hay flow ID ổn định để kết luận các dòng giống nhau là lỗi sao chép thay vì các quan sát hợp lệ lặp lại.

| Phạm vi | Nhóm trùng | Dòng thuộc nhóm trùng | Dòng dư nếu giữ 1/nhóm |
|---|---|---|---|
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | 1,307 | 3,940 | 2,633 |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | 35,351 | 107,704 | 72,353 |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | 3,224 | 10,112 | 6,888 |
| Monday-WorkingHours.pcap_ISCX.csv | 9,938 | 36,873 | 26,935 |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | 9,414 | 45,044 | 35,630 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | 2,775 | 8,841 | 6,066 |
| Tuesday-WorkingHours.pcap_ISCX.csv | 8,333 | 32,398 | 24,065 |
| Wednesday-workingHours.pcap_ISCX.csv | 10,735 | 92,644 | 81,909 |
| TOÀN BỘ (kể cả trùng chéo file) | 95,169 | 403,550 | 308,381 |

| Nhãn | Nhóm trùng | Dòng thuộc nhóm trùng |
|---|---|---|
| BENIGN | 59,644 | 236,257 |
| PortScan | 33,390 | 101,501 |
| DoS Hulk | 1,340 | 59,564 |
| SSH-Patator | 148 | 2,826 |
| FTP-Patator | 452 | 2,457 |
| DoS slowloris | 96 | 507 |
| DoS Slowhttptest | 52 | 323 |
| Web Attack � Brute Force | 25 | 62 |
| DDoS | 9 | 20 |
| Bot | 6 | 19 |
| DoS GoldenEye | 7 | 14 |
| TOÀN BỘ | 95,169 | 403,550 |

## 6. Cờ chất lượng, mã lý do và truy vết

| Trường | Ví dụ |
|---|---|
| quality_status | VALID / QUARANTINED / REJECTED |
| quality_reason_codes | [`SENTINEL:init_win_bytes_forward`, `INFINITY:flow_bytes_s`] |
| flow_bytes_s_invalid | true khi rate NULL/NaN/Infinity/âm |
| flow_packets_s_invalid | true khi rate NULL/NaN/Infinity/âm |
| init_win_fwd_missing | true khi raw `init_win_bytes_forward = -1` |
| init_win_bwd_missing | true khi raw `init_win_bytes_backward = -1` |
| lineage | source_file, source_path, batch_id, schema_version, pipeline_version, processed_at |
| raw evidence | _raw_values; _raw_record cho dòng malformed; cast_failure_fields |

Raw CSV và SHA-256 phải được giữ bất biến. Dataset cách ly/từ chối giữ raw evidence để có thể quay lại giá trị ban đầu. `VALID` chỉ là trạng thái chất lượng kỹ thuật, không có nghĩa flow là BENIGN.

## 7. Quality-Aware và chuẩn bị huấn luyện

- Feature model nên dùng giá trị đã làm sạch cùng các cờ `*_invalid`/`*_missing`; có thể giúp mô hình phân biệt NULL do sentinel với giá trị quan sát thật.
- Chia train/validation/test trước. Mọi imputer, scaler, encoder, selector hoặc resampling có học tham số phải `fit` chỉ trên train, rồi `transform` validation/test. Với dữ liệu mạng theo ngày/file, nên cân nhắc split theo thời gian hoặc file để giảm leakage do các flow giống nhau.
- Không dùng `dropna()` toàn bộ 21 feature: kịch bản này mất hơn một nửa số dòng. Không điền NULL bằng 0 nếu chưa có lý do miền dữ liệu.
- Với mất cân bằng mạnh, ưu tiên class weight hoặc sampling chỉ trên train; không resample validation/test.

## 8. Đối soát và giới hạn bằng chứng

- Đối soát record: **PASS** — `2,830,743 = 2,830,628 + 115 + 0`.
- Đối soát training retention: **PASS** — `2,830,628 = 1,442,539 + 1,388,089`.
- Header/schema/row width/checksum: **PASS trên file cục bộ**.
- Đây chưa phải bằng chứng Spark đã ghi thành công cleaned Parquet thật lên MinIO. Cần `_SUCCESS.json`, hậu kiểm PASS và đọc lại output trước khi tuyên bố batch production hoàn tất.
- Checksum không xác nhận nhãn đúng. Không hiệu chỉnh nhãn, không tạo lại flow và không xóa trùng nếu chưa có bằng chứng từ PCAP/ground-truth/source manifest tương ứng.
- Khảo sát trùng dùng BLAKE2b-128 trên raw record byte-identical; các dòng tương đương ngữ nghĩa nhưng khác định dạng số không được gộp vào tiêu chí này.

## 9. Lệnh tái lập

```powershell
python -m processing.audit_source --source-dir data/raw/cicids2017 --report docs/evidence/local/source-audit.local.json --workers 4
python -m experiments.analyze_training_readiness
python -m experiments.generate_data_quality_report
```

Các bằng chứng máy đọc: `docs/evidence/local/source-audit.local.json`, `docs/analysis/cicids2017-data-quality-training-readiness.json`, `docs/analysis/cicids2017-data-quality-comprehensive.json`.

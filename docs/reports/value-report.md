# Value & Tổng hợp 5V — Giá trị sử dụng dữ liệu và đối soát toàn chuỗi Big Data

> Báo cáo được tổng hợp hoàn toàn từ các artifact đã kiểm chứng trên nhánh `master` (đợt thực nghiệm dữ liệu thật ngày **10/11/2026**, mã `real-20261110-141211`, cùng kiểm toán chất lượng và fixture Zeek trong repository). Phạm vi thuộc đồ án môn **Nhập môn Dữ liệu lớn** (**Storage → Processing** trên Apache Spark 3.5.0 + MinIO); **không** bao gồm huấn luyện hay suy luận mô hình học máy.

## 1. Đối tượng sử dụng và quyết định được hỗ trợ

Trong chuỗi xử lý dữ liệu mạng CICIDS2017 và Zeek JSON Lines, giá trị thực tế (**Value**) không chỉ nằm ở việc chạy xong một job ETL mà nằm ở việc **ai có thể sử dụng trực tiếp đầu ra đã chuẩn hóa và đưa ra quyết định kỹ thuật cụ thể dựa trên bằng chứng đo được**. Bảng dưới đây phân tách 3 nhóm người dùng sát phạm vi đồ án:

**Bảng Q.1. Ba nhóm đối tượng sử dụng đầu ra pipeline và quyết định được hỗ trợ.**

| Đối tượng sử dụng | Đầu ra pipeline sử dụng | Quyết định được hỗ trợ | Giới hạn & điều kiện áp dụng |
| --- | --- | --- | --- |
| Nhóm nghiên cứu / sinh viên triển khai bước sau | `cleaned/flows/` (34 cột), `processed/derived_features/` (38 cột), 4 cờ chất lượng, báo cáo kiểm toán chất lượng | Chọn chiến lược giữ `VALID` + cờ chất lượng kết hợp impute thay vì `dropna()`; thiết kế chia tập train/test giảm leakage và chọn metric phù hợp mất cân bằng lớp | `VALID` chỉ là đạt chất lượng kỹ thuật, không đồng nghĩa `BENIGN`; chưa chứng minh ground truth từ PCAP và mới hậu kiểm 22/79 trường lõi |
| Người vận hành pipeline (vận hành Spark–MinIO) | `_SUCCESS.json`, `quality.json`, `summary.csv`, `performance.json`, `quality/quarantine/`, `quality/rejected/`, hồ sơ benchmark `real-20261110-141211` | Xác định batch hoàn tất hay thất bại (chỉ tin `_SUCCESS.json` `COMPLETE`), khoanh vùng lỗi cứng theo file nguồn, ước lượng thời gian chạy lô (~7 phút 49 giây cho 8 CSV) và tránh tăng worker/shuffle partition không hiệu quả trên lô nhỏ | Số đo thuộc môi trường batch trên 1 máy Docker Desktop (~9,69 GiB RAM cấp cho Docker); chưa đo đa nút vật lý hay peak RAM hệ thống |
| Người phân tích an ninh mạng mức khảo sát | `processed/aggregates/` (46 nhóm tổng hợp, 13 cột), phân bố nhãn nguồn, đầu ra Parquet của luồng Zeek JSONL | Truy vấn nhanh số flow `VALID`, tổng packet, tổng payload byte và thời lượng trung bình theo nhãn/file nguồn; khảo sát chênh lệch giữa tần suất flow và khối lượng payload (ví dụ `Heartbleed` ít flow nhưng payload lớn) | Chỉ phục vụ phân tích khảo sát theo lô; không phải hệ thống phát hiện hay cảnh báo thời gian thực; CSV CICIDS2017 thiếu IP nguồn/đích và timestamp |

## 2. Giá trị thực tế gắn với kết quả xác nhận dữ liệu

### 2.1. Giá trị từ `processed/aggregates/` và `processed/derived_features/`

Thay vì buộc người dùng sau phải đọc lại toàn bộ **8 file CSV thô (884.645.759 byte ≈ 884,65 MB thập phân / 843,66 MiB)** với 79 cột chuỗi và tự xử lý lỗi kiểu/sentinel, pipeline xuất đồng thời các lớp Parquet nén Snappy có schema cố định [A1–A3]:

- **`cleaned/flows/` (34 cột, 2.830.628 dòng `VALID`):** giữ đúng 22 trường lõi (6 integer, 7 long, 8 double, 1 string) cùng 12 trường truy vết và cờ chất lượng.
- **`processed/derived_features/` (38 cột, 2.830.628 dòng `VALID`):** bổ sung 4 đặc trưng dẫn xuất có định nghĩa tường minh (`total_packets`, `total_bytes` dùng `Decimal(20,0)` chống tràn số nguyên 64-bit, `fwd_packet_ratio`, `bwd_packet_ratio` có bảo vệ chia cho 0). Lưu ý `total_bytes` là tổng payload byte theo định nghĩa nguồn, không phải wire bytes [A7].
- **`processed/aggregates/` (13 cột, 46 dòng tổng hợp):** gom sẵn 4 cấp (`1` dòng `batch`, `15` dòng `label`, `8` dòng `source_file`, `22` dòng `source_file_label`), tách bạch số đếm toàn đầu vào (`flow_count`, `valid_ratio`, `error_ratio`) với chỉ số lưu lượng chỉ tính trên `VALID` (`total_packets_sum`, `total_bytes_sum`, `avg_flow_duration`) [A7].

**Lần xác minh trực tiếp mới trên MinIO với đủ 8 CSV:** **CHƯA CHẠY**. Việc ghi và đọc lại Parquet của cả 8 CSV trên MinIO đã được ghi nhận trong thực nghiệm ngày **10/11/2026** [A2–A3]. Bảng Q.2 lấy từ truy vấn Spark ngày **24/11/2026** trên Parquet tạo cục bộ từ 8 CSV thật. Mục 6.1 hướng dẫn chụp snapshot mới khi có batch hoàn tất trên MinIO.

**Bảng Q.2. Tham chiếu truy vấn Value cục bộ ngày 24/11/2026 từ kịch bản bảo vệ (không thay thế live snapshot MinIO mới) [A6].**

Kịch bản bảo vệ (`docs/defense/defense-demo-script.md`, mục B3) ghi nhận lượt truy vấn Spark 3.5.0 cục bộ trên cùng 8 CSV thật đã kiểm toán checksum (46 dòng tổng hợp, đối soát `2.830.743 = 2.830.628 VALID + 115 QUARANTINED + 0 REJECTED`, khớp 100% số dòng `VALID` theo nhãn với báo cáo kiểm toán chất lượng):

| Hạng | Nhãn | Flow `VALID` | Tổng payload (byte) |
| --- | --- | --- | --- |
| 1 | BENIGN | 2.272.982 | 44.289.575.301 |
| 2 | DoS Hulk | 231.073 | 1.860.611.318 |
| 3 | DDoS | 128.027 | 948.109.530 |
| 4 | Heartbleed | 11 | 80.177.574 |
| 5 | DoS GoldenEye | 10.293 | 71.842.537 |

Điểm đáng chú ý về mặt **Value**: `Heartbleed` chỉ có **11 flow** nhưng đứng thứ **4** về tổng payload byte (**80.177.574 byte**), vượt cả `DoS GoldenEye` (**10.293 flow**, **71.842.537 byte**). Điều này cho thấy bảng `processed/aggregates/` cho phép đối chiếu đồng thời số flow và khối lượng payload theo nhãn, mang lại góc nhìn mà việc đếm dòng thô đơn lẻ không thể hiện được.

### 2.2. Giá trị bảo toàn dữ liệu khi giữ `VALID` + 4 cờ chất lượng so với `dropna()`

Một giá trị định lượng trực tiếp của thiết kế chất lượng dữ liệu là **tránh mất mát hơn một nửa tập dữ liệu** do các giá trị sentinel `-1` ở hai trường TCP initial window và các giá trị rate `NaN`/`Infinity`/âm. Đối soát từ `source-audit.json` và báo cáo kiểm toán chất lượng `cicids2017-data-quality-audit.md` cho thấy [A1, A5]:

**Bảng Q.3. So sánh tỷ lệ bảo toàn bản ghi giữa chính sách `VALID` + 4 cờ chất lượng và `dropna()`.**

| Kịch bản sử dụng dữ liệu sau làm sạch | Số flow giữ lại | Tỷ lệ so với đầu vào | Số flow mất | Ghi chú kỹ thuật |
| --- | --- | --- | --- | --- |
| Đầu vào nguồn (8 CSV CICIDS2017) | 2.830.743 | 100,0000% | 0 | 79 cột gốc, chưa kiểm tra/làm sạch |
| Giữ `VALID` + 4 cờ chất lượng (chính sách pipeline) | 2.830.628 | 99,9959% | 115 | Chỉ cách ly 115 dòng lỗi cứng; bảo toàn 1.442.539 dòng `VALID` có NULL hợp lệ kèm cờ để bước sau xử lý |
| Xóa dòng thiếu ngây thơ (`dropna()` trên toàn bộ 21 feature) | 1.388.089 | 49,0362% | 1.442.654 | Mất 50,9638% dữ liệu chủ yếu do sentinel `-1` ở initial window; ít hơn chính sách hiện tại 1.442.539 flow |

Kết quả đối soát khẳng định **`2.830.628 = 1.442.539 + 1.388.089` (`PASS`)**: chính sách của nhóm giữ lại được nhiều hơn **1.442.539 flow `VALID`** so với `dropna()` toàn bộ 21 feature, trong khi vẫn minh bạch hóa toàn bộ **2.448.675 ô** đã chuyển NULL thông qua 4 cờ boolean (`flow_bytes_s_invalid`, `flow_packets_s_invalid`, `init_win_fwd_missing`, `init_win_bwd_missing`).

### 2.3. Minh bạch rủi ro dữ liệu trước bước mô hình hóa

Pipeline không "che giấu" khuyết điểm của dữ liệu nguồn bằng cách ép số âm thành 0, tự sửa chuỗi nhãn hay tự động xóa dòng trùng. Thay vào đó, các rủi ro được định lượng rõ để bước phân tích hoặc mô hình hóa sau ra quyết định đúng [A1, A3, A5]:

**Bảng Q.4. Các rủi ro dữ liệu chính được lượng hóa trước bước xây dựng mô hình.**

| Rủi ro dữ liệu được làm rõ trước bước mô hình | Số liệu đã xác minh từ artifact | Tác động kỹ thuật nếu bỏ qua |
| --- | --- | --- |
| Mất cân bằng lớp cực mạnh | `BENIGN`: 2.273.097 (80,3004%) vs `Heartbleed`: 11 (0,0004%), tỷ lệ ~206.645,2:1 | Accuracy tổng thể dễ che khuất lớp hiếm; bước sau cần báo cáo macro-F1, recall từng lớp và confusion matrix |
| Sentinel `-1` ở TCP initial window & lỗi mềm ở rate | `init_win_bytes_forward` = -1: 1.001.189 ô (35,3684%); `init_win_bytes_backward` = -1: 1.441.552 ô (50,9249%); tổng dòng `VALID` có ≥1 lỗi mềm: 1.444.702 (51,0361%) | Coi `-1` như giá trị đo thật làm sai phân bố; dùng `dropna()` làm mất quá nửa dữ liệu |
| Nhãn chứa ký tự thay thế Unicode `U+FFFD` | 2.180 dòng (0,0770%) thuộc 3 lớp `Web Attack` | Tự sửa chuỗi nhãn không có bằng chứng sẽ làm mất truy vết; pipeline giữ nhãn raw và gắn `LABEL_ENCODING_REPLACEMENT` |
| Bản ghi trùng byte-identical | 95.169 nhóm trùng, 403.550 dòng thuộc nhóm trùng (308.381 dòng dư nếu giữ 1/nhóm) | Chia train/test ngẫu nhiên có nguy cơ leakage; không tự xóa trùng vì CSV thiếu khóa định danh flow |

Nhóm nhấn mạnh nguyên tắc kỹ thuật: **`VALID` chỉ biểu thị bản ghi đạt quy tắc chất lượng của Data Contract v2.3.0, hoàn toàn KHÔNG đồng nghĩa với nhãn `BENIGN`** (mọi lớp tấn công vượt qua quy tắc miền giá trị đều mang trạng thái `VALID`).

### 2.4. Căn cứ tối ưu vận hành từ benchmark Velocity (`real-20261110-141211`)

Các phép đo Velocity trên dữ liệu CICIDS2017 thật cung cấp số liệu thực nghiệm trực tiếp cho việc lập lịch và chọn cấu hình chạy lô trên cụm Docker cục bộ (Intel Core i5-9400, 6 cores, Docker cấp ~9,69 GiB RAM, Spark 3.5.0) [A2, A4]:

**Bảng Q.5. Hiệu năng mở rộng theo kích thước dữ liệu thật (2 workers, `p=4`, 3 lượt đo/mức).**

| Mức workload (tập CSV thật) | Số flow | Dung lượng (MB thập phân) | Input partitions | Trung vị (s) | Min–max (s) | Throughput trung vị (flow/s) |
| --- | --- | --- | --- | --- | --- | --- |
| Nhỏ (Friday Morning) | 191.033 | 58,32 | 2 | 61,575 | 58,382–76,854 | 3.102,432 |
| Vừa (3 file Friday) | 703.245 | 212,35 | 8 | 136,291 | 124,205–179,073 | 5.159,884 |
| Đầy đủ (8 CSV CICIDS2017) | 2.830.743 | 884,65 | 30 | 468,713 | 423,064–481,487 | 6.039,392 |

**Bảng Q.6. So sánh cấu hình số worker và số shuffle partitions trên Friday Morning (191.033 flow, 58,32 MB).**

| Thí nghiệm (Friday Morning, 191.033 flow) | Cấu hình | Trung vị (s) | Throughput trung vị (flow/s) | Speedup |
| --- | --- | --- | --- | --- |
| Số worker (p=4) | 1 worker | 51,338 | 3.721,061 | 1,000x |
| Số worker (p=4) | 2 workers | 57,293 | 3.334,288 | 0,896x |
| Shuffle partitions (2 workers) | p = 4 | 48,729 | 3.920,308 | 1,000x |
| Shuffle partitions (2 workers) | p = 8 | 52,938 | 3.608,592 | 0,920x |
| Shuffle partitions (2 workers) | p = 16 | 55,487 | 3.442,829 | 0,878x |
| Shuffle partitions (2 workers) | p = 32 | 57,562 | 3.318,715 | 0,847x |

**Bảng Q.7. Trung vị thời gian từng công đoạn trên 3 lượt xử lý toàn bộ 8 CSV thật.**

| Công đoạn trên 3 lượt toàn bộ 8 CSV | Trung vị (s) | Min–max (s) |
| --- | --- | --- |
| Đọc dữ liệu (S3A) và tính SHA-256 | 89,571 | 85,580–91,329 |
| Chuyển đổi, validation và báo cáo chất lượng trước làm sạch | 125,108 | 99,796–127,490 |
| Cleaning, hậu kiểm, đối soát và báo cáo chất lượng sau làm sạch | 175,247 | 147,084–185,371 |
| Tạo đặc trưng dẫn xuất và bảng tổng hợp | 31,347 | 29,216–32,348 |
| Ghi 5 dataset Parquet Snappy và đọc lại xác minh | 51,364 | 49,766–52,432 |

Từ 3 bảng đo trên rút ra 3 căn cứ vận hành cụ thể:
1. **Lập kế hoạch chạy lô:** toàn bộ 8 CSV thật (2.830.743 flow, 884,65 MB) hoàn tất xử lý kèm hậu kiểm và 5/5 Parquet read-back với trung vị **468,713 giây** (khoảng **7 phút 49 giây**), throughput trung vị **6.039,392 flow/s**; audit lịch sử xác nhận **31/31 batch `COMPLETE`** (155 output Parquet) [A2–A3].
2. **Tránh cấu hình dư thừa trên batch nhỏ:** trên Friday Morning (2 input partitions), tăng từ 1 lên 2 worker làm chậm thêm ~11,6% (speedup **0,896x**), và tăng `spark.sql.shuffle.partitions` từ 4 lên 8/16/32 đều làm tăng thời gian trung vị (`48,729 s` → `57,562 s`). Tuy nhiên không suy rộng kết luận này cho các batch lớn hơn nhiều hoặc môi trường nhiều máy vật lý [A4].
3. **Định hướng ưu tiên tối ưu:** trong 5 công đoạn đo trên toàn bộ 8 CSV, **`Cleaning, hậu kiểm, đối soát và báo cáo chất lượng sau làm sạch`** có trung vị lớn nhất (**175,247 s**), lớn hơn đáng kể bước tạo đặc trưng/tổng hợp (**31,347 s**). Lưu ý không cộng trung vị từng công đoạn để thay cho trung vị toàn pipeline [A4].

## 3. Phân biệt kết quả đã chứng minh và phần mở rộng dự kiến

Để đảm bảo tính trung thực học thuật, báo cáo tách bạch hoàn toàn giữa các năng lực **đã được kiểm chứng bằng mã nguồn và artifact trên nhánh `master`** với **các hướng mở rộng dự kiến ngoài phạm vi hiện tại**, đồng thời phân biệt rõ 3 nguồn dữ liệu (**CICIDS2017 thật**, **synthetic nhân bản dùng test tải** và **mẫu Zeek 360 dòng từ PCAP tổng hợp**):

**Bảng Q.8. Ranh giới giữa kết quả đã chứng minh và phần mở rộng dự kiến.**

| Hạng mục | Trạng thái trên nhánh `master` | Bằng chứng cụ thể / Phạm vi giới hạn |
| --- | --- | --- |
| Pipeline batch ETL Spark 3.5.0 + MinIO S3A trên 8 CSV CICIDS2017 thật | **Đã chứng minh** | 3 lượt full-data `PASS`, bảo toàn 2.830.743 flow, 5/5 Parquet Snappy read-back `PASS`, audit lịch sử 31 batch / 155 outputs `COMPLETE` [A1–A3] |
| Kiểm thử tải / smoke trên dữ liệu synthetic nhân bản (12k/60k/120k) | **Đã chứng minh (tách riêng)** | Dùng fixture 12 dòng để kiểm thử chức năng và đối chiếu phương pháp (`docs/benchmarks/20261021/`, `scaling-20261110-121252/`); không gộp vào kết luận CICIDS2017 thật [A4] |
| Đa dạng định dạng (Variety) với luồng Zeek JSON Lines (`processing/zeek_ingestion.py`) | **Đã chứng minh mức demo schema/định dạng** | Đọc và xác minh 360 dòng `data/sample/zeek/conn.demo.jsonl` (180 TCP, 120 UDP, 60 ICMP; 200 dòng thiếu `service`) tạo từ PCAP tổng hợp offline [A7] |
| Trích xuất trực tiếp PCAP → flow (`extraction/`) | **Chưa triển khai (mở rộng dự kiến)** | Thư mục `extraction/` hiện chỉ là file khung trống (`extract_flow.py` (1 byte), `utils.py` (1 byte)); pipeline chính nhận CSV flow có sẵn |
| Mô hình học máy nhận diện bất thường / Quality-Aware Review model | **Chưa triển khai trong pipeline chính (mở rộng dự kiến)** | Repo không import `pyspark.ml`, chưa huấn luyện hay đo inference trong `processing/`; dữ liệu `cleaned/flows/` + 4 cờ là đầu vào chuẩn bị cho giai đoạn sau |
| Xử lý streaming / cảnh báo thời gian thực (Kafka, `readStream`/`writeStream`) | **Chưa triển khai (mở rộng dự kiến)** | Hệ thống hiện thuần batch processing trên file tĩnh đã lưu trên MinIO; chưa đo độ trễ cảnh báo trên mạng thực |

## 4. Hạn chế hiện tại và hướng phát triển

Các hạn chế dưới đây được rút trực tiếp từ trạng thái mã nguồn và bằng chứng hiện có trong repository:

1. **Chưa có module trích xuất PCAP → flow hoàn chỉnh:** thư mục `extraction/` hiện chỉ gồm các file khung trống (`extract_flow.py` (1 byte), `utils.py` (1 byte)); pipeline chính nhận đầu vào là CSV flow đã trích xuất sẵn từ CICIDS2017. *Hướng phát triển:* hoàn thiện công cụ trích xuất flow chuẩn hóa từ PCAP (ví dụ qua Zeek/NFStream) có schema khớp Data Contract trước khi đẩy vào MinIO.
2. **Thiếu định danh 5-tuple và timestamp trong 8 CSV CICIDS2017:** bộ CSV nguồn không có IP nguồn/đích, cổng nguồn, giao thức hay mốc thời gian sự kiện ổn định, nên không thể khử trùng theo khóa định danh flow, không tổng hợp theo cửa sổ `event_time` và không tự xác minh nhãn ground truth từ PCAP gốc. *Hướng phát triển:* kết hợp log giàu ngữ cảnh như Zeek `conn.log` (đã có IP, cổng, `uid`, `ts`) cho các kịch bản truy vết kết nối đầy đủ.
3. **Thực nghiệm batch trên một máy vật lý ở quy mô dưới 1 GB:** dữ liệu thật đạt **884,65 MB** thập phân (**843,66 MiB**), chạy trên 1 máy Windows/WSL2 Docker; hai worker dùng chung CPU/RAM/I/O nên chưa chứng minh mở rộng đa nút vật lý, chưa đo tải nhiều GB/TB và chưa thu thập peak RAM hay Spark History Server. *Hướng phát triển:* bổ sung Spark Event Log / History Server, đo tài nguyên hệ thống và thử nghiệm trên cụm nhiều nút độc lập.
4. **Chưa tích hợp mô hình học máy và chưa hỗ trợ streaming:** pipeline hiện chỉ xử lý theo lô (batch processing), chưa có nhánh huấn luyện/suy luận ML và chưa có luồng Kafka/`readStream`/`writeStream`. *Hướng phát triển:* xây dựng bước mô hình hóa tách biệt đọc `cleaned/flows/` + 4 cờ chất lượng (đánh giá bằng macro-F1, recall lớp hiếm và split theo file/ngày), sau đó đo riêng thời gian huấn luyện và độ trễ suy luận.

### Ghi nhận đối soát chéo giữa artifact JSON/CSV và văn bản Markdown

- **SCHEMA_CONTRACT_SHA256_EOL:** Chuẩn artifact gốc: `source-audit.json` ghi SHA-256 dạng CRLF/CRLF (`6c62f9be0652...` / `7d8e9b16d171...`). Văn bản báo cáo cũ: `cicids2017-data-quality-audit.md` ghi SHA-256 dạng LF/LF (`eb52b4a524f9...` / `ae77a691252d...`). Cách xử lý: Cả hai cặp hash đều khớp cùng nội dung file trên đĩa, chỉ khác ký tự xuống dòng LF/CRLF khi checkout Git.

## 5. Tổng hợp chuỗi 5V của báo cáo nhóm

Toàn bộ 5 đặc tính Big Data của đồ án được tổng hợp nhất quán từ cùng một hệ bằng chứng gốc (`real-20261110-141211`, Data Contract v2.3.0, báo cáo kiểm toán chất lượng và fixture Zeek):

**Bảng Q.9. Bảng tổng hợp chuỗi 5V nhất quán toàn đồ án.**

| Chữ V | Tóm tắt số liệu & bằng chứng đã đối soát trên `master` | Nguyên tắc diễn giải đúng phạm vi |
| --- | --- | --- |
| **Volume** | 8 CSV CICIDS2017 thật: **2.830.743 flow**, **884.645.759 byte** (~**884,65 MB** thập phân / ~**843,66 MiB**), 79 cột nguồn, 30 input partitions trên Spark [A1–A2] | Không tuyên bố quy mô TB (dữ liệu thật khoảng 0,885 GB thập phân); đơn vị là network flow, không phải packet PCAP |
| **Variety** | 2 nguồn khác cấu trúc: CSV 79 cột (CICIDS2017) và JSON Lines Zeek `conn.log` (có trường tùy chọn như `service`), dùng schema riêng và cùng xuất Parquet Snappy; mẫu demo Zeek **360 dòng** (180 TCP / 120 UDP / 60 ICMP, 200 dòng thiếu `service`) [A5, A7] | Mẫu Zeek 360 dòng xuất phát từ PCAP tổng hợp để minh họa đa dạng schema/định dạng; không trộn với kết quả benchmark CICIDS2017 thật |
| **Veracity** | Data Contract v2.3.0 (22 trường lõi + 12 metadata/cờ); đối soát **2.830.743 = 2.830.628 `VALID` (99,9959%) + 115 `QUARANTINED` (0,0041%) + 0 `REJECTED`**; **1.444.702** dòng `VALID` (51,0361%) có ≥1 lỗi mềm; 3 lượt full-data hậu kiểm `PASS` & 5/5 read-back `PASS`; audit MinIO 31 batch / 155 outputs `COMPLETE` [A1–A4] | `VALID` là trạng thái đạt chất lượng kỹ thuật, KHÔNG đồng nghĩa `BENIGN`; kiểm toán kỹ thuật không thay thế xác minh ground truth từ PCAP |
| **Velocity** | Batch Spark 3.5.0 + MinIO S3A trên 1 máy Docker: 8 CSV đầy đủ đạt trung vị **468,713 s** (423,064–481,487 s), throughput trung vị **6.039,392 flow/s**; Friday Morning 1 vs 2 worker = **51,338 s** vs **57,293 s** (speedup **0,896x**); `p=4` thấp nhất (**48,729 s**) trong 4/8/16/32 [A2, A4] | Hệ thống là batch processing, không phải streaming/thời gian thực; không suy rộng phép đo 58,32 MB thành quy luật tối ưu tổng quát |
| **Value** | Đầu ra Parquet Snappy chuẩn hóa (`cleaned/flows/` 34 cột, `processed/derived_features/` 38 cột, `processed/aggregates/` 46 dòng / 13 cột) giúp giữ **99,9959%** flow (so với **49,0362%** nếu `dropna()`), minh bạch rủi ro nhãn/sentinel/trùng lặp và cung cấp căn cứ tối ưu vận hành từ benchmark [A3–A6] | Giá trị đã chứng minh thuộc tầng dữ liệu sạch, tổng hợp khảo sát và quản trị vận hành batch; giá trị dự đoán bất thường bằng ML thuộc phần mở rộng tiếp theo |

**Bảng Q.10. Tóm tắt kết quả đối soát chéo tự động giữa các artifact 5V.**

| Hạng mục đối soát tự động (`build_value_report.py`) | Trạng thái | Kết quả xác minh |
| --- | --- | --- |
| SHA-256 & kích thước byte các artifact benchmark (`artifact-sha256.csv`) | PASS | 14/14 artifact JSON/CSV khớp checksum (xử lý LF/CRLF) |
| Schema & Data Contract v2.3.0 so với `source-audit.json` | PASS | 79 cột raw → 22 trường lõi (6 int, 7 long, 8 double, 1 string) |
| Phương trình bảo toàn bản ghi & 3 lượt full-data Spark | PASS | 2.830.743 = 2.830.628 + 115 + 0; 3/3 lượt hậu kiểm PASS, 5/5 Parquet read-back PASS |
| MinIO audit lịch sử & `verification.json` | PASS | 31 batch (27 đo + 4 warmup) `COMPLETE`; 155 Parquet outputs |
| Đối soát `summary.csv` vs `runs.csv` (Velocity) | PASS | 9 cấu hình, 27 lượt đo + 4 warmup khớp trung vị, min–max, throughput và speedup |
| Đối soát tỷ lệ giữ dữ liệu (`VALID` + cờ vs `dropna()`) | PASS | 2.830.628 = 1.442.539 (có NULL hợp lệ) + 1.388.089 (complete-case) |
| Mẫu demo Variety Zeek JSONL trên đĩa | PASS | 360 dòng hợp lệ (180 TCP / 120 UDP / 60 ICMP; 200 dòng thiếu `service`) |
| Xác minh trực tiếp mới trên MinIO (`--value-snapshot`) | CHƯA CHẠY | Tổng hợp từ hồ sơ lịch sử và truy vấn Parquet cục bộ |

## 6. Tái lập báo cáo và tài liệu minh chứng

### 6.1. Lệnh chạy tái lập

Tạo lại báo cáo Markdown và file đối soát `docs/reports/value-verification.json` hoàn toàn offline (không cần bật Docker/Spark/MinIO; thêm `--docx` nếu môi trường đã cài `python-docx`):

```powershell
python -m experiments.build_value_report
python -m experiments.build_value_report --docx
```

Từ thư mục gốc repo, dùng PowerShell khi Docker Spark–MinIO đang chạy và batch CICIDS2017 đã có `_SUCCESS.json` với `run_status` là `COMPLETE`. Spark ghi snapshot vào `/tmp` trong container. `docker cp` sao chép JSON về máy để trình sinh báo cáo đọc được.

```powershell
$batchId = Read-Host 'Nhập batch ID đã COMPLETE'
$composeArgs = @('--env-file', 'docker/.env', '-f', 'docker/docker-compose.yml')
$fileName = 'value-minio-' + [guid]::NewGuid().ToString('N') + '.json'
$containerSnapshot = "/tmp/$fileName"
$localDir = 'docs/evidence/local'
$localSnapshot = Join-Path $localDir $fileName

docker compose @composeArgs exec -T spark-master `
  python3 -m experiments.verify_value_outputs --batch-id $batchId --output $containerSnapshot
if ($LASTEXITCODE -ne 0) { throw 'Chụp snapshot thất bại' }
New-Item -ItemType Directory -Path $localDir -Force | Out-Null
docker cp "spark-master:$containerSnapshot" $localSnapshot
if ($LASTEXITCODE -ne 0) { throw 'Sao chép snapshot thất bại' }
python -m experiments.build_value_report --value-snapshot $localSnapshot --docx
```

Với batch đã ghi bằng `--output-prefix`, truyền cùng prefix vào lệnh `verify_value_outputs`. Ví dụ, batch benchmark dữ liệu thật dùng `--output-prefix 's3a://bigdata-network/benchmarks/real-20261110-141211'`.

Chạy bộ kiểm thử tự động xác minh logic đối soát 5V, phát hiện sai lệch và kiểm tra đầu ra:

```powershell
python -m unittest tests/test_value_report.py -v
```

### 6.2. Danh mục tài liệu minh chứng

- **[A1]** [Source audit 8 CSV](../benchmarks/real-20261110-141211/source-audit.json); [danh sách file & SHA-256](../benchmarks/real-20261110-141211/source-files.csv); [tính toàn vẹn sau benchmark](../benchmarks/real-20261110-141211/source-integrity-after.json).
- **[A2]** [Bảng tổng hợp Velocity](../benchmarks/real-20261110-141211/summary.csv); [31 lượt đo & stage timings](../benchmarks/real-20261110-141211/runs.csv); JSON gốc: [size](../benchmarks/real-20261110-141211/size.json), [partitions](../benchmarks/real-20261110-141211/partitions.json), [worker=1](../benchmarks/real-20261110-141211/workers-1.json), [worker=2](../benchmarks/real-20261110-141211/workers-2.json).
- **[A3]** [Audit MinIO lịch sử (31 batch / 155 outputs)](../benchmarks/real-20261110-141211/minio-audit.json); [kiểm tra nhất quán](../benchmarks/real-20261110-141211/verification.json); [SHA-256 artifact](../benchmarks/real-20261110-141211/artifact-sha256.csv); [before/after Friday Morning](../benchmarks/real-20261110-141211/quality-example.json).
- **[A4]** [Báo cáo Velocity](velocity-report.md); [báo cáo Veracity](veracity-report.md); [hồ sơ benchmark thực tế](../benchmarks/real-20261110-141211/README.md).
- **[A5]** [Kiểm toán chất lượng & retention `dropna()`](cicids2017-data-quality-audit.md); [Ghi chú 5V của nhóm](../explanations/5v-big-data-note.md).
- **[A6]** [Kịch bản bảo vệ & truy vấn demo Value](../defense/defense-demo-script.md).
- **[A7]** [Schema raw](../../data/schemas/raw_flow_schema.json); [Data Contract v2.3.0](../../data/schemas/feature_dictionary.md); [derived features & aggregates](../../processing/features.py); [pipeline & marker cuối](../../processing/main.py); [Zeek JSONL ingestion](../../processing/zeek_ingestion.py); [mẫu Zeek 360 dòng](../../data/sample/zeek/conn.demo.jsonl).

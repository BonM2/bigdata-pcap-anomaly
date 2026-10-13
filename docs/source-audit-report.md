# Báo cáo kiểm tra CICIDS2017 trước upload

Thời điểm audit: 12/10/2026, 23:23 (UTC+07:00).

## Kết luận

Đủ 8 CSV, cấu trúc phù hợp để upload nguyên trạng vào raw. Dữ liệu có các bất
thường giá trị nêu dưới đây và cần áp dụng chính sách QA khi xử lý. Chưa upload
hay chạy cleaning trên 8 CSV thật; không chỉnh sửa/xóa nguồn, không thay Data
Contract, không đưa CSV lớn vào Git.

Đã hoàn thiện config, transform, validation, clean; 10 kiểm thử Spark và 2 kiểm
thử audit bổ sung thành công. SHA-256 của cả 8 CSV được kiểm tra lại khi chạy
upload DRY_RUN và khớp audit.

## File và số bản ghi thực tế

Đếm bằng quét tuần tự nội dung CSV, loại header, không dùng số liệu baseline để
điền kết quả. Mỗi dòng vật lý là một record trong bộ nguồn này.

| File | Bytes | Records | Malformed | Dự kiến quarantine theo 22 trường lõi |
|---|---:|---:|---:|---:|
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | 77.123.859 | 225.745 | 0 | 2 |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | 76.906.168 | 286.467 | 0 | 36 |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | 58.316.725 | 191.033 | 0 | 9 |
| Monday-WorkingHours.pcap_ISCX.csv | 176.927.918 | 529.918 | 0 | 15 |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | 83.102.436 | 288.602 | 0 | 4 |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | 52.023.263 | 170.366 | 0 | 11 |
| Tuesday-WorkingHours.pcap_ISCX.csv | 135.078.995 | 445.909 | 0 | 17 |
| Wednesday-workingHours.pcap_ISCX.csv | 225.166.395 | 692.703 | 0 | 21 |
| **Tổng** | **884.645.759** | **2.830.743** | **0** | **115** |

Tất cả số record từng file và tổng đều khớp baseline trong Data Contract.
Dung lượng tổng: 884.645.759 byte (~843,66 MiB).

## Đối chiếu schema và kiểu dữ liệu

- Cả 8 header đều có 79 cột và khớp chính xác từng vị trí với
  `source_header_exact`, kể cả khoảng trắng. Tất cả dòng dữ liệu có 79 trường;
  không phát hiện lỗi cú pháp CSV.
- `Fwd Header Length` lặp ở cột 35 và 56 (index 34 và 55). Giá trị hai vị trí
  giống nhau trên toàn bộ 2.830.743 dòng đã kiểm tra; vẫn giữ hai alias riêng
  theo contract.
- JSON và feature dictionary thống nhất đủ 22 trường: 6 Integer, 7 Long,
  8 Double, 1 String. Raw vẫn là String; 57 trường ngoài lõi không bị tự gán
  kiểu cleaned.
- Không phát hiện lỗi cú pháp ép kiểu hay tràn Integer/Long ở 21 feature lõi.
  Không phát hiện cổng ngoài 0–65535, TCP flag ngoài 0/1 hoặc nhãn rỗng.
- Có NaN/Infinity và số âm: khả năng parse numeric không đồng nghĩa đạt điều
  kiện chất lượng.

## Bất thường trong trường lõi

| Trường | Hiện tượng | Số giá trị | Chính sách contract |
|---|---|---:|---|
| flow_duration | Âm | 115 | QUARANTINED |
| flow_iat_mean | Âm | 115 | QUARANTINED |
| flow_bytes_s | NaN | 1.358 | NULL + invalid flag |
| flow_bytes_s | +Infinity | 1.509 | NULL + invalid flag |
| flow_bytes_s | Âm hữu hạn | 85 | NULL + invalid flag |
| flow_packets_s | +Infinity | 2.867 | NULL + invalid flag |
| flow_packets_s | Âm hữu hạn | 115 | NULL + invalid flag |
| init_win_bytes_forward | -1 | 1.001.189 | NULL + sentinel flag |
| init_win_bytes_backward | -1 | 1.441.552 | NULL + sentinel flag |
| label | Chứa U+FFFD (�) | 2.180 | Giữ nguyên + encoding flag |

Tất cả giá trị âm ở hai initial-window đều là -1; không gặp âm khác sentinel.
1.358 trường hợp rate thiếu/NaN của baseline thực tế đều là token NaN; không
gặp ô rỗng ở feature lõi. Số liệu theo cột có thể trùng trên cùng record, không
cộng thành tổng record lỗi.

2.180 nhãn chứa ký tự thay thế đều thuộc Thursday WebAttacks: Brute Force
1.507, XSS 652, Sql Injection 21. Không gặp dòng lỗi giải mã UTF-8: U+FFFD đã
hiện diện trong chuỗi nguồn được mã hóa UTF-8, không phải ký tự mới sinh bởi
trình audit. Không tự sửa nhãn hoặc suy đoán dấu gạch nối ban đầu.

## Bất thường ngoài 22 trường lõi

| Trường | Giá trị âm |
|---|---:|
| flow_iat_max | 115 |
| flow_iat_min | 2.891 |
| fwd_iat_min | 17 |
| fwd_header_length | 35 |
| bwd_header_length | 22 |
| fwd_header_length_repeat_2 | 35 |
| min_seg_size_forward | 35 |

Các trường này được kiểm toán và giữ trong raw; contract hiện không đưa chúng
vào cleaned core. Báo cáo không tự bổ sung luật loại bỏ toàn row vì các trường
ngoài lõi.

## Dự kiến phân loại khi áp dụng luật hiện tại

- VALID: 2.830.628; trong đó 1.444.702 có ít nhất một vấn đề mềm.
- QUARANTINED: 115; REJECTED: 0.
- Đối soát: 2.830.743 = 2.830.628 + 115 + 0.
- Đây là kết quả xem trước chính sách trong audit chỉ đọc, **chưa phải output
  của Spark cleaning trên dữ liệu thật**.

## MinIO và Spark đã kiểm chứng

- Docker: MinIO healthy, Spark master và 2 worker đang chạy. Image cũ không
  tải được nên đã build Spark từ `apache/spark:3.5.0` cùng S3A, và MinIO từ
  release upstream `RELEASE.2025-10-15T17-29-55Z`.
- Spark application: `app-20261012161858-0000`; Spark `3.5.0`; S3A smoke job
  **PASS**.
- Fixture tổng hợp tại
  `s3a://bigdata-network/smoke/cicids2017/synthetic_flow.csv`: 79 cột raw,
  22 cột lõi + 12 metadata; 12 records = 4 VALID + 6 QUARANTINED + 2 REJECTED.
- Prefix `raw/cicids2017/` được kiểm tra chỉ đọc và chưa có object tại thời
  điểm báo cáo.
- Bộ test Spark cũng xác minh ghi/đọc lại Parquet Snappy bằng file tạm. Chưa
  ghi cleaned Parquet trên MinIO cho dữ liệu thật.

## Kế hoạch upload đã chuẩn bị, chưa thực thi

Đích: `s3a://bigdata-network/raw/cicids2017/<tên-file-gốc>`. Mặc định
`python -m storage.upload_minio` chỉ DRY_RUN. Đã chạy DRY_RUN thành công cho
đủ 8 file; kiểm tra lại kích thước và SHA-256 đều khớp. Sau khi xem báo cáo
và quyết định tiếp tục, mới chạy:

```powershell
docker compose -f docker/docker-compose.yml exec spark-master python3 -m storage.upload_minio --execute
```

Không ghi đè object raw khác nội dung. Chỉ chạy một uploader tại một thời điểm.
Kiểm tra `processing.check_minio` trên raw sẽ là bước sau upload, chưa thực
hiện trong báo cáo này.

## SHA-256 nguồn

| File | SHA-256 |
|---|---|
| Friday-WorkingHours-Afternoon-DDos.pcap_ISCX.csv | `6ff1580f5f81c0ae28a26f7631721018577f5f7c5e0feac28b795fcfe7b411ee` |
| Friday-WorkingHours-Afternoon-PortScan.pcap_ISCX.csv | `ca1824c51bfbb7b3c72290a11be04366ba8815878c6a1cc5c44cb1cee269e99b` |
| Friday-WorkingHours-Morning.pcap_ISCX.csv | `53a41c24d570ea83b7ac55b2e94df94e7a8216aeb80a2af0246b6bc8bb543000` |
| Monday-WorkingHours.pcap_ISCX.csv | `852c4beb34eda186f32561fa79df7a0747e92e1a6535b01270820dd9ffe17f34` |
| Thursday-WorkingHours-Afternoon-Infilteration.pcap_ISCX.csv | `6bcda3857c2504676034e3ea57762d38393cc734cb377a726bd5cb153961b1b5` |
| Thursday-WorkingHours-Morning-WebAttacks.pcap_ISCX.csv | `d67066211fb1689c78406f1506f4c44704ecb92088353d5c96d96d6474eb819d` |
| Tuesday-WorkingHours.pcap_ISCX.csv | `52b8692ae8c7d2ed04671fe2b98335693c0a92c7ab157d8c8b534d6523080851` |
| Wednesday-workingHours.pcap_ISCX.csv | `893c27dc968bf7a8adef1689f90be55ca4a4dc3088fb63d6ff247ac56856df2a` |

## Bằng chứng và tái lập

- [Hướng dẫn processing](processing.md).
- [Audit chi tiết theo file/cột, min/max, nhãn và hash (local)](source-audit.local.json).
- [Spark MinIO smoke report (local)](minio-smoke.local.json).
- JSON local và CSV thật đều được gitignore; báo cáo Markdown này không chứa
  credentials hay nội dung dataset lớn.

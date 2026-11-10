# Benchmark Velocity — scaling-20261110-121252

Ngày chạy: 10/11/2026, múi giờ Asia/Bangkok.

## Phạm vi

- Apache Spark 3.5.0, Python 3.8.10, Java 11.0.20.1, MinIO/S3A.
- Docker Desktop Linux/WSL2 thấy 6 logical CPUs và 10.406.912.000 byte RAM.
- Hai worker, mỗi worker 2 cores/2 GiB; executor 2 cores/2g.
- `DISK_ONLY`, cache batch 1000, AQE `false`.
- Timer gồm đọc CSV qua S3A, checksum, validation, cleaning, quality report,
  features, aggregation, ghi 5 Parquet Snappy và đọc lại xác minh. Timer không
  gồm Spark startup, seed input hoặc xuất CSV/PNG cuối.
- Một warmup cho mỗi invocation và ba trial tính điểm cho mỗi cấu hình. Bảng dùng
  median của từng trial; min–max giữ nguyên độ biến thiên quan sát được.

## Dữ liệu

Benchmark dùng **synthetic scaling**, nhân bản fixture 12 dòng:

| Workload | Byte | SHA-256 |
|---:|---:|---|
| 12.000 flow | 2.035.373 | `aa2c730b3cb0c1be90d862b388e02a3be9edaf2c8ba4d3f30d22439cbcf5d04a` |
| 60.000 flow | 10.171.373 | `6e88af4f50af88e74232c327778d0078da9ebaaa7ef7a8cc984c420e0ec3c35d` |
| 120.000 flow | 20.341.373 | `c6cc0d71de23c8bb2a747f35fe5032daf11d7a3f4cc3b94aa10ccd7a9b6d61aa` |

Năm CSV WTMC/Engelen thật ở root repository không được dùng: chúng có 84 cột,
trong khi contract pipeline hiện tại yêu cầu 79 cột.

## Kết quả đo thực tế

| Dimension | Cấu hình | n | Median (s) | Min–max (s) | Median flow/s | Speedup |
|---|---:|---:|---:|---:|---:|---:|
| Size | 12.000 flow | 3 | 18,126 | 16,589–31,107 | 662,02 | N/A |
| Size | 60.000 flow | 3 | 31,721 | 25,199–40,311 | 1.891,46 | N/A |
| Size | 120.000 flow | 3 | 39,154 | 36,849–41,498 | 3.064,84 | N/A |
| Workers | 1 | 3 | 32,348 | 30,688–34,665 | 3.709,64 | 1,000x |
| Workers | 2 | 3 | 43,708 | 41,933–50,009 | 2.745,48 | 0,740x |
| Partitions | 4 | 3 | 34,174 | 33,076–51,187 | 3.511,44 | 1,000x |
| Partitions | 8 | 3 | 37,815 | 28,511–40,947 | 3.173,35 | 0,904x |
| Partitions | 16 | 3 | 39,088 | 38,718–39,371 | 3.070,03 | 0,874x |
| Partitions | 32 | 3 | 43,007 | 41,574–47,979 | 2.790,25 | 0,795x |

Warmup size/partition/worker-1/worker-2 lần lượt là 54,032 / 85,424 / 61,739 /
79,472 giây và không được đưa vào bảng.

## Kiểm chứng

- Bốn benchmark JSON: `COMPLETE`.
- 27 trial tính điểm + 4 warmup = 31 batch.
- Audit MinIO: `PASS`, 31 batch và 155 output Parquet có bằng chứng read-back.
- Helper tests: 7/7 `OK`.
- Container tests: 36/36 `OK`.
- Sau benchmark: 2 worker `ALIVE`, không có Spark application đang chạy.

## Diễn giải và giới hạn

- Thời gian tăng khi workload tăng, nhưng throughput cũng tăng do overhead cố định
  chiếm tỷ trọng lớn ở workload nhỏ. Đây không phải bằng chứng scaling tuyến tính.
- Trên workload 120k nhỏ và có hai input partitions, hai worker chậm hơn một worker;
  overhead điều phối/I/O và tài nguyên dùng chung là các khả năng, chưa tách nguyên nhân.
- Trong phép đo partition này, p=4 có median thấp nhất. Kết quả không chứng minh p=4
  tối ưu cho dữ liệu thật hoặc workload lớn hơn.
- Synthetic có độ lặp cao; cache, JIT và nén ảnh hưởng rõ. Không dùng throughput này
  để khẳng định hiệu năng trên PCAP/CICIDS2017/WTMC thật hoặc streaming thời gian thực.

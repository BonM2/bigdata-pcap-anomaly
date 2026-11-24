# Ghi chú 5V của đồ án Big Data

Đồ án dùng Apache Spark và MinIO để xử lý dữ liệu network flow từ CSV CICIDS2017 và log kết nối Zeek dạng JSON Lines.

| Đặc tính | Ý nghĩa | Thể hiện trong đồ án |
|---|---|---|
| **Volume (Khối lượng)** | Khối lượng dữ liệu cần lưu trữ và xử lý | Bộ dữ liệu thật có **2.830.743 network flows**, gồm 8 CSV với tổng dung lượng **884.645.759 byte** (khoảng 843,66 MiB). Spark chia dữ liệu thành các partition để xử lý. |
| **Velocity (Tốc độ)** | Tốc độ dữ liệu được tạo ra hoặc được xử lý | Đồ án đo thời gian xử lý và throughput (flows/giây). Pipeline hiện chạy **batch**, không dùng Kafka, `readStream` hoặc `writeStream`, nên chưa phải hệ thống thời gian thực. |
| **Variety (Đa dạng)** | Sự đa dạng về cấu trúc, kiểu và nguồn dữ liệu | Spark đọc **CSV CICIDS2017** và **JSON Lines của Zeek** từ MinIO. CSV có 79 cột nguồn, gồm đặc trưng flow và nhãn. Log Zeek chứa thời điểm, IP, cổng, giao thức và các trường tùy chọn như `service`. Hai luồng xử lý dùng schema riêng và ghi Parquet Snappy. Demo Zeek đọc lại đủ **360/360 dòng** từ Parquet. [Hướng dẫn demo Zeek](../processing.md#zeek-json) |
| **Veracity (Độ tin cậy)** | Mức chính xác, nhất quán và chất lượng của dữ liệu | Dữ liệu có thể chứa NULL, NaN, Infinity, số âm, sentinel `-1`, header trùng và nhãn lỗi encoding. Pipeline kiểm tra và phân loại từng dòng thành `VALID`, `QUARANTINED` hoặc `REJECTED`, sau đó sinh báo cáo chất lượng. |
| **Value (Giá trị)** | Giá trị thu được sau khi xử lý dữ liệu | Bảng tổng hợp Parquet cho phép truy vấn số flow, tổng packet và tổng byte theo nhãn hoặc file nguồn. Demo đọc batch thật và xếp hạng nhãn theo tổng byte của các flow `VALID`. [Truy vấn demo Value](../defense/defense-demo-script.md#value-top-labels) |

## Câu trả lời ngắn khi bảo vệ

> Nhóm đã xử lý hơn 2,8 triệu flow CICIDS2017, đo throughput của pipeline batch và đối soát các nhóm chất lượng sau làm sạch. Variety thể hiện qua hai luồng CSV CICIDS2017 và JSON Lines của Zeek. Với Value, bảng tổng hợp Parquet cho biết nhãn nào có tổng byte cao nhất trong batch thật.

## Lưu ý khi trình bày

- Không gọi đây là hệ thống streaming hoặc thời gian thực.
- Không nói dữ liệu đạt quy mô TB; bộ thật hiện khoảng 0,885 GB thập phân.
- `VALID` là trạng thái chất lượng dữ liệu, không có nghĩa flow đó an toàn.
- Mẫu Zeek 360 dòng được tạo từ PCAP tổng hợp. Báo cáo benchmark và truy vấn Value sử dụng batch CICIDS2017 thật.

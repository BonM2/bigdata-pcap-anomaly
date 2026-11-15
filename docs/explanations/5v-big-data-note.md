# Ghi chú 5V của đồ án Big Data

Đồ án xử lý dữ liệu network flow CICIDS2017 bằng Apache Spark và lưu trữ trên MinIO.

| Đặc tính | Ý nghĩa | Thể hiện trong đồ án |
|---|---|---|
| **Volume (Khối lượng)** | Khối lượng dữ liệu cần lưu trữ và xử lý | Bộ dữ liệu thật có **2.830.743 network flows**, gồm 8 CSV với tổng dung lượng **884.645.759 byte** (khoảng 843,66 MiB). Spark chia dữ liệu thành các partition để xử lý. |
| **Velocity (Tốc độ)** | Tốc độ dữ liệu được tạo ra hoặc được xử lý | Đồ án đo thời gian xử lý và throughput (flows/giây). Pipeline hiện chạy **batch**, không dùng Kafka, `readStream` hoặc `writeStream`, nên chưa phải hệ thống thời gian thực. |
| **Variety (Đa dạng)** | Sự đa dạng về cấu trúc, kiểu và nguồn dữ liệu | Mỗi flow có 79 trường nguồn, gồm cổng mạng, packet, byte, thời lượng, rate, TCP flags và nhãn tấn công. Tuy nhiên 8 file đều là CSV cùng schema, nên độ đa dạng định dạng chưa cao. |
| **Veracity (Độ tin cậy)** | Mức chính xác, nhất quán và chất lượng của dữ liệu | Dữ liệu có thể chứa NULL, NaN, Infinity, số âm, sentinel `-1`, header trùng và nhãn lỗi encoding. Pipeline kiểm tra và phân loại từng dòng thành `VALID`, `QUARANTINED` hoặc `REJECTED`, sau đó sinh báo cáo chất lượng. |
| **Value (Giá trị)** | Giá trị thu được sau khi xử lý dữ liệu | Pipeline chuẩn hóa, làm sạch, tạo feature, tổng hợp và ghi Parquet + Snappy. Đầu ra có thể dùng cho thống kê lưu lượng, phân tích an ninh mạng và làm dữ liệu đầu vào cho mô hình phát hiện bất thường sau này. |

## Câu trả lời ngắn khi bảo vệ

> Đồ án thể hiện rõ nhất Volume và Veracity. Hệ thống xử lý hơn 2,8 triệu network flows và kiểm soát nhiều lỗi chất lượng dữ liệu bằng Spark. Velocity được đánh giá bằng throughput của pipeline batch; Variety đến từ nhiều trường dữ liệu mạng nhưng chưa đa dạng về định dạng; Value là tạo ra dữ liệu Parquet sạch, có thể truy vết và tiếp tục sử dụng cho phân tích an ninh mạng.

## Lưu ý khi trình bày

- Không gọi đây là hệ thống streaming hoặc thời gian thực.
- Không nói dữ liệu đạt quy mô TB; bộ thật hiện khoảng 0,885 GB thập phân.
- `VALID` là trạng thái chất lượng dữ liệu, không có nghĩa flow đó an toàn.
- Demo synthetic chỉ chứng minh pipeline hoạt động; kết quả synthetic không phải kết quả của CICIDS2017 thật.

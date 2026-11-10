# Đợt đo bị gián đoạn do Windows restart

Ngày 10/11/2026, Asia/Bangkok. Windows Event ID 1074 ghi nhận RuntimeBroker.exe
khởi tạo restart lúc 13:12:41, Event ID 6006 lúc 13:12:52; LastBootUpTime là
13:13:09. Chưa có bằng chứng lỗi Spark/OOM gây gián đoạn.

`interrupted-size.json` được copy nguyên trạng từ container sau khi khởi động lại:
trạng thái vẫn là `RUNNING`, chứa 1 warmup và 3 trial được hoàn tất trước restart.
Đây không phải benchmark COMPLETE và không được đưa vào bảng/biểu đồ hoàn thành.

Các lượt tính điểm đã hoàn tất:

| Input | Records | Seconds |
|---|---:|---:|
| Friday Morning | 191.033 | 55,646567439 |
| Ba file Friday | 703.245 | 129,890345649 |
| Toàn bộ 8 CSV | 2.830.743 | 439,078512202 |

Lượt toàn bộ dataset đã ghi và đọc lại 5 Parquet Snappy, hậu kiểm PASS:
2.830.743 = 2.830.628 VALID + 115 QUARANTINED + 0 REJECTED.
Lượt thứ hai đang chạy khi Windows restart nên không có timing hoàn chỉnh.

Audit nguồn và upload đã hoàn tất trước restart. Dữ liệu raw gốc và output batch
đã hoàn thành được giữ nguyên; batch đang chạy có thể còn output dở dang.
Không xóa hoặc coi batch dở dang là thành công.

Đợt thay thế `real-20261110-141211` dùng experiment ID/output prefix mới, warmup mới
và ba lần lặp mới. Các số đo giữa hai đợt không được gộp để đủ số lần lặp.

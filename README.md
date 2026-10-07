```text
bigdata-pcap-pipeline/
├── .gitignore                      # Chặn các file rác, file PCAP nặng và dữ liệu tạm
├── README.md                       # Hướng dẫn kiến trúc, cài đặt và chạy từ A-Z
├── requirements.txt                # Thư viện Python (pyspark, nfstream, boto3, pandas...)
│
├── docker/                         # Hạ tầng container hóa (TV3)
│   ├── docker-compose.yml          # Cấu hình cụm MinIO + Spark Master + 2 Workers
│   └── spark/
│       └── Dockerfile              # (Tùy chọn) Nếu cần cài thêm thư viện/JAR vào worker
│
├── data/                           # Dữ liệu mẫu và định nghĩa dữ liệu
│   ├── sample/                     # Chỉ chứa file mẫu cực nhỏ (< 5-10MB) để test nhanh
│   │   ├── sample.pcap
│   │   └── sample_flow.csv
│   └── schemas/                    # Định nghĩa cấu trúc dữ liệu
│       ├── raw_flow_schema.json
│       └── feature_dictionary.md   # Bảng Feature Dictionary v0.1 (TV1 quản lý)
│
├── extraction/                     # Trích xuất gói tin PCAP -> Flow (TV2)
│   ├── extract_flow.py             # Script tự động bóc tách PCAP bằng Zeek/TShark/NFStream
│   └── utils.py                    # Các hàm bổ trợ đọc/ghi file
│
├── storage/                        # Tương tác với MinIO / Data Lake (TV3)
│   ├── minio_client.py             # Khởi tạo kết nối S3/MinIO
│   └── upload_minio.py             # Tự động đẩy file thô lên bucket s3a://data-lake/raw/
│
├── processing/                     # Pipeline xử lý PySpark lõi (TV4 + TV1)
│   ├── __init__.py
│   ├── main.py                     # Entry point chạy toàn bộ pipeline
│   ├── config.py                   # Cấu hình kết nối Spark - MinIO (S3A keys, endpoint)
│   ├── clean.py                    # Lọc null, bản ghi dị biệt, loại trùng lặp
│   ├── transform.py                # Ép kiểu dữ liệu, tính toán đặc trưng cơ bản
│   └── features.py                 # Tính Window aggregation, cờ TCP, tỷ lệ an ninh
│
├── experiments/                    # Mã nguồn chạy thực nghiệm & đo đạc (TV4)
│   ├── benchmark_size.py           # Đo thời gian khi tăng dung lượng (1GB, 3GB, 5GB)
│   ├── benchmark_workers.py        # Đo khả năng scale (1 Worker vs 2 Workers)
│   ├── benchmark_partitions.py     # Đo hiệu năng phân vùng (4, 8, 16, 32 partitions)
│   └── plot_results.py             # Vẽ biểu đồ kết quả thực nghiệm ra ảnh
│
└── docs/                           # Tài liệu phục vụ báo cáo cuối kỳ
    ├── architecture.png            # Sơ đồ kiến trúc hệ thống
    ├── data_flow.png               # Sơ đồ luồng dữ liệu
    └── report_draft.md             # Bản nháp nội dung báo cáo
```
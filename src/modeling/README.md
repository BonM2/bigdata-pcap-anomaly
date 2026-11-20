# Network Anomaly Detection Modeling & Quality-Aware Evaluation

Pipeline huấn luyện **Baseline Random Forest (PySpark MLlib)** và đánh giá cơ chế **Quality-Aware (`REVIEW`)** trên dữ liệu sạch `VALID` của **CICIDS2017** (Data Contract `v2.3.0`, Parquet + Snappy trên MinIO).

## Cấu trúc mã nguồn

- `src/modeling/config.py`: Schema 21 đặc trưng số lõi, danh sách cờ chất lượng (`flow_bytes_s_invalid`, `flow_packets_s_invalid`, `init_win_fwd_missing`, `init_win_bwd_missing`), siêu tham số Random Forest và cấu hình Spark/MinIO.
- `src/modeling/data_prep.py`: Xác minh marker `_SUCCESS.json` (`COMPLETE`), kiểm tra toàn vẹn `VALID` Parquet, loại trùng (`.dropDuplicates`) trước chia tập, gán nhãn nhị phân (`ATTACK = 1.0`, `BENIGN = 0.0`), chia phân tầng xác định `70% / 15% / 15%` (`seed=42`) kèm kiểm tra giao nhau = 0.
- `src/modeling/metrics.py`: Tính Confusion Matrix (`TP/FP/FN/TN`), `Precision`, `Recall`, `F1` (ATTACK), `Macro-F1`, `Accuracy`, `FPR`, 3 chỉ số vận hành Quality-Aware (`Coverage`, `Review rate`, `Attack to Review rate`) và chọn ngưỡng bất định trên Validation.
- `src/modeling/report.py`: Trích xuất mẫu thực tế từ Test (`False Positive`, `False Negative`, `REVIEW`) ra CSV/JSON và sinh báo cáo tổng hợp Markdown/JSON.
- `src/modeling/pipeline.py`: CLI điều phối toàn bộ quy trình huấn luyện, chọn ngưỡng và xuất báo cáo.
- `src/modeling/tests/test_modeling.py`: Unit tests & Spark integration tests.

## Yêu cầu môi trường trong Docker Spark 3.5.0

Image `bigdata-spark:3.5.0` (Python 3.8) mặc định chưa cài `numpy` (thư viện bắt buộc cho `pyspark.ml`). Cài bổ sung trong container trước khi chạy:

```powershell
foreach ($svc in @("spark-master", "spark-worker-1", "spark-worker-2")) {
  docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T -u root $svc python3 -m pip install --no-cache-dir numpy==1.24.4
}
```

## Lệnh chạy kiểm thử & thực nghiệm

```powershell
# 1. Chạy unit & Spark integration tests
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master python3 -m unittest src.modeling.tests.test_modeling -v

# 2. Chạy pipeline huấn luyện và đánh giá trên Parquet VALID thật từ MinIO
docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T -e SPARK_EXECUTOR_MEMORY=2g -e SPARK_STORAGE_LEVEL=DISK_ONLY spark-master python3 -m src.modeling.pipeline `
  --cleaned-path s3a://bigdata-network/cleaned/flows/cicids2017_valid_v1 `
  --quality-report-path s3a://bigdata-network/reports/data_quality/cicids2017_valid_v1 `
  --output-dir /tmp/modeling_reports

# 3. Chép kết quả ra thư mục reports/modeling/ trên host (vì /workspace mount read-only)
New-Item -ItemType Directory -Force reports/modeling | Out-Null
docker cp spark-master:/tmp/modeling_reports/. reports/modeling/
```

## Đầu ra (`reports/modeling/`)

- `baseline_config.json`: Toàn bộ cấu hình tái lập (`feature_columns`, `seed`, hyperparameters, `label_mapping`, median Imputer học từ Train).
- `evaluation_summary.json`: Toàn bộ số liệu định lượng, bảng sweep Validation, so sánh Baseline vs Quality-Aware trên Test, phân tích độ nhạy 4 cờ, phân tích theo từng lớp tấn công, thời gian thực thi.
- `evaluation_report.md`: Báo cáo Markdown hoàn chỉnh.
- `validation_threshold_sweep.csv`: Bảng kết quả thử nghiệm các khoảng ngưỡng trên tập Validation.
- `test_examples.csv` và `test_examples.json`: Các ví dụ mẫu thực tế từ tập Test cho `FALSE_POSITIVE`, `FALSE_NEGATIVE`, và `REVIEW`.

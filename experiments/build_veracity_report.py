"""Build Veracity deliverables from committed evidence; never connect to MinIO.

Run: python -m experiments.build_veracity_report [--docx]
Chart dependency: matplotlib. Optional Word export: python-docx.
Only docs/reports/veracity-report.* and veracity-quality.png are written.
"""

import argparse
from collections import Counter
import csv
import hashlib
import json
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/benchmarks/real-20261110-141211"
REPORT = ROOT / "docs/reports/veracity-report.md"
COUNTS = ("input_records", "valid_records", "quarantined_records", "rejected_records")
ISSUES = (
    ("flow_bytes_s", "nan", "NaN", "NULL + flow_bytes_s_invalid"),
    ("flow_bytes_s", "positive_infinity", "+Infinity", "NULL + flow_bytes_s_invalid"),
    ("flow_bytes_s", "negative", "Âm", "NULL + flow_bytes_s_invalid"),
    ("flow_packets_s", "positive_infinity", "+Infinity", "NULL + flow_packets_s_invalid"),
    ("flow_packets_s", "negative", "Âm", "NULL + flow_packets_s_invalid"),
    ("flow_duration", "negative", "Âm", "QUARANTINED"),
    ("flow_iat_mean", "negative", "Âm", "QUARANTINED"),
    ("init_win_bytes_forward", "sentinel_minus_one", "Sentinel -1", "NULL + init_win_fwd_missing"),
    ("init_win_bytes_backward", "sentinel_minus_one", "Sentinel -1", "NULL + init_win_bwd_missing"),
    ("label", "encoding_replacement", "Ký tự U+FFFD", "Giữ nhãn raw + reason code"),
)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def historical_bytes(path, expected_hash):
    """Account for Git's text EOL conversion, without changing source artifacts."""
    payload = path.read_bytes()
    lf = payload.replace(b"\r\n", b"\n")
    for candidate in (payload, lf, lf.replace(b"\n", b"\r\n")):
        if hashlib.sha256(candidate).hexdigest().lower() == expected_hash.lower():
            return candidate
    raise ValueError(f"Historical artifact checksum mismatch: {path.name}")


def number(value):
    return f"{value:,}".replace(",", ".")


def percent(value, total):
    return f"{100 * value / total:.4f}".replace(".", ",") + "%"


def table(headers, rows):
    def line(values):
        return "| " + " | ".join(str(v).replace("|", "\\|") for v in values) + " |"
    return "\n".join([line(headers), line(["---"] * len(headers)), *(line(row) for row in rows)])


def collect_evidence():
    """Fail on disagreement between audit, pipeline summaries and stored markers."""
    names = ("source-audit.json", "size.json", "minio-audit.json", "quality-example.json",
             "source-integrity-after.json", "verification.json")
    # Verify the selected historical artifacts against their original manifest.
    with (EVIDENCE / "artifact-sha256.csv").open(encoding="utf-8-sig", newline="") as stream:
        manifest = {row["File"]: row for row in csv.DictReader(stream)}
    for name in names:
        payload = historical_bytes(EVIDENCE / name, manifest[name]["SHA256"])
        require(len(payload) == int(manifest[name]["Bytes"]), f"Artifact size mismatch: {name}")
    source, size, audit, example, integrity, verification = [read_json(EVIDENCE / name) for name in names]
    require(size["run_status"] == "COMPLETE" and size["dataset_kind"] == "real", "Incomplete/non-real run")
    require(audit["status"] == integrity["status"] == verification["status"] == "PASS", "Failed historical audit")
    require(audit["batch_count"] == len(audit["batches"]) == 31 and audit["parquet_outputs"] == 155,
            "Historical audit batch/output totals disagree")
    schema_path = ROOT / "data/schemas/raw_flow_schema.json"
    contract_path = ROOT / "data/schemas/feature_dictionary.md"
    historical_bytes(schema_path, source["schema_sha256"])
    historical_bytes(contract_path, source["contract_sha256"])
    contract = read_json(schema_path)
    totals, labels, cells = Counter(), Counter(), Counter()
    files = source["files"]
    require(len(files) == 8 and len({f["source_file"] for f in files}) == 8, "Expected eight unique files")
    require(source["structurally_ready_for_upload"], "Source structure failed")
    for f in files:
        require(f["header_matches_exact"] and f["header_columns"] == 79, "Header mismatch")
        require(f["row_width_distribution"] == {"79": f["input_records"]}, "Wrong row width")
        require(f["malformed_records"] == 0 and f["utf8_decode_error_records"] == 0, "Malformed source")
        require(f["duplicate_headers"] == {"Fwd Header Length": [35, 56]}
                and f["duplicate_header_value_mismatches"] == 0, "Duplicate-header evidence changed")
        require(sum(f["label_distribution"].values()) == f["input_records"], "Label counts mismatch")
        totals.update({k: (f["input_records"] if k == "input_records" else f["policy_preview"][k]) for k in COUNTS})
        totals["valid_records_with_soft_issues"] += f["policy_preview"]["valid_records_with_soft_issues"]
        labels.update(f["label_distribution"])
        for field, stats in f["fields"].items():
            for kind, count in stats["counts"].items():
                cells[field, kind] += count
    require(totals["input_records"] == source["input_records"], "Input sum mismatch")
    require(sum(f["input_bytes"] for f in files) == source["input_bytes"], "Byte sum mismatch")
    require(totals["input_records"] == sum(totals[k] for k in COUNTS[1:]), "Record reconciliation failed")
    require({f["file"] for f in integrity["files"] if f["unchanged"]} == {f["source_file"] for f in files},
            "Historical source integrity mismatch")
    full_runs = [r for r in size["runs"] if r["quality"]["input_records"] == totals["input_records"]]
    require(len(full_runs) == 3, "Expected three full-data trials")
    stored = {r["batch_id"]: r for r in audit["batches"]}
    for r in full_runs:
        require(r["quality"] == {k: totals[k] for k in COUNTS}, "Spark and source policy disagree")
        require(r["post_cleaning_status"] == "PASS", "Pipeline post-validation failed")
        expected_sources = {f["source_file"]: f["source_sha256"] for f in files}
        require({f["source_file"]: f["source_sha256"] for f in r["files"]} == expected_sources,
                "Pipeline source checksums mismatch")
        marker = stored[r["batch_id"]]
        require(marker["stored_marker_status"] == "COMPLETE" and marker["quality"] == r["quality"],
                "Stored marker/quality mismatch")
        require(marker["post_cleaning_status"] == "PASS" and marker["parquet_outputs"] == 5,
                "Stored post-validation/output mismatch")
        expected_outputs = {"cleaned_path": totals["valid_records"], "features_path": totals["valid_records"],
                            "quarantine_path": totals["quarantined_records"], "rejected_path": totals["rejected_records"]}
        require(len(r["outputs"]) == 5, "Missing output")
        for key, output in r["outputs"].items():
            require(output["read_back_status"] == "PASS", "Output read-back failed")
            if key in expected_outputs:
                require(output["records"] == expected_outputs[key], "Output count mismatch")
    require(example["post_cleaning_validation"]["status"] == "PASS", "Example post-validation failed")
    require(example["post_cleaning_validation"]["records_with_errors"] == 0, "Example has post-clean errors")
    require(example["record_reconciliation"]["evidence_multiset_preserved"], "Example raw evidence changed")
    require(all(r["outputs"]["aggregates_path"]["records"] == 46 for r in full_runs), "Aggregate count changed")
    return source, totals, cells, labels, full_runs, example, contract


def write_chart(source, cells):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 10})
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), gridspec_kw={"width_ratios": [1.5, 1]})
    values = [cells[field, kind] for field, kind, _, _ in ISSUES]
    axes[0].barh(range(len(values)), values, color="#286b8c")
    axes[0].set_yticks(range(len(values)), [f"{field}: {kind}" for field, kind, _, _ in ISSUES])
    axes[0].invert_yaxis()
    axes[0].set_xscale("log")
    axes[0].set_xlim(50, max(values) * 12)
    axes[0].set_xlabel("Số ô / thang log; có thể chồng lấp theo dòng")
    axes[0].set_title("Lỗi và sentinel quan sát ở nguồn")
    for i, count in enumerate(values):
        axes[0].text(count * 1.08, i, number(count), va="center", fontsize=9)
    short = [f["source_file"].replace("-WorkingHours", "").replace("-workingHours", "").replace(".pcap_ISCX.csv", "")
             for f in source["files"]]
    quarantine = [f["policy_preview"]["quarantined_records"] for f in source["files"]]
    axes[1].barh(range(len(short)), quarantine, color="#a35c21")
    axes[1].set_yticks(range(len(short)), short, fontsize=9)
    axes[1].invert_yaxis()
    axes[1].set_xlim(0, max(quarantine) * 1.2)
    for i, count in enumerate(quarantine):
        axes[1].text(count + 0.5, i, str(count), va="center")
    axes[1].set_xlabel("Số dòng cách ly; tổng 115")
    axes[1].set_title("QUARANTINED theo file")
    fig.suptitle("Veracity — CICIDS2017: 2.830.743 flow / 8 CSV", fontsize=15)
    fig.tight_layout()
    fig.savefig(REPORT.with_name("veracity-quality.png"), dpi=180, facecolor="white")
    plt.close(fig)


def build_markdown(source, totals, cells, labels, full_runs, example, contract):
    n = totals["input_records"]
    classification = table(["Trạng thái", "Số flow", "Tỷ lệ đầu vào"],
                           [[k.upper().replace("_RECORDS", ""), number(totals[k]), percent(totals[k], n)] for k in COUNTS[1:]])
    file_table = table(["File CSV", "Input", "VALID", "Cách ly", "Từ chối"],
                       [[f["source_file"], number(f["input_records"]),
                         *[number(f["policy_preview"][k]) for k in COUNTS[1:]]] for f in source["files"]])
    issue_table = table(["Trường", "Hiện tượng", "Số ô", "% flow đầu vào", "Chính sách"],
                        [[f"`{field}`", title, number(cells[field, kind]), percent(cells[field, kind], n), policy]
                         for field, kind, title, policy in ISSUES])
    trial_table = table(["Lượt", "Batch ID", "VALID / cách ly / từ chối", "Hậu kiểm", "Read-back"],
                        [[r["repeat"], f"`{r['batch_id']}`", " / ".join(number(r["quality"][k]) for k in COUNTS[1:]),
                          r["post_cleaning_status"], "5/5 PASS"] for r in full_runs])
    label_table = table(["Nhãn raw", "Số flow", "Tỷ lệ"],
                        [[label, number(count), percent(count, n)] for label, count in labels.most_common()])
    before, after = example["before_cleaning"]["totals"], example["after_cleaning"]["totals"]
    example_table = table(["Chỉ số (22 trường lõi, mọi trạng thái)", "Trước", "Sau"],
                          [[label, number(before[key]), number(after[key])] for key, label in (
                              ("input_records", "Số dòng"), ("nan_count", "Ô NaN"),
                              ("infinity_count", "Ô Infinity"), ("null_count", "Ô NULL"),
                              ("negative_invalid_count", "Ô âm không hợp lệ"),
                              ("sentinel_count", "Sentinel: raw trước / cờ sau"))])
    types = Counter(f["cleaned_target_type"] for f in contract["fields"] if f["in_cleaned_core"])
    return f"""# Veracity — Đánh giá độ tin cậy và chất lượng dữ liệu

## 1. Mục tiêu và phạm vi

Trong đề tài “Xây dựng và đánh giá phương pháp nhận diện các hoạt động mạng bất thường từ PCAP”, Veracity được đánh giá qua chất lượng kỹ thuật của dữ liệu flow: cấu trúc, kiểu dữ liệu, miền giá trị, tính đầy đủ, khả năng truy vết và tính nhất quán trước/sau xử lý. Mục tiêu là phát hiện dữ liệu không phù hợp, áp dụng chính sách làm sạch có giải thích và chứng minh các bản ghi được phân loại, lưu trữ đúng quy trình.

Báo cáo sử dụng hồ sơ thực nghiệm dữ liệu thật ngày **10/11/2026**, mã `real-20261110-141211`, cùng mã pipeline trong repository. Báo cáo được tổng hợp lại từ các artifact đã lưu; lần tạo tài liệu này không chạy lại Spark, không truy cập MinIO và không sửa CSV. Số liệu theo ô lấy từ source audit; kết quả xử lý và ghi/đọc lại lấy từ các lượt Spark. Hai tầng bằng chứng được phân biệt trong từng bảng [R1–R4].

`VALID` nghĩa là đạt chính sách chất lượng hiện tại, có thể còn NULL được phép và cờ chất lượng. Trạng thái này độc lập với nhãn `BENIGN` hay nhãn tấn công; báo cáo không đo accuracy của mô hình và không xác nhận ground truth từ PCAP.

## 2. Dữ liệu, schema và tính toàn vẹn nguồn

Đầu vào gồm **8 CSV, {number(n)} flow, {number(source['input_bytes'])} byte**, 79 vị trí cột. Đây là dữ liệu flow đã trích xuất, không phải {number(n)} packet. Source audit ghi nhận cả 8 header khớp thứ tự contract, mọi dòng đủ 79 trường, không có dòng malformed hoặc lỗi giải mã UTF-8 [R1].

Header `Fwd Header Length` xuất hiện ở vị trí 35 và 56, tính từ 1. Contract ánh xạ theo vị trí thành `fwd_header_length` và `fwd_header_length_repeat_2`, tránh mất cột khi tên trùng. Audit ghi nhận 0 dòng khác giá trị giữa hai cột; pipeline vẫn giữ đủ raw evidence và không tự suy ra có thể bỏ một cột trong mọi bộ dữ liệu.

Ingestion đọc 79 trường dạng chuỗi; cleaned core chọn 22 trường gồm {types['integer']} integer, {types['long']} long, {types['double']} double và {types['string']} string. Vì vậy, hậu kiểm miền giá trị của 22 trường không phải chứng nhận toàn bộ 79 trường đã được làm sạch để huấn luyện [R5].

SHA-256 từng file được lưu trong manifest và so lại khi đọc nguồn. Hồ sơ kiểm tra sau benchmark ghi nhận cả 8 file không đổi. Script tạo báo cáo kiểm tra thêm SHA-256 các artifact được dùng, schema và contract so với hồ sơ ban đầu; riêng tài liệu dạng text cho phép tái lập LF/CRLF do Git chuyển kiểu xuống dòng khi checkout, không sửa artifact. Checksum chỉ chứng minh tính nhất quán với bằng chứng lưu trữ, không xác nhận nguồn gốc tuyệt đối hoặc nhãn đúng về mặt nghiệp vụ.

## 3. Chính sách phát hiện và xử lý lỗi

Pipeline phân loại theo thứ tự ưu tiên: lỗi cấu trúc thành `REJECTED`; dòng đọc được nhưng vi phạm quy tắc cứng thành `QUARANTINED`; các dòng còn lại thành `VALID`. Lỗi mềm không tự chuyển dòng sang cách ly [R5–R6].

- **Lỗi cứng:** NULL/rỗng/cast failure ở trường bắt buộc ngoài hai rate, số âm không được phép, NaN/Infinity ở trường double ngoài rate, destination port ngoài 0–65535, TCP flag ngoài 0/1, initial window âm khác -1, label NULL/rỗng. Không sửa duration âm thành 0.
- **Lỗi mềm ở rate:** `flow_bytes_s` và `flow_packets_s` không hợp lệ được đổi NULL, kèm cờ `*_invalid` và reason code; không thay bằng 0 hay tính lại nếu chưa có chính sách tương ứng.
- **Sentinel:** initial window bằng -1 được đổi NULL và bật `init_win_fwd_missing` hoặc `init_win_bwd_missing`; không coi -1 là giá trị đo hợp lệ.
- **Nhãn có U+FFFD:** giữ nguyên chuỗi nguồn, thêm `LABEL_ENCODING_REPLACEMENT`; không đoán ký tự thay thế hoặc gộp nhãn.
- **Bản ghi trùng:** giữ nguyên vì CSV không có khóa quan sát flow ổn định để xác định trùng do lỗi sao chép. Báo cáo này không định lượng trùng và không tuyên bố dữ liệu không trùng.

**Bảng R.1. Lỗi/sentinel quan sát trong 22 trường lõi từ source audit.**

{issue_table}

Đơn vị là số ô theo từng trường và hiện tượng; cùng một dòng có thể xuất hiện ở nhiều hàng. Tỷ lệ là số ô của từng hàng chia tổng flow đầu vào, không phải tỷ lệ lỗi trên toàn bộ 22 × N ô. Không cộng các hàng để suy ra số dòng lỗi. Có **{number(totals['valid_records_with_soft_issues'])} dòng VALID có ít nhất một lỗi mềm**, tương đương **{percent(totals['valid_records_with_soft_issues'], n)}** đầu vào, theo số đếm dòng duy nhất của source audit [R1].

Audit ghi nhận 0 lỗi giải mã UTF-8 nhưng vẫn có {number(cells['label', 'encoding_replacement'])} nhãn chứa ký tự U+FFFD: ký tự thay thế đã có trong văn bản nguồn, không đồng nghĩa thao tác đọc UTF-8 của lần audit bị lỗi.

## 4. Kết quả phân loại và bảo toàn bản ghi

**Bảng R.2. Kết quả toàn bộ 8 CSV; source policy preview khớp ba lượt Spark.**

{classification}

Đối soát đạt: **{number(n)} = {number(totals['valid_records'])} VALID + {number(totals['quarantined_records'])} QUARANTINED + {number(totals['rejected_records'])} REJECTED**. Tỷ lệ cách ly/từ chối chỉ là {percent(totals['quarantined_records'] + totals['rejected_records'], n)}, không đại diện cho toàn bộ lỗi mềm. `REJECTED = 0` phản ánh cấu trúc nguồn của lần chạy này; không có nghĩa parser thiếu nhánh từ chối.

**Bảng R.3. Chi tiết theo file từ source audit [R1].**

{file_table}

115 dòng cách ly có lỗi cứng; các giá trị âm ở `flow_duration` và `flow_iat_mean` có thể cùng thuộc một dòng, không tạo thành 230 dòng cách ly. Mọi bản ghi được giữ trong một trong ba nhóm; không cộng features hoặc aggregates vào phương trình bảo toàn vì đó là các biểu diễn dẫn xuất.

![Lỗi theo ô và số dòng cách ly theo file](veracity-quality.png)

*Hình R.1. Dữ liệu nguồn đã audit; biểu đồ trái dùng trục log và đếm ô, biểu đồ phải đếm dòng. Không cộng hai loại số liệu.*

## 5. Hậu kiểm và minh chứng ghi/đọc lại

**Bảng R.4. Ba lượt Spark xử lý toàn bộ dữ liệu thật [R2–R3].**

{trial_table}

Mỗi lượt lưu năm dataset Parquet/Snappy. Số dòng tương ứng là {number(totals['valid_records'])} cleaned, {number(totals['valid_records'])} features, {number(full_runs[0]['outputs']['aggregates_path']['records'])} nhóm aggregates, {number(totals['quarantined_records'])} quarantine và 0 rejected. Dataset rejected rỗng vẫn được ghi/đọc lại. Read-back kiểm kiểu cột, số dòng và phân bố `source_file`/`label`/`quality_status` cho các output không phải aggregates [R6].

MinIO audit lịch sử đã đọc lại marker, quality JSON và performance JSON của 31 batch, ghi nhận `COMPLETE` và PASS, đồng thời đối chiếu bằng chứng 155 dataset Parquet đã được pipeline đọc lại. Số 31 gồm 27 lượt đo và 4 warmup trên nhiều mức dữ liệu; không phải 31 lượt toàn bộ 8 CSV. Audit đó không đọc mới toàn bộ 155 Parquet; script tổng hợp báo cáo hiện tại cũng chỉ kiểm tra artifact đã lưu [R3].

`quality.json` có `run_status: VERIFIED` chưa đủ xác nhận batch hoàn tất. Điều kiện hoàn thành là marker cuối `_SUCCESS.json` tại thư mục báo cáo chất lượng của đúng batch, với `run_status: COMPLETE`. Marker `_SUCCESS` của riêng một dataset Parquet không thay thế marker cuối của cả pipeline [R6].

### 5.1. Minh họa before/after có số đo Spark

Artifact `quality-example.json` thuộc **Friday Morning, {number(example['input_records'])} flow**, batch `{example['batch_id']}`; đây là dữ liệu thật nhưng chỉ một file, không phải snapshot đầy đủ 8 CSV [R4].

{example_table}

Sau xử lý, NaN/Infinity rate đã thành NULL có cờ; sentinel raw được thay NULL nhưng bộ đếm sentinel sau cleaning dùng cờ đã giữ lại. Vì vậy, sentinel_count không về 0 không có nghĩa giá trị -1 còn trong cleaned output. NULL tăng có chủ đích, không phải dữ liệu tự mất thêm.

Snapshot đếm mọi trạng thái: {number(after['negative_invalid_count'])} ô âm còn lại nằm trong quarantine, nên không mâu thuẫn với hậu kiểm PASS của cleaned VALID. Đối soát mẫu xác nhận multiset raw evidence được bảo toàn và không đổi trạng thái; hậu kiểm ghi nhận 0 dòng vi phạm quy tắc sau làm sạch. Không nhân các số đếm một file này để ước lượng toàn bộ dữ liệu.

## 6. Độ tin cậy của nhãn và tác động đến phân tích

**Bảng R.5. Phân bố nhãn nguồn trước xử lý [R1].**

{label_table}

`BENIGN` chiếm {percent(labels['BENIGN'], n)}; `Heartbleed` chỉ có {labels['Heartbleed']} flow. Mất cân bằng khiến accuracy tổng thể có thể che khuất lớp hiếm; khi bổ sung mô hình cần báo cáo macro-F1, recall từng lớp và confusion matrix. Đây là định hướng đánh giá tiếp theo, chưa phải kết quả huấn luyện.

Hai trường initial window có sentinel ở {percent(cells['init_win_bytes_forward', 'sentinel_minus_one'], n)} và {percent(cells['init_win_bytes_backward', 'sentinel_minus_one'], n)} flow. Không cộng hai tỷ lệ vì có thể cùng dòng. Xóa mọi dòng chứa NULL có thể gây mất nhiều dữ liệu; báo cáo này không đo retention sau `dropna()`. Khi huấn luyện, cần giữ cờ missing/invalid, chọn chính sách impute phù hợp và fit imputer/scaler chỉ trên tập train. Việc chia tập cần xem xét file/ngày và các flow giống nhau để hạn chế leakage; không dùng metadata lineage như feature nếu chưa đánh giá rủi ro rò rỉ nhãn.

Nhãn U+FFFD vẫn được giữ để truy vết. Kiểm tra miền số, checksum và bảo toàn dòng không chứng minh nhãn mô tả đúng hành vi mạng; muốn xác minh cần bằng chứng ground truth/PCAP tương ứng ngoài phạm vi pipeline hiện tại.

## 7. Truy vết, giới hạn và kết luận

Cleaned VALID lưu 22 trường lõi cùng `source_file`, `source_dataset`, `batch_id`, `pipeline_version`, `schema_version`, `processed_at`, trạng thái, reason codes và bốn cờ chất lượng. Quarantine/rejected cùng frame annotated giữ thêm raw evidence, source path và cast-failure evidence. Cleaned VALID không giữ toàn bộ `_raw_values` hoặc `source_path`; muốn truy lại cần kết hợp file nguồn bất biến và manifest/checksum của batch. Chưa có row ID ổn định để định danh duy nhất từng flow trùng giá trị [R5–R6].

Đối soát multiset dùng raw evidence trước/sau cleaning, giữ cả số lần lặp của bản ghi. Hậu kiểm xác minh miền giá trị, cờ và phép đổi rate/window, nhãn và metadata. Read-back kiểm schema/count/distribution, chưa so toàn bộ giá trị số của từng dòng sau ghi; không coi đó là chứng minh nội dung mọi ô hoàn toàn giống nhau.

Kết quả cho thấy pipeline phân loại và kiểm chứng được {number(n)} flow, giữ {percent(totals['valid_records'], n)} ở nhóm VALID, cách ly {totals['quarantined_records']} dòng lỗi cứng và bảo toàn bằng chứng để giải thích lỗi. Đồng thời, {number(totals['valid_records_with_soft_issues'])} dòng VALID vẫn có lỗi mềm được ghi nhận. Giá trị của Veracity ở đây là chính sách xử lý minh bạch, truy vết và hậu kiểm có bằng chứng; chưa chứng nhận nhãn đúng hoặc dữ liệu sẵn sàng cho mọi mô hình.

## 8. Tái lập báo cáo và tài liệu minh chứng

Chạy từ thư mục gốc repository, cần `matplotlib`; xuất Word cần thêm `python-docx`. Không cần khởi động Spark/MinIO để tổng hợp tài liệu:

```powershell
python -m experiments.build_veracity_report --docx
```

Lệnh kiểm tra artifact/checksum và đối chiếu số liệu trước khi tạo lại Markdown, PNG và DOCX. Muốn đo lại dữ liệu nguồn hoặc chạy Spark, làm theo [hướng dẫn xử lý](../processing.md) và [benchmark](../benchmark-guide.md); đó là lần thực nghiệm mới, cần batch ID và bằng chứng mới.

- **[R1]** [Source audit 8 CSV](../benchmarks/real-20261110-141211/source-audit.json); [file/checksum](../benchmarks/real-20261110-141211/source-files.csv); [tính toàn vẹn sau benchmark](../benchmarks/real-20261110-141211/source-integrity-after.json).
- **[R2]** [Các lượt chạy size, gồm ba lượt full-data](../benchmarks/real-20261110-141211/size.json); [hồ sơ thực nghiệm](../benchmarks/real-20261110-141211/README.md).
- **[R3]** [MinIO audit lịch sử](../benchmarks/real-20261110-141211/minio-audit.json); [kiểm tra thực nghiệm](../benchmarks/real-20261110-141211/verification.json); [SHA-256 artifact](../benchmarks/real-20261110-141211/artifact-sha256.csv).
- **[R4]** [Before/after Spark cho Friday Morning](../benchmarks/real-20261110-141211/quality-example.json).
- **[R5]** [Schema](../../data/schemas/raw_flow_schema.json); [Data Contract](../../data/schemas/feature_dictionary.md); [validation](../../processing/validation.py); [cleaning](../../processing/clean.py).
- **[R6]** [Pipeline và marker cuối](../../processing/main.py); [ghi/đọc lại](../../processing/output.py); [định nghĩa quality metrics](../../processing/quality_report.py).
"""


def write_docx(markdown):
    """Render the report's headings, tables, figure, links and code into Word."""
    from docx import Document
    from docx.oxml import OxmlElement
    from docx.oxml.ns import qn
    from docx.shared import Cm, Pt, RGBColor

    doc = Document()
    section = doc.sections[0]
    section.page_width, section.page_height = Cm(21), Cm(29.7)
    section.top_margin = section.bottom_margin = Cm(2)
    section.left_margin = section.right_margin = Cm(1.8)
    normal = doc.styles["Normal"]
    normal.font.name, normal.font.size = "Times New Roman", Pt(11)
    normal.paragraph_format.space_after = Pt(6)
    normal.paragraph_format.line_spacing = 1.12
    for name in ("Title", "Heading 1", "Heading 2"):
        doc.styles[name].font.name = "Times New Roman"
        doc.styles[name].font.color.rgb = RGBColor.from_string("173D50")

    def inline(paragraph, text):
        for part in re.split(r"(\[[^\]]+\]\([^)]+\)|\*\*[^*]+\*\*|`[^`]+`)", text):
            match = re.fullmatch(r"\[([^\]]+)\]\(([^)]+)\)", part)
            if match:
                label, target = match.groups()
                # Links work from the repository and when the DOCX is shared.
                if not target.startswith(("http://", "https://")):
                    target_path = (REPORT.parent / target).resolve().relative_to(ROOT).as_posix()
                    target = "https://github.com/BonM2/bigdata-pcap-anomaly/blob/feat/velocity-benchmark-report-20261110/" + target_path
                link = OxmlElement("w:hyperlink")
                link.set(qn("r:id"), paragraph.part.relate_to(target,
                         "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink", is_external=True))
                run = OxmlElement("w:r")
                props = OxmlElement("w:rPr")
                color = OxmlElement("w:color")
                color.set(qn("w:val"), "176B89")
                props.append(color)
                run.append(props)
                node = OxmlElement("w:t")
                node.text = label
                run.append(node)
                link.append(run)
                paragraph._p.append(link)
            else:
                bold, code = part.startswith("**"), part.startswith("`")
                run = paragraph.add_run(part[2:-2] if bold else part[1:-1] if code else part)
                run.bold = bold
                if code:
                    run.font.name, run.font.size = "Consolas", Pt(9)

    lines = markdown.splitlines()
    i, in_code = 0, False
    while i < len(lines):
        line = lines[i]
        i += 1
        if line.startswith("```"):
            in_code = not in_code
        elif not line:
            continue
        elif in_code:
            p = doc.add_paragraph()
            r = p.add_run(line)
            r.font.name, r.font.size = "Consolas", Pt(9)
        elif line.startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            doc.add_heading(line[level:].strip(), level=min(level - 1, 2))
        elif line.startswith("| "):
            rows = [line]
            while i < len(lines) and lines[i].startswith("| "):
                rows.append(lines[i])
                i += 1
            grid = doc.add_table(rows=0, cols=len(line.strip("| ").split("|")))
            grid.style = "Table Grid"
            grid.autofit = False
            if "File CSV" in line:
                widths = [8.0, 2.5, 2.5, 2.2, 2.2]
            elif "Batch ID" in line:
                widths = [0.8, 7.0, 4.6, 2.5, 2.5]
            elif "Chính sách" in line:
                widths = [4.1, 2.3, 1.8, 2.2, 7.0]
            elif "Trạng thái" in line or "Nhãn raw" in line:
                widths = [9.4, 4.0, 4.0]
            else:
                widths = [11.0, 3.2, 3.2]
            for column, width in zip(grid.columns, widths):
                column.width = Cm(width)
            for index, row in enumerate(rows):
                if index == 1:  # Markdown separator
                    continue
                cells = grid.add_row().cells
                for cell, width in zip(cells, widths):
                    cell.width = Cm(width)
                for cell, value in zip(cells, row.strip().strip("|").split("|")):
                    inline(cell.paragraphs[0], value.strip())
                    for run in cell.paragraphs[0].runs:
                        run.font.size = Pt(8)
                        if index == 0:
                            run.bold = True
                props = grid.rows[-1]._tr.get_or_add_trPr()
                props.append(OxmlElement("w:cantSplit"))
                if index == 0:
                    props.append(OxmlElement("w:tblHeader"))
            doc.add_paragraph()
        elif line.startswith("!["):
            doc.add_picture(str(REPORT.parent / "veracity-quality.png"), width=Cm(17))
        else:
            bullet = line.startswith("- ")
            p = doc.add_paragraph(style="List Bullet" if bullet else None)
            inline(p, line[2:] if bullet else line.strip("*"))
    footer = section.footer.paragraphs[0]
    footer.alignment = 2
    footer.add_run("Veracity • ")
    page = OxmlElement("w:fldSimple")
    page.set(qn("w:instr"), "PAGE")
    footer._p.append(page)
    doc.core_properties.title = "Veracity — Đánh giá độ tin cậy và chất lượng dữ liệu"
    doc.core_properties.subject = "CICIDS2017 / Spark / MinIO — real-20261110-141211"
    doc.save(REPORT.with_suffix(".docx"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docx", action="store_true", help="Also export Word with python-docx")
    args = parser.parse_args()
    evidence = collect_evidence()
    write_chart(evidence[0], evidence[2])
    markdown = build_markdown(*evidence)
    REPORT.write_text(markdown, encoding="utf-8")
    if args.docx:
        write_docx(markdown)
    print("PASS: historical checksums, contract, 8 sources, 3 full-data trials, stored markers and 15 output read-backs")
    print("Created docs/reports/veracity-report.md, veracity-quality.png" + (", veracity-report.docx" if args.docx else ""))


if __name__ == "__main__":
    main()

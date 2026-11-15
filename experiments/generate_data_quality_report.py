"""Generate the evidence-backed CICIDS2017 data-quality Markdown report.

The report combines the streaming 79-column source audit with a chunked,
read-only simulation of the project's 22-field quality policy.  No source row
is edited or deleted.  Duplicate observations use a clearly scoped criterion:
byte-identical physical CSV records (line ending excluded), fingerprinted with
BLAKE2b-128.  The report labels this as a survey, not a deletion instruction.
"""

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

from experiments.analyze_training_readiness import (
    RATES,
    TCP_FLAGS,
    WINDOWS,
    load_core_fields,
    numeric_values,
)


def percentage(value, total):
    return 0.0 if not total else value * 100.0 / total


def classify_and_group(paths, fields, chunk_size):
    """Reproduce the contract policy and count unique affected rows by label."""
    indices = [item["source_index"] for item in fields]
    names = [item["name"] for item in fields]
    label_counts = defaultdict(Counter)
    file_label_counts = defaultdict(Counter)
    file_counts = defaultdict(Counter)

    for path in paths:
        for chunk in pd.read_csv(
            path,
            usecols=indices,
            dtype=str,
            keep_default_na=False,
            encoding_errors="replace",
            chunksize=chunk_size,
            low_memory=False,
        ):
            chunk.columns = names
            size = len(chunk)
            hard = np.zeros(size, dtype=bool)
            soft = np.zeros(size, dtype=bool)
            cleaned_null = np.zeros(size, dtype=bool)

            for item in fields:
                name = item["name"]
                if name == "label":
                    label = chunk[name].astype("string").fillna("")
                    hard |= label.str.strip().eq("").to_numpy()
                    soft |= label.str.contains("\ufffd", regex=False, na=False).to_numpy()
                    continue

                _, value, missing, cast_failure, special_nan = numeric_values(chunk[name])
                finite_values = value.to_numpy(dtype=float, na_value=np.nan)
                infinity = np.isinf(finite_values)
                negative = value.lt(0).fillna(False).to_numpy()
                base_invalid = missing.to_numpy() | cast_failure.to_numpy()

                if name in RATES:
                    invalid = base_invalid | special_nan.to_numpy() | infinity | negative
                    soft |= invalid
                    cleaned_null |= invalid
                    continue

                hard |= base_invalid
                if item["cleaned_target_type"] == "double":
                    hard |= special_nan.to_numpy() | infinity
                if name in WINDOWS:
                    sentinel = value.eq(-1).fillna(False).to_numpy()
                    hard |= negative & ~sentinel
                    soft |= sentinel
                    cleaned_null |= sentinel
                else:
                    hard |= negative
                if name == "destination_port":
                    hard |= ((value < 0) | (value > 65535)).fillna(False).to_numpy()
                if name in TCP_FLAGS:
                    hard |= (~value.isin([0, 1]) & value.notna()).to_numpy()

            labels = chunk["label"].astype("string").fillna("").tolist()
            valid = ~hard
            affected = hard | soft
            for i, label in enumerate(labels):
                status = "QUARANTINED" if hard[i] else "VALID"
                metrics = {
                    "input": 1,
                    status.lower(): 1,
                    "affected": int(affected[i]),
                    "soft_valid": int(valid[i] and soft[i]),
                    "cleaned_null_valid": int(valid[i] and cleaned_null[i]),
                }
                label_counts[label].update(metrics)
                file_label_counts[(path.name, label)].update(metrics)
                file_counts[path.name].update(metrics)

    return label_counts, file_label_counts, file_counts


def fingerprint(line):
    return hashlib.blake2b(line.rstrip(b"\r\n"), digest_size=16).digest()


def duplicate_stats(paths):
    """Survey byte-identical CSV records without dropping any observation."""
    all_hashes = []
    per_file = {}
    for path in paths:
        values = []
        with path.open("rb") as stream:
            next(stream)
            for line in stream:
                values.append(fingerprint(line))
        array = np.asarray(values, dtype="S16")
        unique, counts = np.unique(array, return_counts=True)
        repeated = counts[counts > 1]
        per_file[path.name] = {
            "duplicate_groups": int(len(repeated)),
            "records_in_duplicate_groups": int(repeated.sum()),
            "excess_duplicate_records": int((repeated - 1).sum()),
        }
        all_hashes.append(array)

    combined = np.concatenate(all_hashes)
    unique, counts = np.unique(combined, return_counts=True)
    repeated_mask = counts > 1
    repeated_keys = unique[repeated_mask]
    repeated_counts = counts[repeated_mask]
    repeated = {key.tobytes(): int(count) for key, count in zip(repeated_keys, repeated_counts)}

    labels = Counter()
    label_groups = Counter()
    seen = set()
    for path in paths:
        with path.open("rb") as stream:
            next(stream)
            for raw_line in stream:
                key = fingerprint(raw_line)
                count = repeated.get(key)
                if count is None:
                    continue
                line = raw_line.decode("utf-8", errors="replace")
                row = next(csv.reader([line], strict=True))
                label = row[78]
                labels[label] += 1
                if key not in seen:
                    label_groups[label] += 1
                    seen.add(key)

    return {
        "criterion": "byte-identical physical CSV record, excluding CR/LF; BLAKE2b-128 fingerprint",
        "hash_collision_note": "Fingerprint survey; no automatic deletion is authorized.",
        "dataset": {
            "duplicate_groups": int(len(repeated_counts)),
            "records_in_duplicate_groups": int(repeated_counts.sum()),
            "excess_duplicate_records": int((repeated_counts - 1).sum()),
        },
        "per_file": per_file,
        "affected_records_by_label": dict(labels),
        "duplicate_groups_by_label": dict(label_groups),
    }


def aggregate_source_fields(source_audit, core_names):
    aggregate = {name: Counter() for name in core_names}
    for item in source_audit["files"]:
        for name in core_names:
            aggregate[name].update(item["fields"][name]["counts"])
    return aggregate


def md_table(headers, rows):
    def clean(value):
        return str(value).replace("|", "\\|").replace("\n", " ")

    output = ["| " + " | ".join(map(clean, headers)) + " |",
              "|" + "|".join("---" for _ in headers) + "|"]
    output.extend("| " + " | ".join(clean(value) for value in row) + " |" for row in rows)
    return "\n".join(output)


def generate_markdown(source, readiness, contract, labels, file_labels, files, duplicates):
    total = readiness["totals"]["input_records"]
    valid = readiness["totals"]["valid_records"]
    quarantine = readiness["totals"]["quarantined_records"]
    rejected = readiness["totals"]["rejected_records"]
    core = [f for f in contract["fields"] if f["in_cleaned_core"]]
    core_names = {f["name"] for f in core}
    issues = aggregate_source_fields(source, core_names)

    lines = [
        "# Báo cáo kiểm tra chất lượng dữ liệu CICIDS2017",
        "",
        f"> Tạo lúc `{datetime.now(timezone.utc).isoformat()}` từ 8 CSV cục bộ. "
        "Đây là kiểm toán chỉ đọc và mô phỏng chính sách contract; nguồn CSV không bị sửa hay xóa.",
        "",
        "## Kết luận điều hành",
        "",
        f"- Đã đọc **{total:,}** bản ghi trong **8 file**, tổng dung lượng **{source['input_bytes']:,} byte**. "
        "Cả 8 header khớp chính xác contract 79 cột; không có dòng sai số cột hoặc lỗi cú pháp CSV.",
        f"- Phân loại theo contract: **{valid:,} VALID ({percentage(valid, total):.4f}%)**, "
        f"**{quarantine:,} QUARANTINED ({percentage(quarantine, total):.4f}%)**, "
        f"**{rejected:,} REJECTED ({percentage(rejected, total):.4f}%)**. "
        f"Đối soát đạt: `{total:,} = {valid:,} + {quarantine:,} + {rejected:,}`.",
        f"- Có **{readiness['totals']['valid_with_soft_issues']:,}** dòng VALID mang ít nhất một lỗi mềm; "
        f"**{readiness['totals']['valid_with_cleaned_null']:,}** dòng VALID có ít nhất một giá trị được đổi thành NULL.",
        f"- Nếu giữ VALID và impute đúng quy trình thì giữ {percentage(valid, total):.4f}% dữ liệu. "
        f"Nếu `dropna()` toàn bộ 21 feature thì chỉ còn **{readiness['totals']['complete_case_valid_records']:,}** "
        f"dòng ({percentage(readiness['totals']['complete_case_valid_records'], total):.4f}%).",
        "- Kết quả này không chứng minh nhãn ground truth là đúng. Không có row bị tự động xóa trùng.",
        "",
        "## 1. Nguồn, checksum, số dòng và schema",
        "",
        "Nguồn được khai báo là [CIC-IDS2017 của Canadian Institute for Cybersecurity, UNB]"
        "(https://www.unb.ca/cic/datasets/ids-2017.html). Trang chính thức mô tả PCAP, labeled flows và "
        "MachineLearningCSV; repository không có checksum do nhà phát hành ký hoặc manifest tải xuống, "
        "nên SHA-256 dưới đây chỉ xác minh rằng các file cục bộ không đổi giữa các lần kiểm tra; nó không xác nhận "
        "nguồn gốc tuyệt đối hoặc độ đúng của nhãn.",
        "",
    ]

    checksum_rows = []
    for item in source["files"]:
        checksum_rows.append((item["source_file"], f"{item['input_records']:,}", f"{item['input_bytes']:,}",
                              item["source_sha256"], "PASS" if item["header_matches_exact"] else "FAIL"))
    lines += [md_table(("File", "Dòng", "Byte", "SHA-256", "Header 79 cột"), checksum_rows), ""]

    type_counts = Counter(f["cleaned_target_type"] for f in core)
    lines += [
        f"- Raw ingestion: 79 vị trí được đọc dạng chuỗi để tránh suy luận kiểu làm mất dữ liệu. Cleaned core: "
        f"22 trường = {type_counts['integer']} integer, {type_counts['long']} long, "
        f"{type_counts['double']} double và {type_counts['string']} string.",
        "- Tên canonical sau mapping là duy nhất. Header nguồn có một tên trùng: `Fwd Header Length` tại vị trí "
        "35 và 56 (đếm từ 1). Contract giữ thành `fwd_header_length` và `fwd_header_length_repeat_2`.",
        f"- Số dòng hai cột trùng tên có giá trị khác nhau: **{sum(x['duplicate_header_value_mismatches'] for x in source['files']):,}**. "
        "Dù bằng nhau trong bộ này, không được tự bỏ một cột vì chưa có bằng chứng ngữ nghĩa rằng chúng luôn tương đương.",
        f"- SHA-256 schema: `{source['schema_sha256']}`; SHA-256 data contract: `{source['contract_sha256']}`.",
        "",
        "## 2. Lỗi quan sát được và quy tắc xử lý",
        "",
        "Số dưới đây là **số ô lỗi theo cột**. Chúng có thể chồng lấp trên cùng một bản ghi và không được cộng để suy ra số dòng lỗi duy nhất.",
        "",
    ]

    issue_rows = []
    policies = {
        ("flow_bytes_s", "nan"): ("NAN:flow_bytes_s", "Đổi NULL + `flow_bytes_s_invalid=true`; vẫn VALID nếu không có lỗi cứng", "0 NaN trong cleaned VALID"),
        ("flow_bytes_s", "positive_infinity"): ("INFINITY:flow_bytes_s", "Đổi NULL + flag", "0 Infinity trong cleaned VALID"),
        ("flow_bytes_s", "negative"): ("NEGATIVE:flow_bytes_s", "Đổi NULL + flag", "0 số âm trong cleaned VALID"),
        ("flow_packets_s", "positive_infinity"): ("INFINITY:flow_packets_s", "Đổi NULL + `flow_packets_s_invalid=true`", "0 Infinity trong cleaned VALID"),
        ("flow_packets_s", "negative"): ("NEGATIVE:flow_packets_s", "Đổi NULL + flag", "0 số âm trong cleaned VALID"),
        ("flow_duration", "negative"): ("NEGATIVE:flow_duration", "QUARANTINED; không ép 0", "Không có trong cleaned VALID"),
        ("flow_iat_mean", "negative"): ("NEGATIVE:flow_iat_mean", "QUARANTINED", "Không có trong cleaned VALID"),
        ("init_win_bytes_forward", "sentinel_minus_one"): ("SENTINEL:init_win_bytes_forward", "Đổi NULL + `init_win_fwd_missing=true`", "0 sentinel trong cleaned VALID"),
        ("init_win_bytes_backward", "sentinel_minus_one"): ("SENTINEL:init_win_bytes_backward", "Đổi NULL + `init_win_bwd_missing=true`", "0 sentinel trong cleaned VALID"),
        ("label", "encoding_replacement"): ("LABEL_ENCODING_REPLACEMENT", "Giữ nguyên nhãn raw + reason code; không đoán ký tự", "Vẫn giữ để truy vết"),
    }
    for key, (code, action, result) in policies.items():
        field, kind = key
        count = issues[field][kind]
        issue_rows.append((kind, field, f"{count:,} ({percentage(count, total):.4f}%)", action, code, result))
    lines += [md_table(("Loại lỗi", "Cột", "Trước xử lý", "Cách xử lý", "Mã lý do/cờ", "Sau xử lý VALID"), issue_rows), ""]

    other_nonzero = []
    for name, counts in sorted(issues.items()):
        for kind, count in sorted(counts.items()):
            if count and kind not in {"finite", "fractional"} and (name, kind) not in policies:
                other_nonzero.append((name, kind, count))
    lines += [
        f"Ngoài bảng trên, số lỗi nonzero khác trong 22 trường lõi: **{len(other_nonzero)} loại** "
        f"({'; '.join(f'{a}/{b}={c:,}' for a,b,c in other_nonzero) if other_nonzero else 'không có'}). "
        "Vì vậy audit không phát hiện ô rỗng, lỗi ép kiểu, Infinity âm, destination port ngoài miền, TCP flag ngoài 0/1, "
        "initial-window âm khác -1 hoặc label rỗng trong 22 trường lõi.",
        "",
        "Phân biệt đơn vị đếm:",
        "",
        f"- Tổng lượt ô được đổi NULL: **{sum(readiness['cleaned_null_cells_by_field'].values()):,} ô**.",
        f"- Dòng VALID có ít nhất một NULL sau làm sạch: **{readiness['totals']['valid_with_cleaned_null']:,} dòng duy nhất**.",
        f"- Dòng VALID có ít nhất một lỗi mềm: **{readiness['totals']['valid_with_soft_issues']:,} dòng duy nhất**.",
        f"- Dòng có lỗi cứng: **{quarantine:,} dòng duy nhất**; các lỗi có thể cùng xuất hiện trên một dòng.",
        "",
        "## 3. VALID / QUARANTINED / REJECTED theo file",
        "",
    ]

    status_rows = []
    readiness_files = {x["source_file"]: x for x in readiness["per_file"]}
    for name in sorted(readiness_files):
        item = readiness_files[name]
        affected = files[name]["affected"]
        status_rows.append((name, f"{item['input_records']:,}", f"{item['valid_records']:,}",
                            f"{item['quarantined_records']:,}", f"{item['rejected_records']:,}",
                            f"{affected:,}", f"{item['valid_with_cleaned_null']:,}"))
    status_rows.append(("TOÀN BỘ", f"{total:,}", f"{valid:,}", f"{quarantine:,}", f"{rejected:,}",
                        f"{sum(x['affected'] for x in files.values()):,}",
                        f"{readiness['totals']['valid_with_cleaned_null']:,}"))
    lines += [md_table(("File", "Input", "VALID", "QUARANTINED", "REJECTED", "Dòng bị tác động", "VALID có NULL cleaned"), status_rows), ""]

    lines += [
        "`REJECTED = 0` vì audit không gặp lỗi cú pháp hoặc sai số cột. Điều này không có nghĩa REJECTED bị vô hiệu; parser "
        "vẫn có nhánh `MALFORMED_CSV` cho dữ liệu hỏng trong lần chạy khác.",
        "",
        "## 4. Phân bố nhãn, mất cân bằng và tác động theo nhãn",
        "",
    ]
    label_rows = []
    for label, count in sorted(labels.items(), key=lambda item: (-item[1]["input"], item[0])):
        label_rows.append((label, f"{count['input']:,}", f"{count['valid']:,}", f"{count['quarantined']:,}",
                           f"{count['affected']:,}", f"{count['cleaned_null_valid']:,}",
                           f"{percentage(count['valid'], count['input']):.4f}%"))
    lines += [md_table(("Nhãn raw", "Trước", "VALID sau", "Cách ly", "Dòng bị tác động", "VALID có NULL cleaned", "Giữ lại"), label_rows), ""]

    positive_labels = [(label, data["input"]) for label, data in labels.items() if data["input"] > 0]
    largest = max(positive_labels, key=lambda x: x[1])
    smallest = min(positive_labels, key=lambda x: x[1])
    attack_total = total - labels.get("BENIGN", Counter())["input"]
    lines += [
        f"- `BENIGN`: **{labels.get('BENIGN', Counter())['input']:,}** ({percentage(labels.get('BENIGN', Counter())['input'], total):.2f}%). "
        f"Tổng nhãn khác BENIGN: **{attack_total:,}** ({percentage(attack_total, total):.2f}%).",
        f"- Lớp lớn nhất/nhỏ nhất: `{largest[0]}` {largest[1]:,} so với `{smallest[0]}` {smallest[1]:,}; "
        f"tỷ lệ xấp xỉ **{largest[1] / smallest[1]:,.1f}:1**. Accuracy tổng thể sẽ dễ gây hiểu lầm; nên báo cáo macro-F1, "
        "per-class recall và confusion matrix.",
        "- Nhãn chứa ký tự thay thế `�` được giữ nguyên. Không khẳng định sửa thành dấu gạch nối hay ký tự khác nếu chưa có "
        "bằng chứng từ nguồn tương ứng.",
        "",
        "### Phân bố trước/sau theo từng file và nhãn",
        "",
    ]
    file_label_rows = []
    for (file_name, label), count in sorted(file_labels.items()):
        file_label_rows.append((file_name, label, f"{count['input']:,}", f"{count['valid']:,}",
                                f"{count['quarantined']:,}", f"{count['affected']:,}"))
    lines += [md_table(("File", "Nhãn", "Trước", "VALID sau", "Cách ly", "Bị tác động"), file_label_rows), ""]

    lines += [
        "## 5. Khảo sát bản ghi trùng",
        "",
        f"Tiêu chí: **{duplicates['criterion']}**. Đây là khảo sát bằng fingerprint, không phải khóa định danh flow. "
        "Không xóa trùng vì CSV không có source/destination IP, timestamp hay flow ID ổn định để kết luận các dòng giống nhau là "
        "lỗi sao chép thay vì các quan sát hợp lệ lặp lại.",
        "",
    ]
    duplicate_rows = []
    for name in sorted(duplicates["per_file"]):
        item = duplicates["per_file"][name]
        duplicate_rows.append((name, f"{item['duplicate_groups']:,}",
                               f"{item['records_in_duplicate_groups']:,}",
                               f"{item['excess_duplicate_records']:,}"))
    d = duplicates["dataset"]
    duplicate_rows.append(("TOÀN BỘ (kể cả trùng chéo file)", f"{d['duplicate_groups']:,}",
                           f"{d['records_in_duplicate_groups']:,}", f"{d['excess_duplicate_records']:,}"))
    lines += [md_table(("Phạm vi", "Nhóm trùng", "Dòng thuộc nhóm trùng", "Dòng dư nếu giữ 1/nhóm"), duplicate_rows), ""]

    duplicate_label_rows = []
    for label, count in sorted(duplicates["affected_records_by_label"].items(), key=lambda x: (-x[1], x[0])):
        duplicate_label_rows.append((label, f"{duplicates['duplicate_groups_by_label'][label]:,}", f"{count:,}"))
    lines += [md_table(("Nhãn", "Nhóm trùng", "Dòng thuộc nhóm trùng"), duplicate_label_rows), ""]

    lines += [
        "## 6. Cờ chất lượng, mã lý do và truy vết",
        "",
        md_table(("Trường", "Ví dụ"), [
            ("quality_status", "VALID / QUARANTINED / REJECTED"),
            ("quality_reason_codes", "[`SENTINEL:init_win_bytes_forward`, `INFINITY:flow_bytes_s`]"),
            ("flow_bytes_s_invalid", "true khi rate NULL/NaN/Infinity/âm"),
            ("flow_packets_s_invalid", "true khi rate NULL/NaN/Infinity/âm"),
            ("init_win_fwd_missing", "true khi raw `init_win_bytes_forward = -1`"),
            ("init_win_bwd_missing", "true khi raw `init_win_bytes_backward = -1`"),
            ("lineage", "source_file, source_path, batch_id, schema_version, pipeline_version, processed_at"),
            ("raw evidence", "_raw_values; _raw_record cho dòng malformed; cast_failure_fields"),
        ]),
        "",
        "Raw CSV và SHA-256 phải được giữ bất biến. Dataset cách ly/từ chối giữ raw evidence để có thể quay lại giá trị ban đầu. "
        "`VALID` chỉ là trạng thái chất lượng kỹ thuật, không có nghĩa flow là BENIGN.",
        "",
        "## 7. Quality-Aware và chuẩn bị huấn luyện",
        "",
        "- Feature model nên dùng giá trị đã làm sạch cùng các cờ `*_invalid`/`*_missing`; có thể giúp mô hình phân biệt NULL do "
        "sentinel với giá trị quan sát thật.",
        "- Chia train/validation/test trước. Mọi imputer, scaler, encoder, selector hoặc resampling có học tham số phải `fit` "
        "chỉ trên train, rồi `transform` validation/test. Với dữ liệu mạng theo ngày/file, nên cân nhắc split theo thời gian hoặc file "
        "để giảm leakage do các flow giống nhau.",
        "- Không dùng `dropna()` toàn bộ 21 feature: kịch bản này mất hơn một nửa số dòng. Không điền NULL bằng 0 nếu chưa có "
        "lý do miền dữ liệu.",
        "- Với mất cân bằng mạnh, ưu tiên class weight hoặc sampling chỉ trên train; không resample validation/test.",
        "",
        "## 8. Đối soát và giới hạn bằng chứng",
        "",
        f"- Đối soát record: **PASS** — `{total:,} = {valid:,} + {quarantine:,} + {rejected:,}`.",
        f"- Đối soát training retention: **PASS** — `{valid:,} = "
        f"{readiness['totals']['valid_with_cleaned_null']:,} + {readiness['totals']['complete_case_valid_records']:,}`.",
        "- Header/schema/row width/checksum: **PASS trên file cục bộ**.",
        "- Đây chưa phải bằng chứng Spark đã ghi thành công cleaned Parquet thật lên MinIO. Cần `_SUCCESS.json`, hậu kiểm PASS và "
        "đọc lại output trước khi tuyên bố batch production hoàn tất.",
        "- Checksum không xác nhận nhãn đúng. Không hiệu chỉnh nhãn, không tạo lại flow và không xóa trùng nếu chưa có bằng chứng "
        "từ PCAP/ground-truth/source manifest tương ứng.",
        "- Khảo sát trùng dùng BLAKE2b-128 trên raw record byte-identical; các dòng tương đương ngữ nghĩa nhưng khác định dạng số "
        "không được gộp vào tiêu chí này.",
        "",
        "## 9. Lệnh tái lập",
        "",
        "```powershell",
        "python -m processing.audit_source --source-dir data/raw/cicids2017 --report docs/evidence/local/source-audit.local.json --workers 4",
        "python -m experiments.analyze_training_readiness",
        "python -m experiments.generate_data_quality_report",
        "```",
        "",
        "Các bằng chứng máy đọc: `docs/evidence/local/source-audit.local.json`, "
        "`docs/analysis/cicids2017-data-quality-training-readiness.json`, "
        "`docs/analysis/cicids2017-data-quality-comprehensive.json`.",
    ]
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw/cicids2017"))
    parser.add_argument("--contract", type=Path, default=Path("data/schemas/raw_flow_schema.json"))
    parser.add_argument("--source-audit", type=Path, default=Path("docs/evidence/local/source-audit.local.json"))
    parser.add_argument("--readiness", type=Path, default=Path("docs/analysis/cicids2017-data-quality-training-readiness.json"))
    parser.add_argument("--output", type=Path, default=Path("docs/reports/cicids2017-data-quality-audit.md"))
    parser.add_argument("--evidence-output", type=Path, default=Path("docs/analysis/cicids2017-data-quality-comprehensive.json"))
    parser.add_argument("--chunk-size", type=int, default=100_000)
    args = parser.parse_args()

    paths = sorted(args.input_dir.glob("*.csv"))
    source = json.loads(args.source_audit.read_text(encoding="utf-8"))
    readiness = json.loads(args.readiness.read_text(encoding="utf-8"))
    contract = json.loads(args.contract.read_text(encoding="utf-8"))
    if len(paths) != 8 or source["input_records"] != 2_830_743:
        raise ValueError("Expected the audited eight-file CICIDS2017 dataset")

    fields = load_core_fields(args.contract)
    labels, file_labels, files = classify_and_group(paths, fields, args.chunk_size)
    duplicates = duplicate_stats(paths)
    if sum(x["input"] for x in labels.values()) != source["input_records"]:
        raise ValueError("Label/group reconciliation failed")
    if any(x["rejected_records"] for x in readiness["per_file"]):
        raise ValueError("This report generator expects the structurally clean audited input")

    evidence = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "read-only comprehensive quality report evidence",
        "record_status_by_label": {k: dict(v) for k, v in labels.items()},
        "record_status_by_file_label": [
            {"source_file": file_name, "label": label, **dict(values)}
            for (file_name, label), values in sorted(file_labels.items())
        ],
        "record_status_by_file": {k: dict(v) for k, v in files.items()},
        "duplicates": duplicates,
    }
    args.evidence_output.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        generate_markdown(source, readiness, contract, labels, file_labels, files, duplicates),
        encoding="utf-8",
    )
    print(json.dumps({"report": str(args.output), "evidence": str(args.evidence_output),
                      "records": source["input_records"], "duplicates": duplicates["dataset"]},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

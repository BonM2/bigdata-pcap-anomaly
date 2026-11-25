"""Cross-check 5V artifacts offline and generate the Value & 5V synthesis report.

Run offline (no Spark/MinIO required):
    python -m experiments.build_value_report [--docx] [--value-snapshot PATH]

Outputs:
    - docs/reports/value-report.md
    - docs/reports/value-verification.json
    - docs/reports/value-report.docx (optional, when --docx is passed; requires python-docx)
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVIDENCE_DIR = ROOT / "docs/benchmarks/real-20261110-141211"
DEFAULT_REPORT_PATH = ROOT / "docs/reports/value-report.md"
DEFAULT_VERIFICATION_JSON = ROOT / "docs/reports/value-verification.json"
DEFAULT_QUALITY_AUDIT_MD = ROOT / "docs/reports/cicids2017-data-quality-audit.md"
DEFAULT_DEFENSE_SCRIPT_MD = ROOT / "docs/defense/defense-demo-script.md"
DEFAULT_ZEEK_SAMPLE = ROOT / "data/sample/zeek/conn.demo.jsonl"
DEFAULT_SCHEMA_PATH = ROOT / "data/schemas/raw_flow_schema.json"
DEFAULT_CONTRACT_PATH = ROOT / "data/schemas/feature_dictionary.md"
DEFAULT_EXTRACTION_DIR = ROOT / "extraction"

COUNT_KEYS: Tuple[str, ...] = (
    "input_records",
    "valid_records",
    "quarantined_records",
    "rejected_records",
)

BENCHMARK_ARTIFACTS: Tuple[str, ...] = (
    "source-audit.json",
    "source-files.csv",
    "source-integrity-after.json",
    "size.json",
    "partitions.json",
    "workers-1.json",
    "workers-2.json",
    "summary.csv",
    "runs.csv",
    "minio-audit.json",
    "quality-example.json",
    "verification.json",
    "environment.json",
    "host-machine.json",
)

STAGE_LABELS: Tuple[Tuple[str, str], ...] = (
    ("ingestion_and_checksum", "Đọc dữ liệu (S3A) và tính SHA-256"),
    ("transformation_validation_and_before_report", "Chuyển đổi, validation và báo cáo chất lượng trước làm sạch"),
    ("cleaning_post_validation_and_after_report", "Cleaning, hậu kiểm, đối soát và báo cáo chất lượng sau làm sạch"),
    ("features_and_aggregation", "Tạo đặc trưng dẫn xuất và bảng tổng hợp"),
    ("output_write_and_read_back", "Ghi 5 dataset Parquet Snappy và đọc lại xác minh"),
)

GITHUB_BLOB_BASE = "https://github.com/BonM2/bigdata-pcap-anomaly/blob/master/"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def historical_bytes(path: Path, expected_hash: str) -> Tuple[bytes, str]:
    """Match SHA-256 while accounting for Git LF/CRLF checkout conversions."""
    payload = path.read_bytes()
    lf = payload.replace(b"\r\n", b"\n")
    crlf = lf.replace(b"\n", b"\r\n")
    if hashlib.sha256(payload).hexdigest().lower() == expected_hash.lower():
        eol_tag = "CRLF" if b"\r\n" in payload else "LF"
        return payload, eol_tag
    for mode, candidate in (("LF", lf), ("CRLF", crlf)):
        if hashlib.sha256(candidate).hexdigest().lower() == expected_hash.lower():
            return candidate, mode
    raise ValueError(f"Artifact checksum mismatch: {path.name}")


def fmt_int(value: int) -> str:
    return f"{int(value):,}".replace(",", ".")


def fmt_float(value: float, digits: int = 3) -> str:
    formatted = f"{value:,.{digits}f}"
    return formatted.replace(",", "_").replace(".", ",").replace("_", ".")


def fmt_percent(value: float, total: float, digits: int = 4) -> str:
    if total == 0:
        return fmt_float(0.0, digits) + "%"
    return fmt_float(100.0 * value / total, digits) + "%"


def md_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    def line(values: Sequence[Any]) -> str:
        return "| " + " | ".join(str(v).replace("|", "\\|") for v in values) + " |"

    return "\n".join([
        line(headers),
        line(["---"] * len(headers)),
        *(line(row) for row in rows),
    ])


def _display_path(path: Path) -> str:
    """Return POSIX path relative to ROOT when inside repo (Python 3.8+ compatible)."""
    try:
        return path.resolve().relative_to(ROOT.resolve()).as_posix()
    except ValueError:
        return str(path)


def _parse_int_token(token: str) -> int:
    cleaned = token.strip().replace(",", "").replace(".", "")
    return int(cleaned)


def parse_quality_audit_markdown(path: Path) -> Dict[str, Any]:
    """Parse retention, duplicate survey, label table and schema hashes from cicids2017-data-quality-audit.md."""
    text = path.read_text(encoding="utf-8")

    soft_match = re.search(
        r"Có \*\*([0-9,]+)\*\* dòng VALID mang ít nhất một lỗi mềm;\s*\*\*([0-9,]+)\*\* dòng VALID có ít nhất một giá trị được đổi thành NULL",
        text,
    )
    dropna_match = re.search(
        r"Nếu `dropna\(\)` toàn bộ 21 feature thì chỉ còn \*\*([0-9,]+)\*\* dòng \(([0-9.]+)%\)",
        text,
    )
    retention_eq_match = re.search(
        r"Đối soát training retention:\s*\*\*PASS\*\* — `([0-9,]+) = ([0-9,]+) \+ ([0-9,]+)`",
        text,
    )
    null_cells_match = re.search(r"Tổng lượt ô được đổi NULL:\s*\*\*([0-9,]+) ô\*\*", text)
    dup_match = re.search(
        r"\|\s*TOÀN BỘ \(kể cả trùng chéo file\)\s*\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*\|\s*([0-9,]+)\s*\|",
        text,
    )
    hash_match = re.search(
        r"SHA-256 schema:\s*`([0-9a-fA-F]{64})`;\s*SHA-256 data contract:\s*`([0-9a-fA-F]{64})`",
        text,
    )

    require(soft_match is not None, f"Missing soft/null VALID counts in {path.name}")
    require(dropna_match is not None, f"Missing dropna retention count in {path.name}")
    require(retention_eq_match is not None, f"Missing training retention reconciliation line in {path.name}")
    require(null_cells_match is not None, f"Missing total cleaned NULL cells count in {path.name}")
    require(dup_match is not None, f"Missing dataset duplicate survey row in {path.name}")
    require(hash_match is not None, f"Missing schema/contract SHA-256 line in {path.name}")

    valid_with_soft = _parse_int_token(soft_match.group(1))
    valid_with_cleaned_null = _parse_int_token(soft_match.group(2))
    complete_case_valid = _parse_int_token(dropna_match.group(1))
    dropna_pct_reported = float(dropna_match.group(2))
    eq_valid = _parse_int_token(retention_eq_match.group(1))
    eq_null = _parse_int_token(retention_eq_match.group(2))
    eq_complete = _parse_int_token(retention_eq_match.group(3))

    require(
        eq_valid == eq_null + eq_complete
        and eq_null == valid_with_cleaned_null
        and eq_complete == complete_case_valid,
        f"Inconsistent training retention reconciliation inside {path.name}",
    )

    # Parse Section 4 per-label VALID/quarantined table for cross-checking.
    labels_after: Dict[str, Dict[str, int]] = {}
    in_label_table = False
    for line in text.splitlines():
        if line.startswith("| Nhãn raw | Trước | VALID sau |"):
            in_label_table = True
            continue
        if in_label_table:
            if not line.startswith("|"):
                break
            if line.startswith("|---"):
                continue
            parts = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(parts) >= 6:
                labels_after[parts[0]] = {
                    "before": _parse_int_token(parts[1]),
                    "valid_after": _parse_int_token(parts[2]),
                    "quarantined": _parse_int_token(parts[3]),
                    "affected": _parse_int_token(parts[4]),
                    "valid_with_cleaned_null": _parse_int_token(parts[5]),
                }
    require(len(labels_after) == 15, f"Expected 15 labels in {path.name}, found {len(labels_after)}")

    return {
        "source_path": _display_path(path),
        "valid_records": eq_valid,
        "valid_with_soft_issues": valid_with_soft,
        "valid_with_cleaned_null": valid_with_cleaned_null,
        "complete_case_valid_records": complete_case_valid,
        "dropna_retained_pct_of_input_reported": dropna_pct_reported,
        "total_cleaned_null_cells": _parse_int_token(null_cells_match.group(1)),
        "duplicate_survey": {
            "duplicate_groups": _parse_int_token(dup_match.group(1)),
            "records_in_duplicate_groups": _parse_int_token(dup_match.group(2)),
            "excess_duplicate_records": _parse_int_token(dup_match.group(3)),
        },
        "schema_sha256_reported": hash_match.group(1).lower(),
        "contract_sha256_reported": hash_match.group(2).lower(),
        "labels_after": labels_after,
    }


def parse_defense_value_demo(path: Path) -> Dict[str, Any]:
    """Parse the documented 24/11/2026 local Value demo top-5 table from defense-demo-script.md."""
    text = path.read_text(encoding="utf-8")
    anchor = "### B3. Value từ bảng tổng hợp của 8 CSV thật"
    pos = text.find(anchor)
    require(pos != -1, f"Missing Value demo section in {path.name}")
    section = text[pos:]

    rows: List[Dict[str, Any]] = []
    in_table = False
    for line in section.splitlines():
        if line.startswith("| Hạng | Nhãn | Flow `VALID` | Tổng payload (byte) |"):
            in_table = True
            continue
        if in_table:
            if not line.startswith("|"):
                break
            if "---" in line:
                continue
            parts = [cell.strip() for cell in line.strip().strip("|").split("|")]
            if len(parts) == 4:
                rows.append({
                    "rank": _parse_int_token(parts[0]),
                    "label": parts[1],
                    "valid_records": _parse_int_token(parts[2]),
                    "total_bytes_sum": _parse_int_token(parts[3]),
                })
    require(len(rows) == 5, f"Expected 5 top-label rows in {path.name} B3 section, found {len(rows)}")
    return {
        "source_path": _display_path(path),
        "scope": "Documented local Spark 3.5.0 demo from 24/11/2026 on 8 verified local CSVs (NOT a live MinIO snapshot)",
        "top_labels": rows,
    }


def inspect_zeek_demo(path: Path) -> Dict[str, Any]:
    """Inspect the committed Zeek JSON Lines demo fixture on disk."""
    require(path.exists(), f"Missing Zeek demo fixture: {path}")
    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    protos: Counter[str] = Counter()
    missing_service = 0
    for index, line in enumerate(lines, start=1):
        record = json.loads(line)
        require(isinstance(record, dict), f"Invalid JSON object in {path.name} line {index}")
        protos[str(record.get("proto", ""))] += 1
        if "service" not in record or record["service"] is None:
            missing_service += 1
    require(len(lines) == 360, f"Expected 360 Zeek demo rows, got {len(lines)}")
    return {
        "source_path": _display_path(path),
        "records": len(lines),
        "protocols": dict(sorted(protos.items())),
        "missing_service_records": missing_service,
        "origin_note": "Generated offline by Zeek from synthetic PCAP for Variety schema/format demo",
    }


def inspect_extraction_placeholders(extraction_dir: Path) -> Dict[str, Any]:
    """Confirm whether extraction/ contains only placeholder stubs."""
    files = sorted(p for p in extraction_dir.iterdir() if p.is_file()) if extraction_dir.exists() else []
    entries = [{"file": p.name, "bytes": p.stat().st_size} for p in files]
    all_stubs = bool(entries) and all(item["bytes"] <= 1 for item in entries)
    return {
        "directory": _display_path(extraction_dir),
        "files": entries,
        "is_placeholder_only": all_stubs,
    }


def validate_value_snapshot(
    snapshot: Mapping[str, Any],
    *,
    expected_full_counts: Mapping[str, int],
) -> Dict[str, Any]:
    """Validate an optional live snapshot JSON produced by experiments/verify_value_outputs.py."""
    require(snapshot.get("status") == "PASS", "Live Value snapshot status must be PASS")
    marker = snapshot.get("marker") or {}
    require(marker.get("run_status") == "COMPLETE", "Live Value snapshot _SUCCESS.json run_status must be COMPLETE")
    reconciliation = snapshot.get("reconciliation") or {}
    require(reconciliation.get("status") == "PASS", "Live Value snapshot reconciliation must be PASS")
    require(
        int(reconciliation["input_records"])
        == int(reconciliation["valid_records"])
        + int(reconciliation["quarantined_records"])
        + int(reconciliation["rejected_records"]),
        "Live Value snapshot failed record conservation",
    )
    value_agg = snapshot.get("value_aggregates") or {}
    top_labels = value_agg.get("top_labels_by_total_bytes") or []
    require(len(top_labels) >= 1, "Live Value snapshot missing top_labels_by_total_bytes")
    matches_full_cicids = all(
        int(reconciliation[key]) == int(expected_full_counts[key]) for key in COUNT_KEYS
    )
    return {
        "attached": True,
        "status": "PASS",
        "batch_id": marker.get("batch_id"),
        "generated_at_utc": snapshot.get("generated_at_utc"),
        "matches_full_cicids2017_counts": matches_full_cicids,
        "reconciliation": reconciliation,
        "schema_verification": snapshot.get("schema_verification") or {},
        "datasets": snapshot.get("datasets") or {},
        "value_aggregates": value_agg,
        "samples": snapshot.get("samples") or {},
    }


def collect_value_evidence(
    *,
    evidence_dir: Path = DEFAULT_EVIDENCE_DIR,
    schema_path: Path = DEFAULT_SCHEMA_PATH,
    contract_path: Path = DEFAULT_CONTRACT_PATH,
    quality_audit_md: Path = DEFAULT_QUALITY_AUDIT_MD,
    defense_script_md: Path = DEFAULT_DEFENSE_SCRIPT_MD,
    zeek_sample_path: Path = DEFAULT_ZEEK_SAMPLE,
    extraction_dir: Path = DEFAULT_EXTRACTION_DIR,
    value_snapshot_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Load, verify SHA-256 checksums, and cross-check all 5V artifacts offline."""
    manifest_path = evidence_dir / "artifact-sha256.csv"
    require(manifest_path.exists(), f"Missing artifact checksum manifest: {manifest_path}")
    with manifest_path.open(encoding="utf-8-sig", newline="") as stream:
        manifest = {row["File"]: row for row in csv.DictReader(stream)}

    checksum_results: List[Dict[str, Any]] = []
    for name in BENCHMARK_ARTIFACTS:
        require(name in manifest, f"Missing {name} in artifact-sha256.csv")
        artifact_path = evidence_dir / name
        require(artifact_path.exists(), f"Missing benchmark artifact: {artifact_path}")
        payload, eol_mode = historical_bytes(artifact_path, manifest[name]["SHA256"])
        expected_bytes = int(manifest[name]["Bytes"])
        require(len(payload) == expected_bytes, f"Artifact byte size mismatch for {name}")
        checksum_results.append({
            "file": name,
            "bytes": expected_bytes,
            "sha256": manifest[name]["SHA256"].lower(),
            "eol_match_mode": eol_mode,
            "status": "PASS",
        })

    source = read_json(evidence_dir / "source-audit.json")
    integrity = read_json(evidence_dir / "source-integrity-after.json")
    size_data = read_json(evidence_dir / "size.json")
    partitions_data = read_json(evidence_dir / "partitions.json")
    workers1_data = read_json(evidence_dir / "workers-1.json")
    workers2_data = read_json(evidence_dir / "workers-2.json")
    minio_audit = read_json(evidence_dir / "minio-audit.json")
    quality_example = read_json(evidence_dir / "quality-example.json")
    verification = read_json(evidence_dir / "verification.json")
    environment = read_json(evidence_dir / "environment.json")
    host_machine = read_json(evidence_dir / "host-machine.json")

    # Verify schema & Data Contract against source-audit.json (LF/CRLF tolerant).
    _, schema_eol = historical_bytes(schema_path, source["schema_sha256"])
    _, contract_eol = historical_bytes(contract_path, source["contract_sha256"])
    contract = read_json(schema_path)
    core_fields = [f for f in contract["fields"] if f["in_cleaned_core"]]
    require(len(contract["fields"]) == 79 and len(core_fields) == 22, "Contract field count mismatch")
    core_types = Counter(f["cleaned_target_type"] for f in core_fields)

    # Verify source-audit.json, source-files.csv, and source-integrity-after.json.
    require(source["structurally_ready_for_upload"] is True, "Source audit not structurally ready")
    files = source["files"]
    require(len(files) == 8 and len({f["source_file"] for f in files}) == 8, "Expected 8 unique CSV files")

    totals: Counter[str] = Counter()
    labels: Counter[str] = Counter()
    cells: Counter[Tuple[str, str]] = Counter()
    for item in files:
        require(item["header_matches_exact"] and item["header_columns"] == 79, "Header mismatch in source-audit")
        require(item["row_width_distribution"] == {"79": item["input_records"]}, "Row width mismatch")
        require(item["malformed_records"] == 0 and item["utf8_decode_error_records"] == 0, "Malformed source file")
        require(
            item["duplicate_headers"] == {"Fwd Header Length": [35, 56]}
            and item["duplicate_header_value_mismatches"] == 0,
            "Duplicate-header audit mismatch",
        )
        require(sum(item["label_distribution"].values()) == item["input_records"], "Per-file label sum mismatch")
        for key in COUNT_KEYS:
            totals[key] += item["input_records"] if key == "input_records" else item["policy_preview"][key]
        totals["valid_records_with_soft_issues"] += item["policy_preview"]["valid_records_with_soft_issues"]
        labels.update(item["label_distribution"])
        for field_name, stats in item["fields"].items():
            for kind, count in stats["counts"].items():
                cells[(field_name, kind)] += count

    require(totals["input_records"] == source["input_records"], "Input record sum mismatch")
    require(sum(item["input_bytes"] for item in files) == source["input_bytes"], "Input byte sum mismatch")
    require(
        totals["input_records"]
        == totals["valid_records"] + totals["quarantined_records"] + totals["rejected_records"],
        "Source record conservation failed",
    )
    require(integrity["status"] == "PASS", "source-integrity-after.json status is not PASS")
    require(
        {f["file"] for f in integrity["files"] if f["unchanged"]} == {f["source_file"] for f in files},
        "Post-benchmark source integrity mismatch",
    )

    with (evidence_dir / "source-files.csv").open(encoding="utf-8-sig", newline="") as stream:
        source_csv_rows = list(csv.DictReader(stream))
    require(len(source_csv_rows) == 8, "source-files.csv must list 8 files")
    source_by_name = {f["source_file"]: f for f in files}
    for row in source_csv_rows:
        item = source_by_name[row["File"]]
        require(int(row["Bytes"]) == item["input_bytes"], f"source-files.csv bytes mismatch: {row['File']}")
        require(int(row["Flows"]) == item["input_records"], f"source-files.csv flows mismatch: {row['File']}")
        require(row["SHA256"].lower() == item["source_sha256"].lower(), f"source-files.csv SHA mismatch: {row['File']}")
        require(int(row["PreviewValid"]) == item["policy_preview"]["valid_records"], "PreviewValid mismatch")
        require(
            int(row["PreviewQuarantined"]) == item["policy_preview"]["quarantined_records"],
            "PreviewQuarantined mismatch",
        )
        require(int(row["PreviewRejected"]) == item["policy_preview"]["rejected_records"], "PreviewRejected mismatch")

    # Verify all 4 benchmark JSONs, MinIO audit (31 batches / 155 outputs), and verification.json.
    for label, dataset in (
        ("size.json", size_data),
        ("partitions.json", partitions_data),
        ("workers-1.json", workers1_data),
        ("workers-2.json", workers2_data),
    ):
        require(dataset["run_status"] == "COMPLETE" and dataset["dataset_kind"] == "real", f"Invalid {label}")

    require(minio_audit["status"] == "PASS", "minio-audit.json status is not PASS")
    require(verification["status"] == "PASS", "verification.json status is not PASS")
    require(
        minio_audit["batch_count"] == len(minio_audit["batches"]) == 31
        and minio_audit["parquet_outputs"] == 155,
        "MinIO audit must verify 31 batches and 155 Parquet outputs",
    )
    require(
        verification["unique_batches"] == 31
        and verification["runs_csv_rows"] == 31
        and verification["warmup_rows"] == 4
        and verification["summary_configurations"] == 9,
        "verification.json totals disagree",
    )

    stored_by_batch = {item["batch_id"]: item for item in minio_audit["batches"]}
    require(len(stored_by_batch) == 31, "Duplicate batch IDs in minio-audit.json")
    for item in minio_audit["batches"]:
        q = item["quality"]
        require(
            item["stored_marker_status"] == "COMPLETE"
            and item["post_cleaning_status"] == "PASS"
            and item["parquet_outputs"] == 5
            and q["input_records"] == q["valid_records"] + q["quarantined_records"] + q["rejected_records"],
            f"Batch failed MinIO audit checks: {item['batch_id']}",
        )

    # Cross-check all 31 batch IDs across benchmark JSONs and runs.csv.
    all_json_batches = set()
    for dataset in (size_data, partitions_data, workers1_data, workers2_data):
        for run in dataset["runs"] + dataset.get("warmup_runs", []):
            all_json_batches.add(run["batch_id"])
    require(all_json_batches == set(stored_by_batch), "Benchmark JSON batches disagree with minio-audit.json")

    # Verify the 3 full-data Spark trials in size.json.
    full_runs = [r for r in size_data["runs"] if r["quality"]["input_records"] == totals["input_records"]]
    require(len(full_runs) == 3, f"Expected 3 full-data Spark trials, found {len(full_runs)}")
    expected_source_hashes = {f["source_file"]: f["source_sha256"] for f in files}
    expected_quality_dict = {key: totals[key] for key in COUNT_KEYS}
    output_column_counts: Dict[str, int] = {}

    for run in full_runs:
        require(run["quality"] == expected_quality_dict, "Full-data Spark counts disagree with source-audit")
        require(run["post_cleaning_status"] == "PASS", "Full-data trial post_cleaning_status != PASS")
        require(
            {f["source_file"]: f["source_sha256"] for f in run["files"]} == expected_source_hashes,
            "Full-data trial source SHA-256 mismatch",
        )
        marker = stored_by_batch[run["batch_id"]]
        require(
            marker["stored_marker_status"] == "COMPLETE" and marker["quality"] == run["quality"],
            "Full-data stored marker mismatch",
        )
        require(len(run["outputs"]) == 5, "Full-data trial missing one of 5 Parquet outputs")
        expected_output_counts = {
            "cleaned_path": totals["valid_records"],
            "features_path": totals["valid_records"],
            "aggregates_path": 46,
            "quarantine_path": totals["quarantined_records"],
            "rejected_path": totals["rejected_records"],
        }
        for key, out in run["outputs"].items():
            require(
                out["read_back_status"] == "PASS" and out["compression"] == "snappy",
                f"Output read-back or compression failed: {key}",
            )
            require(
                out["records"] == expected_output_counts[key],
                f"Output record count mismatch for {key}: {out['records']} != {expected_output_counts[key]}",
            )
            output_column_counts[key] = len(out["schema"]["fields"])

    # Verify runs.csv vs summary.csv (Velocity cross-check).
    with (evidence_dir / "runs.csv").open(encoding="utf-8-sig", newline="") as stream:
        runs_rows = list(csv.DictReader(stream))
    with (evidence_dir / "summary.csv").open(encoding="utf-8-sig", newline="") as stream:
        summary_rows = list(csv.DictReader(stream))

    require(len(runs_rows) == 31, f"Expected 31 rows in runs.csv, got {len(runs_rows)}")
    require({r["batch_id"] for r in runs_rows} == set(stored_by_batch), "runs.csv batch IDs disagree with minio-audit")
    require(len(summary_rows) == 9, f"Expected 9 configurations in summary.csv, got {len(summary_rows)}")

    measured_runs = [r for r in runs_rows if r["warmup"] == "False"]
    warmup_runs = [r for r in runs_rows if r["warmup"] == "True"]
    require(len(measured_runs) == 27 and len(warmup_runs) == 4, "Expected 27 measured trials and 4 warmups")

    grouped_runs: Dict[Tuple[str, int, int, int], List[Dict[str, str]]] = {}
    for row in measured_runs:
        group_key = (
            row["dimension"],
            int(row["input_records"]),
            int(row["worker_count"]),
            int(row["shuffle_partitions"]),
        )
        grouped_runs.setdefault(group_key, []).append(row)

    velocity_summary: Dict[str, List[Dict[str, Any]]] = {"size": [], "workers": [], "partitions": []}
    for row in summary_rows:
        group_key = (
            row["dimension"],
            int(row["input_records"]),
            int(row["worker_count"]),
            int(row["shuffle_partitions"]),
        )
        trials = grouped_runs.get(group_key) or []
        require(len(trials) == int(row["repetitions"]) == 3, f"Trial count mismatch for {group_key}")
        durations = [float(t["processing_duration_seconds"]) for t in trials]
        throughputs = [float(t["throughput_flows_per_second"]) for t in trials]
        for t in trials:
            recomputed_tp = int(t["input_records"]) / float(t["processing_duration_seconds"])
            require(
                math.isclose(recomputed_tp, float(t["throughput_flows_per_second"]), rel_tol=1e-9),
                f"Throughput formula mismatch for batch {t['batch_id']}",
            )
        require(
            math.isclose(statistics.median(durations), float(row["median_seconds"]), rel_tol=1e-9),
            f"summary.csv median_seconds mismatch for {group_key}",
        )
        require(
            math.isclose(min(durations), float(row["min_seconds"]), rel_tol=1e-9)
            and math.isclose(max(durations), float(row["max_seconds"]), rel_tol=1e-9),
            f"summary.csv min/max mismatch for {group_key}",
        )
        require(
            math.isclose(statistics.median(throughputs), float(row["median_flows_per_second"]), rel_tol=1e-9),
            f"summary.csv median throughput mismatch for {group_key}",
        )
        speedup = float(row["speedup"]) if row["speedup"] else None
        velocity_summary[row["dimension"]].append({
            "input_records": int(row["input_records"]),
            "input_bytes": int(row["input_bytes"]),
            "worker_count": int(row["worker_count"]),
            "shuffle_partitions": int(row["shuffle_partitions"]),
            "input_partitions": int(trials[0]["input_partitions"]),
            "repetitions": int(row["repetitions"]),
            "median_seconds": float(row["median_seconds"]),
            "min_seconds": float(row["min_seconds"]),
            "max_seconds": float(row["max_seconds"]),
            "median_flows_per_second": float(row["median_flows_per_second"]),
            "speedup": speedup,
        })

    # Verify speedup math against matched baselines (workers=1, partitions=4).
    w_base = next(item for item in velocity_summary["workers"] if item["worker_count"] == 1)
    for item in velocity_summary["workers"]:
        expected_sp = w_base["median_seconds"] / item["median_seconds"]
        require(
            item["speedup"] is not None and math.isclose(item["speedup"], expected_sp, rel_tol=1e-9),
            f"Worker speedup mismatch for worker_count={item['worker_count']}",
        )
    p_base = next(item for item in velocity_summary["partitions"] if item["shuffle_partitions"] == 4)
    for item in velocity_summary["partitions"]:
        expected_sp = p_base["median_seconds"] / item["median_seconds"]
        require(
            item["speedup"] is not None and math.isclose(item["speedup"], expected_sp, rel_tol=1e-9),
            f"Partition speedup mismatch for shuffle_partitions={item['shuffle_partitions']}",
        )

    # Full-data stage timing medians across the 3 full runs.
    full_csv_runs = [
        r for r in measured_runs if r["dimension"] == "size" and int(r["input_records"]) == totals["input_records"]
    ]
    require(len(full_csv_runs) == 3, "Expected 3 full-data rows in runs.csv")
    stage_samples: Dict[str, List[float]] = {}
    for row in full_csv_runs:
        stage_dict = json.loads(row["stage_seconds"])
        for stage_key, seconds in stage_dict.items():
            stage_samples.setdefault(stage_key, []).append(float(seconds))
    stage_medians: List[Dict[str, Any]] = []
    for stage_key, stage_label in STAGE_LABELS:
        vals = stage_samples.get(stage_key) or []
        require(len(vals) == 3, f"Missing stage timings for {stage_key}")
        stage_medians.append({
            "stage": stage_key,
            "label": stage_label,
            "median_seconds": statistics.median(vals),
            "min_seconds": min(vals),
            "max_seconds": max(vals),
        })

    # Parse and cross-check retention vs dropna() from cicids2017-data-quality-audit.md.
    quality_audit = parse_quality_audit_markdown(quality_audit_md)
    require(
        quality_audit["valid_records"] == totals["valid_records"],
        "Quality audit valid_records disagrees with source-audit.json",
    )
    require(
        quality_audit["valid_with_soft_issues"] == totals["valid_records_with_soft_issues"],
        "Quality audit valid_with_soft_issues disagrees with source-audit.json",
    )
    require(
        quality_audit["valid_with_cleaned_null"] <= totals["valid_records_with_soft_issues"],
        "valid_with_cleaned_null cannot exceed valid_records_with_soft_issues",
    )
    require(
        totals["valid_records_with_soft_issues"] - quality_audit["valid_with_cleaned_null"]
        <= cells[("label", "encoding_replacement")],
        "Difference between soft-issue VALID rows and cleaned-NULL VALID rows exceeds label encoding issues",
    )
    require(
        quality_audit["valid_with_cleaned_null"]
        >= max(
            cells[("init_win_bytes_forward", "sentinel_minus_one")],
            cells[("init_win_bytes_backward", "sentinel_minus_one")],
        ),
        "valid_with_cleaned_null must be at least the largest single-column sentinel count",
    )
    for label_name, count_before in labels.items():
        audit_entry = quality_audit["labels_after"].get(label_name)
        require(audit_entry is not None, f"Missing label in quality audit report: {label_name}")
        require(audit_entry["before"] == count_before, f"Label count mismatch for {label_name}")
        require(
            audit_entry["valid_after"] + audit_entry["quarantined"] == audit_entry["before"],
            f"Per-label conservation failed for {label_name}",
        )

    # Parse documented 24/11/2026 defense Value demo and cross-check its VALID counts.
    defense_demo = parse_defense_value_demo(defense_script_md)
    for item in defense_demo["top_labels"]:
        audit_entry = quality_audit["labels_after"].get(item["label"])
        require(audit_entry is not None, f"Unknown label in defense demo: {item['label']}")
        require(
            item["valid_records"] == audit_entry["valid_after"],
            f"Defense demo valid_records mismatch for {item['label']}: "
            f"{item['valid_records']} != {audit_entry['valid_after']}",
        )

    # Inspect Zeek Variety fixture and extraction/ stub status directly on disk.
    zeek_demo = inspect_zeek_demo(zeek_sample_path)
    extraction_status = inspect_extraction_placeholders(extraction_dir)

    # Record concrete discrepancies found between JSON/CSV ground truth and Markdown prose.
    discrepancies: List[Dict[str, str]] = []
    qe_batch_id = quality_example["batch_id"]
    qe_in_size = any(r["batch_id"] == qe_batch_id for r in size_data["runs"])
    qe_in_partitions = any(r["batch_id"] == qe_batch_id for r in partitions_data["runs"])
    readme_text = (evidence_dir / "README.md").read_text(encoding="utf-8")
    if qe_in_size and not qe_in_partitions and "trong nhóm partition" in readme_text:
        discrepancies.append({
            "id": "QUALITY_EXAMPLE_DIMENSION_PROSE",
            "severity": "minor_prose",
            "artifact_truth": (
                f"`quality-example.json` (`batch_id={qe_batch_id}`) thực tế thuộc nhóm `size.json` lượt 1 "
                "(Friday Morning, 191.033 flow), được xác nhận trong `runs.csv` và `size.json`."
            ),
            "markdown_text": (
                "`docs/benchmarks/real-20261110-141211/README.md` mô tả `quality-example.json` "
                "là ví dụ 'trong nhóm partition'."
            ),
            "resolution": "Lấy artifact JSON/CSV gốc (`size.json`, `runs.csv`) làm chuẩn.",
        })

    if (
        quality_audit["schema_sha256_reported"] != source["schema_sha256"].lower()
        or quality_audit["contract_sha256_reported"] != source["contract_sha256"].lower()
    ):
        # Confirm both hashes correspond to LF vs CRLF of the exact same files on disk.
        _, audit_schema_eol = historical_bytes(schema_path, quality_audit["schema_sha256_reported"])
        _, audit_contract_eol = historical_bytes(contract_path, quality_audit["contract_sha256_reported"])
        discrepancies.append({
            "id": "SCHEMA_CONTRACT_SHA256_EOL",
            "severity": "eol_only",
            "artifact_truth": (
                f"`source-audit.json` ghi SHA-256 dạng {schema_eol}/{contract_eol} "
                f"(`{source['schema_sha256'][:12]}...` / `{source['contract_sha256'][:12]}...`)."
            ),
            "markdown_text": (
                f"`cicids2017-data-quality-audit.md` ghi SHA-256 dạng {audit_schema_eol}/{audit_contract_eol} "
                f"(`{quality_audit['schema_sha256_reported'][:12]}...` / `{quality_audit['contract_sha256_reported'][:12]}...`)."
            ),
            "resolution": (
                "Cả hai cặp hash đều khớp cùng nội dung file trên đĩa, chỉ khác ký tự xuống dòng LF/CRLF khi checkout Git."
            ),
        })

    # Optional live MinIO snapshot from experiments/verify_value_outputs.py.
    live_snapshot: Dict[str, Any] = {
        "attached": False,
        "status": "NOT_RUN",
        "note": (
            "No --value-snapshot JSON was supplied during this offline build; "
            "live MinIO ranking and 5-row Parquet samples are omitted rather than fabricated."
        ),
    }
    if value_snapshot_path is not None:
        require(value_snapshot_path.exists(), f"Value snapshot file not found: {value_snapshot_path}")
        snapshot_raw = read_json(value_snapshot_path)
        live_snapshot = validate_value_snapshot(snapshot_raw, expected_full_counts=expected_quality_dict)
        live_snapshot["snapshot_path"] = _display_path(value_snapshot_path)

    input_bytes = int(source["input_bytes"])
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "real-20261110-141211",
        "checksums": checksum_results,
        "schema": {
            "version": contract["contract_reference"]["contract_version"],
            "raw_columns": len(contract["fields"]),
            "core_columns": len(core_fields),
            "core_types": dict(core_types),
            "output_column_counts": output_column_counts,
            "schema_sha256_source_audit": source["schema_sha256"],
            "contract_sha256_source_audit": source["contract_sha256"],
            "schema_eol_match": schema_eol,
            "contract_eol_match": contract_eol,
        },
        "volume": {
            "csv_files": len(files),
            "input_records": totals["input_records"],
            "input_bytes": input_bytes,
            "decimal_mb": input_bytes / 1_000_000.0,
            "decimal_gb": input_bytes / 1_000_000_000.0,
            "binary_mib": input_bytes / (1024.0 ** 2),
            "full_input_partitions": next(
                item["input_partitions"]
                for item in velocity_summary["size"]
                if item["input_records"] == totals["input_records"]
            ),
        },
        "veracity": {
            "totals": {key: totals[key] for key in COUNT_KEYS},
            "valid_records_with_soft_issues": totals["valid_records_with_soft_issues"],
            "cells": {
                "flow_bytes_s_nan": cells[("flow_bytes_s", "nan")],
                "flow_bytes_s_positive_infinity": cells[("flow_bytes_s", "positive_infinity")],
                "flow_bytes_s_negative": cells[("flow_bytes_s", "negative")],
                "flow_packets_s_positive_infinity": cells[("flow_packets_s", "positive_infinity")],
                "flow_packets_s_negative": cells[("flow_packets_s", "negative")],
                "flow_duration_negative": cells[("flow_duration", "negative")],
                "flow_iat_mean_negative": cells[("flow_iat_mean", "negative")],
                "init_win_bytes_forward_sentinel": cells[("init_win_bytes_forward", "sentinel_minus_one")],
                "init_win_bytes_backward_sentinel": cells[("init_win_bytes_backward", "sentinel_minus_one")],
                "label_encoding_replacement": cells[("label", "encoding_replacement")],
            },
            "labels": dict(labels.most_common()),
            "full_trials": [
                {
                    "repeat": r["repeat"],
                    "batch_id": r["batch_id"],
                    "quality": r["quality"],
                    "post_cleaning_status": r["post_cleaning_status"],
                    "parquet_read_back": "5/5 PASS",
                    "aggregates_records": r["outputs"]["aggregates_path"]["records"],
                }
                for r in full_runs
            ],
            "minio_audit": {
                "status": minio_audit["status"],
                "batch_count": minio_audit["batch_count"],
                "measured_trials": len(measured_runs),
                "warmups": len(warmup_runs),
                "parquet_outputs": minio_audit["parquet_outputs"],
            },
            "quality_audit": quality_audit,
            "quality_example": {
                "batch_id": quality_example["batch_id"],
                "source_file": quality_example["files"][0]["source_file"],
                "input_records": quality_example["input_records"],
                "valid_records": quality_example["valid_records"],
                "quarantined_records": quality_example["quarantined_records"],
                "rejected_records": quality_example["rejected_records"],
            },
        },
        "velocity": {
            "summary": velocity_summary,
            "stage_medians_full": stage_medians,
            "environment": {
                "cpu": host_machine["CPU"],
                "physical_cores": host_machine["PhysicalCores"],
                "host_ram_gib": host_machine["HostMemoryBytes"] / (1024.0 ** 3),
                "docker_cpus": environment["docker"]["NCPU"],
                "docker_ram_gib": environment["docker"]["MemTotal"] / (1024.0 ** 3),
                "docker_version": environment["docker"]["ServerVersion"],
                "spark_version": full_runs[0]["performance"]["spark_version"],
            },
        },
        "variety": {
            "zeek_demo": zeek_demo,
        },
        "value": {
            "live_snapshot": live_snapshot,
            "defense_demo_reference": defense_demo,
            "extraction_status": extraction_status,
        },
        "discrepancies": discrepancies,
    }


def build_verification_summary(evidence: Mapping[str, Any]) -> Dict[str, Any]:
    """Create the machine-readable 5V cross-check verification summary JSON."""
    vol = evidence["volume"]
    ver = evidence["veracity"]
    qa = ver["quality_audit"]
    vel_full = next(
        item for item in evidence["velocity"]["summary"]["size"] if item["input_records"] == vol["input_records"]
    )
    w1 = next(item for item in evidence["velocity"]["summary"]["workers"] if item["worker_count"] == 1)
    w2 = next(item for item in evidence["velocity"]["summary"]["workers"] if item["worker_count"] == 2)
    p4 = next(item for item in evidence["velocity"]["summary"]["partitions"] if item["shuffle_partitions"] == 4)
    slowest_stage = max(evidence["velocity"]["stage_medians_full"], key=lambda item: item["median_seconds"])

    return {
        "status": "PASS",
        "generated_at_utc": evidence["generated_at_utc"],
        "experiment_id": evidence["experiment_id"],
        "scope": "Offline 5V cross-check across committed benchmark JSON/CSV artifacts, schema/contract, audit report, and Zeek fixture",
        "checks": {
            "artifact_sha256_count": len(evidence["checksums"]),
            "artifact_sha256_status": "PASS",
            "schema_and_contract_checksum_status": "PASS",
            "record_conservation_status": "PASS",
            "record_conservation": ver["totals"],
            "full_data_spark_trials_pass": f"{len(ver['full_trials'])}/{len(ver['full_trials'])} PASS",
            "full_data_parquet_read_back": "5/5 PASS (all 3 full trials)",
            "minio_historical_audit": ver["minio_audit"],
            "velocity_summary_vs_runs_status": "PASS",
            "retention_vs_dropna_status": "PASS",
            "retention_vs_dropna": {
                "input_records": vol["input_records"],
                "policy_valid_records": ver["totals"]["valid_records"],
                "policy_valid_pct": round(100.0 * ver["totals"]["valid_records"] / vol["input_records"], 4),
                "valid_with_soft_issues": ver["valid_records_with_soft_issues"],
                "valid_with_cleaned_null": qa["valid_with_cleaned_null"],
                "naive_dropna_records": qa["complete_case_valid_records"],
                "naive_dropna_pct_of_input": round(
                    100.0 * qa["complete_case_valid_records"] / vol["input_records"], 4
                ),
                "rows_preserved_over_dropna": ver["totals"]["valid_records"] - qa["complete_case_valid_records"],
            },
            "velocity_highlights": {
                "full_8csv_median_seconds": round(vel_full["median_seconds"], 3),
                "full_8csv_median_flows_per_second": round(vel_full["median_flows_per_second"], 3),
                "workers_1_vs_2_speedup": round(w2["speedup"], 3),
                "workers_1_median_seconds": round(w1["median_seconds"], 3),
                "workers_2_median_seconds": round(w2["median_seconds"], 3),
                "partitions_p4_median_seconds": round(p4["median_seconds"], 3),
                "slowest_full_stage": slowest_stage["stage"],
                "slowest_full_stage_median_seconds": round(slowest_stage["median_seconds"], 3),
            },
            "variety_zeek_demo": evidence["variety"]["zeek_demo"],
            "live_value_snapshot_attached": evidence["value"]["live_snapshot"]["attached"],
            "live_value_snapshot_status": evidence["value"]["live_snapshot"]["status"],
        },
        "discrepancies": evidence["discrepancies"],
    }


def _render_live_snapshot_block(evidence: Mapping[str, Any]) -> str:
    live = evidence["value"]["live_snapshot"]
    ref = evidence["value"]["defense_demo_reference"]

    ref_table = md_table(
        ["Hạng", "Nhãn", "Flow `VALID`", "Tổng payload (byte)"],
        [
            [item["rank"], item["label"], fmt_int(item["valid_records"]), fmt_int(item["total_bytes_sum"])]
            for item in ref["top_labels"]
        ],
    )

    if not live["attached"]:
        return f"""**Trạng thái live snapshot MinIO:** **CHƯA CHẠY**. Báo cáo này được tạo offline. Làm theo mục 6.1 để chụp snapshot của batch hoàn tất, sao chép JSON ra máy và tạo lại báo cáo.

**Bảng Q.2. Tham chiếu truy vấn Value cục bộ ngày 24/11/2026 từ kịch bản bảo vệ (không thay thế live snapshot MinIO mới) [A6].**

Kịch bản bảo vệ (`docs/defense/defense-demo-script.md`, mục B3) ghi nhận lượt truy vấn Spark 3.5.0 cục bộ trên cùng 8 CSV thật đã kiểm toán checksum (46 dòng tổng hợp, đối soát `2.830.743 = 2.830.628 VALID + 115 QUARANTINED + 0 REJECTED`, khớp 100% số dòng `VALID` theo nhãn với báo cáo kiểm toán chất lượng):

{ref_table}

Điểm đáng chú ý về mặt **Value**: `Heartbleed` chỉ có **11 flow** nhưng đứng thứ **4** về tổng payload byte (**80.177.574 byte**), vượt cả `DoS GoldenEye` (**10.293 flow**, **71.842.537 byte**). Điều này cho thấy bảng `processed/aggregates/` cho phép đối chiếu đồng thời số flow và khối lượng payload theo nhãn, mang lại góc nhìn mà việc đếm dòng thô đơn lẻ không thể hiện được."""

    rec = live["reconciliation"]
    ranking = live["value_aggregates"]
    top_rows = ranking["top_labels_by_total_bytes"]
    top_table = md_table(
        ["Hạng", "Nhãn", "Số flow", "Flow `VALID`", "Tổng packet", "Tổng payload (byte)", "Tỷ trọng byte"],
        [
            [
                item["rank"],
                item["label"],
                fmt_int(item["flow_count"]),
                fmt_int(item["valid_records"]),
                fmt_int(item["total_packets_sum"]),
                fmt_int(item["total_bytes_sum"]),
                fmt_float(item["byte_share_pct"], 4) + "%",
            ]
            for item in top_rows
        ],
    )
    dataset_table = md_table(
        ["Dataset Parquet", "Số dòng thực đo", "Số cột", "Đường dẫn đã xác minh"],
        [
            [
                name,
                fmt_int(meta["records"]),
                meta["column_count"],
                f"`{meta['path']}`",
            ]
            for name, meta in live["datasets"].items()
        ],
    )

    sample_blocks: List[str] = []
    sample_titles = (
        ("cleaned", "5 dòng mẫu từ `cleaned/flows/`"),
        ("features", "5 dòng mẫu từ `processed/derived_features/`"),
        ("aggregates", "5 dòng mẫu từ `processed/aggregates/`"),
        ("quarantine", "5 dòng mẫu từ `quality/quarantine/`"),
    )
    for key, title in sample_titles:
        rows = live["samples"].get(key) or []
        if not rows:
            sample_blocks.append(f"*{title}:* Không có bản ghi mẫu trong batch này (0 dòng).")
            continue
        headers = list(rows[0].keys())
        body = [[row.get(col, "") for col in headers] for row in rows]
        sample_blocks.append(f"**{title}:**\n\n{md_table(headers, body)}")

    scope_note = (
        "Khớp hoàn toàn số liệu 8 CSV CICIDS2017 thật."
        if live["matches_full_cicids2017_counts"]
        else "Số dòng batch live khác tập 8 CSV đầy đủ (có thể là tập con hoặc batch kiểm thử); xem chi tiết bên dưới."
    )
    samples_md = "\n\n".join(sample_blocks)
    return f"""**Trạng thái live snapshot MinIO:** **ĐÃ XÁC MINH (`PASS`)** — Batch `{live['batch_id']}`, `_SUCCESS.json` có `run_status = COMPLETE`, chụp lúc `{live['generated_at_utc']}`. Đối soát live: **{fmt_int(rec['input_records'])} = {fmt_int(rec['valid_records'])} VALID + {fmt_int(rec['quarantined_records'])} QUARANTINED + {fmt_int(rec['rejected_records'])} REJECTED**; features = **{fmt_int(rec['features_records'])}**, aggregates = **{fmt_int(rec['aggregates_records'])}**. {scope_note}

**Bảng Q.2a. Xác minh trực tiếp 5 dataset Parquet trên MinIO.**

{dataset_table}

**Bảng Q.2b. Xếp hạng nhãn theo tổng payload byte (`grouping = 'label'`, chỉ tính flow `VALID`).**

{top_table}

{samples_md}"""


def build_markdown(evidence: Mapping[str, Any]) -> str:
    """Render docs/reports/value-report.md directly from cross-checked evidence."""
    vol = evidence["volume"]
    ver = evidence["veracity"]
    qa = ver["quality_audit"]
    vel = evidence["velocity"]
    var = evidence["variety"]
    schema = evidence["schema"]
    n = vol["input_records"]
    valid = ver["totals"]["valid_records"]
    quar = ver["totals"]["quarantined_records"]
    rej = ver["totals"]["rejected_records"]
    soft = ver["valid_records_with_soft_issues"]
    cells = ver["cells"]

    full_vel = next(item for item in vel["summary"]["size"] if item["input_records"] == n)
    small_vel = next(item for item in vel["summary"]["size"] if item["input_records"] == 191033)
    med_vel = next(item for item in vel["summary"]["size"] if item["input_records"] == 703245)
    w1 = next(item for item in vel["summary"]["workers"] if item["worker_count"] == 1)
    w2 = next(item for item in vel["summary"]["workers"] if item["worker_count"] == 2)
    p_rows = sorted(vel["summary"]["partitions"], key=lambda item: item["shuffle_partitions"])

    stakeholder_table = md_table(
        ["Đối tượng sử dụng", "Đầu ra pipeline sử dụng", "Quyết định được hỗ trợ", "Giới hạn & điều kiện áp dụng"],
        [
            [
                "Nhóm nghiên cứu / sinh viên triển khai bước sau",
                "`cleaned/flows/` (34 cột), `processed/derived_features/` (38 cột), 4 cờ chất lượng, báo cáo kiểm toán chất lượng",
                "Chọn chiến lược giữ `VALID` + cờ chất lượng kết hợp impute thay vì `dropna()`; thiết kế chia tập train/test giảm leakage và chọn metric phù hợp mất cân bằng lớp",
                "`VALID` chỉ là đạt chất lượng kỹ thuật, không đồng nghĩa `BENIGN`; chưa chứng minh ground truth từ PCAP và mới hậu kiểm 22/79 trường lõi",
            ],
            [
                "Người vận hành pipeline (vận hành Spark–MinIO)",
                "`_SUCCESS.json`, `quality.json`, `summary.csv`, `performance.json`, `quality/quarantine/`, `quality/rejected/`, hồ sơ benchmark `real-20261110-141211`",
                "Xác định batch hoàn tất hay thất bại (chỉ tin `_SUCCESS.json` `COMPLETE`), khoanh vùng lỗi cứng theo file nguồn, ước lượng thời gian chạy lô (~7 phút 49 giây cho 8 CSV) và tránh tăng worker/shuffle partition không hiệu quả trên lô nhỏ",
                "Số đo thuộc môi trường batch trên 1 máy Docker Desktop (~9,69 GiB RAM cấp cho Docker); chưa đo đa nút vật lý hay peak RAM hệ thống",
            ],
            [
                "Người phân tích an ninh mạng mức khảo sát",
                "`processed/aggregates/` (46 nhóm tổng hợp, 13 cột), phân bố nhãn nguồn, đầu ra Parquet của luồng Zeek JSONL",
                "Truy vấn nhanh số flow `VALID`, tổng packet, tổng payload byte và thời lượng trung bình theo nhãn/file nguồn; khảo sát chênh lệch giữa tần suất flow và khối lượng payload (ví dụ `Heartbleed` ít flow nhưng payload lớn)",
                "Chỉ phục vụ phân tích khảo sát theo lô; không phải hệ thống phát hiện hay cảnh báo thời gian thực; CSV CICIDS2017 thiếu IP nguồn/đích và timestamp",
            ],
        ],
    )

    dropna_kept = qa["complete_case_valid_records"]
    dropna_lost = n - dropna_kept
    policy_lost = n - valid
    extra_preserved = valid - dropna_kept
    retention_table = md_table(
        ["Kịch bản sử dụng dữ liệu sau làm sạch", "Số flow giữ lại", "Tỷ lệ so với đầu vào", "Số flow mất", "Ghi chú kỹ thuật"],
        [
            [
                "Đầu vào nguồn (8 CSV CICIDS2017)",
                fmt_int(n),
                "100,0000%",
                "0",
                "79 cột gốc, chưa kiểm tra/làm sạch",
            ],
            [
                "Giữ `VALID` + 4 cờ chất lượng (chính sách pipeline)",
                fmt_int(valid),
                fmt_percent(valid, n),
                fmt_int(policy_lost),
                f"Chỉ cách ly {fmt_int(quar)} dòng lỗi cứng; bảo toàn {fmt_int(qa['valid_with_cleaned_null'])} dòng `VALID` có NULL hợp lệ kèm cờ để bước sau xử lý",
            ],
            [
                "Xóa dòng thiếu ngây thơ (`dropna()` trên toàn bộ 21 feature)",
                fmt_int(dropna_kept),
                fmt_percent(dropna_kept, n),
                fmt_int(dropna_lost),
                f"Mất {fmt_percent(dropna_lost, n)} dữ liệu chủ yếu do sentinel `-1` ở initial window; ít hơn chính sách hiện tại {fmt_int(extra_preserved)} flow",
            ],
        ],
    )

    benign_count = ver["labels"]["BENIGN"]
    heartbleed_count = ver["labels"]["Heartbleed"]
    imbalance_ratio = benign_count / heartbleed_count
    dup = qa["duplicate_survey"]

    risk_table = md_table(
        ["Rủi ro dữ liệu được làm rõ trước bước mô hình", "Số liệu đã xác minh từ artifact", "Tác động kỹ thuật nếu bỏ qua"],
        [
            [
                "Mất cân bằng lớp cực mạnh",
                f"`BENIGN`: {fmt_int(benign_count)} ({fmt_percent(benign_count, n)}) vs `Heartbleed`: {fmt_int(heartbleed_count)} ({fmt_percent(heartbleed_count, n)}), tỷ lệ ~{fmt_float(imbalance_ratio, 1)}:1",
                "Accuracy tổng thể dễ che khuất lớp hiếm; bước sau cần báo cáo macro-F1, recall từng lớp và confusion matrix",
            ],
            [
                "Sentinel `-1` ở TCP initial window & lỗi mềm ở rate",
                f"`init_win_bytes_forward` = -1: {fmt_int(cells['init_win_bytes_forward_sentinel'])} ô ({fmt_percent(cells['init_win_bytes_forward_sentinel'], n)}); `init_win_bytes_backward` = -1: {fmt_int(cells['init_win_bytes_backward_sentinel'])} ô ({fmt_percent(cells['init_win_bytes_backward_sentinel'], n)}); tổng dòng `VALID` có ≥1 lỗi mềm: {fmt_int(soft)} ({fmt_percent(soft, n)})",
                "Coi `-1` như giá trị đo thật làm sai phân bố; dùng `dropna()` làm mất quá nửa dữ liệu",
            ],
            [
                "Nhãn chứa ký tự thay thế Unicode `U+FFFD`",
                f"{fmt_int(cells['label_encoding_replacement'])} dòng ({fmt_percent(cells['label_encoding_replacement'], n)}) thuộc 3 lớp `Web Attack`",
                "Tự sửa chuỗi nhãn không có bằng chứng sẽ làm mất truy vết; pipeline giữ nhãn raw và gắn `LABEL_ENCODING_REPLACEMENT`",
            ],
            [
                "Bản ghi trùng byte-identical",
                f"{fmt_int(dup['duplicate_groups'])} nhóm trùng, {fmt_int(dup['records_in_duplicate_groups'])} dòng thuộc nhóm trùng ({fmt_int(dup['excess_duplicate_records'])} dòng dư nếu giữ 1/nhóm)",
                "Chia train/test ngẫu nhiên có nguy cơ leakage; không tự xóa trùng vì CSV thiếu khóa định danh flow",
            ],
        ],
    )

    size_table = md_table(
        ["Mức workload (tập CSV thật)", "Số flow", "Dung lượng (MB thập phân)", "Input partitions", "Trung vị (s)", "Min–max (s)", "Throughput trung vị (flow/s)"],
        [
            [
                "Nhỏ (Friday Morning)",
                fmt_int(small_vel["input_records"]),
                fmt_float(small_vel["input_bytes"] / 1_000_000.0, 2),
                small_vel["input_partitions"],
                fmt_float(small_vel["median_seconds"], 3),
                f"{fmt_float(small_vel['min_seconds'], 3)}–{fmt_float(small_vel['max_seconds'], 3)}",
                fmt_float(small_vel["median_flows_per_second"], 3),
            ],
            [
                "Vừa (3 file Friday)",
                fmt_int(med_vel["input_records"]),
                fmt_float(med_vel["input_bytes"] / 1_000_000.0, 2),
                med_vel["input_partitions"],
                fmt_float(med_vel["median_seconds"], 3),
                f"{fmt_float(med_vel['min_seconds'], 3)}–{fmt_float(med_vel['max_seconds'], 3)}",
                fmt_float(med_vel["median_flows_per_second"], 3),
            ],
            [
                "Đầy đủ (8 CSV CICIDS2017)",
                fmt_int(full_vel["input_records"]),
                fmt_float(full_vel["input_bytes"] / 1_000_000.0, 2),
                full_vel["input_partitions"],
                fmt_float(full_vel["median_seconds"], 3),
                f"{fmt_float(full_vel['min_seconds'], 3)}–{fmt_float(full_vel['max_seconds'], 3)}",
                fmt_float(full_vel["median_flows_per_second"], 3),
            ],
        ],
    )

    tuning_table = md_table(
        ["Thí nghiệm (Friday Morning, 191.033 flow)", "Cấu hình", "Trung vị (s)", "Throughput trung vị (flow/s)", "Speedup"],
        [
            [
                "Số worker (p=4)",
                "1 worker",
                fmt_float(w1["median_seconds"], 3),
                fmt_float(w1["median_flows_per_second"], 3),
                fmt_float(w1["speedup"], 3) + "x",
            ],
            [
                "Số worker (p=4)",
                "2 workers",
                fmt_float(w2["median_seconds"], 3),
                fmt_float(w2["median_flows_per_second"], 3),
                fmt_float(w2["speedup"], 3) + "x",
            ],
            *[
                [
                    "Shuffle partitions (2 workers)",
                    f"p = {item['shuffle_partitions']}",
                    fmt_float(item["median_seconds"], 3),
                    fmt_float(item["median_flows_per_second"], 3),
                    fmt_float(item["speedup"], 3) + "x",
                ]
                for item in p_rows
            ],
        ],
    )

    stage_table = md_table(
        ["Công đoạn trên 3 lượt toàn bộ 8 CSV", "Trung vị (s)", "Min–max (s)"],
        [
            [
                item["label"],
                fmt_float(item["median_seconds"], 3),
                f"{fmt_float(item['min_seconds'], 3)}–{fmt_float(item['max_seconds'], 3)}",
            ]
            for item in vel["stage_medians_full"]
        ],
    )

    ext_files = evidence["value"]["extraction_status"]["files"]
    ext_desc = ", ".join(f"`{item['file']}` ({item['bytes']} byte)" for item in ext_files) or "không có file triển khai"

    proven_vs_planned_table = md_table(
        ["Hạng mục", "Trạng thái trên nhánh `master`", "Bằng chứng cụ thể / Phạm vi giới hạn"],
        [
            [
                "Pipeline batch ETL Spark 3.5.0 + MinIO S3A trên 8 CSV CICIDS2017 thật",
                "**Đã chứng minh**",
                f"3 lượt full-data `PASS`, bảo toàn {fmt_int(n)} flow, 5/5 Parquet Snappy read-back `PASS`, audit lịch sử {ver['minio_audit']['batch_count']} batch / {ver['minio_audit']['parquet_outputs']} outputs `COMPLETE` [A1–A3]",
            ],
            [
                "Kiểm thử tải / smoke trên dữ liệu synthetic nhân bản (12k/60k/120k)",
                "**Đã chứng minh (tách riêng)**",
                "Dùng fixture 12 dòng để kiểm thử chức năng và đối chiếu phương pháp (`docs/benchmarks/20261021/`, `scaling-20261110-121252/`); không gộp vào kết luận CICIDS2017 thật [A4]",
            ],
            [
                "Đa dạng định dạng (Variety) với luồng Zeek JSON Lines (`processing/zeek_ingestion.py`)",
                "**Đã chứng minh mức demo schema/định dạng**",
                f"Đọc và xác minh {var['zeek_demo']['records']} dòng `{var['zeek_demo']['source_path']}` ({var['zeek_demo']['protocols'].get('tcp', 0)} TCP, {var['zeek_demo']['protocols'].get('udp', 0)} UDP, {var['zeek_demo']['protocols'].get('icmp', 0)} ICMP; {var['zeek_demo']['missing_service_records']} dòng thiếu `service`) tạo từ PCAP tổng hợp offline [A7]",
            ],
            [
                "Trích xuất trực tiếp PCAP → flow (`extraction/`)",
                "**Chưa triển khai (mở rộng dự kiến)**",
                f"Thư mục `extraction/` hiện chỉ là file khung trống ({ext_desc}); pipeline chính nhận CSV flow có sẵn",
            ],
            [
                "Mô hình học máy nhận diện bất thường / Quality-Aware Review model",
                "**Chưa triển khai trong pipeline chính (mở rộng dự kiến)**",
                "Repo không import `pyspark.ml`, chưa huấn luyện hay đo inference trong `processing/`; dữ liệu `cleaned/flows/` + 4 cờ là đầu vào chuẩn bị cho giai đoạn sau",
            ],
            [
                "Xử lý streaming / cảnh báo thời gian thực (Kafka, `readStream`/`writeStream`)",
                "**Chưa triển khai (mở rộng dự kiến)**",
                "Hệ thống hiện thuần batch processing trên file tĩnh đã lưu trên MinIO; chưa đo độ trễ cảnh báo trên mạng thực",
            ],
        ],
    )

    zeek = var["zeek_demo"]
    five_v_table = md_table(
        ["Chữ V", "Tóm tắt số liệu & bằng chứng đã đối soát trên `master`", "Nguyên tắc diễn giải đúng phạm vi"],
        [
            [
                "**Volume**",
                f"8 CSV CICIDS2017 thật: **{fmt_int(n)} flow**, **{fmt_int(vol['input_bytes'])} byte** (~**{fmt_float(vol['decimal_mb'], 2)} MB** thập phân / ~**{fmt_float(vol['binary_mib'], 2)} MiB**), {schema['raw_columns']} cột nguồn, {vol['full_input_partitions']} input partitions trên Spark [A1–A2]",
                f"Không tuyên bố quy mô TB (dữ liệu thật khoảng {fmt_float(vol['decimal_gb'], 3)} GB thập phân); đơn vị là network flow, không phải packet PCAP",
            ],
            [
                "**Variety**",
                f"2 nguồn khác cấu trúc: CSV {schema['raw_columns']} cột (CICIDS2017) và JSON Lines Zeek `conn.log` (có trường tùy chọn như `service`), dùng schema riêng và cùng xuất Parquet Snappy; mẫu demo Zeek **{zeek['records']} dòng** ({zeek['protocols'].get('tcp', 0)} TCP / {zeek['protocols'].get('udp', 0)} UDP / {zeek['protocols'].get('icmp', 0)} ICMP, {zeek['missing_service_records']} dòng thiếu `service`) [A5, A7]",
                "Mẫu Zeek 360 dòng xuất phát từ PCAP tổng hợp để minh họa đa dạng schema/định dạng; không trộn với kết quả benchmark CICIDS2017 thật",
            ],
            [
                "**Veracity**",
                f"Data Contract v{schema['version']} ({schema['core_columns']} trường lõi + 12 metadata/cờ); đối soát **{fmt_int(n)} = {fmt_int(valid)} `VALID` ({fmt_percent(valid, n)}) + {fmt_int(quar)} `QUARANTINED` ({fmt_percent(quar, n)}) + {fmt_int(rej)} `REJECTED`**; **{fmt_int(soft)}** dòng `VALID` ({fmt_percent(soft, n)}) có ≥1 lỗi mềm; 3 lượt full-data hậu kiểm `PASS` & 5/5 read-back `PASS`; audit MinIO {ver['minio_audit']['batch_count']} batch / {ver['minio_audit']['parquet_outputs']} outputs `COMPLETE` [A1–A4]",
                "`VALID` là trạng thái đạt chất lượng kỹ thuật, KHÔNG đồng nghĩa `BENIGN`; kiểm toán kỹ thuật không thay thế xác minh ground truth từ PCAP",
            ],
            [
                "**Velocity**",
                f"Batch Spark {vel['environment']['spark_version']} + MinIO S3A trên 1 máy Docker: 8 CSV đầy đủ đạt trung vị **{fmt_float(full_vel['median_seconds'], 3)} s** ({fmt_float(full_vel['min_seconds'], 3)}–{fmt_float(full_vel['max_seconds'], 3)} s), throughput trung vị **{fmt_float(full_vel['median_flows_per_second'], 3)} flow/s**; Friday Morning 1 vs 2 worker = **{fmt_float(w1['median_seconds'], 3)} s** vs **{fmt_float(w2['median_seconds'], 3)} s** (speedup **{fmt_float(w2['speedup'], 3)}x**); `p=4` thấp nhất (**{fmt_float(p_rows[0]['median_seconds'], 3)} s**) trong 4/8/16/32 [A2, A4]",
                "Hệ thống là batch processing, không phải streaming/thời gian thực; không suy rộng phép đo 58,32 MB thành quy luật tối ưu tổng quát",
            ],
            [
                "**Value**",
                f"Đầu ra Parquet Snappy chuẩn hóa (`cleaned/flows/` {schema['output_column_counts']['cleaned_path']} cột, `processed/derived_features/` {schema['output_column_counts']['features_path']} cột, `processed/aggregates/` 46 dòng / {schema['output_column_counts']['aggregates_path']} cột) giúp giữ **{fmt_percent(valid, n)}** flow (so với **{fmt_percent(dropna_kept, n)}** nếu `dropna()`), minh bạch rủi ro nhãn/sentinel/trùng lặp và cung cấp căn cứ tối ưu vận hành từ benchmark [A3–A6]",
                "Giá trị đã chứng minh thuộc tầng dữ liệu sạch, tổng hợp khảo sát và quản trị vận hành batch; giá trị dự đoán bất thường bằng ML thuộc phần mở rộng tiếp theo",
            ],
        ],
    )

    crosscheck_table = md_table(
        ["Hạng mục đối soát tự động (`build_value_report.py`)", "Trạng thái", "Kết quả xác minh"],
        [
            [
                "SHA-256 & kích thước byte các artifact benchmark (`artifact-sha256.csv`)",
                "PASS",
                f"{len(evidence['checksums'])}/{len(evidence['checksums'])} artifact JSON/CSV khớp checksum (xử lý LF/CRLF)",
            ],
            [
                "Schema & Data Contract v2.3.0 so với `source-audit.json`",
                "PASS",
                f"{schema['raw_columns']} cột raw → {schema['core_columns']} trường lõi ({schema['core_types']['integer']} int, {schema['core_types']['long']} long, {schema['core_types']['double']} double, {schema['core_types']['string']} string)",
            ],
            [
                "Phương trình bảo toàn bản ghi & 3 lượt full-data Spark",
                "PASS",
                f"{fmt_int(n)} = {fmt_int(valid)} + {fmt_int(quar)} + {fmt_int(rej)}; 3/3 lượt hậu kiểm PASS, 5/5 Parquet read-back PASS",
            ],
            [
                "MinIO audit lịch sử & `verification.json`",
                "PASS",
                f"{ver['minio_audit']['batch_count']} batch ({ver['minio_audit']['measured_trials']} đo + {ver['minio_audit']['warmups']} warmup) `COMPLETE`; {ver['minio_audit']['parquet_outputs']} Parquet outputs",
            ],
            [
                "Đối soát `summary.csv` vs `runs.csv` (Velocity)",
                "PASS",
                "9 cấu hình, 27 lượt đo + 4 warmup khớp trung vị, min–max, throughput và speedup",
            ],
            [
                "Đối soát tỷ lệ giữ dữ liệu (`VALID` + cờ vs `dropna()`)",
                "PASS",
                f"{fmt_int(valid)} = {fmt_int(qa['valid_with_cleaned_null'])} (có NULL hợp lệ) + {fmt_int(dropna_kept)} (complete-case)",
            ],
            [
                "Mẫu demo Variety Zeek JSONL trên đĩa",
                "PASS",
                f"{zeek['records']} dòng hợp lệ ({zeek['protocols'].get('tcp', 0)} TCP / {zeek['protocols'].get('udp', 0)} UDP / {zeek['protocols'].get('icmp', 0)} ICMP; {zeek['missing_service_records']} dòng thiếu `service`)",
            ],
            [
                "Live snapshot MinIO (`--value-snapshot`)",
                "PASS" if evidence["value"]["live_snapshot"]["attached"] else "CHƯA CHẠY",
                f"Batch `{evidence['value']['live_snapshot']['batch_id']}`"
                if evidence["value"]["live_snapshot"]["attached"]
                else "Chạy offline; không bịa bảng mẫu live",
            ],
        ],
    )

    discrepancy_lines = ""
    if evidence["discrepancies"]:
        items = "\n".join(
            f"- **{item['id']}:** Chuẩn artifact gốc: {item['artifact_truth']} Văn bản báo cáo cũ: {item['markdown_text']} Cách xử lý: {item['resolution']}"
            for item in evidence["discrepancies"]
        )
        discrepancy_lines = f"\n\n### Ghi nhận đối soát chéo giữa artifact JSON/CSV và văn bản Markdown\n\n{items}"

    slowest = max(vel["stage_medians_full"], key=lambda item: item["median_seconds"])
    live_block = _render_live_snapshot_block(evidence)

    return f"""# Value & Tổng hợp 5V — Giá trị sử dụng dữ liệu và đối soát toàn chuỗi Big Data

> Báo cáo được tổng hợp hoàn toàn từ các artifact đã kiểm chứng trên nhánh `master` (đợt thực nghiệm dữ liệu thật ngày **10/11/2026**, mã `real-20261110-141211`, cùng kiểm toán chất lượng và fixture Zeek trong repository). Phạm vi thuộc đồ án môn **Nhập môn Dữ liệu lớn** (**Storage → Processing** trên Apache Spark 3.5.0 + MinIO); **không** bao gồm huấn luyện hay suy luận mô hình học máy.

## 1. Đối tượng sử dụng và quyết định được hỗ trợ

Trong chuỗi xử lý dữ liệu mạng CICIDS2017 và Zeek JSON Lines, giá trị thực tế (**Value**) không chỉ nằm ở việc chạy xong một job ETL mà nằm ở việc **ai có thể sử dụng trực tiếp đầu ra đã chuẩn hóa và đưa ra quyết định kỹ thuật cụ thể dựa trên bằng chứng đo được**. Bảng dưới đây phân tách 3 nhóm người dùng sát phạm vi đồ án:

**Bảng Q.1. Ba nhóm đối tượng sử dụng đầu ra pipeline và quyết định được hỗ trợ.**

{stakeholder_table}

## 2. Giá trị thực tế gắn với kết quả xác nhận dữ liệu

### 2.1. Giá trị từ `processed/aggregates/` và `processed/derived_features/`

Thay vì buộc người dùng sau phải đọc lại toàn bộ **8 file CSV thô ({fmt_int(vol['input_bytes'])} byte ≈ {fmt_float(vol['decimal_mb'], 2)} MB thập phân / {fmt_float(vol['binary_mib'], 2)} MiB)** với 79 cột chuỗi và tự xử lý lỗi kiểu/sentinel, pipeline xuất đồng thời các lớp Parquet nén Snappy có schema cố định [A1–A3]:

- **`cleaned/flows/` ({schema['output_column_counts']['cleaned_path']} cột, {fmt_int(valid)} dòng `VALID`):** giữ đúng {schema['core_columns']} trường lõi ({schema['core_types']['integer']} integer, {schema['core_types']['long']} long, {schema['core_types']['double']} double, {schema['core_types']['string']} string) cùng 12 trường truy vết và cờ chất lượng.
- **`processed/derived_features/` ({schema['output_column_counts']['features_path']} cột, {fmt_int(valid)} dòng `VALID`):** bổ sung 4 đặc trưng dẫn xuất có định nghĩa tường minh (`total_packets`, `total_bytes` dùng `Decimal(20,0)` chống tràn số nguyên 64-bit, `fwd_packet_ratio`, `bwd_packet_ratio` có bảo vệ chia cho 0). Lưu ý `total_bytes` là tổng payload byte theo định nghĩa nguồn, không phải wire bytes [A7].
- **`processed/aggregates/` ({schema['output_column_counts']['aggregates_path']} cột, 46 dòng tổng hợp):** gom sẵn 4 cấp (`1` dòng `batch`, `15` dòng `label`, `8` dòng `source_file`, `22` dòng `source_file_label`), tách bạch số đếm toàn đầu vào (`flow_count`, `valid_ratio`, `error_ratio`) với chỉ số lưu lượng chỉ tính trên `VALID` (`total_packets_sum`, `total_bytes_sum`, `avg_flow_duration`) [A7].

{live_block}

### 2.2. Giá trị bảo toàn dữ liệu khi giữ `VALID` + 4 cờ chất lượng so với `dropna()`

Một giá trị định lượng trực tiếp của thiết kế chất lượng dữ liệu là **tránh mất mát hơn một nửa tập dữ liệu** do các giá trị sentinel `-1` ở hai trường TCP initial window và các giá trị rate `NaN`/`Infinity`/âm. Đối soát từ `source-audit.json` và báo cáo kiểm toán chất lượng `cicids2017-data-quality-audit.md` cho thấy [A1, A5]:

**Bảng Q.3. So sánh tỷ lệ bảo toàn bản ghi giữa chính sách `VALID` + 4 cờ chất lượng và `dropna()`.**

{retention_table}

Kết quả đối soát khẳng định **`{fmt_int(valid)} = {fmt_int(qa['valid_with_cleaned_null'])} + {fmt_int(dropna_kept)}` (`PASS`)**: chính sách của nhóm giữ lại được nhiều hơn **{fmt_int(extra_preserved)} flow `VALID`** so với `dropna()` toàn bộ 21 feature, trong khi vẫn minh bạch hóa toàn bộ **{fmt_int(qa['total_cleaned_null_cells'])} ô** đã chuyển NULL thông qua 4 cờ boolean (`flow_bytes_s_invalid`, `flow_packets_s_invalid`, `init_win_fwd_missing`, `init_win_bwd_missing`).

### 2.3. Minh bạch rủi ro dữ liệu trước bước mô hình hóa

Pipeline không "che giấu" khuyết điểm của dữ liệu nguồn bằng cách ép số âm thành 0, tự sửa chuỗi nhãn hay tự động xóa dòng trùng. Thay vào đó, các rủi ro được định lượng rõ để bước phân tích hoặc mô hình hóa sau ra quyết định đúng [A1, A3, A5]:

**Bảng Q.4. Các rủi ro dữ liệu chính được lượng hóa trước bước xây dựng mô hình.**

{risk_table}

Nhóm nhấn mạnh nguyên tắc kỹ thuật: **`VALID` chỉ biểu thị bản ghi đạt quy tắc chất lượng của Data Contract v{schema['version']}, hoàn toàn KHÔNG đồng nghĩa với nhãn `BENIGN`** (mọi lớp tấn công vượt qua quy tắc miền giá trị đều mang trạng thái `VALID`).

### 2.4. Căn cứ tối ưu vận hành từ benchmark Velocity (`real-20261110-141211`)

Các phép đo Velocity trên dữ liệu CICIDS2017 thật cung cấp số liệu thực nghiệm trực tiếp cho việc lập lịch và chọn cấu hình chạy lô trên cụm Docker cục bộ (Intel Core i5-9400, 6 cores, Docker cấp ~{fmt_float(vel['environment']['docker_ram_gib'], 2)} GiB RAM, Spark {vel['environment']['spark_version']}) [A2, A4]:

**Bảng Q.5. Hiệu năng mở rộng theo kích thước dữ liệu thật (2 workers, `p=4`, 3 lượt đo/mức).**

{size_table}

**Bảng Q.6. So sánh cấu hình số worker và số shuffle partitions trên Friday Morning (191.033 flow, 58,32 MB).**

{tuning_table}

**Bảng Q.7. Trung vị thời gian từng công đoạn trên 3 lượt xử lý toàn bộ 8 CSV thật.**

{stage_table}

Từ 3 bảng đo trên rút ra 3 căn cứ vận hành cụ thể:
1. **Lập kế hoạch chạy lô:** toàn bộ 8 CSV thật ({fmt_int(n)} flow, {fmt_float(vol['decimal_mb'], 2)} MB) hoàn tất xử lý kèm hậu kiểm và 5/5 Parquet read-back với trung vị **{fmt_float(full_vel['median_seconds'], 3)} giây** (khoảng **7 phút 49 giây**), throughput trung vị **{fmt_float(full_vel['median_flows_per_second'], 3)} flow/s**; audit lịch sử xác nhận **{ver['minio_audit']['batch_count']}/{ver['minio_audit']['batch_count']} batch `COMPLETE`** ({ver['minio_audit']['parquet_outputs']} output Parquet) [A2–A3].
2. **Tránh cấu hình dư thừa trên batch nhỏ:** trên Friday Morning (2 input partitions), tăng từ 1 lên 2 worker làm chậm thêm ~11,6% (speedup **{fmt_float(w2['speedup'], 3)}x**), và tăng `spark.sql.shuffle.partitions` từ 4 lên 8/16/32 đều làm tăng thời gian trung vị (`{fmt_float(p_rows[0]['median_seconds'], 3)} s` → `{fmt_float(p_rows[-1]['median_seconds'], 3)} s`). Tuy nhiên không suy rộng kết luận này cho các batch lớn hơn nhiều hoặc môi trường nhiều máy vật lý [A4].
3. **Định hướng ưu tiên tối ưu:** trong 5 công đoạn đo trên toàn bộ 8 CSV, **`{slowest['label']}`** có trung vị lớn nhất (**{fmt_float(slowest['median_seconds'], 3)} s**), lớn hơn đáng kể bước tạo đặc trưng/tổng hợp (**{fmt_float(vel['stage_medians_full'][3]['median_seconds'], 3)} s**). Lưu ý không cộng trung vị từng công đoạn để thay cho trung vị toàn pipeline [A4].

## 3. Phân biệt kết quả đã chứng minh và phần mở rộng dự kiến

Để đảm bảo tính trung thực học thuật, báo cáo tách bạch hoàn toàn giữa các năng lực **đã được kiểm chứng bằng mã nguồn và artifact trên nhánh `master`** với **các hướng mở rộng dự kiến ngoài phạm vi hiện tại**, đồng thời phân biệt rõ 3 nguồn dữ liệu (**CICIDS2017 thật**, **synthetic nhân bản dùng test tải** và **mẫu Zeek 360 dòng từ PCAP tổng hợp**):

**Bảng Q.8. Ranh giới giữa kết quả đã chứng minh và phần mở rộng dự kiến.**

{proven_vs_planned_table}

## 4. Hạn chế hiện tại và hướng phát triển

Các hạn chế dưới đây được rút trực tiếp từ trạng thái mã nguồn và bằng chứng hiện có trong repository:

1. **Chưa có module trích xuất PCAP → flow hoàn chỉnh:** thư mục `extraction/` hiện chỉ gồm các file khung trống ({ext_desc}); pipeline chính nhận đầu vào là CSV flow đã trích xuất sẵn từ CICIDS2017. *Hướng phát triển:* hoàn thiện công cụ trích xuất flow chuẩn hóa từ PCAP (ví dụ qua Zeek/NFStream) có schema khớp Data Contract trước khi đẩy vào MinIO.
2. **Thiếu định danh 5-tuple và timestamp trong 8 CSV CICIDS2017:** bộ CSV nguồn không có IP nguồn/đích, cổng nguồn, giao thức hay mốc thời gian sự kiện ổn định, nên không thể khử trùng theo khóa định danh flow, không tổng hợp theo cửa sổ `event_time` và không tự xác minh nhãn ground truth từ PCAP gốc. *Hướng phát triển:* kết hợp log giàu ngữ cảnh như Zeek `conn.log` (đã có IP, cổng, `uid`, `ts`) cho các kịch bản truy vết kết nối đầy đủ.
3. **Thực nghiệm batch trên một máy vật lý ở quy mô dưới 1 GB:** dữ liệu thật đạt **{fmt_float(vol['decimal_mb'], 2)} MB** thập phân (**{fmt_float(vol['binary_mib'], 2)} MiB**), chạy trên 1 máy Windows/WSL2 Docker; hai worker dùng chung CPU/RAM/I/O nên chưa chứng minh mở rộng đa nút vật lý, chưa đo tải nhiều GB/TB và chưa thu thập peak RAM hay Spark History Server. *Hướng phát triển:* bổ sung Spark Event Log / History Server, đo tài nguyên hệ thống và thử nghiệm trên cụm nhiều nút độc lập.
4. **Chưa tích hợp mô hình học máy và chưa hỗ trợ streaming:** pipeline hiện chỉ xử lý theo lô (batch processing), chưa có nhánh huấn luyện/suy luận ML và chưa có luồng Kafka/`readStream`/`writeStream`. *Hướng phát triển:* xây dựng bước mô hình hóa tách biệt đọc `cleaned/flows/` + 4 cờ chất lượng (đánh giá bằng macro-F1, recall lớp hiếm và split theo file/ngày), sau đó đo riêng thời gian huấn luyện và độ trễ suy luận.{discrepancy_lines}

## 5. Tổng hợp chuỗi 5V của báo cáo nhóm

Toàn bộ 5 đặc tính Big Data của đồ án được tổng hợp nhất quán từ cùng một hệ bằng chứng gốc (`real-20261110-141211`, Data Contract v{schema['version']}, báo cáo kiểm toán chất lượng và fixture Zeek):

**Bảng Q.9. Bảng tổng hợp chuỗi 5V nhất quán toàn đồ án.**

{five_v_table}

**Bảng Q.10. Tóm tắt kết quả đối soát chéo tự động giữa các artifact 5V.**

{crosscheck_table}

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
if ($LASTEXITCODE -ne 0) {{ throw 'Chụp snapshot thất bại' }}
New-Item -ItemType Directory -Path $localDir -Force | Out-Null
docker cp "spark-master:$containerSnapshot" $localSnapshot
if ($LASTEXITCODE -ne 0) {{ throw 'Sao chép snapshot thất bại' }}
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
"""


def _table_column_widths(headers: Sequence[str]) -> List[float]:
    """Return balanced column widths (in cm, summing to ~17.4 cm) for A4 Word tables."""
    total_cm = 17.4
    count = len(headers)
    if count <= 1:
        return [total_cm]
    joined = " ".join(headers)
    if count == 4 and "Đối tượng sử dụng" in joined:
        return [3.3, 4.0, 5.4, 4.7]
    if count == 5 and "Kịch bản" in joined:
        return [4.5, 2.4, 2.4, 2.3, 5.8]
    if count == 3 and "Chữ V" in joined:
        return [2.2, 9.0, 6.2]
    if count == 3 and "Rủi ro" in joined:
        return [4.0, 7.6, 5.8]
    if count == 3 and "Công đoạn" in joined:
        return [10.4, 2.8, 4.2]
    if count == 3:
        return [5.6, 5.6, 6.2]
    if count == 7 and "Mức workload" in joined:
        return [3.5, 2.2, 2.1, 1.7, 2.0, 3.2, 2.7]
    if count == 7:
        return [1.1, 3.3, 2.2, 2.3, 2.7, 3.4, 2.4]
    equal = round(total_cm / count, 2)
    widths = [equal] * count
    widths[-1] = round(total_cm - equal * (count - 1), 2)
    return widths


def write_docx(markdown: str, output_path: Path) -> Path:
    """Render Markdown headings, tables, links and code blocks to Word (.docx)."""
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
    normal.paragraph_format.space_after = Pt(5)
    normal.paragraph_format.line_spacing = 1.12
    for name in ("Title", "Heading 1", "Heading 2", "Heading 3"):
        if name in doc.styles:
            doc.styles[name].font.name = "Times New Roman"
            doc.styles[name].font.color.rgb = RGBColor.from_string("173D50")

    def inline(paragraph: Any, text: str) -> None:
        for part in re.split(r"(\[[^\]]+\]\([^)]+\)|\*\*[^*]+\*\*|`[^`]+`)", text):
            if not part:
                continue
            match = re.fullmatch(r"\[([^\]]+)\]\(([^)]+)\)", part)
            if match:
                label, target = match.groups()
                if not target.startswith(("http://", "https://")):
                    resolved = (output_path.parent / target).resolve()
                    try:
                        rel = resolved.relative_to(ROOT).as_posix()
                        target = GITHUB_BLOB_BASE + rel
                    except ValueError:
                        target = resolved.as_uri()
                link = OxmlElement("w:hyperlink")
                link.set(
                    qn("r:id"),
                    paragraph.part.relate_to(
                        target,
                        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/hyperlink",
                        is_external=True,
                    ),
                )
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
                bold = part.startswith("**") and part.endswith("**")
                code = part.startswith("`") and part.endswith("`")
                content = part[2:-2] if bold else part[1:-1] if code else part
                run = paragraph.add_run(content)
                run.bold = bold
                if code:
                    run.font.name, run.font.size = "Consolas", Pt(8.5)

    lines = markdown.splitlines()
    i, in_code = 0, False
    while i < len(lines):
        line = lines[i]
        i += 1
        if line.startswith("```"):
            in_code = not in_code
        elif not line.strip():
            continue
        elif in_code:
            p = doc.add_paragraph()
            r = p.add_run(line)
            r.font.name, r.font.size = "Consolas", Pt(8.5)
        elif line.startswith("#"):
            level = len(line) - len(line.lstrip("#"))
            doc.add_heading(line[level:].strip(), level=min(level - 1, 3))
        elif line.startswith("> "):
            p = doc.add_paragraph()
            p.paragraph_format.left_indent = Cm(0.5)
            inline(p, line[2:].strip())
            for run in p.runs:
                run.italic = True
        elif line.startswith("| "):
            raw_rows = [line]
            while i < len(lines) and lines[i].startswith("| "):
                raw_rows.append(lines[i])
                i += 1
            headers = [cell.strip() for cell in raw_rows[0].strip().strip("|").split("|")]
            widths = _table_column_widths(headers)
            grid = doc.add_table(rows=0, cols=len(headers))
            grid.style = "Table Grid"
            grid.autofit = False
            for column, width in zip(grid.columns, widths):
                column.width = Cm(width)
            for index, raw_row in enumerate(raw_rows):
                if index == 1 and set(raw_row.replace("|", "").replace("-", "").replace(":", "").strip()) == set():
                    continue
                cells = grid.add_row().cells
                values = [cell.strip().replace("\\|", "|") for cell in raw_row.strip().strip("|").split("|")]
                for cell, width, value in zip(cells, widths, values):
                    cell.width = Cm(width)
                    inline(cell.paragraphs[0], value)
                    for run in cell.paragraphs[0].runs:
                        run.font.size = Pt(8)
                        if index == 0:
                            run.bold = True
                props = grid.rows[-1]._tr.get_or_add_trPr()
                props.append(OxmlElement("w:cantSplit"))
                if index == 0:
                    props.append(OxmlElement("w:tblHeader"))
            doc.add_paragraph()
        else:
            bullet = line.startswith("- ")
            numbered = re.match(r"^[0-9]+\.\s+", line) is not None
            if bullet:
                p = doc.add_paragraph(style="List Bullet")
                inline(p, line[2:])
            elif numbered:
                p = doc.add_paragraph(style="List Number")
                inline(p, re.sub(r"^[0-9]+\.\s+", "", line))
            else:
                p = doc.add_paragraph()
                inline(p, line)

    footer = section.footer.paragraphs[0]
    footer.alignment = 2
    footer.add_run("Value & 5V • ")
    page = OxmlElement("w:fldSimple")
    page.set(qn("w:instr"), "PAGE")
    footer._p.append(page)
    doc.core_properties.title = "Value & Tổng hợp 5V — Báo cáo đồ án Dữ liệu lớn"
    doc.core_properties.subject = "CICIDS2017 / Zeek / Spark / MinIO — real-20261110-141211"
    docx_path = output_path.with_suffix(".docx")
    docx_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(docx_path)
    return docx_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence-dir",
        type=Path,
        default=DEFAULT_EVIDENCE_DIR,
        help="Path to historical benchmark evidence folder (default: docs/benchmarks/real-20261110-141211).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_REPORT_PATH,
        help="Destination Markdown report path (default: docs/reports/value-report.md).",
    )
    parser.add_argument(
        "--verification-json",
        type=Path,
        default=DEFAULT_VERIFICATION_JSON,
        help="Destination JSON cross-check summary path (default: docs/reports/value-verification.json).",
    )
    parser.add_argument(
        "--value-snapshot",
        type=Path,
        default=None,
        help="Optional live MinIO snapshot JSON generated by experiments/verify_value_outputs.py.",
    )
    parser.add_argument(
        "--docx",
        action="store_true",
        help="Also export a Word document alongside --output (requires python-docx).",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    evidence = collect_value_evidence(
        evidence_dir=args.evidence_dir,
        value_snapshot_path=args.value_snapshot,
    )
    verification_summary = build_verification_summary(evidence)
    markdown = build_markdown(evidence)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(markdown, encoding="utf-8")

    args.verification_json.parent.mkdir(parents=True, exist_ok=True)
    args.verification_json.write_text(
        json.dumps(verification_summary, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )

    created = [str(args.output), str(args.verification_json)]
    if args.docx:
        docx_path = write_docx(markdown, args.output)
        created.append(str(docx_path))

    print(
        f"PASS: verified {len(evidence['checksums'])} benchmark artifacts, schema/contract, "
        f"3 full-data Spark trials, 31 batches / 155 Parquet outputs, Velocity summary vs runs, "
        f"retention vs dropna(), and 360-row Zeek demo"
    )
    print("Created: " + ", ".join(created))
    if evidence["discrepancies"]:
        print(f"Note: recorded {len(evidence['discrepancies'])} prose-vs-artifact discrepancy item(s) in verification JSON.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

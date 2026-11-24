"""Verify completed batch Parquet outputs on MinIO (or local Spark output) for Value evidence.

Run inside the Spark container when Docker (Spark + MinIO) is running:
    docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master \\
        python3 -m experiments.verify_value_outputs --batch-id <BATCH_ID>

Dry-run plan without connecting to MinIO or starting Spark:
    python -m experiments.verify_value_outputs --batch-id <BATCH_ID> --dry-run
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import math
from pathlib import Path
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

from processing.config import PROJECT_ROOT, ProcessingConfig, load_contract
from processing.output import _location


DEFAULT_SNAPSHOT_PATH = PROJECT_ROOT / "docs/evidence/local/value-minio-snapshot.json"

CLEANED_METADATA_COLUMNS: Tuple[str, ...] = (
    "source_file",
    "source_dataset",
    "batch_id",
    "pipeline_version",
    "schema_version",
    "processed_at",
    "quality_status",
    "quality_reason_codes",
    "flow_bytes_s_invalid",
    "flow_packets_s_invalid",
    "init_win_fwd_missing",
    "init_win_bwd_missing",
)

DERIVED_FEATURE_COLUMNS: Tuple[str, ...] = (
    "total_packets",
    "total_bytes",
    "fwd_packet_ratio",
    "bwd_packet_ratio",
)

EXPECTED_AGGREGATE_COLUMNS: Tuple[str, ...] = (
    "flow_count",
    "valid_records",
    "quarantined_records",
    "rejected_records",
    "total_packets_sum",
    "total_bytes_sum",
    "avg_flow_duration",
    "grouping",
    "source_file",
    "label",
    "valid_ratio",
    "error_ratio",
    "batch_id",
)

SAMPLE_COLUMNS: Dict[str, Tuple[str, ...]] = {
    "cleaned": (
        "source_file",
        "destination_port",
        "flow_duration",
        "total_fwd_packets",
        "total_backward_packets",
        "flow_bytes_s",
        "flow_packets_s",
        "init_win_bytes_forward",
        "init_win_bytes_backward",
        "label",
        "quality_status",
        "flow_bytes_s_invalid",
        "init_win_fwd_missing",
        "init_win_bwd_missing",
    ),
    "features": (
        "source_file",
        "label",
        "destination_port",
        "total_fwd_packets",
        "total_backward_packets",
        "total_packets",
        "total_bytes",
        "fwd_packet_ratio",
        "bwd_packet_ratio",
        "quality_status",
    ),
    "aggregates": (
        "grouping",
        "source_file",
        "label",
        "flow_count",
        "valid_records",
        "quarantined_records",
        "rejected_records",
        "total_packets_sum",
        "total_bytes_sum",
        "avg_flow_duration",
    ),
    "quarantine": (
        "source_file",
        "label",
        "destination_port",
        "flow_duration",
        "flow_iat_mean",
        "quality_status",
        "quality_reason_codes",
    ),
}

COUNT_KEYS: Tuple[str, ...] = (
    "input_records",
    "valid_records",
    "quarantined_records",
    "rejected_records",
)


def _validate_safe_uri(uri: str, label: str) -> str:
    """Validate that a URI uses safe s3a:// or file:// syntax without credentials or traversal."""
    scheme, _, normalized_path = _location(uri)
    if scheme == "s3a" and normalized_path == "/":
        raise ValueError(f"{label} must point to an object or prefix under a bucket: {uri}")
    if "raw" in normalized_path.split("/"):
        raise ValueError(f"{label} must reference processed outputs, not raw/: {uri}")
    return uri.rstrip("/")


def resolve_batch_uris(
    config: ProcessingConfig,
    *,
    batch_id: Optional[str] = None,
    output_prefix: Optional[str] = None,
    success_marker: Optional[str] = None,
) -> Dict[str, str]:
    """Build expected marker, report and 5-dataset URIs for either default or benchmark layouts."""
    if success_marker:
        marker_uri = _validate_safe_uri(success_marker, "success_marker")
        if not marker_uri.endswith("/_SUCCESS.json"):
            raise ValueError("success_marker must end with '/_SUCCESS.json'")
        report_dir = marker_uri.rsplit("/", 1)[0]
        return {
            "success_marker": marker_uri,
            "quality_report": f"{report_dir}/quality.json",
        }

    resolved_batch_id = batch_id or config.batch_id
    if not resolved_batch_id or any(
        ch not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
        for ch in resolved_batch_id
    ):
        raise ValueError("batch_id must contain only letters, digits, '-' or '_'")

    if output_prefix:
        prefix = _validate_safe_uri(output_prefix, "output_prefix")
        return {
            "batch_id": resolved_batch_id,
            "success_marker": f"{prefix}/report/{resolved_batch_id}/_SUCCESS.json",
            "quality_report": f"{prefix}/report/{resolved_batch_id}/quality.json",
            "cleaned_path": f"{prefix}/cleaned/{resolved_batch_id}",
            "features_path": f"{prefix}/features/{resolved_batch_id}",
            "aggregates_path": f"{prefix}/aggregates/{resolved_batch_id}",
            "quarantine_path": f"{prefix}/quarantine/{resolved_batch_id}",
            "rejected_path": f"{prefix}/rejected/{resolved_batch_id}",
        }

    return {
        "batch_id": resolved_batch_id,
        "success_marker": _validate_safe_uri(config.report_path, "report_path")
        + f"/{resolved_batch_id}/_SUCCESS.json",
        "quality_report": _validate_safe_uri(config.report_path, "report_path")
        + f"/{resolved_batch_id}/quality.json",
        "cleaned_path": _validate_safe_uri(config.cleaned_path, "cleaned_path")
        + f"/{resolved_batch_id}",
        "features_path": _validate_safe_uri(config.features_path, "features_path")
        + f"/{resolved_batch_id}",
        "aggregates_path": _validate_safe_uri(config.aggregates_path, "aggregates_path")
        + f"/{resolved_batch_id}",
        "quarantine_path": _validate_safe_uri(config.quarantine_path, "quarantine_path")
        + f"/{resolved_batch_id}",
        "rejected_path": _validate_safe_uri(config.rejected_path, "rejected_path")
        + f"/{resolved_batch_id}",
    }


def validate_completion_marker(
    marker: Mapping[str, Any],
    *,
    expected_batch_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Verify the final _SUCCESS.json completion marker before trusting any batch output."""
    if not isinstance(marker, Mapping):
        raise ValueError("Completion marker payload must be a JSON object")
    run_status = marker.get("run_status")
    if run_status != "COMPLETE":
        raise ValueError(
            f"Batch is not complete: _SUCCESS.json has run_status={run_status!r} (expected 'COMPLETE')"
        )
    batch_id = marker.get("batch_id")
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("Completion marker is missing a valid batch_id")
    if expected_batch_id and batch_id != expected_batch_id:
        raise ValueError(
            f"Completion marker batch_id mismatch: expected {expected_batch_id!r}, got {batch_id!r}"
        )
    quality_report = marker.get("quality_report")
    if not isinstance(quality_report, str) or not quality_report.strip():
        raise ValueError("Completion marker is missing quality_report URI")
    return {
        "run_status": run_status,
        "batch_id": batch_id,
        "quality_report": quality_report,
        "performance_report": marker.get("performance_report"),
    }


def validate_quality_summary(
    quality: Mapping[str, Any],
    marker: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate batch quality.json consistency with _SUCCESS.json and conservation rules."""
    if quality.get("batch_id") != marker.get("batch_id"):
        raise ValueError("quality.json batch_id disagrees with _SUCCESS.json")
    if quality.get("run_status") != "VERIFIED":
        raise ValueError(
            f"quality.json run_status must be 'VERIFIED', got {quality.get('run_status')!r}"
        )
    post = quality.get("post_cleaning_validation") or {}
    if post.get("status") != "PASS":
        raise ValueError("quality.json post_cleaning_validation.status is not PASS")
    schema_check = post.get("schema_validation") or {}
    if schema_check and schema_check.get("status") != "PASS":
        raise ValueError("quality.json post_cleaning_validation.schema_validation.status is not PASS")
    reconciliation = quality.get("record_reconciliation") or {}
    if reconciliation.get("status") != "PASS" or not reconciliation.get("evidence_multiset_preserved"):
        raise ValueError("quality.json record_reconciliation did not preserve multiset evidence")

    counts = {key: int(quality[key]) for key in COUNT_KEYS}
    if counts["input_records"] != (
        counts["valid_records"] + counts["quarantined_records"] + counts["rejected_records"]
    ):
        raise ValueError("quality.json failed record conservation equation")

    return {
        "batch_id": quality["batch_id"],
        "run_status": quality["run_status"],
        "schema_version": quality.get("schema_version"),
        "pipeline_version": quality.get("pipeline_version"),
        "counts": counts,
        "post_cleaning_status": post["status"],
        "evidence_multiset_preserved": True,
        "source_files": [
            {
                "source_file": item.get("source_file"),
                "input_records": item.get("input_records"),
                "input_bytes": item.get("input_bytes"),
                "source_sha256": item.get("source_sha256"),
            }
            for item in quality.get("files", [])
        ],
    }


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("Boolean value cannot be coerced to integer metric")
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"Non-finite numeric metric: {value!r}")
        return int(value)
    return int(str(value))


def _to_optional_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def to_json_safe(value: Any, *, max_string_length: int = 120) -> Any:
    """Convert Spark/Python values (Decimal, datetime, NaN/Inf, structs) to strict JSON values."""
    if value is None or isinstance(value, bool) or isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "NaN"
        if math.isinf(value):
            return "Infinity" if value > 0 else "-Infinity"
        return round(value, 6)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, str):
        return value if len(value) <= max_string_length else value[: max_string_length - 3] + "..."
    if isinstance(value, Mapping):
        return {str(k): to_json_safe(v, max_string_length=max_string_length) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json_safe(item, max_string_length=max_string_length) for item in value]
    text = str(value)
    return text if len(text) <= max_string_length else text[: max_string_length - 3] + "..."


def format_sample_rows(
    rows: Sequence[Mapping[str, Any]],
    columns: Sequence[str],
    *,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Select curated readable columns and return at most `limit` JSON-safe sample rows."""
    if limit < 0:
        raise ValueError("Sample limit must be non-negative")
    formatted: List[Dict[str, Any]] = []
    for row in list(rows)[:limit]:
        selected_cols = [col for col in columns if col in row] or list(row.keys())
        formatted.append({col: to_json_safe(row[col]) for col in selected_cols})
    return formatted


def rank_aggregates_by_bytes(
    aggregate_rows: Sequence[Mapping[str, Any]],
    *,
    top_n: int = 5,
) -> Dict[str, Any]:
    """Rank `grouping == 'label'` aggregates by total_bytes_sum descending (tie-break label asc)."""
    if top_n < 1:
        raise ValueError("top_n must be at least 1")

    label_entries: List[Dict[str, Any]] = []
    batch_summary: Optional[Dict[str, Any]] = None
    source_file_summaries: List[Dict[str, Any]] = []

    for row in aggregate_rows:
        grouping = row.get("grouping")
        if grouping == "batch":
            batch_summary = {
                "flow_count": _to_int(row.get("flow_count", 0)),
                "valid_records": _to_int(row.get("valid_records", 0)),
                "quarantined_records": _to_int(row.get("quarantined_records", 0)),
                "rejected_records": _to_int(row.get("rejected_records", 0)),
                "total_packets_sum": _to_int(row.get("total_packets_sum", 0)),
                "total_bytes_sum": _to_int(row.get("total_bytes_sum", 0)),
                "avg_flow_duration": _to_optional_float(row.get("avg_flow_duration")),
                "valid_ratio": _to_optional_float(row.get("valid_ratio")),
                "error_ratio": _to_optional_float(row.get("error_ratio")),
            }
        elif grouping == "label":
            valid_records = _to_int(row.get("valid_records", 0))
            if valid_records <= 0:
                continue
            label_entries.append({
                "label": str(row.get("label", "")),
                "flow_count": _to_int(row.get("flow_count", 0)),
                "valid_records": valid_records,
                "quarantined_records": _to_int(row.get("quarantined_records", 0)),
                "rejected_records": _to_int(row.get("rejected_records", 0)),
                "total_packets_sum": _to_int(row.get("total_packets_sum", 0)),
                "total_bytes_sum": _to_int(row.get("total_bytes_sum", 0)),
                "avg_flow_duration": _to_optional_float(row.get("avg_flow_duration")),
            })
        elif grouping == "source_file":
            source_file_summaries.append({
                "source_file": str(row.get("source_file", "")),
                "flow_count": _to_int(row.get("flow_count", 0)),
                "valid_records": _to_int(row.get("valid_records", 0)),
                "quarantined_records": _to_int(row.get("quarantined_records", 0)),
                "rejected_records": _to_int(row.get("rejected_records", 0)),
                "total_packets_sum": _to_int(row.get("total_packets_sum", 0)),
                "total_bytes_sum": _to_int(row.get("total_bytes_sum", 0)),
                "avg_flow_duration": _to_optional_float(row.get("avg_flow_duration")),
            })

    if not label_entries:
        raise ValueError("Aggregates table contains no grouping='label' rows with valid_records > 0")

    label_entries.sort(key=lambda item: (-item["total_bytes_sum"], item["label"]))
    total_valid_bytes = sum(item["total_bytes_sum"] for item in label_entries)
    total_valid_flows = sum(item["valid_records"] for item in label_entries)
    total_valid_packets = sum(item["total_packets_sum"] for item in label_entries)

    ranked_labels: List[Dict[str, Any]] = []
    for index, item in enumerate(label_entries, start=1):
        byte_share = (100.0 * item["total_bytes_sum"] / total_valid_bytes) if total_valid_bytes else 0.0
        flow_share = (100.0 * item["valid_records"] / total_valid_flows) if total_valid_flows else 0.0
        bytes_per_flow = item["total_bytes_sum"] / item["valid_records"] if item["valid_records"] else 0.0
        ranked_labels.append({
            "rank": index,
            **item,
            "byte_share_pct": round(byte_share, 4),
            "flow_share_pct": round(flow_share, 4),
            "avg_payload_bytes_per_valid_flow": round(bytes_per_flow, 2),
        })

    source_file_summaries.sort(key=lambda item: (-item["total_bytes_sum"], item["source_file"]))

    return {
        "total_aggregate_rows": len(aggregate_rows),
        "label_count": len(ranked_labels),
        "total_valid_flows_from_labels": total_valid_flows,
        "total_valid_packets_from_labels": total_valid_packets,
        "total_valid_bytes_from_labels": total_valid_bytes,
        "batch_summary": batch_summary,
        "top_labels_by_total_bytes": ranked_labels[:top_n],
        "all_labels_by_total_bytes": ranked_labels,
        "source_file_summaries": source_file_summaries,
    }


def verify_dataset_schemas(
    dataset_columns: Mapping[str, Sequence[str]],
    contract: Mapping[str, Any],
) -> Dict[str, Any]:
    """Check actual cleaned, features and aggregates columns against Data Contract v2.3.0."""
    expected_cleaned = list(contract["cleaned_core_field_names"]) + list(CLEANED_METADATA_COLUMNS)
    expected_features = expected_cleaned + list(DERIVED_FEATURE_COLUMNS)
    expected_aggregates = list(EXPECTED_AGGREGATE_COLUMNS)

    cleaned_actual = list(dataset_columns.get("cleaned", []))
    features_actual = list(dataset_columns.get("features", []))
    aggregates_actual = list(dataset_columns.get("aggregates", []))

    errors: List[str] = []
    if cleaned_actual != expected_cleaned:
        missing = sorted(set(expected_cleaned) - set(cleaned_actual))
        extra = sorted(set(cleaned_actual) - set(expected_cleaned))
        errors.append(
            f"cleaned columns mismatch (got {len(cleaned_actual)}, expected {len(expected_cleaned)}; "
            f"missing={missing}, extra={extra})"
        )
    if features_actual != expected_features:
        missing = sorted(set(expected_features) - set(features_actual))
        extra = sorted(set(features_actual) - set(expected_features))
        errors.append(
            f"features columns mismatch (got {len(features_actual)}, expected {len(expected_features)}; "
            f"missing={missing}, extra={extra})"
        )
    if set(aggregates_actual) != set(expected_aggregates):
        missing = sorted(set(expected_aggregates) - set(aggregates_actual))
        extra = sorted(set(aggregates_actual) - set(expected_aggregates))
        errors.append(
            f"aggregates columns mismatch (got {len(aggregates_actual)}, expected {len(expected_aggregates)}; "
            f"missing={missing}, extra={extra})"
        )

    return {
        "status": "PASS" if not errors else "FAIL",
        "errors": errors,
        "cleaned_column_count": len(cleaned_actual),
        "features_column_count": len(features_actual),
        "aggregates_column_count": len(aggregates_actual),
    }


def build_snapshot_payload(
    *,
    marker_info: Mapping[str, Any],
    quality_info: Mapping[str, Any],
    dataset_metadata: Mapping[str, Mapping[str, Any]],
    aggregate_rows: Sequence[Mapping[str, Any]],
    sample_rows: Mapping[str, Sequence[Mapping[str, Any]]],
    contract: Mapping[str, Any],
    top_n: int = 5,
    sample_size: int = 5,
) -> Dict[str, Any]:
    """Assemble and cross-check the full Value verification snapshot JSON payload."""
    expected_counts = quality_info["counts"]
    actual_cleaned = int(dataset_metadata["cleaned"]["records"])
    actual_features = int(dataset_metadata["features"]["records"])
    actual_quarantine = int(dataset_metadata["quarantine"]["records"])
    actual_rejected = int(dataset_metadata["rejected"]["records"])
    actual_aggregates = int(dataset_metadata["aggregates"]["records"])

    reconciliation_errors: List[str] = []
    if actual_cleaned != expected_counts["valid_records"]:
        reconciliation_errors.append(
            f"cleaned count {actual_cleaned} != quality valid_records {expected_counts['valid_records']}"
        )
    if actual_features != actual_cleaned:
        reconciliation_errors.append(
            f"features count {actual_features} != cleaned count {actual_cleaned}"
        )
    if actual_quarantine != expected_counts["quarantined_records"]:
        reconciliation_errors.append(
            f"quarantine count {actual_quarantine} != quality quarantined_records {expected_counts['quarantined_records']}"
        )
    if actual_rejected != expected_counts["rejected_records"]:
        reconciliation_errors.append(
            f"rejected count {actual_rejected} != quality rejected_records {expected_counts['rejected_records']}"
        )
    if actual_cleaned + actual_quarantine + actual_rejected != expected_counts["input_records"]:
        reconciliation_errors.append("Parquet output counts fail input = valid + quarantined + rejected")

    schema_check = verify_dataset_schemas(
        {name: meta["columns"] for name, meta in dataset_metadata.items()},
        contract,
    )
    ranking = rank_aggregates_by_bytes(aggregate_rows, top_n=top_n)
    if ranking["total_valid_flows_from_labels"] != actual_cleaned:
        reconciliation_errors.append(
            f"aggregates label valid_records sum {ranking['total_valid_flows_from_labels']} != cleaned count {actual_cleaned}"
        )
    if ranking["batch_summary"] is not None:
        batch_bytes = ranking["batch_summary"]["total_bytes_sum"]
        if batch_bytes != ranking["total_valid_bytes_from_labels"]:
            reconciliation_errors.append(
                f"batch total_bytes_sum {batch_bytes} != sum of label total_bytes_sum {ranking['total_valid_bytes_from_labels']}"
            )
    if actual_aggregates != len(aggregate_rows):
        reconciliation_errors.append(
            f"aggregates count {actual_aggregates} != collected rows {len(aggregate_rows)}"
        )

    all_errors = list(reconciliation_errors) + list(schema_check["errors"])
    status = "PASS" if not all_errors else "FAIL"
    if status != "PASS":
        raise ValueError("Live Value output verification failed: " + "; ".join(all_errors))

    formatted_samples = {
        key: format_sample_rows(sample_rows.get(key, ()), SAMPLE_COLUMNS[key], limit=sample_size)
        for key in ("cleaned", "features", "aggregates", "quarantine")
    }

    return {
        "status": status,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "scope": (
            "Live Spark read-only verification of _SUCCESS.json, quality.json and 5 Parquet outputs "
            "(cleaned, derived_features, aggregates, quarantine, rejected)"
        ),
        "marker": dict(marker_info),
        "quality_summary": dict(quality_info),
        "reconciliation": {
            "status": "PASS",
            "input_records": expected_counts["input_records"],
            "valid_records": actual_cleaned,
            "features_records": actual_features,
            "aggregates_records": actual_aggregates,
            "quarantined_records": actual_quarantine,
            "rejected_records": actual_rejected,
            "equation_holds": True,
        },
        "schema_verification": schema_check,
        "datasets": {
            name: {
                "path": meta["path"],
                "records": int(meta["records"]),
                "column_count": len(meta["columns"]),
                "columns": list(meta["columns"]),
                "dtypes": list(meta.get("dtypes", [])),
            }
            for name, meta in dataset_metadata.items()
        },
        "value_aggregates": ranking,
        "samples": formatted_samples,
    }


def _read_text_uri(spark: Any, uri: str) -> str:
    """Read a UTF-8 text object from s3a:// or file:// using Spark's Hadoop FileSystem."""
    from processing.output import hadoop_path

    fs, path = hadoop_path(spark, uri)
    if not fs.exists(path):
        raise FileNotFoundError(f"Required object not found on storage: {uri}")
    stream = fs.open(path)
    reader = spark._jvm.java.io.BufferedReader(
        spark._jvm.java.io.InputStreamReader(stream, "UTF-8")
    )
    lines: List[str] = []
    try:
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(line)
    finally:
        reader.close()
    return "\n".join(lines)


def _select_sample_rows(frame: Any, preferred_columns: Sequence[str], limit: int) -> List[Dict[str, Any]]:
    """Collect a small, deterministic-width slice of sample rows from a Spark DataFrame."""
    selected = [col for col in preferred_columns if col in frame.columns]
    projected = frame.select(*selected) if selected else frame
    return [row.asDict(recursive=True) for row in projected.limit(limit).collect()]


def run_live_verification(
    config: ProcessingConfig,
    uris: Mapping[str, str],
    *,
    top_n: int = 5,
    sample_size: int = 5,
    local_mode: bool = False,
) -> Dict[str, Any]:
    """Connect to Spark/MinIO, verify _SUCCESS.json + 5 Parquet datasets, and return snapshot dict."""
    try:
        from processing.config import create_spark_session
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PySpark is not installed in this Python environment. Run inside Docker spark-master:\n"
            "  docker compose --env-file docker/.env -f docker/docker-compose.yml exec -T spark-master "
            "python3 -m experiments.verify_value_outputs --batch-id <BATCH_ID>"
        ) from exc

    contract = load_contract(config.schema_path)
    enable_minio = not local_mode and any(uri.startswith("s3a://") for uri in uris.values())
    spark_cfg = replace(config, spark_app_name="value-output-verification")
    if local_mode:
        spark_cfg = replace(spark_cfg, spark_master="local[2]")

    spark = create_spark_session(spark_cfg, enable_minio=enable_minio)
    spark.sparkContext.setLogLevel("WARN")
    try:
        marker_raw = json.loads(_read_text_uri(spark, uris["success_marker"]))
        marker_info = validate_completion_marker(
            marker_raw,
            expected_batch_id=uris.get("batch_id"),
        )
        quality_uri = marker_info["quality_report"]
        quality_raw = json.loads(_read_text_uri(spark, quality_uri))
        quality_info = validate_quality_summary(quality_raw, marker_info)

        outputs_map = quality_raw.get("outputs") or {}
        dataset_paths = {
            "cleaned": (outputs_map.get("cleaned_path") or {}).get("path") or uris.get("cleaned_path"),
            "features": (outputs_map.get("features_path") or {}).get("path") or uris.get("features_path"),
            "aggregates": (outputs_map.get("aggregates_path") or {}).get("path") or uris.get("aggregates_path"),
            "quarantine": (outputs_map.get("quarantine_path") or {}).get("path") or uris.get("quarantine_path"),
            "rejected": (outputs_map.get("rejected_path") or {}).get("path") or uris.get("rejected_path"),
        }
        missing_paths = [name for name, path in dataset_paths.items() if not path]
        if missing_paths:
            raise ValueError(f"Could not resolve dataset URIs for: {', '.join(missing_paths)}")

        dataset_metadata: Dict[str, Dict[str, Any]] = {}
        sample_rows: Dict[str, List[Dict[str, Any]]] = {}
        aggregate_rows: List[Dict[str, Any]] = []

        for name, uri in dataset_paths.items():
            frame = spark.read.parquet(uri)
            row_count = frame.count()
            dataset_metadata[name] = {
                "path": uri,
                "records": row_count,
                "columns": list(frame.columns),
                "dtypes": [list(item) for item in frame.dtypes],
            }
            if name == "aggregates":
                aggregate_rows = [row.asDict(recursive=True) for row in frame.collect()]
                sample_rows[name] = aggregate_rows[:sample_size]
            elif name in SAMPLE_COLUMNS:
                sample_rows[name] = _select_sample_rows(frame, SAMPLE_COLUMNS[name], sample_size)

        return build_snapshot_payload(
            marker_info=marker_info,
            quality_info=quality_info,
            dataset_metadata=dataset_metadata,
            aggregate_rows=aggregate_rows,
            sample_rows=sample_rows,
            contract=contract,
            top_n=top_n,
            sample_size=sample_size,
        )
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a completed CICIDS2017 batch on MinIO (or local Parquet output): "
            "check _SUCCESS.json, verify 5 Parquet datasets, rank labels by total payload bytes, "
            "and export a machine-readable JSON snapshot."
        )
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument(
        "--batch-id",
        help="Batch ID under standard MinIO prefixes (or under --output-prefix).",
    )
    source_group.add_argument(
        "--success-marker",
        help="Full URI to _SUCCESS.json (e.g. s3a://bigdata-network/reports/data_quality/<batch>/_SUCCESS.json).",
    )
    parser.add_argument(
        "--output-prefix",
        help=(
            "Optional benchmark/custom prefix containing cleaned/, features/, aggregates/, "
            "quarantine/, rejected/, report/ (used with --batch-id)."
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_SNAPSHOT_PATH,
        help=f"Output JSON snapshot path (default: {DEFAULT_SNAPSHOT_PATH.relative_to(PROJECT_ROOT)}).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=5,
        help="Number of top labels ranked by total_bytes_sum to highlight (default: 5).",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Number of representative sample rows per dataset (default: 5).",
    )
    parser.add_argument(
        "--local",
        action="store_true",
        help="Read file:// outputs with local[2] Spark without MinIO S3A credentials.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate configuration and print inspection plan without connecting to Spark/MinIO.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting an existing --output snapshot file.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.output_prefix and args.success_marker:
        parser.error("--output-prefix cannot be combined with --success-marker")
    if args.top_n < 1:
        parser.error("--top-n must be >= 1")
    if args.sample_size < 0:
        parser.error("--sample-size must be >= 0")
    if args.output.exists() and not args.overwrite and not args.dry_run:
        parser.error(f"Snapshot destination already exists: {args.output} (pass --overwrite to replace)")

    config = ProcessingConfig.from_env()
    try:
        uris = resolve_batch_uris(
            config,
            batch_id=args.batch_id,
            output_prefix=args.output_prefix,
            success_marker=args.success_marker,
        )
    except ValueError as error:
        print(f"ERROR: Invalid configuration or URI: {error}", file=sys.stderr)
        return 2

    uses_s3a = any(uri.startswith("s3a://") for uri in uris.values())
    if args.local and uses_s3a:
        print("ERROR: --local cannot be used with s3a:// URIs.", file=sys.stderr)
        return 2

    config_status = "READY"
    config_note: Optional[str] = None
    try:
        config.validate(require_minio=uses_s3a and not args.local)
    except ValueError as error:
        config_status = "MISSING_MINIO_CONFIG"
        config_note = str(error)

    if args.dry_run:
        plan = {
            "mode": "DRY_RUN",
            "config_status": config_status,
            "config_note": config_note,
            "spark_master": "local[2]" if args.local else config.spark_master,
            "minio_endpoint": None if args.local else config.minio_endpoint,
            "minio_bucket": None if args.local else config.minio_bucket,
            "target_uris": uris,
            "top_n": args.top_n,
            "sample_size": args.sample_size,
            "output_path": str(args.output),
        }
        print(json.dumps(plan, ensure_ascii=False, indent=2, allow_nan=False))
        return 0

    if config_status != "READY":
        print(
            f"ERROR: Spark/MinIO configuration is not ready: {config_note}\n"
            "Ensure Docker services (minio, spark-master, spark-worker-1, spark-worker-2) are running "
            "and execute inside spark-master with docker/.env loaded.",
            file=sys.stderr,
        )
        return 1

    try:
        snapshot = run_live_verification(
            config,
            uris,
            top_n=args.top_n,
            sample_size=args.sample_size,
            local_mode=args.local,
        )
    except FileNotFoundError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # noqa: BLE001 — fail closed without leaking secrets
        print(
            f"ERROR: Live verification failed ({type(error).__name__}): {error}\n"
            "Check that Spark/MinIO Docker containers are up and the target batch completed with _SUCCESS.json.",
            file=sys.stderr,
        )
        return 1

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(snapshot, ensure_ascii=False, indent=2, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    top_preview = [
        {
            "rank": item["rank"],
            "label": item["label"],
            "valid_records": item["valid_records"],
            "total_bytes_sum": item["total_bytes_sum"],
        }
        for item in snapshot["value_aggregates"]["top_labels_by_total_bytes"]
    ]
    print(
        json.dumps(
            {
                "status": snapshot["status"],
                "batch_id": snapshot["marker"]["batch_id"],
                "output": str(args.output),
                "reconciliation": snapshot["reconciliation"],
                "top_labels_by_total_bytes": top_preview,
            },
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

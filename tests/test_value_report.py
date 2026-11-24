"""Unit tests for 5V cross-checking, Value report generation, and MinIO output verification helpers."""

from __future__ import annotations

import ast
import csv
from datetime import datetime, timezone
from decimal import Decimal
import io
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from experiments.build_value_report import (
    DEFAULT_EVIDENCE_DIR,
    DEFAULT_QUALITY_AUDIT_MD,
    ROOT,
    build_markdown,
    build_verification_summary,
    collect_value_evidence,
    main as build_report_main,
)
from experiments.verify_value_outputs import (
    CLEANED_METADATA_COLUMNS,
    DERIVED_FEATURE_COLUMNS,
    EXPECTED_AGGREGATE_COLUMNS,
    build_snapshot_payload,
    format_sample_rows,
    main as verify_outputs_main,
    rank_aggregates_by_bytes,
    resolve_batch_uris,
    validate_completion_marker,
    validate_quality_summary,
)
from processing.config import ProcessingConfig, load_contract


def _copy_evidence_tree(destination: Path) -> Path:
    target = destination / "evidence"
    shutil.copytree(DEFAULT_EVIDENCE_DIR, target)
    return target


def _make_valid_live_snapshot(contract: dict) -> dict:
    cleaned_cols = list(contract["cleaned_core_field_names"]) + list(CLEANED_METADATA_COLUMNS)
    features_cols = cleaned_cols + list(DERIVED_FEATURE_COLUMNS)
    aggregates_cols = list(EXPECTED_AGGREGATE_COLUMNS)

    marker = validate_completion_marker(
        {
            "run_status": "COMPLETE",
            "batch_id": "live-batch-20261125",
            "quality_report": "s3a://bigdata-network/reports/data_quality/live-batch-20261125/quality.json",
            "performance_report": "s3a://bigdata-network/reports/performance/live-batch-20261125/performance.json",
        },
        expected_batch_id="live-batch-20261125",
    )
    quality = validate_quality_summary(
        {
            "batch_id": "live-batch-20261125",
            "run_status": "VERIFIED",
            "schema_version": "v2.3.0",
            "pipeline_version": "0.1.0",
            "input_records": 100,
            "valid_records": 95,
            "quarantined_records": 5,
            "rejected_records": 0,
            "post_cleaning_validation": {
                "status": "PASS",
                "schema_validation": {"status": "PASS"},
            },
            "record_reconciliation": {
                "status": "PASS",
                "evidence_multiset_preserved": True,
            },
            "files": [{"source_file": "sample.csv", "input_records": 100, "input_bytes": 1024}],
        },
        marker,
    )
    aggregate_rows = [
        {
            "grouping": "batch",
            "source_file": None,
            "label": None,
            "flow_count": 100,
            "valid_records": 95,
            "quarantined_records": 5,
            "rejected_records": 0,
            "total_packets_sum": Decimal("1000"),
            "total_bytes_sum": Decimal("50000"),
            "avg_flow_duration": 120.5,
            "valid_ratio": 0.95,
            "error_ratio": 0.05,
            "batch_id": "live-batch-20261125",
        },
        {
            "grouping": "label",
            "source_file": None,
            "label": "BENIGN",
            "flow_count": 80,
            "valid_records": 75,
            "quarantined_records": 5,
            "rejected_records": 0,
            "total_packets_sum": Decimal("700"),
            "total_bytes_sum": Decimal("35000"),
            "avg_flow_duration": 100.0,
            "valid_ratio": 0.9375,
            "error_ratio": 0.0625,
            "batch_id": "live-batch-20261125",
        },
        {
            "grouping": "label",
            "source_file": None,
            "label": "DoS Hulk",
            "flow_count": 15,
            "valid_records": 15,
            "quarantined_records": 0,
            "rejected_records": 0,
            "total_packets_sum": Decimal("200"),
            "total_bytes_sum": Decimal("10000"),
            "avg_flow_duration": 200.0,
            "valid_ratio": 1.0,
            "error_ratio": 0.0,
            "batch_id": "live-batch-20261125",
        },
        {
            "grouping": "label",
            "source_file": None,
            "label": "Heartbleed",
            "flow_count": 5,
            "valid_records": 5,
            "quarantined_records": 0,
            "rejected_records": 0,
            "total_packets_sum": Decimal("100"),
            "total_bytes_sum": Decimal("5000"),
            "avg_flow_duration": 300.0,
            "valid_ratio": 1.0,
            "error_ratio": 0.0,
            "batch_id": "live-batch-20261125",
        },
    ]
    dataset_metadata = {
        "cleaned": {
            "path": "s3a://bigdata-network/cleaned/flows/live-batch-20261125",
            "records": 95,
            "columns": cleaned_cols,
            "dtypes": [[col, "string"] for col in cleaned_cols],
        },
        "features": {
            "path": "s3a://bigdata-network/processed/derived_features/live-batch-20261125",
            "records": 95,
            "columns": features_cols,
            "dtypes": [[col, "string"] for col in features_cols],
        },
        "aggregates": {
            "path": "s3a://bigdata-network/processed/aggregates/live-batch-20261125",
            "records": 4,
            "columns": aggregates_cols,
            "dtypes": [[col, "string"] for col in aggregates_cols],
        },
        "quarantine": {
            "path": "s3a://bigdata-network/quality/quarantine/live-batch-20261125",
            "records": 5,
            "columns": cleaned_cols + ["_raw_values"],
            "dtypes": [],
        },
        "rejected": {
            "path": "s3a://bigdata-network/quality/rejected/live-batch-20261125",
            "records": 0,
            "columns": cleaned_cols + ["_raw_values"],
            "dtypes": [],
        },
    }
    sample_rows = {
        "cleaned": [
            {
                "source_file": "sample.csv",
                "destination_port": 443,
                "flow_duration": 1000,
                "total_fwd_packets": 10,
                "total_backward_packets": 5,
                "flow_bytes_s": 1234.5,
                "flow_packets_s": 15.0,
                "init_win_bytes_forward": None,
                "init_win_bytes_backward": 256,
                "label": "BENIGN",
                "quality_status": "VALID",
                "flow_bytes_s_invalid": False,
                "init_win_fwd_missing": True,
                "init_win_bwd_missing": False,
            }
        ],
        "features": [
            {
                "source_file": "sample.csv",
                "label": "Heartbleed",
                "destination_port": 443,
                "total_fwd_packets": 10,
                "total_backward_packets": 10,
                "total_packets": Decimal("20"),
                "total_bytes": Decimal("5000"),
                "fwd_packet_ratio": 0.5,
                "bwd_packet_ratio": 0.5,
                "quality_status": "VALID",
            }
        ],
        "aggregates": aggregate_rows,
        "quarantine": [
            {
                "source_file": "sample.csv",
                "label": "BENIGN",
                "destination_port": 80,
                "flow_duration": -1,
                "flow_iat_mean": -1.0,
                "quality_status": "QUARANTINED",
                "quality_reason_codes": ["NEGATIVE:flow_duration", "NEGATIVE:flow_iat_mean"],
            }
        ],
    }
    return build_snapshot_payload(
        marker_info=marker,
        quality_info=quality,
        dataset_metadata=dataset_metadata,
        aggregate_rows=aggregate_rows,
        sample_rows=sample_rows,
        contract=contract,
        top_n=5,
        sample_size=5,
    )


class RealEvidenceCrossCheckTests(unittest.TestCase):
    def test_master_5v_artifacts_pass_all_crosschecks(self) -> None:
        evidence = collect_value_evidence()
        summary = build_verification_summary(evidence)

        self.assertEqual(summary["status"], "PASS")
        self.assertEqual(summary["checks"]["artifact_sha256_status"], "PASS")
        self.assertEqual(summary["checks"]["record_conservation"], {
            "input_records": 2830743,
            "valid_records": 2830628,
            "quarantined_records": 115,
            "rejected_records": 0,
        })
        self.assertEqual(summary["checks"]["full_data_spark_trials_pass"], "3/3 PASS")
        self.assertEqual(summary["checks"]["minio_historical_audit"]["batch_count"], 31)
        self.assertEqual(summary["checks"]["minio_historical_audit"]["parquet_outputs"], 155)

        retention = summary["checks"]["retention_vs_dropna"]
        self.assertEqual(retention["policy_valid_records"], 2830628)
        self.assertEqual(retention["valid_with_cleaned_null"], 1442539)
        self.assertEqual(retention["naive_dropna_records"], 1388089)
        self.assertEqual(
            retention["valid_with_cleaned_null"] + retention["naive_dropna_records"],
            retention["policy_valid_records"],
        )

        vel = summary["checks"]["velocity_highlights"]
        self.assertAlmostEqual(vel["full_8csv_median_seconds"], 468.713, places=3)
        self.assertAlmostEqual(vel["full_8csv_median_flows_per_second"], 6039.392, places=3)
        self.assertAlmostEqual(vel["workers_1_vs_2_speedup"], 0.896, places=3)
        self.assertEqual(vel["slowest_full_stage"], "cleaning_post_validation_and_after_report")
        self.assertAlmostEqual(vel["slowest_full_stage_median_seconds"], 175.247, places=3)

        self.assertEqual(summary["checks"]["variety_zeek_demo"]["records"], 360)
        self.assertEqual(summary["checks"]["variety_zeek_demo"]["missing_service_records"], 200)
        self.assertFalse(summary["checks"]["live_value_snapshot_attached"])

    def test_offline_markdown_contains_five_sections_and_obeys_guardrails(self) -> None:
        evidence = collect_value_evidence()
        markdown = build_markdown(evidence)

        required_headings = (
            "## 1. Đối tượng sử dụng và quyết định được hỗ trợ",
            "## 2. Giá trị thực tế gắn với kết quả xác nhận dữ liệu",
            "## 3. Phân biệt kết quả đã chứng minh và phần mở rộng dự kiến",
            "## 4. Hạn chế hiện tại và hướng phát triển",
            "## 5. Tổng hợp chuỗi 5V của báo cáo nhóm",
            "## 6. Tái lập báo cáo và tài liệu minh chứng",
        )
        for heading in required_headings:
            self.assertIn(heading, markdown)

        # Must honestly state offline status when no live snapshot is provided.
        self.assertIn("CHƯA CHẠY", markdown)
        self.assertNotIn("Bảng Q.2a. Xác minh trực tiếp 5 dataset Parquet trên MinIO", markdown)

        # Guardrails from 5v-big-data-note.md and course scope.
        self.assertIn("884,65 MB", markdown)
        self.assertIn("843,66 MiB", markdown)
        self.assertIn("KHÔNG đồng nghĩa với nhãn `BENIGN`", markdown)
        self.assertIn("batch processing", markdown.lower())
        self.assertNotIn("Random Forest", markdown)
        self.assertNotIn("đã chạy streaming thời gian thực", markdown.lower())

        # Ensure our new modules never import ML packages.
        for module_rel in ("experiments/build_value_report.py", "experiments/verify_value_outputs.py"):
            tree = ast.parse((ROOT / module_rel).read_text(encoding="utf-8"))
            imported: set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported.add(node.module)
            for forbidden in ("pyspark.ml", "sklearn", "xgboost", "lightgbm", "torch"):
                self.assertFalse(
                    any(name == forbidden or name.startswith(forbidden + ".") for name in imported),
                    f"Forbidden ML import {forbidden} in {module_rel}",
                )

    def test_markdown_embeds_live_snapshot_when_provided(self) -> None:
        contract = load_contract()
        snapshot = _make_valid_live_snapshot(contract)
        with tempfile.TemporaryDirectory() as folder:
            snapshot_path = Path(folder) / "snapshot.json"
            snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
            evidence = collect_value_evidence(value_snapshot_path=snapshot_path)
            markdown = build_markdown(evidence)

        self.assertTrue(evidence["value"]["live_snapshot"]["attached"])
        self.assertIn("ĐÃ XÁC MINH (`PASS`)", markdown)
        self.assertIn("live-batch-20261125", markdown)
        self.assertIn("Bảng Q.2a. Xác minh trực tiếp 5 dataset Parquet trên MinIO", markdown)
        self.assertIn("Bảng Q.2b. Xếp hạng nhãn theo tổng payload byte", markdown)
        self.assertIn("5 dòng mẫu từ `cleaned/flows/`", markdown)
        self.assertIn("5 dòng mẫu từ `processed/derived_features/`", markdown)
        self.assertIn("5 dòng mẫu từ `processed/aggregates/`", markdown)
        self.assertIn("5 dòng mẫu từ `quality/quarantine/`", markdown)

    def test_cli_builds_report_and_verification_json_in_temp_dir(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            report_path = Path(folder) / "value-report.md"
            verif_path = Path(folder) / "value-verification.json"
            with patch("sys.stdout", new=io.StringIO()):
                exit_code = build_report_main([
                    "--output", str(report_path),
                    "--verification-json", str(verif_path),
                ])
            self.assertEqual(exit_code, 0)
            self.assertTrue(report_path.exists())
            self.assertTrue(verif_path.exists())
            verif = json.loads(verif_path.read_text(encoding="utf-8"))
            self.assertEqual(verif["status"], "PASS")


class TamperDetectionNegativeTests(unittest.TestCase):
    def test_detects_artifact_checksum_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            ev = _copy_evidence_tree(Path(folder))
            verification_path = ev / "verification.json"
            data = json.loads(verification_path.read_text(encoding="utf-8"))
            data["unique_batches"] = 999
            verification_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                collect_value_evidence(evidence_dir=ev)

    def test_detects_record_conservation_violation_even_when_manifest_updated(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            ev = _copy_evidence_tree(Path(folder))
            source_path = ev / "source-audit.json"
            data = json.loads(source_path.read_text(encoding="utf-8"))
            data["files"][0]["policy_preview"]["valid_records"] -= 1
            raw_bytes = json.dumps(data, indent=2).encode("utf-8")
            source_path.write_bytes(raw_bytes)
            self._update_manifest_entry(ev, "source-audit.json", raw_bytes)

            with self.assertRaisesRegex(ValueError, "conservation"):
                collect_value_evidence(evidence_dir=ev)

    def test_detects_velocity_summary_vs_runs_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            ev = _copy_evidence_tree(Path(folder))
            summary_path = ev / "summary.csv"
            with summary_path.open(encoding="utf-8-sig", newline="") as stream:
                rows = list(csv.DictReader(stream))
            rows[0]["median_seconds"] = "999.999"
            buf = io.StringIO(newline="")
            writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
            raw_bytes = buf.getvalue().encode("utf-8")
            summary_path.write_bytes(raw_bytes)
            self._update_manifest_entry(ev, "summary.csv", raw_bytes)

            with self.assertRaisesRegex(ValueError, "median_seconds mismatch"):
                collect_value_evidence(evidence_dir=ev)

    def test_detects_retention_reconciliation_tampering(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            tampered_audit = Path(folder) / "audit.md"
            text = DEFAULT_QUALITY_AUDIT_MD.read_text(encoding="utf-8")
            tampered_audit.write_text(text.replace("1,388,089", "1,388,000"), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "retention"):
                collect_value_evidence(quality_audit_md=tampered_audit)

    def test_rejects_incomplete_live_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            bad_snapshot = Path(folder) / "bad.json"
            bad_snapshot.write_text(
                json.dumps({"status": "PASS", "marker": {"run_status": "RUNNING"}}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "COMPLETE"):
                collect_value_evidence(value_snapshot_path=bad_snapshot)

    @staticmethod
    def _update_manifest_entry(evidence_dir: Path, filename: str, payload: bytes) -> None:
        import hashlib

        manifest_path = evidence_dir / "artifact-sha256.csv"
        with manifest_path.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        for row in rows:
            if row["File"] == filename:
                row["Bytes"] = str(len(payload))
                row["SHA256"] = hashlib.sha256(payload).hexdigest().upper()
        with manifest_path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=["File", "Bytes", "SHA256"], quoting=csv.QUOTE_ALL)
            writer.writeheader()
            writer.writerows(rows)


class VerifyValueOutputsHelperTests(unittest.TestCase):
    def test_validate_completion_marker_enforces_complete_and_batch_id(self) -> None:
        ok = validate_completion_marker(
            {
                "run_status": "COMPLETE",
                "batch_id": "b-01",
                "quality_report": "s3a://bigdata-network/reports/data_quality/b-01/quality.json",
            },
            expected_batch_id="b-01",
        )
        self.assertEqual(ok["run_status"], "COMPLETE")

        with self.assertRaisesRegex(ValueError, "not complete"):
            validate_completion_marker({"run_status": "VERIFIED", "batch_id": "b-01", "quality_report": "x"})

        with self.assertRaisesRegex(ValueError, "batch_id mismatch"):
            validate_completion_marker(
                {"run_status": "COMPLETE", "batch_id": "b-02", "quality_report": "x"},
                expected_batch_id="b-01",
            )

    def test_rank_aggregates_by_bytes_filters_and_sorts_deterministically(self) -> None:
        rows = [
            {
                "grouping": "batch",
                "label": None,
                "flow_count": 30,
                "valid_records": 28,
                "quarantined_records": 2,
                "rejected_records": 0,
                "total_packets_sum": Decimal("300"),
                "total_bytes_sum": Decimal("1000"),
                "avg_flow_duration": 50.0,
            },
            {
                "grouping": "label",
                "label": "Alpha",
                "flow_count": 10,
                "valid_records": 10,
                "quarantined_records": 0,
                "rejected_records": 0,
                "total_packets_sum": Decimal("50"),
                "total_bytes_sum": Decimal("400"),
                "avg_flow_duration": 20.0,
            },
            {
                "grouping": "label",
                "label": "Beta",
                "flow_count": 10,
                "valid_records": 10,
                "quarantined_records": 0,
                "rejected_records": 0,
                "total_packets_sum": Decimal("150"),
                "total_bytes_sum": Decimal("400"),
                "avg_flow_duration": 30.0,
            },
            {
                "grouping": "label",
                "label": "Gamma",
                "flow_count": 8,
                "valid_records": 8,
                "quarantined_records": 0,
                "rejected_records": 0,
                "total_packets_sum": Decimal("100"),
                "total_bytes_sum": Decimal("200"),
                "avg_flow_duration": 40.0,
            },
            {
                "grouping": "label",
                "label": "ZeroValid",
                "flow_count": 2,
                "valid_records": 0,
                "quarantined_records": 2,
                "rejected_records": 0,
                "total_packets_sum": Decimal("0"),
                "total_bytes_sum": Decimal("0"),
                "avg_flow_duration": None,
            },
            {
                "grouping": "source_file",
                "source_file": "day1.csv",
                "label": None,
                "flow_count": 30,
                "valid_records": 28,
                "quarantined_records": 2,
                "rejected_records": 0,
                "total_packets_sum": Decimal("300"),
                "total_bytes_sum": Decimal("1000"),
                "avg_flow_duration": 50.0,
            },
        ]
        result = rank_aggregates_by_bytes(rows, top_n=2)
        self.assertEqual(result["label_count"], 3)
        self.assertEqual(result["total_valid_bytes_from_labels"], 1000)
        self.assertEqual([r["label"] for r in result["top_labels_by_total_bytes"]], ["Alpha", "Beta"])
        self.assertEqual([r["rank"] for r in result["all_labels_by_total_bytes"]], [1, 2, 3])
        self.assertAlmostEqual(result["top_labels_by_total_bytes"][0]["byte_share_pct"], 40.0)
        self.assertEqual(result["batch_summary"]["valid_records"], 28)

    def test_format_sample_rows_produces_strict_json_without_nan(self) -> None:
        rows = [
            {
                "label": "BENIGN",
                "total_bytes": Decimal("123456"),
                "ratio": float("nan"),
                "inf_val": float("inf"),
                "ts": datetime(2026, 11, 25, 1, 0, tzinfo=timezone.utc),
                "reasons": ["SENTINEL:init_win_bytes_forward"],
                "extra_ignored": "secret",
            }
            for _ in range(7)
        ]
        formatted = format_sample_rows(rows, ("label", "total_bytes", "ratio", "inf_val", "ts", "reasons"), limit=5)
        self.assertEqual(len(formatted), 5)
        self.assertEqual(formatted[0]["total_bytes"], 123456)
        self.assertEqual(formatted[0]["ratio"], "NaN")
        self.assertEqual(formatted[0]["inf_val"], "Infinity")
        self.assertNotIn("extra_ignored", formatted[0])
        # Must serialize cleanly under strict JSON (allow_nan=False).
        serialized = json.dumps(formatted, allow_nan=False)
        self.assertIn("123456", serialized)

    def test_resolve_batch_uris_supports_default_and_custom_prefix_and_blocks_raw(self) -> None:
        cfg = ProcessingConfig.from_env({})
        default_uris = resolve_batch_uris(cfg, batch_id="batch-123")
        self.assertEqual(
            default_uris["success_marker"],
            "s3a://bigdata-network/reports/data_quality/batch-123/_SUCCESS.json",
        )
        self.assertEqual(
            default_uris["cleaned_path"],
            "s3a://bigdata-network/cleaned/flows/batch-123",
        )
        self.assertEqual(
            default_uris["features_path"],
            "s3a://bigdata-network/processed/derived_features/batch-123",
        )
        self.assertEqual(
            default_uris["aggregates_path"],
            "s3a://bigdata-network/processed/aggregates/batch-123",
        )

        bench_uris = resolve_batch_uris(
            cfg,
            batch_id="bench-1",
            output_prefix="s3a://bigdata-network/benchmarks/real-20261110-141211",
        )
        self.assertEqual(
            bench_uris["success_marker"],
            "s3a://bigdata-network/benchmarks/real-20261110-141211/report/bench-1/_SUCCESS.json",
        )
        self.assertEqual(
            bench_uris["aggregates_path"],
            "s3a://bigdata-network/benchmarks/real-20261110-141211/aggregates/bench-1",
        )

        with self.assertRaisesRegex(ValueError, "raw/"):
            resolve_batch_uris(cfg, batch_id="b1", output_prefix="s3a://bigdata-network/raw/cicids2017")

    def test_verify_cli_dry_run_exits_zero_without_spark_or_minio(self) -> None:
        stdout = io.StringIO()
        with patch("sys.stdout", new=stdout):
            code = verify_outputs_main(["--batch-id", "demo-batch-01", "--dry-run"])
        self.assertEqual(code, 0)
        plan = json.loads(stdout.getvalue())
        self.assertEqual(plan["mode"], "DRY_RUN")
        self.assertEqual(
            plan["target_uris"]["aggregates_path"],
            "s3a://bigdata-network/processed/aggregates/demo-batch-01",
        )


if __name__ == "__main__":
    unittest.main()

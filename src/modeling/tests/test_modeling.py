"""Unit and Spark integration tests for the anomaly modeling pipeline."""

import json
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from src.modeling.config import (
    ALL_FEATURE_QUALITY_FLAGS,
    CORE_FEATURE_COLUMNS,
    EXCLUDED_FROM_BASELINE_FEATURES,
    RATE_QUALITY_FLAGS,
    ModelingConfig,
)
from src.modeling.metrics import confusion_metrics, select_best_threshold


class PureMetricsTests(unittest.TestCase):
    def test_confusion_metrics_attack_positive_convention(self):
        # TP=80, FP=10, FN=20, TN=890 => total=1000
        m = confusion_metrics(tp=80, fp=10, fn=20, tn=890)
        self.assertEqual(m["total_evaluated"], 1000)
        self.assertAlmostEqual(m["precision"], 80 / 90, places=6)
        self.assertAlmostEqual(m["recall"], 80 / 100, places=6)
        self.assertAlmostEqual(m["accuracy"], 970 / 1000, places=6)
        self.assertAlmostEqual(m["fpr"], 10 / 900, places=6)
        expected_f1_atk = 2 * (80 / 90) * (80 / 100) / ((80 / 90) + (80 / 100))
        expected_p_ben = 890 / 910
        expected_r_ben = 890 / 900
        expected_f1_ben = 2 * expected_p_ben * expected_r_ben / (expected_p_ben + expected_r_ben)
        self.assertAlmostEqual(m["f1"], expected_f1_atk, places=6)
        self.assertAlmostEqual(m["macro_f1"], 0.5 * (expected_f1_atk + expected_f1_ben), places=6)

    def test_baseline_features_exclude_all_quality_flags(self):
        self.assertEqual(len(CORE_FEATURE_COLUMNS), 21)
        for flag in ALL_FEATURE_QUALITY_FLAGS:
            self.assertNotIn(flag, CORE_FEATURE_COLUMNS)
            self.assertIn(flag, EXCLUDED_FROM_BASELINE_FEATURES)
        self.assertNotIn("quality_reason_codes", CORE_FEATURE_COLUMNS)
        self.assertNotIn("label", CORE_FEATURE_COLUMNS)

    def test_select_best_threshold_respects_budget_and_balance(self):
        candidates = [
            {
                "tau_low": 0.40,
                "tau_high": 0.60,
                "coverage": 0.99,
                "review_rate": 0.01,
                "attack_to_review_rate": 0.02,
                "auto_metrics": {"f1": 0.990, "macro_f1": 0.992, "fpr": 0.002},
            },
            {
                "tau_low": 0.35,
                "tau_high": 0.65,
                "coverage": 0.985,
                "review_rate": 0.015,
                "attack_to_review_rate": 0.03,
                "auto_metrics": {"f1": 0.996, "macro_f1": 0.997, "fpr": 0.001},
            },
            {
                "tau_low": 0.20,
                "tau_high": 0.80,
                "coverage": 0.90,
                "review_rate": 0.10,  # exceeds 5% budget
                "attack_to_review_rate": 0.20,
                "auto_metrics": {"f1": 0.9999, "macro_f1": 0.9999, "fpr": 0.0001},
            },
        ]
        res = select_best_threshold(candidates, max_review_rate=0.05, review_cost_weight=0.05)
        self.assertEqual(res["selected"]["tau_low"], 0.35)
        self.assertEqual(res["selected"]["tau_high"], 0.65)


try:
    import pyspark  # noqa: F401
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


@unittest.skipUnless(HAS_PYSPARK, "PySpark not installed on host; run inside spark-master container")
class SparkModelingIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("test-modeling-pipeline")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def _build_synthetic_valid_parquet(self, path: str) -> None:
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DoubleType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        schema = StructType([
            StructField("destination_port", IntegerType(), True),
            StructField("flow_duration", LongType(), True),
            StructField("total_fwd_packets", LongType(), True),
            StructField("total_backward_packets", LongType(), True),
            StructField("total_length_of_fwd_packets", LongType(), True),
            StructField("total_length_of_bwd_packets", LongType(), True),
            StructField("flow_bytes_s", DoubleType(), True),
            StructField("flow_packets_s", DoubleType(), True),
            StructField("flow_iat_mean", DoubleType(), True),
            StructField("flow_iat_std", DoubleType(), True),
            StructField("min_packet_length", DoubleType(), True),
            StructField("max_packet_length", DoubleType(), True),
            StructField("packet_length_mean", DoubleType(), True),
            StructField("packet_length_std", DoubleType(), True),
            StructField("fin_flag_count", IntegerType(), True),
            StructField("syn_flag_count", IntegerType(), True),
            StructField("rst_flag_count", IntegerType(), True),
            StructField("psh_flag_count", IntegerType(), True),
            StructField("ack_flag_count", IntegerType(), True),
            StructField("init_win_bytes_forward", LongType(), True),
            StructField("init_win_bytes_backward", LongType(), True),
            StructField("label", StringType(), True),
            StructField("source_file", StringType(), True),
            StructField("source_dataset", StringType(), True),
            StructField("batch_id", StringType(), True),
            StructField("pipeline_version", StringType(), True),
            StructField("schema_version", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("quality_status", StringType(), True),
            StructField("quality_reason_codes", ArrayType(StringType()), True),
            StructField("flow_bytes_s_invalid", BooleanType(), True),
            StructField("flow_packets_s_invalid", BooleanType(), True),
            StructField("init_win_fwd_missing", BooleanType(), True),
            StructField("init_win_bwd_missing", BooleanType(), True),
        ])

        now = datetime.now(timezone.utc)
        rows = []
        # 120 BENIGN rows (with some duplicates and some quality flags)
        for i in range(120):
            rate_bad = (i % 25 == 0)
            win_bad = (i % 3 == 0)
            row = (
                443 if i % 2 == 0 else 53,
                10000 + i * 50,
                5 + (i % 10),
                4 + (i % 8),
                500 + i * 10,
                1200 + i * 20,
                None if rate_bad else float(1000.0 + i * 5),
                None if rate_bad else float(50.0 + i),
                float(200.0 + i),
                float(50.0 + (i % 15)),
                40.0,
                1500.0,
                float(300.0 + (i % 100)),
                float(120.0 + (i % 30)),
                0,
                1 if i % 2 == 0 else 0,
                0,
                1,
                1,
                None if win_bad else 8192,
                None if win_bad else 256,
                "BENIGN",
                "Monday.csv",
                "CICIDS2017",
                "test-batch",
                "0.1.0",
                "v2.3.0",
                now,
                "VALID",
                (["INFINITY:flow_bytes_s", "INFINITY:flow_packets_s"] if rate_bad else [])
                + (["SENTINEL:init_win_bytes_forward", "SENTINEL:init_win_bytes_backward"] if win_bad else []),
                rate_bad,
                rate_bad,
                win_bad,
                win_bad,
            )
            rows.append(row)
        # Add 10 exact duplicates across different source_file to test anti-leakage dedup
        for i in range(10):
            dup = list(rows[i])
            dup[22] = "Tuesday.csv"
            rows.append(tuple(dup))

        # 60 DoS Hulk + 15 Web Attack (with LABEL_ENCODING_REPLACEMENT!) + 9 Heartbleed
        for i in range(60):
            rate_bad = (i % 20 == 0)
            rows.append((
                80, 500000 + i * 100, 100 + i, 1, 20000 + i * 100, 60,
                None if rate_bad else float(40000.0 + i * 10),
                None if rate_bad else float(200.0 + i),
                float(5000.0 + i * 2), float(3000.0 + i),
                60.0, 1460.0, float(900.0 + (i % 50)), float(400.0),
                0, 1, 0, 1, 1, 29200, 0,
                "DoS Hulk", "Wednesday.csv", "CICIDS2017", "test-batch", "0.1.0", "v2.3.0", now,
                "VALID",
                (["NAN:flow_bytes_s", "INFINITY:flow_packets_s"] if rate_bad else []),
                rate_bad, rate_bad, False, False,
            ))
        for i in range(15):
            rows.append((
                80, 120000 + i * 40, 12 + i, 10 + i, 1500 + i * 30, 3000 + i * 20,
                float(3500.0 + i), float(120.0 + i), float(1200.0 + i), float(400.0 + i),
                52.0, 1200.0, float(450.0 + i), float(180.0),
                0, 1, 0, 1, 1, 65535, 29200,
                "Web Attack \ufffd XSS", "Thursday.csv", "CICIDS2017", "test-batch", "0.1.0", "v2.3.0", now,
                "VALID",
                ["LABEL_ENCODING_REPLACEMENT"],
                False, False, False, False,
            ))
        for i in range(9):
            rows.append((
                444, 9000000 + i * 500, 200 + i, 190 + i, 9000 + i * 10, 800000 + i * 50,
                float(80000.0 + i), float(40.0 + i), float(25000.0 + i), float(1000.0 + i),
                66.0, 16384.0, float(2000.0 + i), float(900.0),
                0, 1, 0, 1, 1, 29200, 29200,
                "Heartbleed", "Wednesday.csv", "CICIDS2017", "test-batch", "0.1.0", "v2.3.0", now,
                "VALID",
                [],
                False, False, False, False,
            ))

        df = self.spark.createDataFrame(rows, schema)
        df.write.mode("overwrite").option("compression", "snappy").parquet(path)

    def test_end_to_end_pipeline_no_leakage_and_artifacts(self):
        from src.modeling.pipeline import run_modeling_pipeline

        with tempfile.TemporaryDirectory() as tmp:
            parquet_path = str(Path(tmp) / "cleaned_valid")
            out_dir = Path(tmp) / "reports"
            self._build_synthetic_valid_parquet(parquet_path)

            cfg = ModelingConfig(
                spark_master="local[2]",
                shuffle_partitions=2,
                cleaned_path=parquet_path,
                num_trees=10,
                max_depth=5,
                seed=42,
                small_class_threshold=30,
                examples_per_category=3,
            )
            summary = run_modeling_pipeline(self.spark, cfg, out_dir, verify_disjoint=True)

            # Verify dedup removed the 10 cross-file duplicates
            self.assertEqual(summary["data_preparation"]["records_before_dedup"], 214)
            self.assertEqual(summary["data_preparation"]["records_after_dedup"], 204)
            self.assertEqual(summary["data_preparation"]["duplicate_records_removed"], 10)

            # Verify zero overlap across splits
            self.assertEqual(
                summary["split_summary"]["disjoint_overlap_verification"],
                {"train_val": 0, "train_test": 0, "val_test": 0},
            )
            # Verify tiny class Heartbleed (9 rows) present in all 3 splits
            hb = summary["split_summary"]["per_label_counts"]["Heartbleed"]
            self.assertGreaterEqual(hb["train"], 1)
            self.assertGreaterEqual(hb["val"], 1)
            self.assertGreaterEqual(hb["test"], 1)

            # Verify Coverage + Review rate == 1.0
            qa = summary["test_evaluation"]["quality_aware_primary"]
            self.assertAlmostEqual(qa["coverage"] + qa["review_rate"], 1.0, places=6)

            # Verify all 6 artifact files generated and valid JSON/Markdown
            for fname in (
                "baseline_config.json",
                "evaluation_summary.json",
                "evaluation_report.md",
                "test_examples.json",
                "test_examples.csv",
                "validation_threshold_sweep.csv",
            ):
                p = out_dir / fname
                self.assertTrue(p.exists(), f"Missing artifact: {fname}")
                self.assertGreater(p.stat().st_size, 0)

            cfg_json = json.loads((out_dir / "baseline_config.json").read_text(encoding="utf-8"))
            self.assertEqual(cfg_json["feature_count"], 21)
            self.assertEqual(cfg_json["preprocessing"]["fitted_on"], "train_only")
            self.assertEqual(cfg_json["quality_aware_config"]["primary_flag_columns"], list(RATE_QUALITY_FLAGS))
            self.assertIn("LABEL_ENCODING_REPLACEMENT", cfg_json["quality_aware_config"]["excluded_leaky_reason_codes"])


if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from experiments.plot_results import summarize
from experiments.benchmark import master_topology, trial_order
from experiments.seed_scaling import scaled_payload
from processing.config import PROJECT_ROOT


class BenchmarkSummaryTests(unittest.TestCase):
    def test_incomplete_results_rejected(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "result.json"
            path.write_text(json.dumps({"run_status": "FAILED", "runs": []}))
            with self.assertRaisesRegex(ValueError, "Incomplete"):
                summarize([path])

    def test_grouping_requires_same_workload_and_timing_scope(self):
        base = {"input_records": 12, "input_bytes": 3420, "spark_master": "local[2]", "worker_count": None,
                "shuffle_partitions": 4, "executor_cores": "2", "executor_memory": "1g",
                "timing_scope": "unit-test fixture timings", "processing_duration_seconds": 2,
                "throughput_flows_per_second": 6}
        runs = [{"files": [{"source_sha256": "fixture-a", "input_bytes": 3420}], "performance": base},
                {"files": [{"source_sha256": "fixture-a", "input_bytes": 3420}],
                 "performance": {**base, "processing_duration_seconds": 4, "throughput_flows_per_second": 3}},
                {"files": [{"source_sha256": "fixture-b", "input_bytes": 3420}], "performance": base}]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "result.json"
            path.write_text(json.dumps({"run_status": "COMPLETE", "dimension": "partitions",
                                        "dataset_kind": "synthetic", "runs": runs}))
            rows = summarize([path])
        self.assertEqual(len(rows), 2)
        combined = next(r for r in rows if r["repetitions"] == 2)
        self.assertEqual(combined["median_seconds"], 3)
        self.assertEqual(combined["median_flows_per_second"], 4.5)

    def test_speedup_uses_matched_baseline_and_excludes_warmup(self):
        base = {"input_records": 120, "input_bytes": 34200, "spark_master": "spark://test:7077",
                "worker_count": 1, "worker_resources": [[2, 2048]], "shuffle_partitions": 4,
                "executor_cores": "2", "executor_memory": "2g", "timing_scope": "fixture-only",
                "processing_duration_seconds": 10, "throughput_flows_per_second": 12}
        files = [{"source_sha256": "fixture", "input_bytes": 34200}]
        one = {"files": files, "performance": base}
        two = {"files": files, "performance": {**base, "worker_count": 2,
                "worker_resources": [[2, 2048], [2, 2048]], "processing_duration_seconds": 5}}
        incompatible = {"files": files, "performance": {**two["performance"], "executor_memory": "1g"}}
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "result.json"
            path.write_text(json.dumps({"run_status": "COMPLETE", "dimension": "workers",
                "dataset_kind": "synthetic", "runs": [one, two, incompatible],
                "warmup_runs": [{"files": files, "performance": {**base, "processing_duration_seconds": 1000}}]}))
            rows = summarize([path])
        self.assertEqual(next(r for r in rows if r["worker_count"] == 1)["median_seconds"], 10)
        self.assertEqual(next(r for r in rows if r["worker_count"] == 2 and r["executor_memory"] == "2g")["speedup"], 2)
        self.assertIsNone(next(r for r in rows if r["executor_memory"] == "1g")["speedup"])

    def test_partition_baseline_four_and_size_not_speedup(self):
        base = {"input_records": 12, "input_bytes": 3420, "spark_master": "local[2]", "worker_count": None,
                "shuffle_partitions": 4, "executor_cores": "2", "executor_memory": "1g",
                "timing_scope": "fixture-only", "processing_duration_seconds": 8, "throughput_flows_per_second": 1.5}
        runs = [{"files": [{"source_sha256": "fixture", "input_bytes": 3420}], "performance": {**base,
                  "shuffle_partitions": n, "processing_duration_seconds": seconds}} for n, seconds in [(4, 8), (8, 4)]]
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "result.json"
            for dimension in ("partitions", "size"):
                path.write_text(json.dumps({"run_status": "COMPLETE", "dimension": dimension,
                                            "dataset_kind": "synthetic", "runs": runs}))
                rows = summarize([path])
                self.assertEqual([r["speedup"] for r in rows], [1, 2] if dimension == "partitions" else [None, None])

    def test_alternating_trial_order(self):
        self.assertEqual(trial_order(["same"], [4, 8, 16, 32], 2),
            [("same", n, r) for r, ns in [(1, [4, 8, 16, 32]), (2, [32, 16, 8, 4])] for n in ns])

    def test_synthetic_scaling_preserves_header_and_rows(self):
        fixture = (PROJECT_ROOT / "data/sample/sample_flow.csv").read_bytes()
        payload = scaled_payload(fixture, 3).splitlines()
        self.assertEqual(len(payload), 37)
        self.assertEqual(payload[0], fixture.splitlines()[0])
        self.assertEqual(payload[1:13], payload[13:25])
        with self.assertRaises(ValueError):
            scaled_payload(fixture, 0)

    def test_topology_mismatch_rejected(self):
        from io import BytesIO
        workers = [{"id": "b", "state": "ALIVE", "cores": 2, "memory": 2048},
                   {"id": "a", "state": "ALIVE", "cores": 2, "memory": 2048}]
        with patch("experiments.benchmark.urlopen", return_value=BytesIO(json.dumps({"workers": workers}).encode())):
            self.assertEqual([w["id"] for w in master_topology("http://test", 2)], ["a", "b"])
        with patch("experiments.benchmark.urlopen", return_value=BytesIO(json.dumps({"workers": workers}).encode())):
            with self.assertRaisesRegex(ValueError, "Expected 1"):
                master_topology("http://test", 1)

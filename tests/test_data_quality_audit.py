import csv
from importlib.util import find_spec
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

if any(find_spec(dependency) is None for dependency in ("numpy", "pandas", "matplotlib")):
    raise unittest.SkipTest("Run the Veracity experiment tests on the host with pandas and matplotlib installed")

import pandas as pd

from experiments.analyze_training_readiness import analyze_paths, numeric_values
from experiments.generate_data_quality_report import classify_and_group, duplicate_stats, fingerprint


class DuplicateStatsTests(unittest.TestCase):
    def test_counts_duplicate_labels_when_digest_ends_with_zero(self):
        header = b",".join(f"column_{index}".encode() for index in range(79))
        benign = b",".join([b"1"] + [b"0"] * 77 + [b"BENIGN"])
        attack = b",".join([b"2"] + [b"0"] * 77 + [b"DoS Hulk"])
        unique = b",".join([b"3"] + [b"0"] * 77 + [b"BENIGN"])
        self.assertEqual(fingerprint(benign)[-1], 0)

        with TemporaryDirectory() as folder:
            first = Path(folder) / "first.csv"
            second = Path(folder) / "second.csv"
            first.write_bytes(b"\r\n".join([header, benign, benign, attack, unique]) + b"\r\n")
            second.write_bytes(b"\n".join([header, benign, attack]) + b"\n")
            stats = duplicate_stats([first, second])

        self.assertEqual(stats["dataset"], {
            "duplicate_groups": 2,
            "records_in_duplicate_groups": 5,
            "excess_duplicate_records": 3,
        })
        self.assertEqual(stats["duplicate_groups_by_label"], {"BENIGN": 1, "DoS Hulk": 1})
        self.assertEqual(stats["affected_records_by_label"], {"BENIGN": 3, "DoS Hulk": 2})


class IntegerAuditTests(unittest.TestCase):
    def test_parses_signed_ascii_integers_with_exact_bounds(self):
        for kind, bits in (("integer", 32), ("long", 64)):
            with self.subTest(kind=kind):
                minimum, maximum = -(2 ** (bits - 1)), 2 ** (bits - 1) - 1
                valid = [str(minimum), str(maximum), " +00080 ", "-0", "80\r\n", "0" * 5000 + "8"]
                invalid = [str(minimum - 1), str(maximum + 1), "80.5", "80.0", "8e1", "NaN",
                           "Infinity", "\u0668\u0660", "\t80", "80\t", "+", "9" * 5000]
                raw = pd.Series(valid + invalid + ["", "   ", None], dtype="string")
                _, values, missing, cast_failure, nan = numeric_values(raw, kind)

                self.assertEqual(values.dropna().tolist(), [minimum, maximum, 80, 0, 80, 8])
                self.assertEqual(missing.tolist(), [False] * (len(valid) + len(invalid)) + [True] * 3)
                self.assertEqual(cast_failure.tolist(), [False] * len(valid) + [True] * len(invalid) + [False] * 3)
                self.assertFalse(nan.any())

    def test_both_audits_quarantine_integer_cast_failures(self):
        fields = [
            {"source_index": 0, "name": "destination_port", "cleaned_target_type": "integer"},
            {"source_index": 1, "name": "flow_duration", "cleaned_target_type": "long"},
            {"source_index": 2, "name": "label", "cleaned_target_type": "string"},
        ]
        with TemporaryDirectory() as folder:
            path = Path(folder) / "casts.csv"
            with path.open("w", encoding="utf-8", newline="") as stream:
                csv.writer(stream).writerows([
                    ["Destination Port", "Flow Duration", "Label"],
                    ["80.5", "1", "fractional port"],
                    ["8e1", "1", "scientific notation"],
                    ["2147483648", "1", "integer overflow"],
                    ["80", "9223372036854775808", "long overflow"],
                    [" +00080 ", "9223372036854775807", "valid integers"],
                ])
            totals, issues, _, _ = analyze_paths([path], fields, chunk_size=2)
            labels, _, _ = classify_and_group([path], fields, chunk_size=2)

        self.assertEqual(totals["valid_records"], 1)
        self.assertEqual(totals["quarantined_records"], 4)
        self.assertEqual(issues["destination_port_missing_or_cast_failure"], 3)
        self.assertEqual(issues["flow_duration_missing_or_cast_failure"], 1)
        self.assertEqual(issues["destination_port_out_of_range"], 0)
        self.assertEqual({label: counts["quarantined"] for label, counts in labels.items()}, {
            "fractional port": 1,
            "scientific notation": 1,
            "integer overflow": 1,
            "long overflow": 1,
            "valid integers": 0,
        })

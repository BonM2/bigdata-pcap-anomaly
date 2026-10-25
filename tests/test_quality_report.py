import json

from processing.clean import clean_classified_flows
from processing.quality_report import json_text, quality_snapshot
from processing.transform import transform_flows
from processing.validation import validate_flows
from tests.spark_case import SparkCase


class QualityReportTests(SparkCase):
    def test_measured_before_after_cells_and_labels(self):
        before = validate_flows(transform_flows(self.raw([
            {"flow_bytes_s": "NaN", "init_win_bytes_forward": "-1", "label": " x "},
            {"flow_bytes_s": "Infinity", "flow_packets_s": "junk"},
            {"destination_port": "65536", "flow_duration": "-1"},
            {"_corrupt_record": "bad", "source_file": "bad.csv"},
        ]), self.config)).cache()
        self.addCleanup(before.unpersist)
        after = clean_classified_flows(before).annotated
        pre, post = quality_snapshot(before), quality_snapshot(after, cleaned=True)
        self.assertEqual(pre["totals"]["input_records"], 4)
        self.assertEqual(pre["totals"]["nan_count"], 1)
        self.assertEqual(pre["totals"]["infinity_count"], 1)
        self.assertEqual(pre["totals"]["cast_error_count"], 1)
        self.assertEqual(pre["totals"]["invalid_range_count"], 2)
        self.assertEqual(post["totals"]["nan_count"], 0)
        self.assertEqual(post["totals"]["infinity_count"], 0)
        self.assertEqual(post["totals"]["null_count"], 4)
        self.assertEqual(post["totals"]["cast_error_count"], 1)
        self.assertEqual(pre["totals"]["sentinel_count"], post["totals"]["sentinel_count"])
        self.assertEqual(sorted(pre["label_distribution"], key=str), sorted(post["label_distribution"], key=str))
        self.assertEqual(len(post["files"]), 2)
        self.assertEqual(json.loads(json_text(post))["schema_validation"]["status"], "PASS")

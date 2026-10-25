from decimal import Decimal

from pyspark.sql import functions as F

from processing.clean import clean_flows
from processing.features import add_features, aggregate_flows
from tests.spark_case import SparkCase


class FeatureTests(SparkCase):
    def test_formulas_zero_denominator_and_long_overflow(self):
        valid = clean_flows(self.raw([
            {"total_fwd_packets": "3", "total_backward_packets": "1", "total_length_of_fwd_packets": "100",
             "total_length_of_bwd_packets": "25"}, {},
            {"total_fwd_packets": "9223372036854775807", "total_backward_packets": "9223372036854775807"},
        ]), self.config).valid
        rows = add_features(valid).orderBy("total_packets").collect()
        self.assertIsNone(rows[0].fwd_packet_ratio)
        self.assertIsNone(rows[0].bwd_packet_ratio)
        self.assertEqual(rows[1].total_packets, 4)
        self.assertEqual(rows[1].total_bytes, 125)
        self.assertEqual((rows[1].fwd_packet_ratio, rows[1].bwd_packet_ratio), (0.75, 0.25))
        self.assertEqual(rows[2].total_packets, Decimal("18446744073709551614"))
        self.assertEqual(rows[2].fwd_packet_ratio, 0.5)

    def test_source_checks_and_invalid_values_propagate_null(self):
        frame = clean_flows(self.raw([{"total_fwd_packets": "-1"}, {"total_backward_packets": None}]), self.config).annotated
        self.assertTrue(all(r.total_packets is None and r.fwd_packet_ratio is None for r in add_features(frame).collect()))
        with self.assertRaisesRegex(ValueError, "LongType"):
            add_features(frame.drop("total_fwd_packets"))
        with self.assertRaisesRegex(ValueError, "LongType"):
            add_features(frame.withColumn("total_fwd_packets", F.lit("1")))

    def test_aggregation_counts_all_volume_valid_only(self):
        result = clean_flows(self.raw([
            {"label": "A", "total_fwd_packets": "3", "total_backward_packets": "1", "flow_duration": "10",
             "total_length_of_fwd_packets": "100"},
            {"label": "A", "total_fwd_packets": "2", "flow_duration": "30"},
            {"label": "A", "total_fwd_packets": "999", "flow_duration": "-1"},
            {"label": "B", "_corrupt_record": "bad", "source_file": "other.csv"},
        ]), self.config)
        groups = aggregate_flows(result.annotated)
        label = groups.filter("grouping = 'label' AND label = 'A'").first()
        self.assertEqual((label.flow_count, label.valid_records, label.quarantined_records), (3, 2, 1))
        self.assertEqual((label.total_packets_sum, label.total_bytes_sum, label.avg_flow_duration), (6, 100, 20.0))
        self.assertAlmostEqual(label.valid_ratio, 2 / 3)
        batch = groups.filter("grouping = 'batch'").first()
        self.assertEqual((batch.flow_count, batch.valid_ratio, batch.error_ratio), (4, 0.5, 0.5))
        file = groups.filter("grouping = 'source_file' AND source_file = 'other.csv'").first()
        self.assertEqual((file.flow_count, file.rejected_records, file.total_packets_sum), (1, 1, 0))
        self.assertIsNone(file.avg_flow_duration)

    def test_empty_aggregation(self):
        result = clean_flows(self.raw([]), self.config)
        rows = aggregate_flows(result.annotated).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].flow_count, 0)
        self.assertIsNone(rows[0].valid_ratio)

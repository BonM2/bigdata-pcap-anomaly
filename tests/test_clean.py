from processing.clean import clean_flows
from tests.spark_case import SparkCase


class CleanTests(SparkCase):
    def test_soft_errors_sentinels_and_zero_backward_survive(self):
        result = clean_flows(self.raw([
            {"flow_bytes_s": v, "flow_packets_s": "-3", "init_win_bytes_forward": "-1",
             "init_win_bytes_backward": "-1", "label": " Web Attack \ufffd "}
            for v in [None, "", "NaN", "Infinity", "-Infinity", "junk"]
        ]), self.config)
        rows = result.valid.collect()
        self.assertEqual(len(rows), 6)
        for row in rows:
            self.assertIsNone(row.flow_bytes_s)
            self.assertIsNone(row.flow_packets_s)
            self.assertTrue(row.flow_bytes_s_invalid)
            self.assertTrue(row.init_win_fwd_missing)
            self.assertIsNone(row.init_win_bytes_forward)
            self.assertEqual(row.total_backward_packets, 0)
            self.assertEqual(row.label, " Web Attack \ufffd ")
            self.assertIn("LABEL_ENCODING_REPLACEMENT", row.quality_reason_codes)
        self.assertEqual(len(result.valid.columns), 22 + 12)

    def test_hard_errors_rejection_priority_and_reconciliation(self):
        changes = [
            {}, {}, {"flow_duration": "-1"}, {"destination_port": "65536"},
            {"syn_flag_count": "2"}, {"init_win_bytes_backward": "-2"},
            {"flow_iat_std": "Infinity"}, {"packet_length_mean": "NaN"},
            {"total_fwd_packets": "1.5"}, {"label": " "}, {"flow_duration": None},
            {"flow_duration": "-3", "_corrupt_record": "bad,row"},
        ]
        result = clean_flows(self.raw(changes), self.config)
        self.assertEqual(result.counts(), {
            "input_records": 12, "valid_records": 2, "quarantined_records": 9, "rejected_records": 1})
        rejected = result.rejected.first()
        self.assertIn("MALFORMED_CSV", rejected.quality_reason_codes)
        self.assertEqual(rejected._corrupt_record, "bad,row")
        self.assertEqual(result.valid.count(), 2)  # duplicate observations remain intact

    def test_valid_parquet_round_trip(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            valid = clean_flows(self.raw([{}, {"flow_bytes_s": "Infinity"}]), self.config).valid
            valid.write.option("compression", "snappy").parquet(folder + "/flows")
            reread = self.spark.read.parquet(folder + "/flows")
            self.assertEqual(reread.count(), 2)
            self.assertEqual(reread.dtypes, valid.dtypes)
            self.assertEqual([r.label for r in reread.collect()], ["BENIGN", "BENIGN"])

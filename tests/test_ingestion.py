import csv
import hashlib
import io
import tempfile
from pathlib import Path

from processing.clean import clean_flows
from processing.ingestion import read_raw_flows, validate_header
from tests.spark_case import SparkCase


class IngestionTests(SparkCase):
    def test_strict_rows_headers_hashes_and_duplicate_aliases(self):
        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer)
        writer.writerow([f["source_header_exact"] for f in self.contract["fields"]])
        good = ["0"] * 78 + ["label, with comma"]
        writer.writerow(good)
        writer.writerow(good[:-1])
        writer.writerow(good + ["extra"])
        buffer.write('"unterminated\n\n')
        payload = buffer.getvalue().encode("utf-8")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "input.csv"
            path.write_bytes(payload)
            raw, manifest = read_raw_flows(self.spark, path.as_uri())
            self.assertEqual(raw.count(), 5)
            self.assertEqual(manifest[0]["source_sha256"], hashlib.sha256(payload).hexdigest())
            self.assertIn("fwd_header_length_repeat_2", raw.columns)
            result = clean_flows(raw, self.config)
            self.assertEqual(result.counts(), {
                "input_records": 5, "valid_records": 1, "quarantined_records": 0, "rejected_records": 4})
            self.assertEqual(result.valid.first().label, "label, with comma")

    def test_wrong_header_fails_before_read(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "input.csv"
            path.write_text("wrong,header\n1,2\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "header mismatch"):
                read_raw_flows(self.spark, path.as_uri())

    def test_header_order_is_checked(self):
        headers = [f["source_header_exact"] for f in self.contract["fields"]]
        headers[1], headers[2] = headers[2], headers[1]
        with self.assertRaisesRegex(ValueError, "mismatch"):
            validate_header(",".join(headers), self.contract)

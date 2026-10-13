import hashlib
import tempfile
import unittest
from pathlib import Path

from processing.audit_source import audit_file
from processing.config import PROJECT_ROOT, load_contract


class AuditTests(unittest.TestCase):
    def test_fixture_matches_policy_and_detects_original_bytes(self):
        payload = (PROJECT_ROOT / "data/sample/sample_flow.csv").read_bytes()
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "fixture.csv"
            path.write_bytes(payload)
            report = audit_file(path, load_contract())
        self.assertEqual(report["input_records"], 12)
        self.assertEqual(report["policy_preview"], {
            "valid_records": 4, "quarantined_records": 6, "rejected_records": 2,
            "valid_records_with_soft_issues": 3})
        self.assertEqual(report["source_sha256"], hashlib.sha256(payload).hexdigest())
        self.assertEqual(report["duplicate_headers"], {"Fwd Header Length": [35, 56]})

    def test_bad_utf8_label_is_counted_and_preserved_as_replacement(self):
        contract = load_contract()
        header = ",".join(f["source_header_exact"] for f in contract["fields"]).encode()
        payload = header + b"\n" + b"0," * 78 + b"Web Attack \x96 XSS\n"
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "fixture.csv"
            path.write_bytes(payload)
            report = audit_file(path, contract)
        self.assertEqual(report["utf8_decode_error_records"], 1)
        self.assertEqual(report["label_replacement_records"], 1)
        self.assertEqual(report["policy_preview"]["valid_records"], 1)

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from processing.zeek_ingestion import run
from tests.spark_case import SparkCase


class ZeekIngestionTests(SparkCase):
    def test_maps_literal_dotted_keys_and_round_trips_optional_values(self):
        first = {
            "ts": 1747147647.668533, "uid": "example-1", "proto": "tcp",
            "id.orig_h": "192.0.2.1", "id.orig_p": 50000,
            "id.resp_h": "192.0.2.2", "id.resp_p": 80,
        }
        second = dict(first, uid="example-2", duration=0.125, orig_bytes=71)
        with TemporaryDirectory() as folder:
            path = Path(folder) / "conn.log"
            path.write_text("\n".join(map(json.dumps, (first, second))), encoding="utf-8")
            output = (Path(folder) / "parquet").as_uri()
            report = run(self.spark, path.as_uri(), output)
            rows = self.spark.read.parquet(output).orderBy("uid").collect()

            self.assertEqual(report["run_status"], "VERIFIED")
            self.assertEqual(report["output"]["read_back_status"], "PASS")
            self.assertEqual(report["output"]["records"], 2)
            self.assertEqual(report["protocol_port_counts_top20"], [
                {"proto": "tcp", "destination_port": 80, "count": 2},
            ])
            self.assertEqual(rows[0].source_ip, "192.0.2.1")
            self.assertEqual(rows[0].source_port, 50000)
            self.assertEqual(rows[0].connection_start_epoch_seconds, first["ts"])
            self.assertIsNone(rows[0].duration_seconds)
            self.assertEqual(rows[1].duration_seconds, 0.125)
            self.assertEqual(rows[1].orig_payload_bytes, 71)

            with self.assertRaises(FileExistsError):
                run(self.spark, path.as_uri(), output)

    def test_invalid_port_prevents_parquet_write(self):
        row = {
            "ts": 1747147647.668533, "uid": "invalid-port", "proto": "tcp",
            "id.orig_h": "192.0.2.1", "id.orig_p": 50000,
            "id.resp_h": "192.0.2.2", "id.resp_p": 65536,
        }
        with TemporaryDirectory() as folder:
            path = Path(folder) / "conn.log"
            path.write_text(json.dumps(row), encoding="utf-8")
            output = Path(folder) / "parquet"
            with self.assertRaisesRegex(ValueError, "0/1 valid records"):
                run(self.spark, path.as_uri(), output.as_uri())
            self.assertFalse(output.exists())

from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType

from processing.transform import transform_flows
from tests.spark_case import SparkCase


class TransformTests(SparkCase):
    def test_types_precision_label_and_raw_evidence(self):
        raw = self.raw([{"destination_port": " 443 ", "flow_duration": "9223372036854775807",
                         "flow_iat_mean": "1.25e2", "label": " Web Attack \ufffd "}])
        frame = transform_flows(raw, self.config)
        row = frame.first()
        self.assertEqual(row.flow_duration, 9223372036854775807)
        self.assertEqual(row.destination_port, 443)
        self.assertEqual(row.flow_iat_mean, 125.0)
        self.assertEqual(row.label, " Web Attack \ufffd ")
        self.assertEqual(row._raw_values.destination_port, " 443 ")
        self.assertEqual(row.cast_failure_fields, [])
        self.assertIsInstance(frame.schema["destination_port"].dataType, IntegerType)
        self.assertIsInstance(frame.schema["flow_duration"].dataType, LongType)
        self.assertIsInstance(frame.schema["flow_iat_mean"].dataType, DoubleType)
        self.assertIsInstance(frame.schema["label"].dataType, StringType)

    def test_cast_failures_do_not_throw_under_ansi_or_truncate(self):
        values = ["2147483648", "1.5", "1e2", "garbage", "NaN", ""]
        frame = transform_flows(self.raw([{"destination_port": v} for v in values]), self.config)
        rows = frame.collect()
        for row in rows:
            self.assertIsNone(row.destination_port)
        self.assertTrue(all("destination_port" in r.cast_failure_fields for r in rows[:-1]))
        self.assertEqual(rows[-1].cast_failure_fields, [])
        overflow = transform_flows(self.raw([{"flow_duration": "9223372036854775808"}]), self.config).first()
        self.assertIn("flow_duration", overflow.cast_failure_fields)

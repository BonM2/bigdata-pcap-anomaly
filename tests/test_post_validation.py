from pyspark.sql import functions as F

from processing.clean import clean_classified_flows
from processing.transform import transform_flows
from processing.validation import (post_clean_summary, reconcile_stages, schema_validation,
                                   validate_cleaned_flows, validate_flows)
from tests.spark_case import SparkCase


class PostValidationTests(SparkCase):
    def stages(self, changes):
        before = validate_flows(transform_flows(self.raw(changes), self.config)).cache()
        self.addCleanup(before.unpersist)
        before.count()
        result = clean_classified_flows(before)
        result.annotated.cache()
        self.addCleanup(result.annotated.unpersist)
        result.annotated.count()
        return before, result

    def test_permitted_nulls_labels_and_statuses(self):
        before, result = self.stages([
            {}, {}, {"flow_bytes_s": "Infinity", "init_win_bytes_forward": "-1", "label": " odd \ufffd "},
            {"flow_duration": "-1"}, {"_corrupt_record": "bad", "label": None},
        ])
        summary = post_clean_summary(validate_cleaned_flows(result.annotated))
        self.assertEqual(summary["status"], "PASS")
        self.assertEqual((summary["valid_records"], summary["quarantined_records"], summary["rejected_records"]), (3, 1, 1))
        check = reconcile_stages(before, result.annotated, 5)
        self.assertTrue(check["evidence_multiset_preserved"])
        self.assertTrue(all(r["quality_status_before"] == r["quality_status"] for r in check["transitions"]))

    def test_detect_tampering_and_remaining_invalid_values(self):
        _, result = self.stages([{}])
        damaged = (result.annotated.withColumn("flow_bytes_s_invalid", F.lit(True))
                   .withColumn("label", F.lit("changed"))
                   .withColumn("flow_duration", F.lit(-1).cast("long"))
                   .withColumn("destination_port", F.lit(65536))
                   .withColumn("flow_iat_mean", F.lit(float("nan")))
                   .withColumn("flow_iat_std", F.lit(float("inf")))
                   .withColumn("packet_length_mean", F.lit(None).cast("double"))
                   .withColumn("source_file", F.lit(None).cast("string")))
        errors = validate_cleaned_flows(damaged).first().post_clean_errors
        for code in ("LABEL_CHANGED", "FLAG_MISMATCH:flow_bytes_s", "NEGATIVE:flow_duration",
                     "OUT_OF_RANGE:destination_port", "NAN:flow_iat_mean", "INFINITY:flow_iat_std",
                     "NULL:packet_length_mean", "MISSING_METADATA:source_file"):
            self.assertIn(code, errors)

    def test_schema_failure_and_wrong_null_flag(self):
        _, result = self.stages([{"flow_bytes_s": "junk"}])
        wrong = result.annotated.withColumn("destination_port", F.col("destination_port").cast("string"))
        self.assertEqual(schema_validation(wrong, cleaned=True)["status"], "FAIL")
        with self.assertRaisesRegex(ValueError, "schema"):
            validate_cleaned_flows(wrong)
        extra = result.valid.withColumn("unexpected", F.lit(1))
        self.assertEqual(schema_validation(extra, cleaned=True, exact=True)["status"], "FAIL")
        wrong = result.annotated.withColumn("flow_bytes_s_invalid", F.lit(None).cast("boolean"))
        errors = validate_cleaned_flows(wrong).first().post_clean_errors
        self.assertIn("FLAG_MISMATCH:flow_bytes_s", errors)
        self.assertIn("NULL:flow_bytes_s", errors)

    def test_no_loss_duplicate_replacement_or_status_transition(self):
        before, result = self.stages([{}, {}, {"label": "other"}])
        with self.assertRaisesRegex(ValueError, "count changed"):
            reconcile_stages(before, result.annotated.limit(2), 3)
        replacement = result.annotated.filter("label = 'BENIGN'").limit(1)
        replacement = replacement.unionByName(replacement).unionByName(replacement)
        with self.assertRaisesRegex(ValueError, "evidence or duplicates"):
            reconcile_stages(before, replacement, 3)
        changed = result.annotated.withColumn("quality_status", F.lit("QUARANTINED"))
        with self.assertRaisesRegex(ValueError, "transition"):
            reconcile_stages(before, changed, 3)
        with self.assertRaisesRegex(ValueError, "Unknown"):
            reconcile_stages(before, result.annotated.withColumn("quality_status", F.lit(None)), 3)

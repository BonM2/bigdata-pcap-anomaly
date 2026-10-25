import json
import tempfile
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from processing.config import PROJECT_ROOT
from processing.main import local_output_config, run_pipeline
from processing.output import output_plan, write_verified_parquet
from tests.spark_case import SparkCase


class PipelineTests(SparkCase):
    def config_at(self, folder, batch="fixture"):
        return local_output_config(replace(self.config, batch_id=batch,
            input_path=(PROJECT_ROOT / "data/sample/sample_flow.csv").as_uri()), folder)

    def test_fixture_end_to_end_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as folder:
            config = self.config_at(folder)
            report = run_pipeline(self.spark, config, write_outputs=True)
            self.assertEqual((report["input_records"], report["valid_records"], report["quarantined_records"], report["rejected_records"]),
                             (12, 4, 6, 2))
            root = Path(folder) / "reports/data_quality/fixture"
            self.assertEqual(json.loads((root / "_SUCCESS.json").read_text())["run_status"], "COMPLETE")
            self.assertEqual(json.loads((root / "quality.json").read_text())["post_cleaning_validation"]["status"], "PASS")
            self.assertTrue((root / "summary.csv").exists())
            self.assertEqual(len(report["outputs"]), 5)
            self.assertEqual(report["outputs"]["cleaned_path"]["records"], 4)
            self.assertEqual(len(self.spark.read.parquet(report["outputs"]["cleaned_path"]["path"]).columns), 34)
            self.assertTrue(all(o["read_back_status"] == "PASS" for o in report["outputs"].values()))
            with self.assertRaises(FileExistsError):
                run_pipeline(self.spark, config, write_outputs=True)
            self.assertFalse((root / "_FAILED.json").exists())

    def test_write_failure_never_marks_complete(self):
        with tempfile.TemporaryDirectory() as folder:
            config = self.config_at(folder)
            with patch("processing.main.write_verified_parquet", side_effect=RuntimeError("injected write failure")):
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    run_pipeline(self.spark, config, write_outputs=True)
            root = Path(folder) / "reports/data_quality/fixture"
            self.assertFalse((root / "_SUCCESS.json").exists())
            self.assertEqual(json.loads((root / "_FAILED.json").read_text())["run_status"], "FAILED")

    def test_post_validation_failure_prevents_completion(self):
        with tempfile.TemporaryDirectory() as folder:
            config = self.config_at(folder)
            with patch("processing.main.post_clean_summary", return_value={"status": "FAIL"}):
                with self.assertRaisesRegex(ValueError, "Post-clean validation failed"):
                    run_pipeline(self.spark, config, write_outputs=True)
            root = Path(folder) / "reports/data_quality/fixture"
            self.assertTrue((root / "_FAILED.json").exists())
            self.assertFalse((root / "_SUCCESS.json").exists())
            self.assertFalse((Path(folder) / "cleaned/flows/fixture").exists())

    def test_parquet_footer_confirms_snappy(self):
        with tempfile.TemporaryDirectory() as folder:
            evidence = write_verified_parquet(self.spark.range(10), folder + "/parquet", expected_count=10)
            files = list((Path(folder) / "parquet").glob("part-*.parquet"))
            self.assertTrue(files)
            jvm = self.spark._jvm
            conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            for file in files:
                source = jvm.org.apache.parquet.hadoop.util.HadoopInputFile.fromPath(
                    jvm.org.apache.hadoop.fs.Path(file.as_uri()), conf)
                reader = jvm.org.apache.parquet.hadoop.ParquetFileReader.open(source)
                try:
                    for block in reader.getFooter().getBlocks():
                        for column in block.getColumns():
                            self.assertEqual(str(column.getCodec()), "SNAPPY")
                finally:
                    reader.close()
            self.assertEqual(evidence["read_back_status"], "PASS")

    def test_read_only_does_not_create_output_and_empty_input_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            config = self.config_at(folder)
            report = run_pipeline(self.spark, config)
            self.assertEqual(report["run_status"], "VALIDATED_NO_OUTPUT")
            self.assertEqual(list(Path(folder).iterdir()), [])
            header = ",".join(f["source_header_exact"] for f in self.contract["fields"])
            source = Path(folder) / "empty.csv"
            source.write_text(header + "\n")
            config = local_output_config(replace(config, input_path=source.as_uri()), Path(folder).parent / "unused-output")
            with self.assertRaisesRegex(ValueError, "Empty input"):
                run_pipeline(self.spark, config)

    def test_parquet_read_back_detects_wrong_count(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(RuntimeError, "count mismatch"):
                write_verified_parquet(self.spark.range(2), folder + "/output", expected_count=3)

    def test_empty_valid_and_error_outputs_round_trip(self):
        from processing.clean import clean_flows
        for changes in ([{}], [{"flow_duration": "-1"}], [{"_corrupt_record": "bad"}]):
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as folder:
                result = clean_flows(self.raw(changes), self.config)
                result.annotated.cache()
                try:
                    result.annotated.count()
                    counts = []
                    for name in ("valid", "quarantined", "rejected"):
                        evidence = write_verified_parquet(getattr(result, name), folder + "/" + name, verify_labels=True)
                        counts.append(evidence["records"])
                    self.assertEqual(sum(counts), 1)
                finally:
                    result.annotated.unpersist(blocking=True)

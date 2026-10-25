"""Opt-in synthetic-only S3A end-to-end check, isolated under smoke/pipeline/."""

import argparse
from dataclasses import replace
from pathlib import Path

from processing.config import ProcessingConfig, create_spark_session
from processing.main import run_pipeline
from processing.output import OUTPUT_FIELDS, output_plan
from processing.quality_report import json_text
from tests.seed_minio import main as seed_fixture


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Seed synthetic fixture and write new smoke batch outputs")
    parser.add_argument("--report", type=Path, help="New local evidence JSON file")
    args = parser.parse_args()
    if args.report and args.report.exists():
        parser.error("Local report exists; choose a new file")
    config = ProcessingConfig.from_env()
    base = f"s3a://{config.minio_bucket}/smoke/pipeline"
    config = replace(config, input_path=f"s3a://{config.minio_bucket}/smoke/cicids2017/synthetic_flow.csv",
                     **{name: base + "/" + name[:-5] for name in OUTPUT_FIELDS})
    if not args.execute:
        print(json_text({"mode": "PLAN_ONLY", "input": config.input_path, "outputs": output_plan(config)}))
        return
    seed_fixture()
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = run_pipeline(spark, config, write_outputs=True)
        report["dataset_kind"] = "synthetic-test-fixture"
        expected = {"input_records": 12, "valid_records": 4, "quarantined_records": 6, "rejected_records": 2}
        if any(report[k] != value for k, value in expected.items()):
            raise AssertionError("Synthetic smoke counts differ from fixture policy")
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            with args.report.open("x", encoding="utf-8") as stream:
                stream.write(json_text(report))
        print(json_text({k: report[k] for k in ("run_status", "batch_id", *expected)}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

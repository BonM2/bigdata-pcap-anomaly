"""Small read-only Spark smoke job: python -m processing.check_minio."""

import argparse
import json
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import functions as F

from processing.clean import clean_flows
from processing.config import ProcessingConfig, create_spark_session, load_contract
from processing.ingestion import read_raw_flows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, help="Optional local JSON report (contains no credentials)")
    parser.add_argument("--skip-checksum", action="store_true", help="Skip the extra SHA-256 read; not a complete lineage audit")
    args = parser.parse_args()
    config = ProcessingConfig.from_env()
    if not config.input_path.startswith("s3a://"):
        parser.error("INPUT_PATH must use s3a:// for the MinIO smoke check")
    contract = load_contract(config.schema_path)
    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    raw = None
    try:
        raw, manifest = read_raw_flows(spark, config.input_path, contract, checksum=not args.skip_checksum)
        raw.persist(StorageLevel.MEMORY_AND_DISK)
        counts = {r["source_path"]: r.asDict() for r in raw.groupBy("source_path").agg(
            F.count("*").alias("input_records"),
            F.sum(F.when(F.col("_malformed_reason").isNotNull(), 1).otherwise(0)).alias("malformed_records"),
        ).collect()}
        for item in manifest:
            per_file = counts.get(item["source_path"], {})
            item.update({"input_records": per_file.get("input_records", 0),
                         "malformed_records": per_file.get("malformed_records", 0)})
        result = clean_flows(raw, config, contract)
        quality = result.counts()
        total = sum(item["input_records"] for item in manifest)
        if total != quality["input_records"]:
            raise RuntimeError("Record reconciliation failed")
        report = {
            "status": "PASS" if total else "EMPTY_INPUT",
            "scope": "MinIO/S3A read, header/row-width audit and in-memory cleaning; no output written",
            "spark_app_id": spark.sparkContext.applicationId,
            "spark_version": spark.version,
            "batch_id": config.batch_id,
            "raw_columns": len(contract["fields"]),
            "core_columns": len(contract["cleaned_core_field_names"]),
            "cleaned_schema": result.valid.schema.jsonValue(),
            **quality,
            "files": manifest,
        }
        text = json.dumps(report, ensure_ascii=False, indent=2)
        print(text)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(text + "\n", encoding="utf-8")
        if not total:
            raise RuntimeError("CSV headers found, but no data records to verify")
    finally:
        if raw is not None:
            raw.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()

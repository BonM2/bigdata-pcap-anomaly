"""Read-only audit of completed benchmark markers and quality JSON in MinIO."""

import argparse
import json
from pathlib import Path
from urllib.parse import urlsplit

from processing.config import ProcessingConfig


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--example-report", type=Path, help="Also copy the first trial's actual quality JSON")
    args = parser.parse_args()
    for path in (args.report, args.example_report):
        if path and path.exists():
            parser.error("Audit destination already exists")
    import boto3
    config = ProcessingConfig.from_env()
    config.validate()
    client = boto3.client("s3", endpoint_url=config.minio_endpoint,
                          aws_access_key_id=config.minio_access_key, aws_secret_access_key=config.minio_secret_key,
                          region_name=config.minio_region)

    def read_json(uri):
        parsed = urlsplit(uri)
        if parsed.scheme != "s3a" or parsed.netloc != config.minio_bucket:
            raise ValueError("Audit only supports S3A paths in the configured bucket")
        response = client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))
        with response["Body"] as stream:
            return json.load(stream)

    verified, example = [], None
    for path in args.inputs:
        experiment = json.loads(path.read_text(encoding="utf-8"))
        if experiment["run_status"] != "COMPLETE":
            raise ValueError("Cannot audit an incomplete benchmark as successful")
        for run in experiment["runs"] + experiment.get("warmup_runs", []):
            uri = run["quality_report_path"]
            marker = read_json(uri.rsplit("/", 1)[0] + "/_SUCCESS.json")
            quality = read_json(uri)
            if (marker["run_status"] != "COMPLETE" or marker["batch_id"] != run["batch_id"]
                    or quality["batch_id"] != run["batch_id"] or quality["run_status"] != "VERIFIED"
                    or quality["post_cleaning_validation"]["status"] != "PASS"
                    or quality["post_cleaning_validation"]["schema_validation"]["status"] != "PASS"
                    or not quality["record_reconciliation"]["evidence_multiset_preserved"]):
                raise ValueError("Stored completion or quality evidence failed")
            if any(quality[k] != v for k, v in run["quality"].items()):
                raise ValueError("Stored quality counts differ from benchmark")
            if (quality["input_records"] != sum(quality[k] for k in ("valid_records", "quarantined_records", "rejected_records"))
                    or len(quality["outputs"]) != 5
                    or quality["outputs"] != run["outputs"]
                    or any(o["read_back_status"] != "PASS" or o["compression"] != "snappy" for o in quality["outputs"].values())):
                raise ValueError("Stored Parquet read-back evidence failed")
            timing = read_json(marker["performance_report"])
            if timing["processing_duration_seconds"] != run["performance"]["processing_duration_seconds"]:
                raise ValueError("Stored performance differs from measured trial")
            verified.append({"batch_id": run["batch_id"], "warmup": run["repeat"] < 0,
                             "quality_report_path": uri, "stored_marker_status": "COMPLETE",
                             "post_cleaning_status": "PASS", "parquet_outputs": 5,
                             "quality": run["quality"]})
            if example is None:
                example = quality
    if not verified:
        raise ValueError("No measured batches")
    result = {"status": "PASS", "scope": "Re-read stored final markers, quality and performance JSON; "
              "Parquet schema/count/label read-back was executed in each measured pipeline trial",
              "batch_count": len(verified), "parquet_outputs": 5 * len(verified), "batches": verified}
    for path, value in ((args.report, result), (args.example_report, example)):
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("x", encoding="utf-8") as stream:
                json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps({k: result[k] for k in ("status", "batch_count", "parquet_outputs")}))


if __name__ == "__main__":
    main()

"""Opt-in synthetic scaling of the test fixture, never touching raw data."""

import argparse
import hashlib
import json
from pathlib import Path

from processing.config import PROJECT_ROOT, ProcessingConfig


def scaled_payload(fixture, copies):
    if copies < 1:
        raise ValueError("Copies must be positive")
    lines = fixture.splitlines(keepends=True)
    if len(lines) != 13:
        raise ValueError("Expected the 12-row synthetic fixture")
    header = lines[0].rstrip(b"\r\n") + b"\n"
    rows = b"".join(line.rstrip(b"\r\n") + b"\n" for line in lines[1:])
    return header + rows * copies


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--copies", nargs="+", type=int, default=[1000, 5000, 10000])
    parser.add_argument("--prefix", default="smoke/benchmark-scaling/v1")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if not args.prefix.startswith("smoke/benchmark-scaling/") or any(p in ("", ".", "..") for p in args.prefix.split("/")):
        parser.error("Prefix must be isolated under smoke/benchmark-scaling/")
    if args.manifest.exists():
        parser.error("Manifest already exists")
    config = ProcessingConfig.from_env()
    fixture = (PROJECT_ROOT / "data/sample/sample_flow.csv").read_bytes()
    records, payloads = [], []
    for copies in args.copies:
        payload = scaled_payload(fixture, copies)
        key = args.prefix + f"/copies-{copies}/synthetic.csv"
        records.append({"key": key, "uri": f"s3a://{config.minio_bucket}/{key}", "copies": copies,
                        "input_records": 12 * copies, "input_bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest()})
        payloads.append(payload)
    result = {"dataset_kind": "synthetic", "synthetic_scaling": True,
              "limitation": "Replicated test fixture, low entropy; for load only, never attack detection quality",
              "fixture_sha256": hashlib.sha256(fixture).hexdigest(), "files": records,
              "run_status": "PLAN_ONLY"}
    if args.execute:
        import boto3
        from botocore.exceptions import ClientError
        config.validate()
        client = boto3.client("s3", endpoint_url=config.minio_endpoint,
                              aws_access_key_id=config.minio_access_key,
                              aws_secret_access_key=config.minio_secret_key,
                              region_name=config.minio_region)
        # Require existing bucket. Never overwrite objects, even on reruns.
        for item, payload in zip(records, payloads):
            try:
                existing = client.get_object(Bucket=config.minio_bucket, Key=item["key"])
            except ClientError as error:
                if error.response["Error"]["Code"] != "NoSuchKey":
                    raise
                client.put_object(Bucket=config.minio_bucket, Key=item["key"], Body=payload,
                                  ContentType="text/csv", Metadata={"dataset-kind": "synthetic-scaling"})
            else:
                with existing["Body"] as body:
                    if body.read() != payload:
                        raise ValueError("Existing synthetic input differs; refusing overwrite")
        result["run_status"] = "READY"
        args.manifest.parent.mkdir(parents=True, exist_ok=True)
        with args.manifest.open("x", encoding="utf-8") as stream:
            json.dump(result, stream, indent=2)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

"""Prepare an audited CSV upload; writes require the explicit --execute flag.

Run from the repository root with python -m storage.upload_minio. Default mode
only reads local files, verifies hashes, and prints the proposed object keys.
"""

import argparse
import hashlib
import json
from pathlib import Path

from processing.config import PROJECT_ROOT, ProcessingConfig, load_contract


def sha256_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare_upload(source_dir, audit_path, config):
    audit = json.loads(Path(audit_path).read_text(encoding="utf-8"))
    contract = load_contract(config.schema_path)
    if not audit["structurally_ready_for_upload"]:
        raise ValueError("Source audit did not pass structural checks")
    if audit["schema_sha256"] != sha256_file(config.schema_path):
        raise ValueError("Schema changed since the audit")
    if audit["contract_sha256"] != sha256_file(PROJECT_ROOT / "data/schemas/feature_dictionary.md"):
        raise ValueError("Data Contract changed since the audit")
    expected = set(contract["source"]["files"])
    entries = audit["files"]
    if len(entries) != len(expected) or {r["source_file"] for r in entries} != expected:
        raise ValueError("Audit must contain exactly the eight contracted source files")
    if {p.name for p in Path(source_dir).glob("*.csv")} != expected:
        raise ValueError("Source directory must contain exactly the eight contracted CSV files")
    plan = []
    for entry in entries:
        path = Path(source_dir) / entry["source_file"]
        if path.stat().st_size != entry["input_bytes"] or sha256_file(path) != entry["source_sha256"]:
            raise ValueError(f"Source changed since audit: {path.name}")
        plan.append({"source_file": str(path), "bucket": config.minio_bucket,
                     "key": f"raw/cicids2017/{path.name}", "bytes": entry["input_bytes"],
                     "records": entry["input_records"], "sha256": entry["source_sha256"]})
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=PROJECT_ROOT / "data/raw/cicids2017")
    parser.add_argument("--audit-report", type=Path, default=PROJECT_ROOT / "docs/source-audit.local.json")
    parser.add_argument("--execute", action="store_true", help="Actually upload after reviewing the audit and plan")
    args = parser.parse_args()
    config = ProcessingConfig.from_env()
    plan = prepare_upload(args.source_dir, args.audit_report, config)
    print(json.dumps({"mode": "EXECUTE" if args.execute else "DRY_RUN", "objects": plan}, indent=2))
    if not args.execute:
        return
    import boto3
    from botocore.exceptions import ClientError
    config.validate()
    client = boto3.client("s3", endpoint_url=config.minio_endpoint,
                          aws_access_key_id=config.minio_access_key,
                          aws_secret_access_key=config.minio_secret_key,
                          region_name=config.minio_region)
    client.head_bucket(Bucket=config.minio_bucket)
    # Preflight all remote keys before sending any data. Existing differing
    # objects are never overwritten. The raw prefix requires a single writer.
    pending = []
    for item in plan:
        try:
            existing = client.head_object(Bucket=item["bucket"], Key=item["key"])
        except ClientError as error:
            if error.response["Error"]["Code"] != "404":
                raise
            pending.append(item)
        else:
            if existing["Metadata"].get("sha256") != item["sha256"] or existing["ContentLength"] != item["bytes"]:
                raise RuntimeError(f"Existing raw object differs; refusing overwrite: {item['key']}")
    for item in pending:
        client.upload_file(item["source_file"], item["bucket"], item["key"], ExtraArgs={
            "ContentType": "text/csv", "Metadata": {"sha256": item["sha256"], "source-records": str(item["records"])}})
        head = client.head_object(Bucket=item["bucket"], Key=item["key"])
        if head["ContentLength"] != item["bytes"] or head["Metadata"].get("sha256") != item["sha256"]:
            raise RuntimeError(f"Upload verification failed: {item['key']}")
        print(f"Uploaded {item['key']}")

if __name__ == "__main__":
    main()

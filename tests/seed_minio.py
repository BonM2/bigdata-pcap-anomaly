"""Upload the synthetic fixture to smoke/ only; never write to raw/."""

import boto3
from botocore.exceptions import ClientError

from processing.config import PROJECT_ROOT, ProcessingConfig


def main():
    config = ProcessingConfig.from_env()
    config.validate()
    client = boto3.client(
        "s3", endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_access_key,
        aws_secret_access_key=config.minio_secret_key,
        region_name=config.minio_region,
    )
    bucket = config.minio_bucket
    key = "smoke/cicids2017/synthetic_flow.csv"
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as error:
        if error.response["Error"]["Code"] != "404":
            raise
        client.create_bucket(Bucket=bucket)
    payload = (PROJECT_ROOT / "data/sample/sample_flow.csv").read_bytes()
    try:
        existing = client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchKey":
            raise
        client.put_object(Bucket=bucket, Key=key, Body=payload, ContentType="text/csv",
                          Metadata={"dataset-kind": "synthetic-test-fixture"})
    else:
        with existing["Body"] as body:
            if body.read() != payload:
                raise RuntimeError("Existing smoke fixture differs; refusing to overwrite")
    print(f"Synthetic fixture ready: s3a://{bucket}/{key}")


if __name__ == "__main__":
    main()

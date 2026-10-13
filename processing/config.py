"""Environment-based Spark/MinIO configuration and the v2.3.0 data contract."""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping, Optional
from urllib.parse import urlsplit
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA_PATH = PROJECT_ROOT / "data/schemas/raw_flow_schema.json"
SCHEMA_VERSION = "v2.3.0"
PIPELINE_VERSION = "0.1.0"
RATES = ("flow_bytes_s", "flow_packets_s")
WINDOW_FLAGS = {
    "init_win_bytes_forward": "init_win_fwd_missing",
    "init_win_bytes_backward": "init_win_bwd_missing",
}
TCP_FLAGS = ("fin_flag_count", "syn_flag_count", "rst_flag_count", "psh_flag_count", "ack_flag_count")


def load_contract(path=DEFAULT_SCHEMA_PATH):
    """Load project metadata, which is NOT native StructType JSON."""
    with Path(path).open(encoding="utf-8-sig") as stream:
        contract = json.load(stream)
    fields = sorted(contract["fields"], key=lambda item: item["source_index"])
    names = [item["name"] for item in fields]
    core = [item["name"] for item in fields if item["in_cleaned_core"]]
    if (len(fields) != 79 or [f["source_index"] for f in fields] != list(range(79))
            or len(set(names)) != 79 or len(core) != 22
            or core != contract["cleaned_core_field_names"]
            or contract["source"]["expected_column_count"] != 79
            or contract["contract_reference"]["contract_version"] != SCHEMA_VERSION.lstrip("v")):
        raise ValueError("Raw schema must match the 79-column / 22-core-field v2.3.0 contract")
    for item in fields:
        if item["spark_type"] != "string":
            raise ValueError("All raw fields must be strings")
        if item["in_cleaned_core"] and item["cleaned_target_type"] not in {"integer", "long", "double", "string"}:
            raise ValueError(f"Unsupported cleaned type for {item['name']}")
    contract["fields"] = fields
    return contract


def raw_schema(contract=None):
    from pyspark.sql.types import StringType, StructField, StructType
    contract = contract or load_contract()
    return StructType([StructField(f["name"], StringType(), True) for f in contract["fields"]])


@dataclass(frozen=True)
class ProcessingConfig:
    spark_master: str = "local[2]"
    spark_app_name: str = "cicids2017-processing"
    shuffle_partitions: int = 4
    executor_memory: str = "1g"
    executor_cores: int = 2
    spark_packages: str = "org.apache.hadoop:hadoop-aws:3.3.4"
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = field(default="", repr=False)
    minio_secret_key: str = field(default="", repr=False)
    minio_bucket: str = "bigdata-network"
    minio_region: str = "us-east-1"
    schema_path: Path = DEFAULT_SCHEMA_PATH
    input_path: str = "s3a://bigdata-network/raw/cicids2017/*.csv"
    cleaned_path: str = "s3a://bigdata-network/cleaned/flows"
    quarantine_path: str = "s3a://bigdata-network/quality/quarantine"
    rejected_path: str = "s3a://bigdata-network/quality/rejected"
    aggregates_path: str = "s3a://bigdata-network/processed/aggregates"
    report_path: str = "s3a://bigdata-network/reports/data_quality"
    batch_id: str = field(default_factory=lambda: uuid4().hex)
    pipeline_version: str = PIPELINE_VERSION

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None):
        env = os.environ if env is None else env
        bucket = env.get("MINIO_BUCKET", "bigdata-network")
        base = f"s3a://{bucket}"
        return cls(
            spark_master=env.get("SPARK_MASTER_URL", "local[2]"),
            spark_app_name=env.get("SPARK_APP_NAME", "cicids2017-processing"),
            shuffle_partitions=int(env.get("SPARK_SHUFFLE_PARTITIONS", "4")),
            executor_memory=env.get("SPARK_EXECUTOR_MEMORY", "1g"),
            executor_cores=int(env.get("SPARK_EXECUTOR_CORES", "2")),
            spark_packages=env.get("SPARK_PACKAGES", "org.apache.hadoop:hadoop-aws:3.3.4"),
            minio_endpoint=env.get("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=env.get("MINIO_ACCESS_KEY", env.get("AWS_ACCESS_KEY_ID", "")),
            minio_secret_key=env.get("MINIO_SECRET_KEY", env.get("AWS_SECRET_ACCESS_KEY", "")),
            minio_bucket=bucket,
            minio_region=env.get("MINIO_REGION", "us-east-1"),
            schema_path=Path(env.get("SCHEMA_PATH", str(DEFAULT_SCHEMA_PATH))),
            input_path=env.get("INPUT_PATH", f"{base}/raw/cicids2017/*.csv"),
            cleaned_path=env.get("CLEANED_PATH", f"{base}/cleaned/flows"),
            quarantine_path=env.get("QUARANTINE_PATH", f"{base}/quality/quarantine"),
            rejected_path=env.get("REJECTED_PATH", f"{base}/quality/rejected"),
            aggregates_path=env.get("AGGREGATES_PATH", f"{base}/processed/aggregates"),
            report_path=env.get("REPORT_PATH", f"{base}/reports/data_quality"),
            batch_id=env.get("BATCH_ID") or uuid4().hex,
            pipeline_version=env.get("PIPELINE_VERSION", PIPELINE_VERSION),
        )

    def validate(self, require_minio=True):
        load_contract(self.schema_path)
        if self.shuffle_partitions < 1 or self.executor_cores < 1:
            raise ValueError("Spark partitions and executor cores must be positive")
        if not self.batch_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in self.batch_id):
            raise ValueError("BATCH_ID must contain only letters, digits, '-' or '_'")
        if require_minio:
            endpoint = urlsplit(self.minio_endpoint)
            if endpoint.scheme not in {"http", "https"} or not endpoint.hostname or endpoint.username or endpoint.password:
                raise ValueError("MINIO_ENDPOINT must be an http(s) URL without credentials")
            if not self.minio_access_key or not self.minio_secret_key:
                raise ValueError("Set MINIO_ACCESS_KEY and MINIO_SECRET_KEY (or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)")


def create_spark_session(config=None, *, enable_minio=True):
    from pyspark.sql import SparkSession
    config = config or ProcessingConfig.from_env()
    config.validate(require_minio=enable_minio)
    builder = (SparkSession.builder.master(config.spark_master).appName(config.spark_app_name)
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", config.shuffle_partitions)
               .config("spark.executor.memory", config.executor_memory)
               .config("spark.executor.cores", config.executor_cores)
               .config("spark.sql.parquet.compression.codec", "snappy"))
    if enable_minio:
        options = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": config.minio_endpoint,
            "spark.hadoop.fs.s3a.endpoint.region": config.minio_region,
            "spark.hadoop.fs.s3a.access.key": config.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": config.minio_secret_key,
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(config.minio_endpoint.startswith("https://")).lower(),
        }
        if config.spark_packages:
            options["spark.jars.packages"] = config.spark_packages
        for key, value in options.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()

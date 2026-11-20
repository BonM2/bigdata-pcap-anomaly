"""Configuration and schema constants for the CICIDS2017 anomaly modeling pipeline.

Strictly aligned with Data Contract v2.3.0 (22 cleaned core fields + 12 metadata/flag fields).
"""

import os
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Tuple

SCHEMA_VERSION = "v2.3.0"
CONTRACT_VERSION = "2.3.0"
MODELING_PIPELINE_VERSION = "1.0.0"

BENIGN_LABEL = "BENIGN"
POSITIVE_CLASS_NAME = "ATTACK"
NEGATIVE_CLASS_NAME = "BENIGN"
POSITIVE_LABEL_VALUE = 1.0
NEGATIVE_LABEL_VALUE = 0.0

# Exact 21 numeric traffic features from Data Contract v2.3.0 (cleaned/flows/).
# Quality flags and lineage metadata are strictly EXCLUDED from Baseline features.
CORE_FEATURE_COLUMNS = (
    "destination_port",
    "flow_duration",
    "total_fwd_packets",
    "total_backward_packets",
    "total_length_of_fwd_packets",
    "total_length_of_bwd_packets",
    "flow_bytes_s",
    "flow_packets_s",
    "flow_iat_mean",
    "flow_iat_std",
    "min_packet_length",
    "max_packet_length",
    "packet_length_mean",
    "packet_length_std",
    "fin_flag_count",
    "syn_flag_count",
    "rst_flag_count",
    "psh_flag_count",
    "ack_flag_count",
    "init_win_bytes_forward",
    "init_win_bytes_backward",
)

# 4 nullable columns after cleaning (soft errors -> NULL + boolean quality flag)
NULLABLE_FEATURE_COLUMNS = (
    "flow_bytes_s",
    "flow_packets_s",
    "init_win_bytes_forward",
    "init_win_bytes_backward",
)

NON_NULLABLE_FEATURE_COLUMNS = tuple(
    c for c in CORE_FEATURE_COLUMNS if c not in NULLABLE_FEATURE_COLUMNS
)

# Exact mapping between nullable cleaned features and their Veracity boolean flags
NULLABLE_TO_FLAG = {
    "flow_bytes_s": "flow_bytes_s_invalid",
    "flow_packets_s": "flow_packets_s_invalid",
    "init_win_bytes_forward": "init_win_fwd_missing",
    "init_win_bytes_backward": "init_win_bwd_missing",
}

# Primary quality flags for Quality-Aware REVIEW (corrupted/non-finite/negative rates).
# Explicitly excludes LABEL_ENCODING_REPLACEMENT to prevent label leakage at inference time.
RATE_QUALITY_FLAGS = (
    "flow_bytes_s_invalid",
    "flow_packets_s_invalid",
)

WINDOW_QUALITY_FLAGS = (
    "init_win_fwd_missing",
    "init_win_bwd_missing",
)

# All 4 feature-level boolean quality flags (used for sensitivity analysis)
ALL_FEATURE_QUALITY_FLAGS = RATE_QUALITY_FLAGS + WINDOW_QUALITY_FLAGS

LINEAGE_AND_METADATA_COLUMNS = (
    "source_file",
    "source_dataset",
    "batch_id",
    "pipeline_version",
    "schema_version",
    "processed_at",
    "quality_status",
    "quality_reason_codes",
)

EXCLUDED_FROM_BASELINE_FEATURES = LINEAGE_AND_METADATA_COLUMNS + ALL_FEATURE_QUALITY_FLAGS

EXPECTED_CLEANED_TYPES: Dict[str, str] = {
    "destination_port": "int",
    "flow_duration": "bigint",
    "total_fwd_packets": "bigint",
    "total_backward_packets": "bigint",
    "total_length_of_fwd_packets": "bigint",
    "total_length_of_bwd_packets": "bigint",
    "flow_bytes_s": "double",
    "flow_packets_s": "double",
    "flow_iat_mean": "double",
    "flow_iat_std": "double",
    "min_packet_length": "double",
    "max_packet_length": "double",
    "packet_length_mean": "double",
    "packet_length_std": "double",
    "fin_flag_count": "int",
    "syn_flag_count": "int",
    "rst_flag_count": "int",
    "psh_flag_count": "int",
    "ack_flag_count": "int",
    "init_win_bytes_forward": "bigint",
    "init_win_bytes_backward": "bigint",
    "label": "string",
    "source_file": "string",
    "source_dataset": "string",
    "batch_id": "string",
    "pipeline_version": "string",
    "schema_version": "string",
    "processed_at": "timestamp",
    "quality_status": "string",
    "quality_reason_codes": "array<string>",
    "flow_bytes_s_invalid": "boolean",
    "flow_packets_s_invalid": "boolean",
    "init_win_fwd_missing": "boolean",
    "init_win_bwd_missing": "boolean",
}

PROMINENT_EXAMPLE_FEATURES = (
    "destination_port",
    "flow_duration",
    "total_fwd_packets",
    "total_backward_packets",
    "flow_bytes_s",
    "flow_packets_s",
    "packet_length_mean",
    "syn_flag_count",
    "ack_flag_count",
    "init_win_bytes_forward",
    "init_win_bytes_backward",
)

DEFAULT_THRESHOLD_GRID: Tuple[Tuple[float, float], ...] = (
    (0.45, 0.55),
    (0.40, 0.60),
    (0.35, 0.65),
    (0.30, 0.70),
    (0.25, 0.75),
    (0.20, 0.80),
)


@dataclass(frozen=True)
class ModelingConfig:
    spark_master: str = "spark://spark-master:7077"
    spark_app_name: str = "cicids2017-anomaly-modeling"
    shuffle_partitions: int = 32
    executor_memory: str = "2g"
    executor_cores: int = 2
    driver_memory: str = "1g"
    storage_level: str = "DISK_ONLY"
    spark_packages: str = ""
    minio_endpoint: str = "http://minio:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "bigdata-network"
    minio_region: str = "us-east-1"

    # Data & split settings
    cleaned_path: str = ""
    quality_report_path: str = ""
    seed: int = 42
    train_ratio: float = 0.70
    val_ratio: float = 0.15
    test_ratio: float = 0.15
    small_class_threshold: int = 200

    # Preprocessing
    imputer_strategy: str = "median"

    # Baseline Random Forest hyperparameters (memory-safe for Docker ~7.4-9.7 GiB)
    num_trees: int = 50
    max_depth: int = 10
    max_bins: int = 32
    feature_subset_strategy: str = "sqrt"
    impurity: str = "gini"
    subsampling_rate: float = 1.0
    min_instances_per_node: int = 1
    max_memory_in_mb: int = 256
    baseline_threshold: float = 0.50

    # Quality-Aware threshold selection on Validation
    threshold_grid: Tuple[Tuple[float, float], ...] = DEFAULT_THRESHOLD_GRID
    max_review_rate_target: float = 0.05
    review_cost_weight: float = 0.05

    # Example extraction
    examples_per_category: int = 5

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "ModelingConfig":
        env = os.environ if env is None else env
        return cls(
            spark_master=env.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
            spark_app_name=env.get("SPARK_APP_NAME", "cicids2017-anomaly-modeling"),
            shuffle_partitions=int(env.get("SPARK_SHUFFLE_PARTITIONS", "32")),
            executor_memory=env.get("SPARK_EXECUTOR_MEMORY", "2g"),
            executor_cores=int(env.get("SPARK_EXECUTOR_CORES", "2")),
            driver_memory=env.get("SPARK_DRIVER_MEMORY", "1g"),
            storage_level=env.get("SPARK_STORAGE_LEVEL", "DISK_ONLY"),
            spark_packages=env.get("SPARK_PACKAGES", ""),
            minio_endpoint=env.get("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=env.get("MINIO_ACCESS_KEY", env.get("AWS_ACCESS_KEY_ID", "")),
            minio_secret_key=env.get("MINIO_SECRET_KEY", env.get("AWS_SECRET_ACCESS_KEY", "")),
            minio_bucket=env.get("MINIO_BUCKET", "bigdata-network"),
            minio_region=env.get("MINIO_REGION", "us-east-1"),
        )


def create_modeling_spark_session(config: ModelingConfig, *, enable_minio: bool = True):
    """Build a memory-safe SparkSession for MLlib training and evaluation."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(config.spark_master)
        .appName(config.spark_app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.executor.memory", config.executor_memory)
        .config("spark.executor.cores", str(config.executor_cores))
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.adaptive.enabled", "true")
    )
    if enable_minio:
        options = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.endpoint": config.minio_endpoint,
            "spark.hadoop.fs.s3a.endpoint.region": config.minio_region,
            "spark.hadoop.fs.s3a.access.key": config.minio_access_key,
            "spark.hadoop.fs.s3a.secret.key": config.minio_secret_key,
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
                config.minio_endpoint.startswith("https://")
            ).lower(),
        }
        if config.spark_packages:
            options["spark.jars.packages"] = config.spark_packages
        for key, value in options.items():
            builder = builder.config(key, value)
    return builder.getOrCreate()

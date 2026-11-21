"""Validate Zeek conn.log JSON Lines and optionally write verified Parquet."""

import argparse
from dataclasses import replace

from pyspark.sql import functions as F

from processing.config import ProcessingConfig, create_spark_session
from processing.output import _location, _overlaps, hadoop_path, preflight_outputs, write_verified_parquet
from processing.quality_report import json_text


# Zeek encodes dotted keys as literal JSON names. Keep the source units in the
# output names and represent optional values as null when Zeek omits them.
CONNECTION_FIELDS = (
    ("ts", "connection_start_epoch_seconds", "DOUBLE"),
    ("uid", "uid", "STRING"),
    ("id.orig_h", "source_ip", "STRING"),
    ("id.orig_p", "source_port", "INT"),
    ("id.resp_h", "destination_ip", "STRING"),
    ("id.resp_p", "destination_port", "INT"),
    ("proto", "proto", "STRING"),
    ("service", "service", "STRING"),
    ("duration", "duration_seconds", "DOUBLE"),
    ("orig_bytes", "orig_payload_bytes", "BIGINT"),
    ("resp_bytes", "resp_payload_bytes", "BIGINT"),
    ("orig_pkts", "orig_packets", "BIGINT"),
    ("resp_pkts", "resp_packets", "BIGINT"),
)
CONNECTION_SCHEMA = ", ".join(f"`{source}` {kind}" for source, _, kind in CONNECTION_FIELDS)
SUMMARY_LIMIT = 20


def read_connections(spark, input_path):
    lines = spark.read.text(input_path)
    options = {"mode": "FAILFAST", "allowSingleQuotes": "false", "allowNonNumericNumbers": "false"}
    decoded = lines.select(
        F.from_json("value", CONNECTION_SCHEMA, options).alias("connection"),
        F.input_file_name().alias("source_path"),
    )
    return decoded.select(
        *(F.col("connection").getField(source).alias(target) for source, target, _ in CONNECTION_FIELDS),
        "source_path",
        F.lit("zeek").alias("source_dataset"),
    )


def nonnegative_finite(column):
    return (column >= 0) & (column < float("inf"))


def run(spark, input_path, output_path=None):
    if output_path:
        preflight_outputs(spark, {"connections": output_path})

    flows = read_connections(spark, input_path).cache()
    try:
        valid = nonnegative_finite(F.col("connection_start_epoch_seconds"))
        for name in ("uid", "source_ip", "destination_ip", "proto"):
            valid &= F.length(F.trim(name)) > 0
        for name in ("source_port", "destination_port"):
            valid &= F.col(name).between(0, 65535)
        duration = F.col("duration_seconds")
        valid &= duration.isNull() | nonnegative_finite(duration)
        for name in ("orig_payload_bytes", "resp_payload_bytes", "orig_packets", "resp_packets"):
            value = F.col(name)
            valid &= value.isNull() | (value >= 0)

        counts = flows.agg(
            F.count("*").alias("input_records"),
            F.count(F.when(valid, 1)).alias("valid_records"),
        ).first()
        if counts.input_records == 0 or counts.valid_records != counts.input_records:
            raise ValueError(f"Zeek validation failed: {counts.valid_records}/{counts.input_records} valid records")

        breakdown = (flows.groupBy("proto", "destination_port").count()
                     .orderBy(F.desc("count"), "proto", "destination_port").limit(SUMMARY_LIMIT))
        result = {
            "run_status": "VALIDATED_NO_OUTPUT",
            "source_dataset": "zeek",
            "input_path": input_path,
            **counts.asDict(),
            "protocol_port_counts_top20": [row.asDict() for row in breakdown.collect()],
        }
        if output_path:
            result["output"] = write_verified_parquet(flows, output_path, expected_count=counts.input_records)
            result["run_status"] = "VERIFIED"
        return result
    finally:
        flows.unpersist()


def main():
    config = replace(ProcessingConfig.from_env(), spark_app_name="zeek-connection-ingestion")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=f"s3a://{config.minio_bucket}/zeek/conn.log",
                        help="S3A URI of a single Zeek JSON Lines file")
    parser.add_argument("--execute", action="store_true", help="Write and verify a new Parquet batch on MinIO")
    parser.add_argument("--output", help="New S3A output URI, used with --execute")
    args = parser.parse_args()
    if args.output and not args.execute:
        parser.error("--output requires --execute")
    output = None
    try:
        source = _location(args.input)
        if source[0] != "s3a" or source[2] == "/":
            parser.error("--input must be a single S3A object URI")
        if args.execute:
            output = args.output or f"s3a://{config.minio_bucket}/processed/zeek/connections/{config.batch_id}"
            destination = _location(output)
            if destination[0] != "s3a" or destination[2] == "/" or "raw" in destination[2].split("/"):
                parser.error("Output must be a non-raw S3A prefix")
            if _overlaps(source, destination):
                parser.error("Input and output paths must be separate")
    except ValueError as error:
        parser.error(str(error))

    spark = create_spark_session(config)
    spark.sparkContext.setLogLevel("WARN")
    try:
        fs, path = hadoop_path(spark, args.input)
        if not fs.getFileStatus(path).isFile():
            raise ValueError("--input must reference one Zeek object")
        print(json_text(run(spark, args.input, output)), end="")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

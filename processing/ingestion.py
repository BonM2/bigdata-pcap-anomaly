"""Audited, line-oriented CICIDS2017 CSV ingestion over Hadoop (including S3A).

One physical line is one source record for this dataset. Quoted multiline CSV is
not supported: broken physical lines are retained as REJECTED, never skipped.
Only the first physical line of each file is a header; repeated headers are data.
"""

import csv
from functools import partial, reduce

from pyspark.sql.types import StringType, StructField, StructType

from processing.config import load_contract, raw_schema


def validate_header(line, contract):
    actual = next(csv.reader([line.lstrip("\ufeff")], strict=True))
    expected = [f["source_header_exact"] for f in contract["fields"]]
    if actual != expected:
        mismatch = next((i for i, (a, b) in enumerate(zip(actual, expected)) if a != b), None)
        raise ValueError(f"CSV header mismatch: expected {len(expected)} fields, got {len(actual)}; first mismatch index={mismatch}")


def parse_record(line, count=79):
    """Keep the original physical row as evidence, including short/long rows."""
    try:
        values = next(csv.reader([line], strict=True))
        reason = None if len(values) == count else f"COLUMN_COUNT:{len(values)}:EXPECTED:{count}"
    except csv.Error:
        values, reason = [], "CSV_SYNTAX"
    return (values + [None] * count)[:count], reason


def _parse_partition(index, lines, *, source_path, count):
    if index == 0:
        next(lines, None)  # header was checked on the driver before any Spark read
    for line in lines:
        values, reason = parse_record(line, count)
        yield tuple(values) + (source_path.rsplit("/", 1)[-1], source_path, line,
                               line if reason else None, reason)


def read_raw_flows(spark, input_path, contract=None, *, checksum=True):
    """Return (raw DataFrame, file manifest); fail before processing bad headers.

    Hadoop TextInputFormat splits files across executors. Python's strict CSV
    parser makes row width explicit, unlike Spark CSV's PERMISSIVE mode. SHA-256
    uses a bounded-memory JVM stream (an additional full read per input file).
    Input objects must stay immutable throughout the job.
    """
    contract = contract or load_contract()
    jvm, conf = spark._jvm, spark.sparkContext._jsc.hadoopConfiguration()
    pattern = jvm.org.apache.hadoop.fs.Path(input_path)
    fs = pattern.getFileSystem(conf)
    statuses = fs.globStatus(pattern)
    files = []
    for status in statuses or []:
        if status.isDirectory():
            iterator = fs.listFiles(status.getPath(), True)
            while iterator.hasNext():
                files.append(iterator.next())
        else:
            files.append(status)
    files = sorted((f for f in files if str(f.getPath()).lower().endswith(".csv")), key=lambda f: str(f.getPath()))
    if not files:
        raise FileNotFoundError(f"No CSV input objects found at {input_path}")
    manifest = []
    # Audit ALL headers before constructing a processing DataFrame.
    for status in files:
        path = status.getPath()
        reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(fs.open(path), "UTF-8"))
        try:
            header = reader.readLine()
            if header is None:
                raise ValueError(f"Empty source file: {path}")
            validate_header(str(header), contract)
        finally:
            reader.close()
        digest = None
        if checksum:
            stream = fs.open(path)
            try:
                digest = str(jvm.org.apache.commons.codec.digest.DigestUtils.sha256Hex(stream))
            finally:
                stream.close()
        manifest.append({"source_file": str(path.getName()), "source_path": str(path),
                         "input_bytes": int(status.getLen()), "source_sha256": digest,
                         "header_columns": 79, "header_valid": True})
    schema = StructType(raw_schema(contract).fields + [
        StructField(name, StringType(), True) for name in
        ("source_file", "source_path", "_raw_record", "_corrupt_record", "_malformed_reason")
    ])
    frames = []
    for item in manifest:
        parser = partial(_parse_partition, source_path=item["source_path"], count=79)
        rows = spark.sparkContext.textFile(item["source_path"]).mapPartitionsWithIndex(parser)
        frames.append(spark.createDataFrame(rows, schema))
    return reduce(lambda a, b: a.unionByName(b), frames), manifest

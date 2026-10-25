"""Separate, immutable batch outputs with Parquet read-back verification."""

import posixpath
from pathlib import Path
from urllib.parse import unquote, urlsplit

OUTPUT_FIELDS = ("cleaned_path", "features_path", "aggregates_path", "quarantine_path",
                 "rejected_path", "report_path", "performance_path")


def _location(path, *, pattern=False):
    parsed = urlsplit(path)
    if parsed.scheme not in {"s3a", "file", ""} or parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("Paths must use s3a:// or local file paths without credentials/query/fragment")
    if parsed.scheme == "s3a" and not parsed.netloc:
        raise ValueError("S3A paths require a bucket")
    if parsed.scheme == "file" and parsed.netloc not in {"", "localhost"}:
        raise ValueError("Remote file authorities are not supported")
    value = unquote(parsed.path).replace("\\", "/")
    if any(part in {".", ".."} for part in value.split("/")) or value != parsed.path:
        raise ValueError("Paths must be canonical (no traversal, escapes or backslashes)")
    if pattern:
        wildcard = next((i for i, ch in enumerate(value) if ch in "*?[{"), None)
        if wildcard is not None:
            value = value[:wildcard].rsplit("/", 1)[0]
        elif value.lower().endswith(".csv"):
            value = posixpath.dirname(value)
    elif any(ch in value for ch in "*?[{"):
        raise ValueError("Output paths must not contain globs")
    if parsed.scheme in {"", "file"}:
        value = Path(value).resolve().as_posix()
    return ("s3a" if parsed.scheme == "s3a" else "file", parsed.netloc if parsed.scheme == "s3a" else "",
            value.rstrip("/") or "/")


def _overlaps(left, right):
    if left[:2] != right[:2]:
        return False
    a, b = left[2], right[2]
    return a == b or a.startswith(b.rstrip("/") + "/") or b.startswith(a.rstrip("/") + "/")


def output_plan(config):
    config.validate(require_minio=False)
    source = _location(config.input_path, pattern=True)
    bases = {key: _location(getattr(config, key)) for key in OUTPUT_FIELDS}
    for name, location in bases.items():
        if location[2] == "/" or "raw" in location[2].split("/"):
            raise ValueError(f"Refusing raw/root output path: {name}")
        if _overlaps(source, location):
            raise ValueError(f"Input/output overlap: {name}")
        for other, candidate in bases.items():
            if other != name and _overlaps(location, candidate):
                raise ValueError(f"Output prefixes overlap: {name}, {other}")
    return {key: getattr(config, key).rstrip("/") + "/" + config.batch_id for key in OUTPUT_FIELDS}


def hadoop_path(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration()), path


def preflight_outputs(spark, paths):
    for uri in paths.values():
        fs, path = hadoop_path(spark, uri)
        if fs.exists(path):
            raise FileExistsError(f"Batch output already exists: {uri}; use a new BATCH_ID")


def write_text(spark, uri, text):
    fs, path = hadoop_path(spark, uri)
    writer = spark._jvm.java.io.OutputStreamWriter(fs.create(path, False), "UTF-8")
    try:
        writer.write(text)
    finally:
        writer.close()


def write_verified_parquet(frame, path, *, expected_count=None, verify_labels=False):
    expected_count = frame.count() if expected_count is None else expected_count
    frame.write.mode("errorifexists").option("compression", "snappy").parquet(path)
    reread = frame.sparkSession.read.parquet(path)
    if reread.dtypes != frame.dtypes:
        raise RuntimeError(f"Parquet schema mismatch: {path}")
    actual = reread.count()
    if actual != expected_count:
        raise RuntimeError(f"Parquet count mismatch: expected {expected_count}, got {actual}")
    if verify_labels:
        keys = [n for n in ("source_file", "label", "quality_status") if n in frame.columns]
        before = frame.groupBy(*keys).count()
        after = reread.groupBy(*keys).count()
        if before.exceptAll(after).limit(1).count() or after.exceptAll(before).limit(1).count():
            raise RuntimeError(f"Parquet label distribution mismatch: {path}")
    return {"path": path, "records": actual, "schema": reread.schema.jsonValue(),
            "format": "parquet", "compression": "snappy", "read_back_status": "PASS"}

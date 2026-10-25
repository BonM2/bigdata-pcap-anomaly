"""Measured, JSON-safe quality snapshots over the 22 typed core fields."""

import csv
import io
import json

from pyspark.sql import functions as F

from processing.config import SCHEMA_VERSION, TCP_FLAGS, WINDOW_FLAGS, load_contract
from processing.validation import schema_validation


def quality_snapshot(frame, contract=None, *, cleaned=False):
    """Count cells by field and file. Categories may overlap; never sum as rows.

    cast_error_count is historical source cast evidence retained after cleaning.
    NULL counts are actual typed NULL cells, including permitted cleaned NULLs.
    """
    contract = contract or load_contract()
    expressions = [F.count("*").alias("input_records")]
    keys = []

    def count(name, condition):
        expressions.append(F.coalesce(F.sum(F.when(condition, 1).otherwise(0)), F.lit(0)).alias(name))

    for status in ("VALID", "QUARANTINED", "REJECTED"):
        count(status.lower() + "_records", F.col("quality_status") == status)
    count("records_with_reason_codes", F.size("quality_reason_codes") > 0)
    count("invalid_label_count", F.col("label").isNull() | (F.trim("label") == ""))
    for item in contract["fields"]:
        if not item["in_cleaned_core"]:
            continue
        name, kind = item["name"], item["cleaned_target_type"]
        value = F.col(name)
        predicates = {"null_count": value.isNull()}
        if kind != "string":
            negative = value < 0
            if name in WINDOW_FLAGS and not cleaned:
                negative = negative & (value != -1)
            invalid_range = negative
            if name == "destination_port":
                invalid_range = invalid_range | (value > 65535)
            if name in TCP_FLAGS:
                invalid_range = invalid_range | ~value.isin(0, 1)
            predicates.update(negative_invalid_count=negative, invalid_range_count=invalid_range,
                              cast_error_count=F.array_contains("cast_failure_fields", name))
        if kind == "double":
            predicates.update(nan_count=F.isnan(value), infinity_count=F.abs(value) == float("inf"))
        if name in WINDOW_FLAGS:
            predicates["sentinel_count"] = (F.col(WINDOW_FLAGS[name]) if cleaned else value == -1)
        for metric, condition in predicates.items():
            alias = f"{metric}__{name}"
            count(alias, condition)
            keys.append((alias, metric, name))
    rows = [r.asDict() for r in frame.groupBy("source_file").agg(*expressions).collect()]
    scalar_names = ["input_records", "valid_records", "quarantined_records", "rejected_records",
                    "records_with_reason_codes", "invalid_label_count"]

    def unpack(values):
        result = {n: int(values.get(n, 0)) for n in scalar_names}
        result["parsed_records"] = result["input_records"] - result["rejected_records"]
        for alias, metric, name in keys:
            result.setdefault(metric + "_by_field", {})[name] = int(values.get(alias, 0))
        for metric in {k[1] for k in keys}:
            result[metric] = sum(result[metric + "_by_field"].values())
        return result

    totals = {n: sum(r[n] for r in rows) for n in scalar_names + [k[0] for k in keys]}
    labels = [r.asDict() for r in frame.groupBy("source_file", "label", "quality_status").count().collect()]
    return {"scope": "all classified records; 22 typed core fields; cell counts; cast errors retain source history",
            "schema_validation": schema_validation(frame, contract, cleaned=cleaned),
            "totals": unpack(totals),
            "files": [{"source_file": r["source_file"], **unpack(r)} for r in rows],
            "label_distribution": labels}


def build_quality_report(config, manifest, before, after, post_validation, reconciliation, performance, outputs):
    return {
        "run_status": "VERIFIED", "batch_id": config.batch_id,
        "schema_version": SCHEMA_VERSION, "pipeline_version": config.pipeline_version,
        "input_records": before["totals"]["input_records"],
        **{key: after["totals"][key] for key in ("valid_records", "quarantined_records", "rejected_records")},
        "files": manifest, "before_cleaning": before, "after_cleaning": after,
        "cleaned_label_distribution": [r for r in after["label_distribution"] if r["quality_status"] == "VALID"],
        "post_cleaning_validation": post_validation, "record_reconciliation": reconciliation,
        "performance": performance, "outputs": outputs,
        "metric_notes": {
            "null_count": "Typed NULL cells, including cast failures and permitted cleaned rate/window NULLs",
            "cast_error_count": "Source cast failures; retained after cleaning as provenance, not remaining invalid cells",
            "error_ratio": "(quarantined + rejected) / input; soft-issue VALID rows are counted separately",
            "total_bytes": "Sum of forward/backward payload bytes; not wire bytes",
            "completion": "Batch is complete only when _SUCCESS.json exists in the quality report batch directory",
        },
    }


def summary_csv(report):
    stream = io.StringIO(newline="")
    fields = ["stage", "source_file", "input_records", "valid_records", "quarantined_records", "rejected_records",
              "null_count", "nan_count", "infinity_count", "invalid_range_count", "cast_error_count"]
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for stage in ("before_cleaning", "after_cleaning"):
        for row in [{"source_file": "__BATCH__", **report[stage]["totals"]}, *report[stage]["files"]]:
            writer.writerow({"stage": stage, **row})
    return stream.getvalue()


def json_text(value):
    return json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n"

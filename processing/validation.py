"""Data Contract validation. Soft errors stay VALID and retain reason codes."""

from pyspark.sql import functions as F

from processing.config import RATES, SCHEMA_VERSION, TCP_FLAGS, WINDOW_FLAGS, load_contract


STATUSES = ("VALID", "QUARANTINED", "REJECTED")


def schema_validation(frame, contract=None, *, cleaned=False, exact=False):
    """Check types, not Spark nullable hints (Parquet relaxes those hints)."""
    contract = contract or load_contract()
    kinds = {"integer": "int", "long": "bigint", "double": "double", "string": "string"}
    expected = {f["name"]: kinds[f["cleaned_target_type"]]
                for f in contract["fields"] if f["in_cleaned_core"]}
    expected.update({n: "string" for n in (
        "source_file", "source_dataset", "batch_id", "pipeline_version", "schema_version", "quality_status")})
    expected.update(processed_at="timestamp", quality_reason_codes="array<string>")
    if cleaned:
        expected.update({n: "boolean" for n in (
            "flow_bytes_s_invalid", "flow_packets_s_invalid", *WINDOW_FLAGS.values())})
    actual = dict(frame.dtypes)
    errors = [f"{name}: expected {kind}, got {actual.get(name, 'MISSING')}"
              for name, kind in expected.items() if actual.get(name) != kind]
    if len(frame.columns) != len(set(frame.columns)):
        errors.append("Duplicate column names")
    if exact and set(actual) - set(expected):
        errors.append("Unexpected output columns: " + ", ".join(sorted(set(actual) - set(expected))))
    return {"status": "FAIL" if errors else "PASS", "errors": errors,
            "core_field_count": len(contract["cleaned_core_field_names"])}


def classification_counts(frame):
    counts = {r["quality_status"]: r["count"]
              for r in frame.groupBy("quality_status").count().collect()}
    if set(counts) - set(STATUSES):
        raise ValueError("Unknown or NULL quality_status")
    return {"input_records": sum(counts.values()),
            **{s.lower() + "_records": counts.get(s, 0) for s in STATUSES}}


def reconcile_stages(before, after, expected_input_records):
    """Compare multisets of source evidence, preserving duplicate multiplicities.

    The current cleaning policy allows no status transitions. A changed policy
    must explicitly update this check; transitions are still reported as pairs.
    """
    counts = {"before": classification_counts(before), "after": classification_counts(after)}
    for stage in counts.values():
        if stage["input_records"] != expected_input_records:
            raise ValueError("Record reconciliation failed: input count changed")
        if stage["input_records"] != sum(stage[s.lower() + "_records"] for s in STATUSES):
            raise ValueError("Record reconciliation failed: classification totals")
    evidence = [n for n in ("source_file", "source_path", "_raw_record", "_raw_values") if n in before.columns]
    if "_raw_values" not in evidence or "quality_status_before" not in after.columns:
        raise ValueError("Reconciliation requires raw evidence and prior status")
    left = before.select(*evidence, "quality_status")
    right = after.select(*evidence, F.col("quality_status_before").alias("quality_status"))
    if left.exceptAll(right).limit(1).count() or right.exceptAll(left).limit(1).count():
        raise ValueError("Record reconciliation failed: source evidence or duplicates changed")
    transitions = [r.asDict() for r in after.groupBy("quality_status_before", "quality_status").count().collect()]
    if any(r["quality_status_before"] != r["quality_status"] for r in transitions):
        raise ValueError("Cleaning status transition is outside the current policy")
    return {"status": "PASS", **counts, "transitions": transitions,
            "evidence_multiset_preserved": True}


def validate_cleaned_flows(annotated, contract=None):
    """Attach post_clean_errors without changing classification or dropping rows.

    Hard value errors are expected in quarantine/reject. Invariants concerning
    labels, flags and the actual cleaning operation apply to ALL statuses.
    """
    contract = contract or load_contract()
    schema = schema_validation(annotated, contract, cleaned=True)
    if schema["status"] != "PASS":
        raise ValueError("Post-clean schema validation: " + "; ".join(schema["errors"]))
    required = {"_raw_values", "quality_status_before", "cast_failure_fields"}
    if not required.issubset(annotated.columns):
        raise ValueError("Post-clean validation requires annotated raw evidence and prior status")
    errors = []

    def rule(condition, code):
        errors.append(F.when(F.coalesce(condition, F.lit(False)), F.lit(code)))

    valid = F.col("quality_status") == "VALID"
    rule(F.col("quality_status").isNull() | ~F.col("quality_status").isin(*STATUSES), "UNKNOWN_STATUS")
    rule(~F.col("quality_status").eqNullSafe(F.col("quality_status_before")), "STATUS_CHANGED")
    rule(~F.col("label").eqNullSafe(F.col("_raw_values.label")), "LABEL_CHANGED")
    rule(valid & (F.col("label").isNull() | (F.trim("label") == "")), "INVALID_LABEL")
    for name in ("source_file", "source_dataset", "batch_id", "pipeline_version", "schema_version"):
        rule(F.col(name).isNull() | (F.trim(F.col(name)) == ""), f"MISSING_METADATA:{name}")
    for name in ("processed_at", "quality_reason_codes"):
        rule(F.col(name).isNull(), f"MISSING_METADATA:{name}")
    rule(~F.col("source_dataset").eqNullSafe(F.lit(contract["source"]["dataset"])), "SOURCE_DATASET_MISMATCH")
    rule(~F.col("schema_version").eqNullSafe(F.lit(SCHEMA_VERSION)), "SCHEMA_VERSION_MISMATCH")
    for item in contract["fields"]:
        name, kind = item["name"], item["cleaned_target_type"]
        if not item["in_cleaned_core"] or kind == "string":
            continue
        value = F.col(name)
        allowed_null = F.lit(False)
        if name in RATES or name in WINDOW_FLAGS:
            flag = name + "_invalid" if name in RATES else WINDOW_FLAGS[name]
            sql_type = "double" if name in RATES else "bigint"
            original = F.expr(f"try_cast(trim(_raw_values.`{name}`) as {sql_type})")
            if name in RATES:
                expected_flag = original.isNull() | F.isnan(original) | (F.abs(original) == float("inf")) | (original < 0)
            else:
                # Match the strict integer grammar used by transformation.
                original = F.when(F.trim(F.col("_raw_values").getField(name)).rlike(r"^[+-]?[0-9]+$"), original)
                expected_flag = F.coalesce(original == -1, F.lit(False))
            rule(~F.col(flag).eqNullSafe(expected_flag), f"FLAG_MISMATCH:{name}")
            expected_value = F.when(~expected_flag, original)
            rule(~value.eqNullSafe(expected_value), f"CLEANING_MISMATCH:{name}")
            allowed_null = F.coalesce(F.col(flag), F.lit(False))
        rule(valid & value.isNull() & ~allowed_null, f"NULL:{name}")
        rule(valid & (value < 0), f"NEGATIVE:{name}")
        if kind == "double":
            rule(valid & F.isnan(value), f"NAN:{name}")
            rule(valid & (F.abs(value) == float("inf")), f"INFINITY:{name}")
        if name == "destination_port":
            rule(valid & ~value.between(0, 65535), "OUT_OF_RANGE:destination_port")
        if name in TCP_FLAGS:
            rule(valid & ~value.isin(0, 1), f"INVALID_FLAG:{name}")
    return annotated.withColumn("post_clean_errors", F.filter(F.array(*errors), lambda x: x.isNotNull()))


def post_clean_summary(checked):
    rows = checked.select(F.explode("post_clean_errors").alias("error")).groupBy("error").count().collect()
    errors = {r["error"]: r["count"] for r in rows}
    return {"status": "FAIL" if errors else "PASS", "error_counts": errors,
            "records_with_errors": checked.filter(F.size("post_clean_errors") > 0).count(),
            **classification_counts(checked)}


def validate_flows(typed, contract=None):
    contract = contract or load_contract()
    reasons, hard_errors = [], []

    def rule(condition, code, hard=False):
        condition = F.coalesce(condition, F.lit(False))
        reasons.append(F.when(condition, F.lit(code)))
        if hard:
            hard_errors.append(condition)

    malformed = F.lit(False)
    for name in ("_corrupt_record", "_malformed_reason"):
        if name in typed.columns:
            malformed = malformed | F.col(name).isNotNull()
    rule(malformed, "MALFORMED_CSV")

    for item in contract["fields"]:
        if not item["in_cleaned_core"] or item["name"] == "label":
            continue
        name, kind = item["name"], item["cleaned_target_type"]
        value = F.col(name)
        raw = F.col("_raw_values").getField(name)
        soft = name in RATES
        rule(raw.isNull(), f"NULL:{name}", not soft)
        rule(raw.isNotNull() & (F.trim(raw) == ""), f"EMPTY:{name}", not soft)
        rule(F.array_contains("cast_failure_fields", name), f"CAST_FAILURE:{name}", not soft)
        if kind == "double":
            rule(F.isnan(value), f"NAN:{name}", not soft)
            rule(F.abs(value) == float("inf"), f"INFINITY:{name}", not soft)
        if name in WINDOW_FLAGS:
            rule(value == -1, f"SENTINEL:{name}")
            rule((value < 0) & (value != -1), f"NEGATIVE:{name}", True)
        else:
            rule(value < 0, f"NEGATIVE:{name}", not soft)
        if name == "destination_port":
            rule((value < 0) | (value > 65535), "OUT_OF_RANGE:destination_port", True)
        if name in TCP_FLAGS:
            rule(~value.isin(0, 1), f"INVALID_FLAG:{name}", True)

    label = F.col("label")
    rule(label.isNull() | (F.trim(label) == ""), "INVALID_LABEL", True)
    # Preserve the source text, while making the observed decoding issue visible.
    rule(label.contains("\ufffd"), "LABEL_ENCODING_REPLACEMENT")
    hard = F.lit(False)
    for condition in hard_errors:
        hard = hard | condition
    return (typed.withColumn("quality_reason_codes", F.filter(F.array(*reasons), lambda x: x.isNotNull()))
            .withColumn("quality_status", F.when(malformed, "REJECTED")
                        .when(hard, "QUARANTINED").otherwise("VALID")))

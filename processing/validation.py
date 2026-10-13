"""Data Contract validation. Soft errors stay VALID and retain reason codes."""

from pyspark.sql import functions as F

from processing.config import RATES, TCP_FLAGS, WINDOW_FLAGS, load_contract


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

"""Cast canonical fields without losing source values or truncating integers."""

from pyspark.sql import functions as F

from processing.config import ProcessingConfig, SCHEMA_VERSION, load_contract


def transform_flows(raw, config=None, contract=None):
    """Return 22 typed fields, lineage, raw evidence and per-field cast failures.

    Integer strings use a strict signed-decimal grammar: fractions/exponents are
    errors, not silently truncated. try_cast also catches overflow under ANSI mode.
    Label text is never trimmed or rewritten.
    """
    config = config or ProcessingConfig.from_env()
    contract = contract or load_contract(config.schema_path)
    names = [f["name"] for f in contract["fields"]]
    missing = set(names) - set(raw.columns)
    if missing or len(raw.columns) != len(set(raw.columns)):
        raise ValueError(f"Raw input requires unique canonical columns; missing: {sorted(missing)}")
    expressions, failures = [], []
    for item in contract["fields"]:
        if not item["in_cleaned_core"]:
            continue
        name, kind = item["name"], item["cleaned_target_type"]
        value = F.col(name)
        if kind == "string":
            expressions.append(value.alias(name))
            continue
        token = F.trim(value)
        sql_type = {"integer": "int", "long": "bigint", "double": "double"}[kind]
        cast = F.expr(f"try_cast(trim(`{name}`) as {sql_type})")
        if kind in {"integer", "long"}:
            cast = F.when(token.rlike(r"^[+-]?[0-9]+$"), cast)
        expressions.append(cast.alias(name))
        failures.append(F.when(value.isNotNull() & (token != "") & cast.isNull(), F.lit(name)))
    evidence = [F.struct(*[F.col(n) for n in names]).alias("_raw_values")]
    for name in ("source_file", "source_path", "_raw_record", "_corrupt_record", "_malformed_reason"):
        if name in raw.columns:
            evidence.append(F.col(name))
    if "source_file" not in raw.columns:
        raise ValueError("Raw input must provide source_file for lineage")
    return raw.select(
        *expressions, *evidence,
        F.filter(F.array(*failures), lambda x: x.isNotNull()).alias("cast_failure_fields"),
        F.lit("CICIDS2017").alias("source_dataset"),
        F.lit(config.batch_id).alias("batch_id"),
        F.lit(config.pipeline_version).alias("pipeline_version"),
        F.lit(SCHEMA_VERSION).alias("schema_version"),
        F.current_timestamp().alias("processed_at"),
    )

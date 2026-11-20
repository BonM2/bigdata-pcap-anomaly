"""Data loading, contract verification, deduplication, binary labeling, and stratified splitting.

Guarantees zero data leakage:
1. Reads ONLY clean VALID Parquet (never QUARANTINED/REJECTED or synthetic scaled data).
2. Removes exact duplicates BEFORE splitting into Train / Validation / Test.
3. Uses deterministic stratified split by original `label` with seed=42 (70% / 15% / 15%).
4. Programmatically verifies zero overlap between splits.
"""

import json
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame, Window, functions as F

from src.modeling.config import (
    ALL_FEATURE_QUALITY_FLAGS,
    BENIGN_LABEL,
    CORE_FEATURE_COLUMNS,
    EXPECTED_CLEANED_TYPES,
    NEGATIVE_LABEL_VALUE,
    NON_NULLABLE_FEATURE_COLUMNS,
    NULLABLE_FEATURE_COLUMNS,
    NULLABLE_TO_FLAG,
    POSITIVE_LABEL_VALUE,
    RATE_QUALITY_FLAGS,
)


def _read_hadoop_text(spark, uri: str) -> str:
    jvm = spark._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(conf)
    if not fs.exists(path):
        raise FileNotFoundError(f"Expected file does not exist: {uri}")
    stream = fs.open(path)
    reader = jvm.java.io.BufferedReader(jvm.java.io.InputStreamReader(stream, "UTF-8"))
    lines = []
    try:
        while True:
            line = reader.readLine()
            if line is None:
                break
            lines.append(str(line))
    finally:
        reader.close()
    return "\n".join(lines)


def verify_clean_batch_marker(spark, quality_report_batch_uri: str) -> Dict[str, Any]:
    """Verify the pipeline completion marker (_SUCCESS.json) and quality.json metadata."""
    base = quality_report_batch_uri.rstrip("/")
    marker = json.loads(_read_hadoop_text(spark, base + "/_SUCCESS.json"))
    if marker.get("run_status") != "COMPLETE":
        raise RuntimeError(
            f"Batch completion marker is not COMPLETE: {marker.get('run_status')}"
        )
    quality = json.loads(_read_hadoop_text(spark, base + "/quality.json"))
    if quality.get("post_cleaning_validation", {}).get("status") != "PASS":
        raise RuntimeError("Batch quality.json post_cleaning_validation is not PASS")
    return {
        "batch_id": marker.get("batch_id"),
        "marker_status": marker.get("run_status"),
        "quality_report_uri": base + "/quality.json",
        "schema_version": quality.get("schema_version"),
        "pipeline_version": quality.get("pipeline_version"),
        "etl_input_records": int(quality.get("input_records", 0)),
        "etl_valid_records": int(quality.get("valid_records", 0)),
        "etl_quarantined_records": int(quality.get("quarantined_records", 0)),
        "etl_rejected_records": int(quality.get("rejected_records", 0)),
        "source_file_count": len(quality.get("files", [])),
    }


def load_and_validate_clean_valid(spark, cleaned_uri: str) -> Tuple[DataFrame, Dict[str, Any]]:
    """Read cleaned VALID Parquet and enforce strict Data Contract v2.3.0 invariants."""
    df = spark.read.parquet(cleaned_uri)
    actual_types = dict(df.dtypes)

    missing_or_wrong = []
    for col_name, expected_type in EXPECTED_CLEANED_TYPES.items():
        if actual_types.get(col_name) != expected_type:
            missing_or_wrong.append(
                f"{col_name}: expected {expected_type}, got {actual_types.get(col_name, 'MISSING')}"
            )
    if missing_or_wrong:
        raise ValueError(
            "Cleaned VALID Parquet does not match Data Contract v2.3.0: "
            + "; ".join(missing_or_wrong)
        )

    # Single-pass invariant check across all VALID rows
    checks = [
        F.count("*").alias("total_rows"),
        F.sum(F.when(F.col("quality_status") != "VALID", 1).otherwise(0)).alias("non_valid_rows"),
        F.sum(F.when(F.col("label").isNull() | (F.trim(F.col("label")) == ""), 1).otherwise(0)).alias("invalid_label_rows"),
        F.sum(F.when(F.col("flow_bytes_s_invalid"), 1).otherwise(0)).alias("flag_flow_bytes_s_invalid"),
        F.sum(F.when(F.col("flow_packets_s_invalid"), 1).otherwise(0)).alias("flag_flow_packets_s_invalid"),
        F.sum(F.when(F.col("init_win_fwd_missing"), 1).otherwise(0)).alias("flag_init_win_fwd_missing"),
        F.sum(F.when(F.col("init_win_bwd_missing"), 1).otherwise(0)).alias("flag_init_win_bwd_missing"),
        F.sum(
            F.when(
                F.col("flow_bytes_s_invalid") | F.col("flow_packets_s_invalid"),
                1,
            ).otherwise(0)
        ).alias("any_rate_flag_rows"),
        F.sum(
            F.when(
                F.col("flow_bytes_s_invalid")
                | F.col("flow_packets_s_invalid")
                | F.col("init_win_fwd_missing")
                | F.col("init_win_bwd_missing"),
                1,
            ).otherwise(0)
        ).alias("any_4_feature_flag_rows"),
        F.sum(
            F.when(F.array_contains(F.col("quality_reason_codes"), "LABEL_ENCODING_REPLACEMENT"), 1).otherwise(0)
        ).alias("label_encoding_replacement_rows"),
    ]

    for col_name in NON_NULLABLE_FEATURE_COLUMNS:
        cond = F.col(col_name).isNull()
        if EXPECTED_CLEANED_TYPES[col_name] == "double":
            cond = cond | F.isnan(F.col(col_name)) | (F.abs(F.col(col_name)) == float("inf"))
        checks.append(F.sum(F.when(cond, 1).otherwise(0)).alias(f"bad__{col_name}"))

    for col_name in NULLABLE_FEATURE_COLUMNS:
        flag_col = NULLABLE_TO_FLAG[col_name]
        # Verify nullable feature has no NaN/Inf and NULL aligns with its quality flag
        if EXPECTED_CLEANED_TYPES[col_name] == "double":
            bad_float = F.isnan(F.col(col_name)) | (F.abs(F.col(col_name)) == float("inf"))
            checks.append(F.sum(F.when(bad_float, 1).otherwise(0)).alias(f"bad_float__{col_name}"))
        mismatch = F.col(col_name).isNull() != F.coalesce(F.col(flag_col), F.lit(False))
        checks.append(F.sum(F.when(mismatch, 1).otherwise(0)).alias(f"flag_mismatch__{col_name}"))

    row = df.agg(*checks).collect()[0].asDict()
    total_rows = int(row["total_rows"])
    if total_rows == 0:
        raise ValueError(f"Cleaned VALID dataset is empty: {cleaned_uri}")
    if int(row["non_valid_rows"]) != 0:
        raise ValueError(f"Found {row['non_valid_rows']} non-VALID rows in cleaned Parquet!")
    if int(row["invalid_label_rows"]) != 0:
        raise ValueError(f"Found {row['invalid_label_rows']} null/empty labels in cleaned Parquet!")

    for key, val in row.items():
        if key.startswith(("bad__", "bad_float__", "flag_mismatch__")) and int(val or 0) != 0:
            raise ValueError(f"Data integrity violation in cleaned VALID Parquet: {key}={val}")

    audit = {
        "cleaned_uri": cleaned_uri,
        "column_count": len(df.columns),
        "raw_valid_records": total_rows,
        "non_valid_records": 0,
        "flag_counts_before_dedup": {
            "flow_bytes_s_invalid": int(row["flag_flow_bytes_s_invalid"]),
            "flow_packets_s_invalid": int(row["flag_flow_packets_s_invalid"]),
            "init_win_fwd_missing": int(row["flag_init_win_fwd_missing"]),
            "init_win_bwd_missing": int(row["flag_init_win_bwd_missing"]),
            "any_rate_flag": int(row["any_rate_flag_rows"]),
            "any_4_feature_flags": int(row["any_4_feature_flag_rows"]),
            "label_encoding_replacement_excluded_from_rules": int(row["label_encoding_replacement_rows"]),
        },
    }
    return df, audit


def deduplicate_and_label(df: DataFrame, *, storage_level=None) -> Tuple[DataFrame, Dict[str, Any]]:
    """Deduplicate exact flow observations BEFORE splitting and attach binary label.

    Deduplication subset = 21 core features + `label` (which uniquely determines the 4
    feature quality flags as verified by load_and_validate_clean_valid). Excluding
    `source_file` ensures identical flows appearing on different days cannot leak across
    Train / Validation / Test splits.

    If `storage_level` is provided, the returned `labeled_df` is persisted once so
    deduplication is not recomputed across statistics and splitting.
    """
    dedup_subset = list(CORE_FEATURE_COLUMNS) + ["label"]
    raw_valid_count = df.count()

    labeled_df = df.dropDuplicates(subset=dedup_subset).withColumn(
        "label_binary",
        F.when(F.col("label") == F.lit(BENIGN_LABEL), F.lit(NEGATIVE_LABEL_VALUE))
        .otherwise(F.lit(POSITIVE_LABEL_VALUE))
        .cast("double"),
    )
    if storage_level is not None:
        labeled_df = labeled_df.persist(storage_level)

    dedup_count = labeled_df.count()
    removed_duplicates = raw_valid_count - dedup_count

    feature_conflict_groups = (
        labeled_df.groupBy(*CORE_FEATURE_COLUMNS)
        .agg(F.countDistinct("label").alias("distinct_labels"))
        .filter(F.col("distinct_labels") > 1)
        .count()
    )

    # Collect post-dedup label distribution and quality flag counts
    label_rows = (
        labeled_df.groupBy("label", "label_binary")
        .count()
        .orderBy(F.desc("count"), F.asc("label"))
        .collect()
    )
    label_distribution = [
        {
            "label": r["label"],
            "label_binary": float(r["label_binary"]),
            "class_name": "BENIGN" if float(r["label_binary"]) == 0.0 else "ATTACK",
            "count": int(r["count"]),
        }
        for r in label_rows
    ]
    label_mapping = {
        item["label"]: {
            "label_binary": item["label_binary"],
            "class_name": item["class_name"],
        }
        for item in label_distribution
    }

    flag_agg = labeled_df.agg(
        F.sum(F.when(F.col("flow_bytes_s_invalid"), 1).otherwise(0)).alias("flow_bytes_s_invalid"),
        F.sum(F.when(F.col("flow_packets_s_invalid"), 1).otherwise(0)).alias("flow_packets_s_invalid"),
        F.sum(F.when(F.col("init_win_fwd_missing"), 1).otherwise(0)).alias("init_win_fwd_missing"),
        F.sum(F.when(F.col("init_win_bwd_missing"), 1).otherwise(0)).alias("init_win_bwd_missing"),
        F.sum(
            F.when(F.col("flow_bytes_s_invalid") | F.col("flow_packets_s_invalid"), 1).otherwise(0)
        ).alias("any_rate_flag"),
        F.sum(
            F.when(
                F.col("flow_bytes_s_invalid")
                | F.col("flow_packets_s_invalid")
                | F.col("init_win_fwd_missing")
                | F.col("init_win_bwd_missing"),
                1,
            ).otherwise(0)
        ).alias("any_4_feature_flags"),
        F.sum(F.when(F.col("label_binary") == 1.0, 1).otherwise(0)).alias("attack_records"),
        F.sum(F.when(F.col("label_binary") == 0.0, 1).otherwise(0)).alias("benign_records"),
    ).collect()[0].asDict()

    stats = {
        "dedup_subset": dedup_subset,
        "records_before_dedup": int(raw_valid_count),
        "records_after_dedup": int(dedup_count),
        "duplicate_records_removed": int(removed_duplicates),
        "duplicate_removal_rate": float(removed_duplicates / raw_valid_count) if raw_valid_count else 0.0,
        "feature_groups_with_conflicting_labels": int(feature_conflict_groups),
        "distinct_original_labels": len(label_distribution),
        "attack_records_after_dedup": int(flag_agg["attack_records"]),
        "benign_records_after_dedup": int(flag_agg["benign_records"]),
        "attack_ratio_after_dedup": float(flag_agg["attack_records"] / dedup_count) if dedup_count else 0.0,
        "label_distribution_after_dedup": label_distribution,
        "label_mapping": label_mapping,
        "flag_counts_after_dedup": {
            "flow_bytes_s_invalid": int(flag_agg["flow_bytes_s_invalid"]),
            "flow_packets_s_invalid": int(flag_agg["flow_packets_s_invalid"]),
            "init_win_fwd_missing": int(flag_agg["init_win_fwd_missing"]),
            "init_win_bwd_missing": int(flag_agg["init_win_bwd_missing"]),
            "any_rate_flag": int(flag_agg["any_rate_flag"]),
            "any_rate_flag_ratio": float(flag_agg["any_rate_flag"] / dedup_count) if dedup_count else 0.0,
            "any_4_feature_flags": int(flag_agg["any_4_feature_flags"]),
            "any_4_feature_flags_ratio": float(flag_agg["any_4_feature_flags"] / dedup_count) if dedup_count else 0.0,
        },
    }
    return labeled_df, stats


def stratified_split(
    df: DataFrame,
    *,
    train_ratio: float = 0.70,
    val_ratio: float = 0.15,
    test_ratio: float = 0.15,
    seed: int = 42,
    small_class_threshold: int = 200,
    verify_disjoint: bool = True,
) -> Tuple[DataFrame, DataFrame, DataFrame, Dict[str, Any]]:
    """Deterministic, partition-invariant stratified split by original `label`.

    - Uses `xxhash64(seed, core_features..., label)` so assignment is completely
      independent of Spark partition count or row ordering.
    - Large classes (>= small_class_threshold): zero-shuffle modulo 10000 hash bucketing
      for low memory footprint.
    - Small classes (< small_class_threshold, e.g. Heartbleed, Sql Injection, Infiltration):
      exact per-class rank split guaranteeing non-empty Train/Val/Test strata for classes
      with >= 3 records.
    """
    if abs((train_ratio + val_ratio + test_ratio) - 1.0) > 1e-9:
        raise ValueError("Split ratios must sum to 1.0")

    hash_inputs = [F.lit(int(seed))] + [
        F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in CORE_FEATURE_COLUMNS
    ] + [F.col("label")]
    hashed = df.withColumn("_split_hash", F.xxhash64(*hash_inputs))

    class_counts = {
        r["label"]: int(r["count"]) for r in hashed.groupBy("label").count().collect()
    }
    small_labels = sorted(lbl for lbl, cnt in class_counts.items() if cnt < small_class_threshold)

    train_cut = int(round(train_ratio * 10000))
    val_cut = int(round((train_ratio + val_ratio) * 10000))

    if small_labels:
        large_df = hashed.filter(~F.col("label").isin(*small_labels))
        small_df = hashed.filter(F.col("label").isin(*small_labels))
    else:
        large_df = hashed
        small_df = None

    bucket = F.pmod(F.col("_split_hash"), F.lit(10000))
    large_assigned = large_df.withColumn(
        "_split",
        F.when(bucket < train_cut, F.lit("train"))
        .when(bucket < val_cut, F.lit("val"))
        .otherwise(F.lit("test")),
    )

    if small_df is not None and small_labels:
        win = Window.partitionBy("label").orderBy(F.col("_split_hash").asc())
        count_win = Window.partitionBy("label")
        small_ranked = (
            small_df.withColumn("_rn", F.row_number().over(win))
            .withColumn("_cls_n", F.count("*").over(count_win))
        )
        # Ensure for classes with >= 3 rows: at least 1 row in val and 1 row in test
        n_train_expr = F.when(
            F.col("_cls_n") >= 3,
            F.least(F.col("_cls_n") - 2, F.greatest(F.lit(1), F.round(F.col("_cls_n") * F.lit(train_ratio)).cast("int"))),
        ).otherwise(F.lit(1))
        n_val_expr = F.when(
            F.col("_cls_n") >= 3,
            F.least(
                F.col("_cls_n") - n_train_expr - 1,
                F.greatest(F.lit(1), F.round(F.col("_cls_n") * F.lit(val_ratio)).cast("int")),
            ),
        ).when(F.col("_cls_n") == 2, F.lit(1)).otherwise(F.lit(0))

        small_assigned = (
            small_ranked.withColumn(
                "_split",
                F.when(F.col("_rn") <= n_train_expr, F.lit("train"))
                .when(F.col("_rn") <= (n_train_expr + n_val_expr), F.lit("val"))
                .otherwise(F.lit("test")),
            )
            .drop("_rn", "_cls_n")
        )
        assigned = large_assigned.unionByName(small_assigned)
    else:
        assigned = large_assigned

    train_df = assigned.filter(F.col("_split") == "train").drop("_split", "_split_hash")
    val_df = assigned.filter(F.col("_split") == "val").drop("_split", "_split_hash")
    test_df = assigned.filter(F.col("_split") == "test").drop("_split", "_split_hash")

    # Collect per-split and per-class statistics
    split_rows = (
        assigned.groupBy("_split", "label", "label_binary")
        .count()
        .collect()
    )
    totals = {"train": 0, "val": 0, "test": 0}
    attacks = {"train": 0, "val": 0, "test": 0}
    per_label: Dict[str, Dict[str, int]] = {}
    for r in split_rows:
        sp = r["_split"]
        lbl = r["label"]
        cnt = int(r["count"])
        totals[sp] += cnt
        if float(r["label_binary"]) == 1.0:
            attacks[sp] += cnt
        per_label.setdefault(lbl, {"train": 0, "val": 0, "test": 0})[sp] = cnt

    grand_total = sum(totals.values())
    if grand_total != sum(class_counts.values()):
        raise RuntimeError("Stratified split lost or duplicated records!")

    # Leakage verification: zero shared (features, label) keys across splits
    overlap_counts = {"train_val": 0, "train_test": 0, "val_test": 0}
    if verify_disjoint:
        key_cols = list(CORE_FEATURE_COLUMNS) + ["label"]
        overlap_counts["train_val"] = train_df.select(*key_cols).intersect(val_df.select(*key_cols)).count()
        overlap_counts["train_test"] = train_df.select(*key_cols).intersect(test_df.select(*key_cols)).count()
        overlap_counts["val_test"] = val_df.select(*key_cols).intersect(test_df.select(*key_cols)).count()
        if any(v != 0 for v in overlap_counts.values()):
            raise RuntimeError(f"Data leakage detected between splits: {overlap_counts}")

    split_stats = {
        "seed": seed,
        "target_ratios": {"train": train_ratio, "val": val_ratio, "test": test_ratio},
        "small_class_exact_rank_labels": small_labels,
        "counts": totals,
        "actual_ratios": {
            k: float(v / grand_total) if grand_total else 0.0 for k, v in totals.items()
        },
        "attack_counts": attacks,
        "benign_counts": {k: totals[k] - attacks[k] for k in totals},
        "attack_ratios": {
            k: float(attacks[k] / totals[k]) if totals[k] else 0.0 for k in totals
        },
        "per_label_counts": dict(sorted(per_label.items(), key=lambda kv: -sum(kv[1].values()))),
        "disjoint_overlap_verification": overlap_counts,
    }
    return train_df, val_df, test_df, split_stats


def cast_features_to_double(df: DataFrame) -> DataFrame:
    """Create double-cast feature columns (`f__<name>`) while preserving original columns."""
    out = df
    for col_name in CORE_FEATURE_COLUMNS:
        out = out.withColumn(f"f__{col_name}", F.col(col_name).cast("double"))
    return out

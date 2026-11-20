"""End-to-end training & Quality-Aware evaluation pipeline for CICIDS2017 anomaly detection.

Usage inside spark-master container:
  python3 -m src.modeling.pipeline \
    --cleaned-path s3a://bigdata-network/cleaned/flows/<batch_id> \
    --quality-report-path s3a://bigdata-network/reports/data_quality/<batch_id> \
    --output-dir /tmp/modeling_reports
"""

import argparse
import logging
import platform
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Any, Dict, List, Optional

from pyspark import SparkContext, StorageLevel
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import Imputer, VectorAssembler
from pyspark.sql import Column, DataFrame, functions as F

from src.modeling.config import (
    ALL_FEATURE_QUALITY_FLAGS,
    BENIGN_LABEL,
    CONTRACT_VERSION,
    CORE_FEATURE_COLUMNS,
    EXCLUDED_FROM_BASELINE_FEATURES,
    MODELING_PIPELINE_VERSION,
    NEGATIVE_CLASS_NAME,
    NEGATIVE_LABEL_VALUE,
    NULLABLE_FEATURE_COLUMNS,
    POSITIVE_CLASS_NAME,
    POSITIVE_LABEL_VALUE,
    RATE_QUALITY_FLAGS,
    SCHEMA_VERSION,
    ModelingConfig,
    create_modeling_spark_session,
)
from src.modeling.data_prep import (
    cast_features_to_double,
    deduplicate_and_label,
    load_and_validate_clean_valid,
    stratified_split,
    verify_clean_batch_marker,
)
from src.modeling.metrics import (
    evaluate_baseline_df,
    evaluate_per_attack_class_df,
    evaluate_quality_aware_df,
    select_best_threshold,
)
from src.modeling.report import (
    extract_test_examples,
    render_evaluation_report_md,
    write_examples_csv,
    write_json,
    write_validation_sweep_csv,
)

LOG = logging.getLogger(__name__)


def build_baseline_ml_pipeline(config: ModelingConfig) -> Pipeline:
    """Build PySpark ML Pipeline: Imputer (nullable cols) -> VectorAssembler -> RandomForest.

    Strictly fits ONLY on Train; excludes all quality flags and metadata columns.
    """
    nullable_double_cols = [f"f__{c}" for c in NULLABLE_FEATURE_COLUMNS]
    imputed_cols = [f"imp__{c}" for c in NULLABLE_FEATURE_COLUMNS]

    imputer = Imputer(
        inputCols=nullable_double_cols,
        outputCols=imputed_cols,
        strategy=config.imputer_strategy,
    )

    assembled_inputs = [
        f"imp__{c}" if c in NULLABLE_FEATURE_COLUMNS else f"f__{c}"
        for c in CORE_FEATURE_COLUMNS
    ]
    assembler = VectorAssembler(
        inputCols=assembled_inputs,
        outputCol="features",
        handleInvalid="error",
    )

    rf = RandomForestClassifier(
        featuresCol="features",
        labelCol="label_binary",
        predictionCol="rf_prediction",
        probabilityCol="probability",
        rawPredictionCol="rawPrediction",
        numTrees=config.num_trees,
        maxDepth=config.max_depth,
        maxBins=config.max_bins,
        featureSubsetStrategy=config.feature_subset_strategy,
        impurity=config.impurity,
        subsamplingRate=config.subsampling_rate,
        minInstancesPerNode=config.min_instances_per_node,
        maxMemoryInMB=config.max_memory_in_mb,
        seed=config.seed,
    )

    return Pipeline(stages=[imputer, assembler, rf])


def _extract_surrogate_medians(imputer_model) -> Dict[str, float]:
    rows = imputer_model.surrogateDF.collect()
    if not rows:
        return {}
    rec = rows[0].asDict()
    result = {}
    for c in NULLABLE_FEATURE_COLUMNS:
        key = f"f__{c}"
        if key in rec and rec[key] is not None:
            result[c] = float(rec[key])
    return result


def _extract_feature_importances(rf_model) -> List[Dict[str, Any]]:
    weights = list(rf_model.featureImportances.toArray())
    items = [
        {"feature": name, "importance": float(weight)}
        for name, weight in zip(CORE_FEATURE_COLUMNS, weights)
    ]
    return sorted(items, key=lambda x: -x["importance"])


def _vector_to_array_double(col: Column) -> Column:
    """Invoke Spark's built-in JVM vector_to_array without requiring pandas."""
    sc = SparkContext._active_spark_context
    return Column(sc._jvm.org.apache.spark.ml.functions.vector_to_array(col._jc, "float64"))


def _score_split(pipeline_model, split_df: DataFrame, keep_all_features: bool = False) -> DataFrame:
    """Transform a split with the Train-fitted PipelineModel and extract P(ATTACK | x)."""
    transformed = pipeline_model.transform(cast_features_to_double(split_df))
    scored = transformed.withColumn(
        "p_attack", _vector_to_array_double(F.col("probability"))[1].cast("double")
    )
    base_cols = [
        "label",
        "label_binary",
        "p_attack",
        "source_file",
        "quality_reason_codes",
    ] + list(ALL_FEATURE_QUALITY_FLAGS)
    if keep_all_features:
        cols = list(CORE_FEATURE_COLUMNS) + base_cols
    else:
        cols = base_cols
    return scored.select(*cols)


def run_modeling_pipeline(
    spark,
    config: ModelingConfig,
    output_dir: Path,
    *,
    verify_disjoint: bool = True,
) -> Dict[str, Any]:
    """Execute the full training, Validation threshold selection, and Test evaluation workflow."""
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = perf_counter()
    timings: Dict[str, float] = {}
    storage_lvl = getattr(StorageLevel, config.storage_level)
    cached_frames: List[DataFrame] = []

    def persist(frame: DataFrame) -> DataFrame:
        frame.persist(storage_lvl)
        cached_frames.append(frame)
        return frame

    def mark(stage: str, prev: float) -> float:
        now = perf_counter()
        timings[stage] = round(now - prev, 4)
        LOG.info("stage=%s seconds=%.3f", stage, timings[stage])
        return now

    try:
        tick = perf_counter()
        batch_verification: Dict[str, Any] = {}
        if config.quality_report_path:
            batch_verification = verify_clean_batch_marker(spark, config.quality_report_path)
            LOG.info("Verified clean batch marker: %s", batch_verification.get("batch_id"))

        # 1. Load clean VALID Parquet & verify invariants
        raw_valid_df, load_audit = load_and_validate_clean_valid(spark, config.cleaned_path)
        tick = mark("load_and_validate_valid_parquet", tick)

        # 2. Deduplicate BEFORE splitting & create binary label (persisted once to avoid recompute)
        dedup_df, prep_stats = deduplicate_and_label(raw_valid_df, storage_level=storage_lvl)
        cached_frames.append(dedup_df)
        tick = mark("deduplication_and_labeling", tick)

        # 3. Stratified split 70% Train / 15% Validation / 15% Test (seed=42)
        train_df, val_df, test_df, split_stats = stratified_split(
            dedup_df,
            train_ratio=config.train_ratio,
            val_ratio=config.val_ratio,
            test_ratio=config.test_ratio,
            seed=config.seed,
            small_class_threshold=config.small_class_threshold,
            verify_disjoint=verify_disjoint,
        )
        # Keep ONLY the 21 double features + label_binary during RF fitting to minimize executor RAM
        train_fit_cols = [f"f__{c}" for c in CORE_FEATURE_COLUMNS] + ["label_binary"]
        train_for_fit = persist(cast_features_to_double(train_df).select(*train_fit_cols))
        _ = train_for_fit.count()
        val_df = persist(val_df)
        test_df = persist(test_df)
        _ = val_df.count()
        _ = test_df.count()
        dedup_df.unpersist(blocking=True)
        cached_frames.remove(dedup_df)
        tick = mark("stratified_split_and_leakage_check", tick)

        # 4. Fit Imputer + VectorAssembler + Baseline Random Forest ONLY on Train
        pipeline = build_baseline_ml_pipeline(config)
        pipeline_model = pipeline.fit(train_for_fit)
        train_for_fit.unpersist(blocking=True)
        cached_frames.remove(train_for_fit)
        tick = mark("baseline_rf_training", tick)

        imputer_model = pipeline_model.stages[0]
        rf_model = pipeline_model.stages[2]
        surrogate_medians = _extract_surrogate_medians(imputer_model)
        feature_importances = _extract_feature_importances(rf_model)

        # Build & persist baseline_config.json
        baseline_config = {
            "generated_at_utc": started_at,
            "modeling_pipeline_version": MODELING_PIPELINE_VERSION,
            "data_source": {
                "cleaned_uri": config.cleaned_path,
                "quality_report_uri": config.quality_report_path or None,
                "schema_version": SCHEMA_VERSION,
                "contract_version": CONTRACT_VERSION,
                "batch_verification": batch_verification or None,
                "non_valid_records_included": 0,
                "synthetic_records_included": 0,
            },
            "feature_columns": list(CORE_FEATURE_COLUMNS),
            "feature_count": len(CORE_FEATURE_COLUMNS),
            "nullable_feature_columns": list(NULLABLE_FEATURE_COLUMNS),
            "excluded_quality_and_metadata_columns": list(EXCLUDED_FROM_BASELINE_FEATURES),
            "preprocessing": {
                "fitted_on": "train_only",
                "imputer_strategy": config.imputer_strategy,
                "imputer_input_columns": list(NULLABLE_FEATURE_COLUMNS),
                "imputer_surrogate_values_from_train": surrogate_medians,
                "scaler": None,
                "scaler_note": "Tree-based Random Forest splits are invariant to monotonic feature scaling.",
            },
            "label_config": {
                "original_label_column": "label",
                "binary_label_column": "label_binary",
                "positive_class": f"{POSITIVE_CLASS_NAME} ({POSITIVE_LABEL_VALUE})",
                "negative_class": f"{NEGATIVE_CLASS_NAME} ({NEGATIVE_LABEL_VALUE})",
                "benign_label_exact": BENIGN_LABEL,
                "label_mapping": prep_stats["label_mapping"],
            },
            "split_config": {
                "dedup_before_split": True,
                "dedup_subset": prep_stats["dedup_subset"],
                "stratify_by": "label",
                "ratios": {
                    "train": config.train_ratio,
                    "val": config.val_ratio,
                    "test": config.test_ratio,
                },
                "seed": config.seed,
            },
            "model_type": "PySpark MLlib RandomForestClassifier",
            "seed": config.seed,
            "hyperparameters": {
                "numTrees": config.num_trees,
                "maxDepth": config.max_depth,
                "maxBins": config.max_bins,
                "featureSubsetStrategy": config.feature_subset_strategy,
                "impurity": config.impurity,
                "subsamplingRate": config.subsampling_rate,
                "minInstancesPerNode": config.min_instances_per_node,
                "maxMemoryInMB": config.max_memory_in_mb,
                "seed": config.seed,
                "decision_threshold": config.baseline_threshold,
            },
            "quality_aware_config": {
                "primary_flag_columns": list(RATE_QUALITY_FLAGS),
                "sensitivity_flag_columns": list(ALL_FEATURE_QUALITY_FLAGS),
                "excluded_leaky_reason_codes": ["LABEL_ENCODING_REPLACEMENT"],
                "validation_threshold_grid": [list(pair) for pair in config.threshold_grid],
                "max_review_rate_budget": config.max_review_rate_target,
                "review_cost_weight": config.review_cost_weight,
            },
            "spark_environment": {
                "spark_version": spark.version,
                "python_version": platform.python_version(),
                "spark_master": spark.sparkContext.master,
                "spark_app_id": spark.sparkContext.applicationId,
                "executor_cores": spark.sparkContext.getConf().get("spark.executor.cores", str(config.executor_cores)),
                "executor_memory": spark.sparkContext.getConf().get("spark.executor.memory", config.executor_memory),
                "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
                "storage_level": config.storage_level,
            },
        }
        write_json(output_dir / "baseline_config.json", baseline_config)

        # 5. Score Validation set & tune [tau_low, tau_high] ONLY on Validation
        val_scored = persist(_score_split(pipeline_model, val_df, keep_all_features=False))
        _ = val_scored.count()
        val_df.unpersist(blocking=True)
        cached_frames.remove(val_df)
        tick = mark("validation_inference", tick)

        val_baseline = evaluate_baseline_df(val_scored, threshold=config.baseline_threshold)
        val_sweep_primary = [
            evaluate_quality_aware_df(
                val_scored,
                flag_columns=RATE_QUALITY_FLAGS,
                tau_low=lo,
                tau_high=hi,
                decision_threshold=config.baseline_threshold,
            )
            for lo, hi in config.threshold_grid
        ]
        val_selection = select_best_threshold(
            val_sweep_primary,
            max_review_rate=config.max_review_rate_target,
            review_cost_weight=config.review_cost_weight,
        )
        selected_low = val_selection["selected"]["tau_low"]
        selected_high = val_selection["selected"]["tau_high"]

        # Sensitivity check on Validation with 4 flags at the same selected thresholds
        val_sensitivity_4flags = evaluate_quality_aware_df(
            val_scored,
            flag_columns=ALL_FEATURE_QUALITY_FLAGS,
            tau_low=selected_low,
            tau_high=selected_high,
            decision_threshold=config.baseline_threshold,
        )
        val_scored.unpersist(blocking=True)
        cached_frames.remove(val_scored)
        tick = mark("validation_threshold_selection", tick)

        # 6. Fair evaluation on the EXACT SAME Test set & Baseline RF model
        test_scored = persist(_score_split(pipeline_model, test_df, keep_all_features=True))
        _ = test_scored.count()
        test_df.unpersist(blocking=True)
        cached_frames.remove(test_df)
        tick = mark("test_inference", tick)

        test_baseline = evaluate_baseline_df(test_scored, threshold=config.baseline_threshold)
        test_qa_primary = evaluate_quality_aware_df(
            test_scored,
            flag_columns=RATE_QUALITY_FLAGS,
            tau_low=selected_low,
            tau_high=selected_high,
            decision_threshold=config.baseline_threshold,
        )
        test_qa_sensitivity_4flags = evaluate_quality_aware_df(
            test_scored,
            flag_columns=ALL_FEATURE_QUALITY_FLAGS,
            tau_low=selected_low,
            tau_high=selected_high,
            decision_threshold=config.baseline_threshold,
        )
        test_per_label = evaluate_per_attack_class_df(
            test_scored,
            flag_columns=RATE_QUALITY_FLAGS,
            tau_low=selected_low,
            tau_high=selected_high,
            decision_threshold=config.baseline_threshold,
        )
        tick = mark("test_evaluation", tick)

        # 7. Extract real Test examples (FP, FN, REVIEW)
        examples = extract_test_examples(
            test_scored,
            flag_columns=RATE_QUALITY_FLAGS,
            tau_low=selected_low,
            tau_high=selected_high,
            decision_threshold=config.baseline_threshold,
            n_per_category=config.examples_per_category,
            seed=config.seed,
        )
        test_scored.unpersist(blocking=True)
        cached_frames.remove(test_scored)
        tick = mark("test_example_extraction", tick)

        timings["total_pipeline_seconds"] = round(perf_counter() - t0, 4)

        summary = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "modeling_pipeline_version": MODELING_PIPELINE_VERSION,
            "source_batch_verification": batch_verification,
            "load_audit": load_audit,
            "data_preparation": prep_stats,
            "split_summary": split_stats,
            "feature_importances": feature_importances,
            "validation_baseline": val_baseline,
            "validation_threshold_selection": val_selection,
            "validation_sensitivity_4flags": val_sensitivity_4flags,
            "test_evaluation": {
                "baseline": test_baseline,
                "quality_aware_primary": test_qa_primary,
                "quality_aware_sensitivity_4flags": test_qa_sensitivity_4flags,
                "per_label_breakdown": test_per_label,
            },
            "test_examples_summary": {
                k: len(v) for k, v in examples.items()
            },
            "test_examples": examples,
            "timings_seconds": timings,
        }

        # 8. Export all artifacts
        write_json(output_dir / "evaluation_summary.json", summary)
        write_json(output_dir / "test_examples.json", examples)
        write_examples_csv(output_dir / "test_examples.csv", examples)
        write_validation_sweep_csv(
            output_dir / "validation_threshold_sweep.csv",
            val_selection["candidates"],
        )
        report_md = render_evaluation_report_md(summary, baseline_config, examples)
        (output_dir / "evaluation_report.md").write_text(report_md, encoding="utf-8")

        LOG.info("All modeling artifacts written to %s", output_dir)
        return summary
    finally:
        for frame in reversed(cached_frames):
            try:
                frame.unpersist(blocking=True)
            except Exception:
                LOG.warning("Failed to unpersist cached DataFrame during cleanup")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cleaned-path", required=True, help="URI to clean VALID Parquet (e.g. s3a://bigdata-network/cleaned/flows/<batch>)")
    parser.add_argument("--quality-report-path", default="", help="Optional URI to quality report batch dir to verify _SUCCESS.json")
    parser.add_argument("--output-dir", type=Path, required=True, help="Writable local output directory for modeling reports")
    parser.add_argument("--local", action="store_true", help="Run Spark in local[2] mode without MinIO S3A")
    parser.add_argument("--num-trees", type=int, default=50)
    parser.add_argument("--max-depth", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = ModelingConfig.from_env()
    config = replace(
        config,
        cleaned_path=args.cleaned_path,
        quality_report_path=args.quality_report_path,
        num_trees=args.num_trees,
        max_depth=args.max_depth,
        seed=args.seed,
        shuffle_partitions=args.shuffle_partitions,
    )
    if args.local:
        config = replace(config, spark_master="local[2]", executor_memory="1g")

    spark = create_modeling_spark_session(config, enable_minio=not args.local)
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = run_modeling_pipeline(spark, config, args.output_dir)
        bt = summary["test_evaluation"]["baseline"]
        qa = summary["test_evaluation"]["quality_aware_primary"]
        qa_m = qa["auto_metrics"]
        print(
            "=== MODELING COMPLETE ===\n"
            f"Selected Validation Band: [{qa['tau_low']:.2f}, {qa['tau_high']:.2f}]\n"
            f"Test Baseline : F1={bt['f1']:.4f} | Macro-F1={bt['macro_f1']:.4f} | P={bt['precision']:.4f} | R={bt['recall']:.4f} | FPR={bt['fpr']:.4f}\n"
            f"Test QA (AUTO): F1={qa_m['f1']:.4f} | Macro-F1={qa_m['macro_f1']:.4f} | P={qa_m['precision']:.4f} | R={qa_m['recall']:.4f} | FPR={qa_m['fpr']:.4f}\n"
            f"Test QA Ops   : Coverage={qa['coverage']:.4f} | ReviewRate={qa['review_rate']:.4f} | AttackToReview={qa['attack_to_review_rate']:.4f}\n"
            f"Total Seconds : {summary['timings_seconds']['total_pipeline_seconds']:.2f}s"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

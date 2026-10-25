
"""Batch Storage -> Processing. Default CLI prints a plan without reading/writing data."""

import argparse
import logging
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import functions as F

from processing.clean import clean_classified_flows
from processing.config import ProcessingConfig, create_spark_session, load_contract
from processing.features import add_features, aggregate_flows
from processing.ingestion import read_raw_flows
from processing.output import OUTPUT_FIELDS, output_plan, preflight_outputs, write_text, write_verified_parquet
from processing.quality_report import build_quality_report, json_text, quality_snapshot, summary_csv
from processing.transform import transform_flows
from processing.validation import (post_clean_summary, reconcile_stages, schema_validation,
                                   validate_cleaned_flows, validate_flows)

LOG = logging.getLogger(__name__)


def local_output_config(config, root):
    root = Path(root).resolve().as_uri()
    folders = ("cleaned/flows", "processed/derived_features", "processed/aggregates", "quality/quarantine",
               "quality/rejected", "reports/data_quality", "reports/performance")
    return replace(config, **dict(zip(OUTPUT_FIELDS, [root + "/" + folder for folder in folders])))


def run_pipeline(spark, config, *, write_outputs=False):
    """Fail closed; the final report marker is the ONLY batch completion signal.

    Spark startup and final JSON publication are outside measured ETL duration.
    Data is immutable during a run. Use one writer per unique batch ID.
    """
    started = datetime.now(timezone.utc).isoformat()
    start = perf_counter()
    cached, timings, outputs = [], {}, {}
    paths = output_plan(config)
    contract = load_contract(config.schema_path)
    write_started = False

    def persist(frame):
        frame.persist(getattr(StorageLevel, config.storage_level))
        cached.append(frame)
        return frame

    def elapsed(stage, previous):
        now = perf_counter()
        timings[stage] = now - previous
        LOG.info("stage=%s seconds=%.3f batch=%s", stage, timings[stage], config.batch_id)
        return now

    try:
        if write_outputs:
            preflight_outputs(spark, paths)
            write_text(spark, paths["report_path"] + "/_STARTED.json", json_text({
                "run_status": "RUNNING", "batch_id": config.batch_id, "started_at": started}))
            write_started = True
        tick = perf_counter()
        raw, manifest = read_raw_flows(spark, config.input_path, contract)
        raw = persist(raw)
        source_counts = {r["source_path"]: r["count"] for r in raw.groupBy("source_path").count().collect()}
        input_count = sum(source_counts.values())
        for item in manifest:
            item["input_records"] = source_counts.get(item["source_path"], 0)
        if not input_count:
            raise ValueError("Empty input: no data records")
        tick = elapsed("ingestion_and_checksum", tick)
        typed = transform_flows(raw, config, contract)
        classified = persist(validate_flows(typed, contract))
        before = quality_snapshot(classified, contract)
        raw.unpersist(blocking=True)
        tick = elapsed("transformation_validation_and_before_report", tick)
        result = clean_classified_flows(classified, contract)
        # Cache the shared annotated plan before deriving any output views.
        persist(result.annotated)
        checked = persist(validate_cleaned_flows(result.annotated, contract).select("quality_status", "post_clean_errors"))
        post = post_clean_summary(checked)
        post["schema_validation"] = schema_validation(result.valid, contract, cleaned=True, exact=True)
        if post["status"] != "PASS" or post["schema_validation"]["status"] != "PASS":
            raise ValueError("Post-clean validation failed: " + json_text(post))
        reconciliation = reconcile_stages(classified, result.annotated, input_count)
        after = quality_snapshot(result.annotated, contract, cleaned=True)
        checked.unpersist(blocking=True)
        classified.unpersist(blocking=True)
        tick = elapsed("cleaning_post_validation_and_after_report", tick)
        features = persist(add_features(result.valid))
        aggregates = persist(aggregate_flows(result.annotated).withColumn("batch_id", F.lit(config.batch_id)))
        feature_count, aggregate_count = features.count(), aggregates.count()
        if feature_count != post["valid_records"]:
            raise ValueError("Feature engineering changed the record count")
        tick = elapsed("features_and_aggregation", tick)
        if write_outputs:
            for name, frame, count in (
                ("cleaned_path", result.valid, post["valid_records"]),
                ("features_path", features, feature_count),
                ("aggregates_path", aggregates, aggregate_count),
                ("quarantine_path", result.quarantined, post["quarantined_records"]),
                ("rejected_path", result.rejected, post["rejected_records"]),
            ):
                outputs[name] = write_verified_parquet(frame, paths[name], expected_count=count,
                                                       verify_labels=name != "aggregates_path")
            elapsed("output_write_and_read_back", tick)
        duration = perf_counter() - start
        performance = {
            "processing_started_at": started, "processing_finished_at": datetime.now(timezone.utc).isoformat(),
            "processing_duration_seconds": duration, "stage_duration_seconds": timings,
            "throughput_flows_per_second": input_count / duration,
            "input_records": input_count, "input_bytes": sum(f["input_bytes"] for f in manifest),
            "timing_scope": "CSV/checksum read through validation, reports computation, features, aggregation"
                            + (", writes and Parquet read-back" if write_outputs else "; no output writes")
                            + "; excludes Spark startup and final JSON/CSV publication",
            "spark_version": spark.version, "spark_app_id": spark.sparkContext.applicationId,
            "spark_master": spark.sparkContext.master,
            "shuffle_partitions": int(spark.conf.get("spark.sql.shuffle.partitions")),
            "adaptive_query_execution": spark.conf.get("spark.sql.adaptive.enabled"),
            "cache_batch_size": int(spark.conf.get("spark.sql.inMemoryColumnarStorage.batchSize")),
            "storage_level": config.storage_level,
            "input_partitions": raw.rdd.getNumPartitions(), "feature_partitions": features.rdd.getNumPartitions(),
            "executor_cores": spark.sparkContext.getConf().get("spark.executor.cores", "unknown"),
            "executor_memory": spark.sparkContext.getConf().get("spark.executor.memory", "unknown"),
            "worker_count": None,
            "worker_count_note": "Not inferred from executor count; obtain topology evidence from Spark Master UI",
        }
        report = build_quality_report(config, manifest, before, after, post, reconciliation, performance, outputs)
        report["computed_output_records"] = {"features": feature_count, "aggregates": aggregate_count}
        if write_outputs:
            write_text(spark, paths["performance_path"] + "/performance.json", json_text(performance))
            write_text(spark, paths["report_path"] + "/quality.json", json_text(report))
            write_text(spark, paths["report_path"] + "/summary.csv", summary_csv(report))
            write_text(spark, paths["report_path"] + "/_SUCCESS.json", json_text({
                "run_status": "COMPLETE", "batch_id": config.batch_id,
                "quality_report": paths["report_path"] + "/quality.json",
                "performance_report": paths["performance_path"] + "/performance.json"}))
        else:
            report["run_status"] = "VALIDATED_NO_OUTPUT"
        return report
    except Exception as error:
        LOG.exception("Pipeline failed; batch is incomplete: %s", config.batch_id)
        if write_started:
            try:
                # Exception messages can contain SDK credentials; persist only class.
                write_text(spark, paths["report_path"] + "/_FAILED.json", json_text({
                    "run_status": "FAILED", "batch_id": config.batch_id,
                    "error_type": type(error).__name__, "stage_duration_seconds": timings}))
            except Exception:
                LOG.error("Unable to persist failure marker; no completion marker was issued")
        raise
    finally:
        for frame in reversed(cached):
            try:
                frame.unpersist(blocking=True)
            except Exception:
                LOG.warning("Cache cleanup unavailable; Spark may have stopped")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="CSV URI/glob; otherwise INPUT_PATH")
    parser.add_argument("--local-output", type=Path, help="Use separate local output tree (e.g. /tmp/pipeline-sample)")
    parser.add_argument("--local", action="store_true", help="Use local[2] Spark and no MinIO connector")
    parser.add_argument("--local-report", type=Path, help="Also save measured report to a NEW local JSON file")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--execute", action="store_true", help="Write and verify a NEW batch; review default plan first")
    mode.add_argument("--validate-only", action="store_true", help="Run all computations, without storage writes")
    args = parser.parse_args()
    if args.local_report and args.local_report.exists():
        parser.error("Local report exists; choose a new filename")
    config = ProcessingConfig.from_env()
    if args.input:
        config = replace(config, input_path=args.input)
    if args.local_output:
        config = local_output_config(config, args.local_output)
    if args.local:
        config = replace(config, spark_master="local[2]")
        if config.input_path.startswith("s3a:") or any(getattr(config, n).startswith("s3a:") for n in OUTPUT_FIELDS):
            parser.error("--local requires local input and --local-output")
    paths = output_plan(config)
    if not (args.execute or args.validate_only):
        print(json_text({"mode": "PLAN_ONLY", "batch_id": config.batch_id, "input": config.input_path, "outputs": paths}))
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = create_spark_session(config, enable_minio=not args.local)
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = run_pipeline(spark, config, write_outputs=args.execute)
        if args.local_report:
            args.local_report.parent.mkdir(parents=True, exist_ok=True)
            with args.local_report.open("x", encoding="utf-8") as stream:
                stream.write(json_text(report))
        print(json_text(report))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

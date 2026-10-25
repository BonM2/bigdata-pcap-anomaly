"""Measured benchmark runner; never uploads inputs or changes worker topology."""

import argparse
import json
import os
import platform
from dataclasses import replace
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

from processing.config import ProcessingConfig, create_spark_session
from processing.output import OUTPUT_FIELDS, output_plan


def master_topology(url, expected_workers):
    with urlopen(url.rstrip("/") + "/json/", timeout=10) as response:
        master = json.load(response)
    live = [w for w in master["workers"] if w["state"] == "ALIVE"]
    if len(live) != expected_workers:
        raise ValueError(f"Expected {expected_workers} live workers, observed {len(live)}")
    return [{k: w.get(k) for k in ("id", "host", "cores", "memory", "state")}
            for w in sorted(live, key=lambda worker: worker["id"])]


def trial_order(inputs, partitions, repeats):
    """Alternate the configuration order to reduce monotonic warm-cache bias."""
    combinations = [(source, number) for source in inputs for number in partitions]
    return [(source, number, repeat) for repeat in range(1, repeats + 1)
            for source, number in (combinations if repeat % 2 else list(reversed(combinations)))]


def environment(spark):
    limits = {}
    for name in ("memory.max", "cpu.max"):
        path = Path("/sys/fs/cgroup") / name
        limits[name] = path.read_text().strip() if path.exists() else None
    return {"python": platform.python_version(), "platform": platform.platform(),
            "logical_cpus_visible": os.cpu_count(), "container_limits": limits,
            "spark": spark.version, "java": spark._jvm.java.lang.System.getProperty("java.version"),
            "driver_max_heap_bytes": spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
            "driver_host": platform.node()}


def main(dimension="partitions"):
    from processing.main import local_output_config, run_pipeline
    from processing.quality_report import json_text
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", help="Input URI for partition/worker experiments")
    parser.add_argument("--inputs", nargs="+", help="Distinct input URI/globs for size experiments")
    parser.add_argument("--partitions", nargs="+", type=int, default=[4, 8, 16, 32])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmups", type=int, default=1, help="Unscored first-configuration trials, saved separately")
    parser.add_argument("--experiment-id", default="unspecified", help="Comparison cohort; use the same ID for matched worker runs")
    parser.add_argument("--synthetic-scaling", action="store_true", help="Input consists of replicated synthetic fixture rows")
    parser.add_argument("--adaptive", choices=["true", "false"], default="false", help="Disable AQE by default for controlled shuffle comparisons")
    parser.add_argument("--output-prefix", help="Isolated S3A output prefix for all new benchmark batches")
    parser.add_argument("--dataset-kind", required=True, choices=["synthetic", "real"])
    parser.add_argument("--results", type=Path, required=True, help="NEW local JSON result file")
    parser.add_argument("--local-output", type=Path, help="New local output base, e.g. /tmp/bench-output")
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--execute", action="store_true", help="Actually run/write benchmark batches")
    parser.add_argument("--expected-workers", type=int)
    parser.add_argument("--master-ui", help="Spark Master UI URL; required for worker experiment")
    args = parser.parse_args()
    if args.repeats < 1 or args.warmups < 0 or any(n < 1 for n in args.partitions):
        parser.error("Repeats and partitions must be positive")
    if args.synthetic_scaling and args.dataset_kind != "synthetic":
        parser.error("Synthetic scaling must be labelled synthetic")
    if bool(args.master_ui) != bool(args.expected_workers):
        parser.error("Supply --master-ui and --expected-workers together")
    if args.local_output and args.output_prefix:
        parser.error("Choose only one output destination")
    if dimension == "size" and not args.inputs:
        parser.error("Size experiment requires --inputs")
    if dimension == "workers" and (args.local or not args.master_ui or not args.expected_workers or args.expected_workers < 1):
        parser.error("Worker experiment requires distributed Spark, --master-ui and positive --expected-workers")
    if args.results.exists():
        parser.error("Result file already exists; use a new filename")
    config = ProcessingConfig.from_env()
    if args.input:
        config = replace(config, input_path=args.input)
    elif dimension == "size":
        config = replace(config, input_path=args.inputs[0])
    if args.local_output:
        config = local_output_config(config, args.local_output)
    if args.output_prefix:
        if not args.output_prefix.startswith("s3a://"):
            parser.error("--output-prefix must be an S3A URI")
        config = replace(config, **{name: args.output_prefix.rstrip("/") + "/" + name[:-5]
                                    for name in OUTPUT_FIELDS})
    if args.local:
        config = replace(config, spark_master="local[2]")
        if any(source.startswith("s3a:") for source in (args.inputs or [config.input_path])) or not args.local_output:
            parser.error("--local requires local input and --local-output")
    trials = []
    inputs = args.inputs if dimension == "size" else [config.input_path]
    partitions = args.partitions if dimension == "partitions" else [config.shuffle_partitions]
    order = [(inputs[0], partitions[0], -n) for n in range(1, args.warmups + 1)]
    order += trial_order(inputs, partitions, args.repeats)
    for source, number, repeat in order:
        trial = replace(config, input_path=source, shuffle_partitions=number, batch_id="bench-" + uuid4().hex)
        trials.append((trial, repeat))
    for trial, _ in trials:
        output_plan(trial)
    if not args.execute:
        print(json_text({"mode": "PLAN_ONLY", "dimension": dimension, "dataset_kind": args.dataset_kind,
                         "trials": [{"input": c.input_path, "partitions": c.shuffle_partitions,
                                     "repeat": r, "outputs": output_plan(c)} for c, r in trials]}))
        return
    results = {"dimension": dimension, "dataset_kind": args.dataset_kind, "run_status": "RUNNING", "runs": [],
               "experiment_id": args.experiment_id, "synthetic_scaling": args.synthetic_scaling, "warmup_runs": [],
               "requested_repeats": args.repeats, "requested_warmups": args.warmups,
               "method": "Full pipeline with Parquet read-back; one session per invocation; warmups saved but excluded; "
                         "alternating configuration order per repeat; Spark startup/final report publication excluded; "
                         "Spark caches released each trial; OS/MinIO/JVM caches not flushed"}
    args.results.parent.mkdir(parents=True, exist_ok=True)
    # Reserve before computation, so an existing experiment cannot be overwritten.
    with args.results.open("x", encoding="utf-8") as stream:
        stream.write(json_text(results))
    spark = None
    try:
        spark = create_spark_session(config, enable_minio=not args.local)
        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.adaptive.enabled", args.adaptive)
        results["environment"] = environment(spark)
        for trial, repeat in trials:
            topology = master_topology(args.master_ui, args.expected_workers) if args.master_ui else None
            spark.conf.set("spark.sql.shuffle.partitions", trial.shuffle_partitions)
            report = run_pipeline(spark, trial, write_outputs=True)
            if topology is not None and master_topology(args.master_ui, args.expected_workers) != topology:
                raise ValueError("Worker topology changed during trial")
            result = {"batch_id": trial.batch_id, "repeat": repeat, "input": trial.input_path,
                      "files": report["files"], "performance": report["performance"],
                      "quality": {k: report[k] for k in ("input_records", "valid_records", "quarantined_records", "rejected_records")},
                      "worker_topology": topology, "outputs": report["outputs"],
                      "post_cleaning_status": report["post_cleaning_validation"]["status"],
                      "quality_report_path": output_plan(trial)["report_path"] + "/quality.json"}
            if topology is not None:
                result["performance"]["worker_count"] = len(topology)
                result["performance"]["worker_count_note"] = "Master API topology verified before and after trial"
                result["performance"]["worker_resources"] = sorted((w["cores"], w["memory"]) for w in topology)
                with urlopen(args.master_ui.rstrip("/") + "/json/", timeout=10) as response:
                    master = json.load(response)
                apps = [a for a in master.get("activeapps", []) if a["id"] == spark.sparkContext.applicationId]
                if not apps or apps[0].get("cores") != len(topology) * trial.executor_cores:
                    raise ValueError("Application did not receive the expected worker cores")
                result["application_resources"] = {k: apps[0].get(k) for k in ("id", "cores", "memoryperexecutor", "state")}
            results["warmup_runs" if repeat < 0 else "runs"].append(result)
            args.results.write_text(json_text(results), encoding="utf-8")
            print(json.dumps({"dimension": dimension, "repeat": repeat, "partitions": trial.shuffle_partitions,
                              "records": report["input_records"], "seconds": report["performance"]["processing_duration_seconds"],
                              "batch_id": trial.batch_id}), flush=True)
        results["run_status"] = "COMPLETE"
    except Exception as error:
        results.update(run_status="FAILED", error_type=type(error).__name__)
        raise
    finally:
        args.results.write_text(json_text(results), encoding="utf-8")
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

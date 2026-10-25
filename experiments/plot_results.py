
"""Export measured runs, comparable medians/speedup, and benchmark charts."""

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path


def summarize(paths):
    groups = defaultdict(list)
    for path in paths:
        result = json.loads(Path(path).read_text(encoding="utf-8"))
        if result["run_status"] != "COMPLETE":
            raise ValueError(f"Incomplete benchmark: {path}")
        for run in result["runs"]:
            p = run["performance"]
            signature = tuple(sorted((f["source_sha256"], f["input_bytes"]) for f in run["files"]))
            key = (result["dimension"], result["dataset_kind"], signature, p["input_records"], p["input_bytes"],
                   p["spark_master"], p["worker_count"], p["shuffle_partitions"], p["executor_cores"],
                   p["executor_memory"], p.get("storage_level", "UNRECORDED"),
                   p.get("cache_batch_size"), p.get("adaptive_query_execution"), p["timing_scope"],
                   result.get("experiment_id", "legacy"), result.get("synthetic_scaling", False),
                   json.dumps({k: v for k, v in result.get("environment", {}).items() if k != "driver_host"}, sort_keys=True),
                   tuple(sorted(set(tuple(w) for w in p.get("worker_resources", [])))),
                   run["input"].split(":", 1)[0] if "input" in run else "UNRECORDED",
                   tuple(sorted(set(o["path"].split(":", 1)[0] for o in run.get("outputs", {}).values()))))
            groups[key].append(p)
    rows = []
    for key, values in groups.items():
        durations = [v["processing_duration_seconds"] for v in values]
        rows.append(dict(zip(("dimension", "dataset_kind", "workload_sha256_and_bytes", "input_records", "input_bytes",
                              "spark_master", "worker_count", "shuffle_partitions", "executor_cores", "executor_memory",
                              "storage_level", "cache_batch_size", "adaptive_query_execution", "timing_scope",
                              "experiment_id", "synthetic_scaling", "environment", "per_worker_resources",
                              "input_protocol", "output_protocols"), key),
                         repetitions=len(values), median_seconds=statistics.median(durations),
                         min_seconds=min(durations), max_seconds=max(durations),
                         median_flows_per_second=statistics.median(v["throughput_flows_per_second"] for v in values)))
    for row in rows:
        dimension = row["dimension"]
        axis = {"partitions": "shuffle_partitions", "workers": "worker_count"}.get(dimension)
        row["baseline_value"], row["speedup"] = None, None
        if not axis:
            continue  # Different input sizes are not a fixed-workload speedup.
        comparison_keys = [k for k in ("dimension", "dataset_kind", "workload_sha256_and_bytes", "input_records",
                           "input_bytes", "spark_master", "worker_count", "shuffle_partitions", "executor_cores",
                           "executor_memory", "storage_level", "cache_batch_size", "adaptive_query_execution",
                           "timing_scope", "experiment_id", "synthetic_scaling", "environment", "per_worker_resources",
                           "input_protocol", "output_protocols") if k != axis]
        baseline_value = 1 if dimension == "workers" else 4
        baseline = next((r for r in rows if r[axis] == baseline_value
                         and all(r[k] == row[k] for k in comparison_keys)), None)
        if baseline:
            row["baseline_value"] = baseline_value
            row["speedup"] = baseline["median_seconds"] / row["median_seconds"]
    return rows


def measured_runs(paths):
    rows = []
    for path in paths:
        result = json.loads(Path(path).read_text(encoding="utf-8"))
        if result["run_status"] != "COMPLETE":
            raise ValueError(f"Incomplete benchmark: {path}")
        for run in result["runs"] + result.get("warmup_runs", []):
            p = run["performance"]
            rows.append({"dimension": result["dimension"], "experiment_id": result.get("experiment_id"),
                         "dataset_kind": result["dataset_kind"], "synthetic_scaling": result.get("synthetic_scaling", False),
                         "batch_id": run["batch_id"], "repeat": run["repeat"], "warmup": run["repeat"] < 0,
                         **{k: p.get(k) for k in ("input_records", "input_bytes", "processing_duration_seconds",
                             "throughput_flows_per_second", "worker_count", "shuffle_partitions", "input_partitions",
                             "feature_partitions", "executor_memory", "executor_cores", "storage_level",
                             "adaptive_query_execution", "spark_app_id", "timing_scope")},
                         "quality_report_path": run.get("quality_report_path"),
                         "stage_seconds": json.dumps(p.get("stage_duration_seconds", {}), sort_keys=True)})
    return rows


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def chart(rows, path):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    dimensions = [d for d in ("size", "workers", "partitions") if any(r["dimension"] == d for r in rows)]
    fig, axes = plt.subplots(3, len(dimensions), figsize=(5 * len(dimensions), 10), squeeze=False)
    for column, dimension in enumerate(dimensions):
        values = [r for r in rows if r["dimension"] == dimension]
        axis = {"size": "input_records", "workers": "worker_count", "partitions": "shuffle_partitions"}[dimension]
        values.sort(key=lambda r: r[axis] or 0)
        labels = [(f"{r[axis]:,}\n{r['input_bytes']/1e6:.2f} MB" if dimension == "size" else str(r[axis])) for r in values]
        for i, metric in enumerate(("median_seconds", "median_flows_per_second", "speedup")):
            ax = axes[i, column]
            if metric == "speedup" and all(r[metric] is None for r in values):
                ax.text(.5, .5, "Speedup not comparable\n(different workloads / no matched baseline)",
                        ha="center", va="center", transform=ax.transAxes)
            else:
                heights = [r[metric] or 0 for r in values]
                bars = ax.bar(range(len(values)), heights, color="#26749b")
                tops = [r["max_seconds"] for r in values] if metric == "median_seconds" else heights
                for bar, value, top in zip(bars, heights, tops):
                    ax.annotate(f"{value:.2f}", (bar.get_x() + bar.get_width()/2, top),
                                xytext=(0, 4), textcoords="offset points", ha="center", va="bottom", fontsize=8)
                ax.set_ylim(0, max(tops) * 1.25 if max(tops) else 1)
                if metric == "median_seconds":
                    ax.errorbar(range(len(values)), heights,
                                yerr=[[r[metric]-r["min_seconds"] for r in values],
                                      [r["max_seconds"]-r[metric] for r in values]], fmt="none", ecolor="black", capsize=3)
            ax.set_xticks(range(len(labels)))
            ax.set_xticklabels(labels, fontsize=8)
            ax.set_title(dimension.title() + " / " + {"median_seconds": "seconds (min-max)",
                         "median_flows_per_second": "records/s", "speedup": "speedup"}[metric])
            ax.set_xlabel(axis)
    kind = "Synthetic scaling" if all(r["synthetic_scaling"] for r in rows) else "Measured workloads"
    fig.suptitle("Measured Storage -> Processing benchmark\n" + kind + "; medians exclude saved warmup trials", fontsize=13)
    fig.tight_layout(rect=(0, 0, 1, .95))
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=160)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inputs", nargs="+", type=Path)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--png", type=Path)
    parser.add_argument("--runs-csv", type=Path, help="Optional all-trials CSV including marked warmups")
    args = parser.parse_args()
    rows = summarize(args.inputs)
    if not rows:
        raise ValueError("No measured runs")
    for path in (args.csv, args.png, args.runs_csv):
        if path and path.exists():
            raise FileExistsError(path)
    write_csv(args.csv, rows)
    if args.runs_csv:
        write_csv(args.runs_csv, measured_runs(args.inputs))
    if args.png:
        chart(rows, args.png)


if __name__ == "__main__":
    main()

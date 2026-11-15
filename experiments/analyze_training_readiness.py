"""Measure CICIDS2017 quality and training-row retention with chunked pandas."""

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


RATES = {"flow_bytes_s", "flow_packets_s"}
WINDOWS = {"init_win_bytes_forward", "init_win_bytes_backward"}
TCP_FLAGS = {"fin_flag_count", "syn_flag_count", "rst_flag_count", "psh_flag_count", "ack_flag_count"}


def load_core_fields(contract_path):
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    return sorted(
        (item for item in contract["fields"] if item["in_cleaned_core"]),
        key=lambda item: item["source_index"],
    )


def numeric_values(raw):
    token = raw.astype("string").fillna("").str.strip()
    value = pd.to_numeric(token, errors="coerce")
    special_nan = token.str.casefold().isin({"nan", "+nan", "-nan"})
    missing = token.eq("")
    cast_failure = ~missing & value.isna() & ~special_nan
    return token, value, missing, cast_failure, special_nan


def analyze_paths(paths, fields, chunk_size):
    indices = [item["source_index"] for item in fields]
    names = [item["name"] for item in fields]
    totals = Counter()
    issue_cells = Counter()
    cleaned_null_cells = Counter()
    per_file = []

    for path in paths:
        file_counts = Counter()
        for chunk in pd.read_csv(
            path,
            usecols=indices,
            dtype=str,
            keep_default_na=False,
            chunksize=chunk_size,
            low_memory=False,
        ):
            chunk.columns = names
            size = len(chunk)
            hard = np.zeros(size, dtype=bool)
            soft = np.zeros(size, dtype=bool)
            cleaned_null = np.zeros(size, dtype=bool)

            for item in fields:
                name = item["name"]
                if name == "label":
                    label = chunk[name].astype("string").fillna("").str.strip()
                    invalid = label.eq("")
                    encoding = label.str.contains("\ufffd", regex=False, na=False)
                    hard |= invalid.to_numpy()
                    soft |= encoding.to_numpy()
                    issue_cells["invalid_label"] += int(invalid.sum())
                    issue_cells["label_encoding_replacement"] += int(encoding.sum())
                    continue

                token, value, missing, cast_failure, nan = numeric_values(chunk[name])
                infinity = np.isinf(value.to_numpy(dtype=float, na_value=np.nan))
                negative = value.lt(0).fillna(False).to_numpy()
                base_invalid = missing.to_numpy() | cast_failure.to_numpy()

                if name in RATES:
                    invalid = base_invalid | nan.to_numpy() | infinity | negative
                    soft |= invalid
                    cleaned_null |= invalid
                    issue_cells[f"{name}_invalid"] += int(invalid.sum())
                    cleaned_null_cells[name] += int(invalid.sum())
                    continue

                hard |= base_invalid
                issue_cells[f"{name}_missing_or_cast_failure"] += int(base_invalid.sum())
                if item["cleaned_target_type"] == "double":
                    non_finite = nan.to_numpy() | infinity
                    hard |= non_finite
                    issue_cells[f"{name}_non_finite"] += int(non_finite.sum())

                if name in WINDOWS:
                    sentinel = value.eq(-1).fillna(False).to_numpy()
                    negative_other = negative & ~sentinel
                    soft |= sentinel
                    cleaned_null |= sentinel
                    hard |= negative_other
                    issue_cells[f"{name}_sentinel"] += int(sentinel.sum())
                    cleaned_null_cells[name] += int(sentinel.sum())
                    issue_cells[f"{name}_negative_other"] += int(negative_other.sum())
                else:
                    hard |= negative
                    issue_cells[f"{name}_negative"] += int(negative.sum())

                if name == "destination_port":
                    out_of_range = ((value < 0) | (value > 65535)).fillna(False).to_numpy()
                    hard |= out_of_range
                    issue_cells["destination_port_out_of_range"] += int(out_of_range.sum())
                if name in TCP_FLAGS:
                    invalid_flag = (~value.isin([0, 1]) & value.notna()).to_numpy()
                    hard |= invalid_flag
                    issue_cells[f"{name}_invalid_flag"] += int(invalid_flag.sum())

            valid = ~hard
            file_counts["input_records"] += size
            file_counts["valid_records"] += int(valid.sum())
            file_counts["quarantined_records"] += int(hard.sum())
            file_counts["rejected_records"] += 0  # Structural audit/parser failure is fail-closed.
            file_counts["valid_with_soft_issues"] += int((valid & soft).sum())
            file_counts["valid_with_cleaned_null"] += int((valid & cleaned_null).sum())
            file_counts["complete_case_valid_records"] += int((valid & ~cleaned_null).sum())

        totals.update(file_counts)
        per_file.append({"source_file": path.name, **dict(file_counts)})

    return dict(totals), dict(issue_cells), dict(cleaned_null_cells), per_file


def render_chart(path, totals, issue_cells):
    input_records = totals["input_records"]
    valid = totals["valid_records"]
    quarantined = totals["quarantined_records"]
    rejected = totals["rejected_records"]
    complete = totals["complete_case_valid_records"]
    soft = totals["valid_with_soft_issues"]

    fig, axes = plt.subplots(1, 3, figsize=(19, 7), constrained_layout=True)

    axes[0].axis("off")
    boxes = [
        (0.5, 0.84, "INPUT", input_records, "#2f80ed"),
        (0.5, 0.49, "VALID", valid, "#27ae60"),
        (0.20, 0.19, "QUARANTINED", quarantined, "#f2994a"),
        (0.80, 0.19, "REJECTED", rejected, "#eb5757"),
    ]
    for x, y, label, count, color in boxes:
        axes[0].text(x, y, f"{label}\n{count:,}", ha="center", va="center", fontsize=13,
                     bbox={"boxstyle": "round,pad=0.7", "facecolor": color, "edgecolor": "none", "alpha": 0.9},
                     color="white", fontweight="bold")
    axes[0].annotate("", xy=(0.5, 0.59), xytext=(0.5, 0.76), arrowprops={"arrowstyle": "->", "lw": 1.8})
    axes[0].annotate("", xy=(0.24, 0.29), xytext=(0.44, 0.76), arrowprops={"arrowstyle": "->", "lw": 1.5})
    axes[0].annotate("", xy=(0.76, 0.29), xytext=(0.56, 0.76), arrowprops={"arrowstyle": "->", "lw": 1.5})
    axes[0].set_title("Record reconciliation", fontsize=14, fontweight="bold")

    visible_issues = {
        "Init window backward (-1)": issue_cells.get("init_win_bytes_backward_sentinel", 0),
        "Init window forward (-1)": issue_cells.get("init_win_bytes_forward_sentinel", 0),
        "Flow packets/s invalid": issue_cells.get("flow_packets_s_invalid", 0),
        "Flow bytes/s invalid": issue_cells.get("flow_bytes_s_invalid", 0),
        "Label encoding": issue_cells.get("label_encoding_replacement", 0),
        "Hard-invalid rows": quarantined,
    }
    visible_issues = [(name, count) for name, count in visible_issues.items() if count]
    visible_issues.sort(key=lambda item: item[1])
    names, values = zip(*visible_issues)
    bars = axes[1].barh(names, values, color="#9b51e0")
    axes[1].set_xscale("log")
    axes[1].set_xlabel("Affected cells/rows (log scale)")
    axes[1].set_title("Observed quality issues", fontsize=14, fontweight="bold")
    axes[1].grid(axis="x", linestyle="--", alpha=0.3)
    for bar, value in zip(bars, values):
        axes[1].text(value * 1.08, bar.get_y() + bar.get_height() / 2, f"{value:,}", va="center")
    axes[1].set_xlim(left=max(1, min(values) / 2), right=max(values) * 3)

    scenarios = ["Raw input", "Policy-aware\n(impute NULL)", "Naive dropna\n(all 21 features)"]
    retained = [input_records, valid, complete]
    percentages = [value * 100 / input_records for value in retained]
    bars = axes[2].bar(scenarios, percentages, color=["#2f80ed", "#27ae60", "#eb5757"])
    axes[2].set_ylim(0, 108)
    axes[2].set_ylabel("Rows retained (%)")
    axes[2].set_title("Training retention scenarios", fontsize=14, fontweight="bold")
    axes[2].grid(axis="y", linestyle="--", alpha=0.3)
    for bar, value, percentage in zip(bars, retained, percentages):
        lost = input_records - value
        axes[2].text(bar.get_x() + bar.get_width() / 2, percentage + 1,
                     f"{percentage:.4f}%\n{value:,}\nloss: {lost:,}", ha="center", va="bottom")
    axes[2].text(0.5, 0.04, f"VALID rows with soft issues: {soft:,}", transform=axes[2].transAxes,
                 ha="center", color="#555555")

    fig.suptitle("CICIDS2017 data quality and training readiness", fontsize=17, fontweight="bold")
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def write_csv_report(path, totals, cleaned_null_cells, per_file):
    rows = [{"scope": "batch", "name": key, "count": value} for key, value in totals.items()]
    rows += [{"scope": "cleaned_null_cells", "name": key, "count": value}
             for key, value in cleaned_null_cells.items()]
    for item in per_file:
        rows.extend({"scope": item["source_file"], "name": key, "count": value}
                    for key, value in item.items() if key != "source_file")
    with path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=("scope", "name", "count"))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw/cicids2017"))
    parser.add_argument("--contract", type=Path, default=Path("data/schemas/raw_flow_schema.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("docs/analysis"))
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--expected-files", type=int, default=8)
    parser.add_argument("--expected-records", type=int, default=2_830_743)
    args = parser.parse_args()

    paths = sorted(args.input_dir.glob("*.csv"))
    if len(paths) != args.expected_files:
        raise ValueError(f"Expected {args.expected_files} CSV files, found {len(paths)}")
    fields = load_core_fields(args.contract)
    totals, issue_cells, cleaned_null_cells, per_file = analyze_paths(paths, fields, args.chunk_size)
    if totals["input_records"] != args.expected_records:
        raise ValueError(f"Expected {args.expected_records} records, found {totals['input_records']}")
    if totals["input_records"] != (totals["valid_records"] + totals["quarantined_records"]
                                    + totals["rejected_records"]):
        raise ValueError("Record reconciliation failed")
    if totals["valid_records"] != (totals["valid_with_cleaned_null"]
                                    + totals["complete_case_valid_records"]):
        raise ValueError("Training-retention reconciliation failed")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = "cicids2017-data-quality-training-readiness"
    csv_path, json_path, png_path = (args.output_dir / f"{stem}.{suffix}" for suffix in ("csv", "json", "png"))
    write_csv_report(csv_path, totals, cleaned_null_cells, per_file)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "analysis_kind": "data-quality-and-training-readiness",
        "training_note": "No model is trained; retention scenarios quantify preprocessing impact.",
        "totals": totals,
        "issue_cell_counts": issue_cells,
        "cleaned_null_cells_by_field": cleaned_null_cells,
        "training_scenarios": {
            "policy_aware_with_imputation": {
                "records": totals["valid_records"],
                "lost_from_input": totals["input_records"] - totals["valid_records"],
            },
            "naive_dropna_all_features": {
                "records": totals["complete_case_valid_records"],
                "lost_from_input": totals["input_records"] - totals["complete_case_valid_records"],
            },
        },
        "per_file": per_file,
    }
    json_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    render_chart(png_path, totals, issue_cells)
    print(json.dumps({"totals": totals, "training_scenarios": report["training_scenarios"],
                      "csv": str(csv_path), "json": str(json_path), "chart": str(png_path)},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

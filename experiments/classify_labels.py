"""Group CICIDS2017 ground-truth labels and render an auditable Matplotlib summary."""

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


FAMILY_ORDER = (
    "BENIGN",
    "DOS",
    "PORT_SCAN",
    "DDOS",
    "BRUTE_FORCE",
    "WEB_ATTACK",
    "BOTNET",
    "INFILTRATION",
    "HEARTBLEED",
    "UNKNOWN",
)


def classify_label(label):
    """Map a published CICIDS2017 label to a documented attack family."""
    value = str(label).strip()
    folded = value.casefold()
    if folded == "benign":
        return "BENIGN"
    if folded.startswith("dos "):
        return "DOS"
    if folded == "ddos":
        return "DDOS"
    if folded == "portscan":
        return "PORT_SCAN"
    if folded in {"ftp-patator", "ssh-patator"}:
        return "BRUTE_FORCE"
    if folded.startswith("web attack"):
        return "WEB_ATTACK"
    if folded == "bot":
        return "BOTNET"
    if folded == "infiltration":
        return "INFILTRATION"
    if folded == "heartbleed":
        return "HEARTBLEED"
    return "UNKNOWN"


def read_label_counts(paths, chunk_size=250_000):
    """Count labels without loading the 884 MB source dataset into memory."""
    counts = Counter()
    for path in paths:
        found = False
        for chunk in pd.read_csv(
            path,
            usecols=lambda name: name.strip() == "Label",
            chunksize=chunk_size,
            low_memory=False,
        ):
            found = True
            labels = chunk.iloc[:, 0].astype("string").fillna("<MISSING>").str.strip()
            counts.update({str(label): int(count) for label, count in labels.value_counts().items()})
        if not found:
            raise ValueError(f"No data rows found in {path}")
    return counts


def group_counts(label_counts):
    grouped = Counter()
    for label, count in label_counts.items():
        grouped[classify_label(label)] += count
    return {family: int(grouped.get(family, 0)) for family in FAMILY_ORDER}


def write_csv_report(path, label_counts, family_counts):
    total = sum(label_counts.values())
    with path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=("raw_label", "family", "count", "percentage"))
        writer.writeheader()
        for label, count in sorted(label_counts.items(), key=lambda item: (-item[1], item[0])):
            writer.writerow({
                "raw_label": label,
                "family": classify_label(label),
                "count": count,
                "percentage": round(count * 100 / total, 6),
            })
        writer.writerow({})
        for family in FAMILY_ORDER:
            count = family_counts[family]
            writer.writerow({
                "raw_label": "__FAMILY_TOTAL__",
                "family": family,
                "count": count,
                "percentage": round(count * 100 / total, 6),
            })


def render_chart(path, family_counts, total):
    benign = family_counts["BENIGN"]
    attack = total - benign
    attack_families = [(name, count) for name, count in family_counts.items()
                       if name not in {"BENIGN", "UNKNOWN"} and count]
    attack_families.sort(key=lambda item: item[1])

    fig, axes = plt.subplots(1, 2, figsize=(14, 7), constrained_layout=True)
    colors = ["#2f80ed", "#eb5757"]
    axes[0].pie(
        [benign, attack],
        labels=["BENIGN", "ATTACK"],
        autopct=lambda pct: f"{pct:.1f}%\n({round(pct * total / 100):,})",
        startangle=90,
        colors=colors,
        wedgeprops={"width": 0.42, "edgecolor": "white"},
        textprops={"fontsize": 11},
    )
    axes[0].set_title("Binary ground-truth grouping")

    names = [name for name, _ in attack_families]
    values = [count for _, count in attack_families]
    bars = axes[1].barh(names, values, color="#f2994a")
    axes[1].set_xscale("log")
    axes[1].set_xlabel("Flow count (log scale)")
    axes[1].set_title("Attack families")
    axes[1].grid(axis="x", linestyle="--", alpha=0.35)
    for bar, value in zip(bars, values):
        axes[1].text(value * 1.08, bar.get_y() + bar.get_height() / 2, f"{value:,}", va="center")
    if values:
        axes[1].set_xlim(left=max(1, min(values) / 2), right=max(values) * 2.7)

    fig.suptitle("CICIDS2017 label classification", fontsize=16, fontweight="bold")
    fig.text(0.5, 0.01,
             f"Total: {total:,} flows | Source labels grouped into families; not model predictions",
             ha="center", fontsize=10, color="#555555")
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, default=Path("data/raw/cicids2017"))
    parser.add_argument("--output-dir", type=Path, default=Path("docs/analysis"))
    parser.add_argument("--chunk-size", type=int, default=250_000)
    parser.add_argument("--expected-files", type=int, default=8)
    args = parser.parse_args()

    paths = sorted(args.input_dir.glob("*.csv"))
    if len(paths) != args.expected_files:
        raise ValueError(f"Expected {args.expected_files} CSV files, found {len(paths)} in {args.input_dir}")
    if args.chunk_size <= 0:
        raise ValueError("--chunk-size must be positive")

    label_counts = read_label_counts(paths, args.chunk_size)
    family_counts = group_counts(label_counts)
    total = sum(label_counts.values())
    if family_counts["UNKNOWN"]:
        raise ValueError(f"Unmapped labels detected: {family_counts['UNKNOWN']} records")

    args.output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.output_dir / "cicids2017-label-classification.csv"
    json_path = args.output_dir / "cicids2017-label-classification.json"
    png_path = args.output_dir / "cicids2017-label-classification.png"
    write_csv_report(csv_path, label_counts, family_counts)
    json_path.write_text(json.dumps({
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "classification_kind": "ground-truth-label-family-grouping",
        "is_model_prediction": False,
        "source_files": [path.name for path in paths],
        "input_records": total,
        "raw_label_counts": dict(sorted(label_counts.items())),
        "family_counts": family_counts,
        "binary_counts": {"BENIGN": family_counts["BENIGN"], "ATTACK": total - family_counts["BENIGN"]},
    }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    render_chart(png_path, family_counts, total)

    print(json.dumps({
        "input_records": total,
        "family_counts": family_counts,
        "csv": str(csv_path),
        "json": str(json_path),
        "chart": str(png_path),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

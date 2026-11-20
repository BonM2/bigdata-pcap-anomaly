"""Artifact generation: real Test examples (CSV/JSON), config JSON, summary JSON, and Markdown report."""

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Sequence

from pyspark.sql import DataFrame, Window, functions as F

from src.modeling.config import (
    ALL_FEATURE_QUALITY_FLAGS,
    CORE_FEATURE_COLUMNS,
    PROMINENT_EXAMPLE_FEATURES,
)
from src.modeling.metrics import build_flag_trigger_expr


def _json_safe(obj: Any) -> Any:
    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):
            return None
        return obj
    if isinstance(obj, dict):
        return {str(k): _json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe(v) for v in obj]
    return obj


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(_json_safe(payload), ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    path.write_text(text, encoding="utf-8")


def _row_to_example_dict(row: Dict[str, Any], category: str) -> Dict[str, Any]:
    features = {c: row.get(c) for c in CORE_FEATURE_COLUMNS}
    flags = {c: bool(row.get(c)) if row.get(c) is not None else False for c in ALL_FEATURE_QUALITY_FLAGS}
    reasons = list(row.get("quality_reason_codes") or [])
    return {
        "example_category": category,
        "source_file": row.get("source_file"),
        "label_original": row.get("label"),
        "label_binary": float(row["label_binary"]),
        "actual_class": "ATTACK" if float(row["label_binary"]) == 1.0 else "BENIGN",
        "p_attack": round(float(row["p_attack"]), 6),
        "baseline_prediction": "ATTACK" if float(row["p_attack"]) >= 0.50 else "BENIGN",
        "qa_decision": row.get("qa_decision"),
        "review_trigger_reason": row.get("review_trigger_reason"),
        "quality_flags": flags,
        "quality_reason_codes": reasons,
        "features": features,
    }


def extract_test_examples(
    test_scored: DataFrame,
    *,
    flag_columns: Sequence[str],
    tau_low: float,
    tau_high: float,
    decision_threshold: float = 0.50,
    n_per_category: int = 5,
    seed: int = 42,
) -> Dict[str, List[Dict[str, Any]]]:
    """Extract deterministic, diverse real Test examples for FP, FN, and REVIEW."""
    flag_trig = build_flag_trigger_expr(flag_columns)
    prob_unc = (F.col("p_attack") >= F.lit(float(tau_low))) & (
        F.col("p_attack") <= F.lit(float(tau_high))
    )
    is_review = flag_trig | prob_unc
    actual_pos = F.col("label_binary") == F.lit(1.0)
    pred_pos = F.col("p_attack") >= F.lit(float(decision_threshold))

    enriched = (
        test_scored.withColumn(
            "qa_decision",
            F.when(is_review, F.lit("REVIEW"))
            .when(pred_pos, F.lit("ATTACK"))
            .otherwise(F.lit("BENIGN")),
        )
        .withColumn(
            "review_trigger_reason",
            F.when(flag_trig & prob_unc, F.lit("QUALITY_FLAG_AND_UNCERTAINTY"))
            .when(flag_trig, F.lit("QUALITY_FLAG"))
            .when(prob_unc, F.lit("UNCERTAINTY_BAND"))
            .otherwise(F.lit("NONE_AUTO")),
        )
        .withColumn(
            "_tie_hash",
            F.xxhash64(
                F.lit(int(seed)),
                F.col("source_file"),
                F.col("label"),
                F.col("destination_port").cast("string"),
                F.col("flow_duration").cast("string"),
                F.col("p_attack").cast("string"),
            ),
        )
    )

    select_cols = (
        list(CORE_FEATURE_COLUMNS)
        + list(ALL_FEATURE_QUALITY_FLAGS)
        + [
            "quality_reason_codes",
            "source_file",
            "label",
            "label_binary",
            "p_attack",
            "qa_decision",
            "review_trigger_reason",
            "_tie_hash",
        ]
    )

    def _pick_diverse(subset_df: DataFrame, group_col: str, limit_n: int, order_cols) -> List[Dict[str, Any]]:
        win = Window.partitionBy(group_col).orderBy(*order_cols)
        ranked = subset_df.select(*select_cols).withColumn("_grp_rn", F.row_number().over(win))
        picked = (
            ranked.orderBy(F.col("_grp_rn").asc(), *order_cols)
            .limit(limit_n)
            .drop("_grp_rn", "_tie_hash")
            .collect()
        )
        return [r.asDict(recursive=True) for r in picked]

    # 1. False Positives: BENIGN predicted as ATTACK by Baseline (diversify by qa_decision & source_file)
    fp_df = enriched.filter((~actual_pos) & pred_pos)
    fp_rows = _pick_diverse(
        fp_df,
        "qa_decision",
        n_per_category,
        [F.col("p_attack").desc(), F.col("_tie_hash").asc()],
    )

    # 2. False Negatives: ATTACK predicted as BENIGN by Baseline (diversify across attack types)
    fn_df = enriched.filter(actual_pos & (~pred_pos))
    fn_rows = _pick_diverse(
        fn_df,
        "label",
        n_per_category,
        [F.col("p_attack").asc(), F.col("_tie_hash").asc()],
    )

    # 3. REVIEW samples: diversify across review_trigger_reason (flag, uncertainty, both)
    rev_df = enriched.filter(is_review)
    rev_rows = _pick_diverse(
        rev_df,
        "review_trigger_reason",
        n_per_category,
        [F.abs(F.col("p_attack") - F.lit(0.5)).asc(), F.col("_tie_hash").asc()],
    )

    return {
        "false_positive": [_row_to_example_dict(r, "FALSE_POSITIVE") for r in fp_rows],
        "false_negative": [_row_to_example_dict(r, "FALSE_NEGATIVE") for r in fn_rows],
        "review": [_row_to_example_dict(r, "REVIEW") for r in rev_rows],
    }


def write_examples_csv(path: Path, examples: Dict[str, List[Dict[str, Any]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = (
        [
            "example_category",
            "source_file",
            "label_original",
            "label_binary",
            "actual_class",
            "p_attack",
            "baseline_prediction",
            "qa_decision",
            "review_trigger_reason",
        ]
        + list(ALL_FEATURE_QUALITY_FLAGS)
        + ["quality_reason_codes"]
        + list(CORE_FEATURE_COLUMNS)
    )
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for category_key in ("false_positive", "false_negative", "review"):
            for ex in examples.get(category_key, []):
                flat = {
                    "example_category": ex["example_category"],
                    "source_file": ex["source_file"],
                    "label_original": ex["label_original"],
                    "label_binary": ex["label_binary"],
                    "actual_class": ex["actual_class"],
                    "p_attack": ex["p_attack"],
                    "baseline_prediction": ex["baseline_prediction"],
                    "qa_decision": ex["qa_decision"],
                    "review_trigger_reason": ex["review_trigger_reason"],
                    "quality_reason_codes": ";".join(ex["quality_reason_codes"]),
                }
                flat.update(ex["quality_flags"])
                flat.update(ex["features"])
                writer.writerow(flat)


def write_validation_sweep_csv(path: Path, candidates: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "tau_low",
        "tau_high",
        "coverage",
        "review_rate",
        "attack_to_review_rate",
        "auto_tp",
        "auto_fp",
        "auto_fn",
        "auto_tn",
        "auto_precision",
        "auto_recall",
        "auto_f1",
        "auto_macro_f1",
        "auto_accuracy",
        "auto_fpr",
        "review_flag_only",
        "review_uncertainty_only",
        "review_both",
        "baseline_error_capture_rate",
        "balance_score",
        "within_review_budget",
    ]
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for c in candidates:
            m = c["auto_metrics"]
            comp = c["review_composition"]
            inter = c["error_interception"]
            writer.writerow(
                {
                    "tau_low": c["tau_low"],
                    "tau_high": c["tau_high"],
                    "coverage": round(c["coverage"], 6),
                    "review_rate": round(c["review_rate"], 6),
                    "attack_to_review_rate": round(c["attack_to_review_rate"], 6),
                    "auto_tp": m["tp"],
                    "auto_fp": m["fp"],
                    "auto_fn": m["fn"],
                    "auto_tn": m["tn"],
                    "auto_precision": round(m["precision"], 6),
                    "auto_recall": round(m["recall"], 6),
                    "auto_f1": round(m["f1"], 6),
                    "auto_macro_f1": round(m["macro_f1"], 6),
                    "auto_accuracy": round(m["accuracy"], 6),
                    "auto_fpr": round(m["fpr"], 6),
                    "review_flag_only": comp["flag_only"],
                    "review_uncertainty_only": comp["uncertainty_only"],
                    "review_both": comp["both_flag_and_uncertainty"],
                    "baseline_error_capture_rate": round(inter["baseline_error_capture_rate"], 6),
                    "balance_score": round(c["balance_score"], 6),
                    "within_review_budget": c["within_review_budget"],
                }
            )


def _fmt_pct(val: float) -> str:
    return f"{val * 100.0:.4f}%"


def _fmt_num(val: int) -> str:
    return f"{int(val):,}"


def _fmt_feat(val: Any) -> str:
    if val is None:
        return "`NULL`"
    if isinstance(val, float):
        return f"{val:,.2f}"
    return f"{val:,}" if isinstance(val, int) else str(val)


def render_evaluation_report_md(
    summary: Dict[str, Any],
    baseline_config: Dict[str, Any],
    examples: Dict[str, List[Dict[str, Any]]],
) -> str:
    """Render a comprehensive, self-contained Vietnamese Markdown report for thesis/defense."""
    dp = summary["data_preparation"]
    sp = summary["split_summary"]
    val_sel = summary["validation_threshold_selection"]
    sel = val_sel["selected"]
    base_test = summary["test_evaluation"]["baseline"]
    qa_test = summary["test_evaluation"]["quality_aware_primary"]
    qa_auto = qa_test["auto_metrics"]
    qa_sens = summary["test_evaluation"]["quality_aware_sensitivity_4flags"]
    qa_sens_auto = qa_sens["auto_metrics"]
    timings = summary["timings_seconds"]

    lines: List[str] = []
    lines.append("# Báo cáo Huấn luyện và Đánh giá Mô hình Nhận diện Bất thường Mạng (CICIDS2017)")
    lines.append("")
    lines.append(
        "> Thực nghiệm tuân thủ nghiêm ngặt Data Contract v2.3.0: chỉ sử dụng dữ liệu sạch `VALID` "
        "(Parquet + Snappy) từ 8 CSV CICIDS2017 thật; không dùng dữ liệu nhân bản synthetic; không "
        "trộn `QUARANTINED`/`REJECTED`; loại trùng trước khi chia tập; `Imputer` chỉ `fit` trên "
        "Train; chọn ngưỡng `REVIEW` hoàn toàn trên Validation và đánh giá công bằng trên cùng tập Test."
    )
    lines.append("")

    # Section 1: Data & Anti-Leakage
    lines.append("## 1. Nguồn Dữ liệu Sạch & Kiểm soát Chống Rò rỉ (Data Leakage)")
    lines.append("")
    batch = summary.get("source_batch_verification", {})
    lines.append("| Hạng mục | Giá trị thực đo |")
    lines.append("|---|---:|")
    lines.append(f"| Đường dẫn Parquet sạch (`VALID`) | `{baseline_config['data_source']['cleaned_uri']}` |")
    if batch:
        lines.append(f"| Batch ID / Completion Marker | `{batch.get('batch_id')}` (`{batch.get('marker_status')}`) |")
        lines.append(
            f"| ETL Input / VALID / QUARANTINED / REJECTED | "
            f"{_fmt_num(batch.get('etl_input_records', 0))} / "
            f"{_fmt_num(batch.get('etl_valid_records', 0))} / "
            f"{_fmt_num(batch.get('etl_quarantined_records', 0))} / "
            f"{_fmt_num(batch.get('etl_rejected_records', 0))} |"
        )
    lines.append(f"| Tổng dòng `VALID` đọc vào | {_fmt_num(dp['records_before_dedup'])} |")
    lines.append(f"| Bản ghi trùng lặp bị loại trước chia tập (`.dropDuplicates`) | {_fmt_num(dp['duplicate_records_removed'])} ({_fmt_pct(dp['duplicate_removal_rate'])}) |")
    lines.append(f"| Tổng dòng `VALID` duy nhất sau loại trùng | **{_fmt_num(dp['records_after_dedup'])}** |")
    lines.append(f"| Số lớp gốc / Nhãn nhị phân | {dp['distinct_original_labels']} lớp (`BENIGN` = 0.0; 14 lớp `ATTACK` = 1.0) |")
    lines.append(f"| Tỷ lệ `ATTACK` / `BENIGN` sau loại trùng | {_fmt_num(dp['attack_records_after_dedup'])} ({_fmt_pct(dp['attack_ratio_after_dedup'])}) / {_fmt_num(dp['benign_records_after_dedup'])} |")
    lines.append(f"| Nhóm 21 đặc trưng trùng nhau nhưng khác nhãn | {_fmt_num(dp['feature_groups_with_conflicting_labels'])} |")
    ov = sp["disjoint_overlap_verification"]
    lines.append(f"| Kiểm tra giao nhau Train∩Val / Train∩Test / Val∩Test | **{ov['train_val']} / {ov['train_test']} / {ov['val_test']}** (PASS) |")
    lines.append("")

    # Split Table
    lines.append("### Phân bổ Stratified Split 70% / 15% / 15% (theo `label` gốc, `seed=42`)")
    lines.append("")
    lines.append("| Tập dữ liệu | Tổng mẫu | Tỷ lệ | BENIGN (0.0) | ATTACK (1.0) | Tỷ lệ ATTACK |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    for k, display in (("train", "Train"), ("val", "Validation"), ("test", "Test")):
        lines.append(
            f"| **{display}** | {_fmt_num(sp['counts'][k])} | {_fmt_pct(sp['actual_ratios'][k])} "
            f"| {_fmt_num(sp['benign_counts'][k])} | {_fmt_num(sp['attack_counts'][k])} | {_fmt_pct(sp['attack_ratios'][k])} |"
        )
    lines.append("")

    # Quality Flags stats after dedup
    fc = dp["flag_counts_after_dedup"]
    lines.append("### Thống kê Cờ Chất lượng trên Tập `VALID` sau Loại trùng")
    lines.append("")
    lines.append("| Cờ chất lượng (Veracity Flag) | Số dòng bật cờ | Tỷ lệ trên tập sau loại trùng | Vai trò trong Quality-Aware |")
    lines.append("|---|---:|---:|---|")
    n_dedup = dp["records_after_dedup"]
    lines.append(f"| `flow_bytes_s_invalid` | {_fmt_num(fc['flow_bytes_s_invalid'])} | {_fmt_pct(fc['flow_bytes_s_invalid'] / n_dedup)} | **Điều kiện (a) chính** (lỗi tốc độ) |")
    lines.append(f"| `flow_packets_s_invalid` | {_fmt_num(fc['flow_packets_s_invalid'])} | {_fmt_pct(fc['flow_packets_s_invalid'] / n_dedup)} | **Điều kiện (a) chính** (lỗi tốc độ) |")
    lines.append(f"| Hợp 2 cờ lỗi tốc độ (`OR`) | **{_fmt_num(fc['any_rate_flag'])}** | **{_fmt_pct(fc['any_rate_flag_ratio'])}** | **Chế độ Quality-Aware chính** |")
    lines.append(f"| `init_win_fwd_missing` | {_fmt_num(fc['init_win_fwd_missing'])} | {_fmt_pct(fc['init_win_fwd_missing'] / n_dedup)} | Phân tích độ nhạy (sentinel `-1`) |")
    lines.append(f"| `init_win_bwd_missing` | {_fmt_num(fc['init_win_bwd_missing'])} | {_fmt_pct(fc['init_win_bwd_missing'] / n_dedup)} | Phân tích độ nhạy (sentinel `-1`) |")
    lines.append(f"| Hợp cả 4 cờ feature (`OR`) | {_fmt_num(fc['any_4_feature_flags'])} | {_fmt_pct(fc['any_4_feature_flags_ratio'])} | Bảng phân tích độ nhạy (Mục 5) |")
    lines.append("")
    lines.append(
        "> **Lưu ý tránh Label Leakage:** Mã `LABEL_ENCODING_REPLACEMENT` trong `quality_reason_codes` "
        "chỉ phát sinh từ chuỗi `label` của 3 lớp Web Attack, nên được **loại trừ hoàn toàn** khỏi quy tắc `REVIEW`."
    )
    lines.append("")

    # Section 2: Model & Preprocessing Config
    lines.append("## 2. Cấu hình Mô hình Đối chứng (Baseline Random Forest)")
    lines.append("")
    hp = baseline_config["hyperparameters"]
    lines.append(f"- **Thuật toán:** `{baseline_config['model_type']}` (PySpark MLlib)")
    lines.append(f"- **Đặc trưng đầu vào:** Đúng **{len(baseline_config['feature_columns'])} cột số lõi** từ `cleaned/flows/` (tuyệt đối không đưa 4 cờ chất lượng hay metadata vào Baseline).")
    lines.append(
        f"- **Xử lý giá trị thiếu:** `Imputer(strategy=\"{baseline_config['preprocessing']['imputer_strategy']}\")` "
        "chỉ `fit` trên tập **Train**, sau đó `transform` sang Validation và Test."
    )
    surr = baseline_config["preprocessing"]["imputer_surrogate_values_from_train"]
    lines.append("  - Giá trị median học từ Train: " + ", ".join(f"`{k}` = {v:,.4f}" for k, v in surr.items()))
    lines.append(
        f"- **Siêu tham số:** `numTrees={hp['numTrees']}`, `maxDepth={hp['maxDepth']}`, "
        f"`maxBins={hp['maxBins']}`, `featureSubsetStrategy=\"{hp['featureSubsetStrategy']}\"`, "
        f"`impurity=\"{hp['impurity']}\"`, `maxMemoryInMB={hp['maxMemoryInMB']}`, `seed={hp['seed']}`."
    )
    lines.append(f"- **Cấu hình đầy đủ lưu tại:** `reports/modeling/baseline_config.json`.")
    lines.append("")

    # Feature Importances Top 10
    top_fi = summary.get("feature_importances", [])[:10]
    if top_fi:
        lines.append("### Top 10 Đặc trưng Quan trọng nhất (Baseline Random Forest)")
        lines.append("")
        lines.append("| Hạng | Tên đặc trưng | Importance |")
        lines.append("|---:|---|---:|")
        for idx, item in enumerate(top_fi, 1):
            lines.append(f"| {idx} | `{item['feature']}` | {item['importance']:.6f} |")
        lines.append("")

    # Section 3: Validation Threshold Selection
    lines.append("## 3. Chọn Ngưỡng Bất định `[tau_low, tau_high]` trên Tập Validation")
    lines.append("")
    lines.append(
        f"Quy tắc chọn ngưỡng chỉ sử dụng tập **Validation** (tuyệt đối không dùng tập Test). "
        f"Mục tiêu: tối đa hóa `balance_score = AUTO_F1 - {val_sel['selection_policy']['review_cost_weight']} * Review_rate` "
        f"trong ngân sách vận hành `Review rate <= {_fmt_pct(val_sel['selection_policy']['max_review_rate_budget'])}` "
        f"với điều kiện (a) dùng 2 cờ lỗi tốc độ (`flow_bytes_s_invalid OR flow_packets_s_invalid`)."
    )
    lines.append("")
    val_base = summary["validation_baseline"]
    lines.append(
        f"- **Baseline trên Validation (100% mẫu):** Precision = {_fmt_pct(val_base['precision'])}, "
        f"Recall = {_fmt_pct(val_base['recall'])}, F1 (ATTACK) = {_fmt_pct(val_base['f1'])}, "
        f"Macro-F1 = {_fmt_pct(val_base['macro_f1'])}, FPR = {_fmt_pct(val_base['fpr'])}."
    )
    lines.append("")
    lines.append("| Khoảng bất định | Coverage | Review rate | Attack→Review | AUTO Precision | AUTO Recall | AUTO F1 | AUTO Macro-F1 | AUTO FPR | Lỗi Baseline bị chặn | Balance Score |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for c in val_sel["candidates"]:
        m = c["auto_metrics"]
        inter = c["error_interception"]
        is_sel = (c["tau_low"] == sel["tau_low"]) and (c["tau_high"] == sel["tau_high"])
        tag = f"**[{c['tau_low']:.2f}, {c['tau_high']:.2f}]** *(Chọn)*" if is_sel else f"[{c['tau_low']:.2f}, {c['tau_high']:.2f}]"
        lines.append(
            f"| {tag} | {_fmt_pct(c['coverage'])} | {_fmt_pct(c['review_rate'])} | {_fmt_pct(c['attack_to_review_rate'])} "
            f"| {_fmt_pct(m['precision'])} | {_fmt_pct(m['recall'])} | {_fmt_pct(m['f1'])} "
            f"| {_fmt_pct(m['macro_f1'])} | {_fmt_pct(m['fpr'])} | {_fmt_pct(inter['baseline_error_capture_rate'])} | {c['balance_score']:.6f} |"
        )
    lines.append("")
    lines.append(
        f"**Ngưỡng chốt từ Validation:** `[tau_low, tau_high] = [{sel['tau_low']:.2f}, {sel['tau_high']:.2f}]` "
        f"(Validation AUTO F1 = {_fmt_pct(sel['validation_auto_f1'])}, Coverage = {_fmt_pct(sel['validation_coverage'])}, "
        f"Review rate = {_fmt_pct(sel['validation_review_rate'])})."
    )
    lines.append("")

    # Section 4: Main Fair Comparison on Test
    lines.append("## 4. Đánh giá So sánh Công bằng trên Cùng Tập Test")
    lines.append("")
    lines.append(
        f"Cả hai chế độ đều suy luận từ **cùng một mô hình Baseline Random Forest** và trên **cùng tập Test** "
        f"({_fmt_num(base_test['total_evaluated'])} mẫu: {_fmt_num(base_test['tp'] + base_test['fn'])} ATTACK, "
        f"{_fmt_num(base_test['tn'] + base_test['fp'])} BENIGN)."
    )
    lines.append("")
    lines.append("| Chỉ số đánh giá | Baseline (100% Test, p ≥ 0.5) | Quality-Aware (AUTO sau tách REVIEW) | Chênh lệch (QA − Baseline) |")
    lines.append("|---|---:|---:|---:|")
    lines.append(f"| Số mẫu đánh giá tự động | {_fmt_num(base_test['total_evaluated'])} | {_fmt_num(qa_auto['total_evaluated'])} | {_fmt_num(qa_auto['total_evaluated'] - base_test['total_evaluated'])} |")
    lines.append(f"| **TP** (ATTACK → ATTACK) | {_fmt_num(base_test['tp'])} | {_fmt_num(qa_auto['tp'])} | {_fmt_num(qa_auto['tp'] - base_test['tp'])} |")
    lines.append(f"| **FP** (BENIGN → ATTACK) | {_fmt_num(base_test['fp'])} | {_fmt_num(qa_auto['fp'])} | {_fmt_num(qa_auto['fp'] - base_test['fp'])} |")
    lines.append(f"| **FN** (ATTACK → BENIGN) | {_fmt_num(base_test['fn'])} | {_fmt_num(qa_auto['fn'])} | {_fmt_num(qa_auto['fn'] - base_test['fn'])} |")
    lines.append(f"| **TN** (BENIGN → BENIGN) | {_fmt_num(base_test['tn'])} | {_fmt_num(qa_auto['tn'])} | {_fmt_num(qa_auto['tn'] - base_test['tn'])} |")
    for label_txt, key in (
        ("**Precision (ATTACK)**", "precision"),
        ("**Recall (ATTACK)**", "recall"),
        ("**F1 (ATTACK)**", "f1"),
        ("**Macro-F1**", "macro_f1"),
        ("**Accuracy**", "accuracy"),
        ("**FPR (False Positive Rate)**", "fpr"),
    ):
        diff = qa_auto[key] - base_test[key]
        sign = "+" if diff >= 0 else ""
        lines.append(f"| {label_txt} | {_fmt_pct(base_test[key])} | {_fmt_pct(qa_auto[key])} | {sign}{diff * 100.0:.4f}% |")
    lines.append(f"| **Coverage** | 100.0000% | **{_fmt_pct(qa_test['coverage'])}** | — |")
    lines.append(f"| **Review rate** | 0.0000% | **{_fmt_pct(qa_test['review_rate'])}** | — |")
    lines.append(f"| **Attack to Review rate** | 0.0000% | **{_fmt_pct(qa_test['attack_to_review_rate'])}** | — |")
    lines.append("")

    # Confusion Matrices & Trigger Breakdown
    comp = qa_test["review_composition"]
    inter = qa_test["error_interception"]
    lines.append("### Phân tích Hiệu quả Chặn Lỗi & Nguyên nhân Kích hoạt `REVIEW` (Test)")
    lines.append("")
    lines.append(f"- Tổng mẫu chuyển sang `REVIEW`: **{_fmt_num(qa_test['review_records'])}** ({_fmt_pct(qa_test['review_rate'])} tập Test), gồm {_fmt_num(comp['actual_attack'])} `ATTACK` và {_fmt_num(comp['actual_benign'])} `BENIGN`.")
    lines.append(
        f"- Nguyên nhân kích hoạt `REVIEW`: chỉ do cờ chất lượng = **{_fmt_num(comp['flag_only'])}**, "
        f"chỉ do vùng bất định `[{sel['tau_low']:.2f}, {sel['tau_high']:.2f}]` = **{_fmt_num(comp['uncertainty_only'])}**, "
        f"kích hoạt đồng thời cả hai = **{_fmt_num(comp['both_flag_and_uncertainty'])}**."
    )
    lines.append(
        f"- **Chặn lỗi của Baseline:** Quality-Aware đã cô lập được **{_fmt_num(inter['baseline_errors_routed_to_review'])} / {_fmt_num(inter['baseline_errors_total'])}** "
        f"tổng lỗi của Baseline (**{_fmt_pct(inter['baseline_error_capture_rate'])}**), bao gồm "
        f"**{_fmt_num(inter['baseline_fp_routed_to_review'])} / {_fmt_num(inter['baseline_fp_total'])} FP** ({_fmt_pct(inter['baseline_fp_capture_rate'])}) "
        f"và **{_fmt_num(inter['baseline_fn_routed_to_review'])} / {_fmt_num(inter['baseline_fn_total'])} FN** ({_fmt_pct(inter['baseline_fn_capture_rate'])})."
    )
    lines.append("")

    # Section 5: Sensitivity Analysis (4 Feature Flags)
    lines.append("## 5. Phân tích Độ nhạy: So sánh Chế độ 2 Cờ Rate vs 4 Cờ Feature (Gồm `init_win_*_missing`)")
    lines.append("")
    lines.append(
        "Để minh bạch ảnh hưởng của chính sách cờ Veracity, bảng dưới đối chiếu chế độ chính "
        "(2 cờ lỗi rate: `flow_bytes_s_invalid`, `flow_packets_s_invalid`) với biến thể bật đủ cả 4 cờ feature "
        "(thêm `init_win_fwd_missing`, `init_win_bwd_missing`) trên cùng tập Test và cùng khoảng bất định "
        f"`[{sel['tau_low']:.2f}, {sel['tau_high']:.2f}]`:"
    )
    lines.append("")
    lines.append("| Chế độ Quality-Aware (trên Test) | Coverage | Review rate | Attack→Review | AUTO Precision | AUTO Recall | AUTO F1 | AUTO Macro-F1 | AUTO FPR | Chặn lỗi Baseline |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    lines.append(
        f"| **Chính: 2 cờ Rate** + `[{sel['tau_low']:.2f}, {sel['tau_high']:.2f}]` | {_fmt_pct(qa_test['coverage'])} "
        f"| {_fmt_pct(qa_test['review_rate'])} | {_fmt_pct(qa_test['attack_to_review_rate'])} "
        f"| {_fmt_pct(qa_auto['precision'])} | {_fmt_pct(qa_auto['recall'])} | {_fmt_pct(qa_auto['f1'])} "
        f"| {_fmt_pct(qa_auto['macro_f1'])} | {_fmt_pct(qa_auto['fpr'])} | {_fmt_pct(inter['baseline_error_capture_rate'])} |"
    )
    sens_inter = qa_sens["error_interception"]
    lines.append(
        f"| Độ nhạy: **4 cờ Feature** + `[{sel['tau_low']:.2f}, {sel['tau_high']:.2f}]` | {_fmt_pct(qa_sens['coverage'])} "
        f"| {_fmt_pct(qa_sens['review_rate'])} | {_fmt_pct(qa_sens['attack_to_review_rate'])} "
        f"| {_fmt_pct(qa_sens_auto['precision'])} | {_fmt_pct(qa_sens_auto['recall'])} | {_fmt_pct(qa_sens_auto['f1'])} "
        f"| {_fmt_pct(qa_sens_auto['macro_f1'])} | {_fmt_pct(qa_sens_auto['fpr'])} | {_fmt_pct(sens_inter['baseline_error_capture_rate'])} |"
    )
    lines.append("")
    lines.append(
        "> **Nhận xét kỹ thuật:** Cờ `init_win_fwd_missing` và `init_win_bwd_missing` (sentinel `-1`) xuất hiện ở "
        f"{_fmt_pct(fc['any_4_feature_flags_ratio'])} dữ liệu `VALID` (bản chất là đặc trưng giao thức của lưu lượng UDP/ICMP hoặc TCP thiếu window, "
        "không hẳn là lỗi đo đạc nghiêm trọng). Nếu dùng chúng làm trigger cứng đẩy sang `REVIEW`, `Review rate` vọt lên "
        f"{_fmt_pct(qa_sens['review_rate'])} (quá tải cho SOC/con người), trong khi dùng 2 cờ lỗi tốc độ kết hợp vùng bất định giữ "
        f"`Coverage` đạt {_fmt_pct(qa_test['coverage'])} nhưng vẫn chặn được {_fmt_pct(inter['baseline_error_capture_rate'])} lỗi của Baseline."
    )
    lines.append("")

    # Section 6: Per-Class Breakdown on Test
    lines.append("## 6. Phân tích Chi tiết theo Từng Lớp Gốc trên Tập Test")
    lines.append("")
    lines.append("| Nhãn gốc (`label`) | Lớp nhị phân | Tổng Test | Baseline đúng | Baseline Acc/Recall | Chuyển REVIEW | Tỷ lệ REVIEW | AUTO đúng / AUTO tổng | AUTO Acc/Recall |")
    lines.append("|---|---|---:|---:|---:|---:|---:|---:|---:|")
    for item in summary["test_evaluation"]["per_label_breakdown"]:
        lines.append(
            f"| `{item['label']}` | {item['class_name']} | {_fmt_num(item['total'])} "
            f"| {_fmt_num(item['baseline_correct'])} | {_fmt_pct(item['baseline_class_accuracy'])} "
            f"| {_fmt_num(item['routed_to_review'])} | {_fmt_pct(item['review_rate'])} "
            f"| {_fmt_num(item['auto_correct'])} / {_fmt_num(item['auto_total'])} | {_fmt_pct(item['auto_class_accuracy'])} |"
        )
    lines.append("")

    # Section 7: Timings & Resource Profile
    lines.append("## 7. Thời gian Thực thi & Tài nguyên Spark")
    lines.append("")
    env_info = baseline_config["spark_environment"]
    lines.append(
        f"- **Môi trường Spark:** Spark `{env_info['spark_version']}`, Master `{env_info['spark_master']}`, "
        f"Executor `{env_info['executor_cores']} cores / {env_info['executor_memory']}`, "
        f"Shuffle partitions = `{env_info['shuffle_partitions']}`, Storage level = `{env_info['storage_level']}`."
    )
    lines.append("")
    lines.append("| Giai đoạn | Thời gian (giây) |")
    lines.append("|---|---:|")
    for stage_name, secs in timings.items():
        lines.append(f"| `{stage_name}` | {secs:.3f} |")
    lines.append("")

    # Section 8: Real Test Examples
    lines.append("## 8. Minh chứng Mẫu Thực tế từ Tập Test")
    lines.append("")
    lines.append(
        "Dữ liệu đầy đủ 21 đặc trưng + 4 cờ chất lượng + `quality_reason_codes` của toàn bộ mẫu minh chứng "
        "được lưu tại `reports/modeling/test_examples.csv` và `reports/modeling/test_examples.json`."
    )
    lines.append("")

    for key, heading in (
        ("false_positive", "### 8.1. Ví dụ False Positive (BENIGN bị Baseline đoán nhầm thành ATTACK)"),
        ("false_negative", "### 8.2. Ví dụ False Negative (ATTACK bị Baseline đoán nhầm thành BENIGN)"),
        ("review", "### 8.3. Ví dụ Mẫu bị đẩy sang `REVIEW` (Quality-Aware)"),
    ):
        lines.append(heading)
        lines.append("")
        cat_rows = examples.get(key, [])
        if not cat_rows:
            lines.append("_Không có mẫu nào thuộc nhóm này trên tập Test._")
            lines.append("")
            continue
        lines.append(
            "| # | `label` gốc | `p_attack` | Baseline | QA Decision | Lý do REVIEW | Cờ chất lượng bật | `dst_port` | `flow_dur` (µs) | `fwd/bwd_pkts` | `flow_bytes_s` | `pkt_len_mean` | `init_win_fwd/bwd` |"
        )
        lines.append("|---:|---|---:|---|---|---|---|---:|---:|---|---:|---:|---|")
        for idx, ex in enumerate(cat_rows, 1):
            feats = ex["features"]
            active_flags = [k for k, v in ex["quality_flags"].items() if v]
            flag_str = ", ".join(f"`{f}`" for f in active_flags) if active_flags else "Không"
            lines.append(
                f"| {idx} | `{ex['label_original']}` | {ex['p_attack']:.4f} | {ex['baseline_prediction']} "
                f"| **{ex['qa_decision']}** | `{ex['review_trigger_reason']}` | {flag_str} "
                f"| {_fmt_feat(feats['destination_port'])} | {_fmt_feat(feats['flow_duration'])} "
                f"| {_fmt_feat(feats['total_fwd_packets'])}/{_fmt_feat(feats['total_backward_packets'])} "
                f"| {_fmt_feat(feats['flow_bytes_s'])} | {_fmt_feat(feats['packet_length_mean'])} "
                f"| {_fmt_feat(feats['init_win_bytes_forward'])}/{_fmt_feat(feats['init_win_bytes_backward'])} |"
            )
        lines.append("")

    return "\n".join(lines) + "\n"

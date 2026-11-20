"""Classification metrics, Quality-Aware decision rules, and Validation threshold selection.

Convention:
- Positive class (1.0): ATTACK (14 attack classes)
- Negative class (0.0): BENIGN
"""

from typing import Any, Dict, Iterable, List, Sequence, Tuple

from pyspark.sql import DataFrame, functions as F


def _safe_div(num: float, den: float) -> float:
    return float(num / den) if den > 0 else 0.0


def confusion_metrics(tp: int, fp: int, fn: int, tn: int) -> Dict[str, Any]:
    """Compute complete binary & macro classification metrics with ATTACK as positive class."""
    tp, fp, fn, tn = int(tp), int(fp), int(fn), int(tn)
    total = tp + fp + fn + tn

    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2.0 * precision * recall, precision + recall)

    precision_benign = _safe_div(tn, tn + fn)
    recall_benign = _safe_div(tn, tn + fp)
    f1_benign = _safe_div(2.0 * precision_benign * recall_benign, precision_benign + recall_benign)

    macro_f1 = 0.5 * (f1 + f1_benign)
    accuracy = _safe_div(tp + tn, total)
    fpr = _safe_div(fp, fp + tn)
    fnr = _safe_div(fn, fn + tp)

    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "total_evaluated": total,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "precision_benign": precision_benign,
        "recall_benign": recall_benign,
        "f1_benign": f1_benign,
        "macro_f1": macro_f1,
        "accuracy": accuracy,
        "fpr": fpr,
        "fnr": fnr,
    }


def build_flag_trigger_expr(flag_columns: Sequence[str]):
    """Combine feature-level boolean quality flags with logical OR."""
    if not flag_columns:
        return F.lit(False)
    expr = F.lit(False)
    for name in flag_columns:
        expr = expr | F.coalesce(F.col(name), F.lit(False))
    return expr


def evaluate_baseline_df(scored_df: DataFrame, *, threshold: float = 0.50) -> Dict[str, Any]:
    """Evaluate Baseline RF on 100% of scored records (p_attack >= threshold -> ATTACK)."""
    actual_pos = F.col("label_binary") == F.lit(1.0)
    pred_pos = F.col("p_attack") >= F.lit(float(threshold))

    row = scored_df.agg(
        F.sum(F.when(actual_pos & pred_pos, 1).otherwise(0)).alias("tp"),
        F.sum(F.when((~actual_pos) & pred_pos, 1).otherwise(0)).alias("fp"),
        F.sum(F.when(actual_pos & (~pred_pos), 1).otherwise(0)).alias("fn"),
        F.sum(F.when((~actual_pos) & (~pred_pos), 1).otherwise(0)).alias("tn"),
    ).collect()[0].asDict()

    metrics = confusion_metrics(
        int(row["tp"] or 0),
        int(row["fp"] or 0),
        int(row["fn"] or 0),
        int(row["tn"] or 0),
    )
    metrics["decision_threshold"] = float(threshold)
    metrics["coverage"] = 1.0
    metrics["review_rate"] = 0.0
    metrics["attack_to_review_rate"] = 0.0
    return metrics


def evaluate_quality_aware_df(
    scored_df: DataFrame,
    *,
    flag_columns: Sequence[str],
    tau_low: float,
    tau_high: float,
    decision_threshold: float = 0.50,
) -> Dict[str, Any]:
    """Evaluate Quality-Aware REVIEW policy on a scored DataFrame in a single Spark pass.

    REVIEW rule:
      (a) Any boolean flag in `flag_columns` is True; OR
      (b) p_attack in [tau_low, tau_high].
    AUTO samples (~REVIEW) are classified by `p_attack >= decision_threshold`.
    """
    if not (0.0 <= tau_low <= decision_threshold <= tau_high <= 1.0):
        raise ValueError(
            f"Invalid threshold band [{tau_low}, {tau_high}] around {decision_threshold}"
        )

    flag_trig = build_flag_trigger_expr(flag_columns)
    prob_unc = (F.col("p_attack") >= F.lit(float(tau_low))) & (
        F.col("p_attack") <= F.lit(float(tau_high))
    )
    is_review = flag_trig | prob_unc
    is_auto = ~is_review

    actual_pos = F.col("label_binary") == F.lit(1.0)
    pred_pos = F.col("p_attack") >= F.lit(float(decision_threshold))
    baseline_fp = (~actual_pos) & pred_pos
    baseline_fn = actual_pos & (~pred_pos)

    row = scored_df.agg(
        F.count("*").alias("total_records"),
        F.sum(F.when(actual_pos, 1).otherwise(0)).alias("total_attack"),
        F.sum(F.when(~actual_pos, 1).otherwise(0)).alias("total_benign"),
        # AUTO confusion matrix
        F.sum(F.when(is_auto & actual_pos & pred_pos, 1).otherwise(0)).alias("auto_tp"),
        F.sum(F.when(is_auto & (~actual_pos) & pred_pos, 1).otherwise(0)).alias("auto_fp"),
        F.sum(F.when(is_auto & actual_pos & (~pred_pos), 1).otherwise(0)).alias("auto_fn"),
        F.sum(F.when(is_auto & (~actual_pos) & (~pred_pos), 1).otherwise(0)).alias("auto_tn"),
        # Operational / REVIEW counts
        F.sum(F.when(is_auto, 1).otherwise(0)).alias("auto_records"),
        F.sum(F.when(is_review, 1).otherwise(0)).alias("review_records"),
        F.sum(F.when(is_review & actual_pos, 1).otherwise(0)).alias("review_actual_attack"),
        F.sum(F.when(is_review & (~actual_pos), 1).otherwise(0)).alias("review_actual_benign"),
        # Trigger breakdown
        F.sum(F.when(flag_trig & (~prob_unc), 1).otherwise(0)).alias("review_flag_only"),
        F.sum(F.when((~flag_trig) & prob_unc, 1).otherwise(0)).alias("review_uncertainty_only"),
        F.sum(F.when(flag_trig & prob_unc, 1).otherwise(0)).alias("review_both_flag_and_uncertainty"),
        # Baseline error interception diagnostics
        F.sum(F.when(baseline_fp, 1).otherwise(0)).alias("baseline_total_fp"),
        F.sum(F.when(baseline_fn, 1).otherwise(0)).alias("baseline_total_fn"),
        F.sum(F.when(is_review & baseline_fp, 1).otherwise(0)).alias("baseline_fp_to_review"),
        F.sum(F.when(is_review & baseline_fn, 1).otherwise(0)).alias("baseline_fn_to_review"),
    ).collect()[0].asDict()

    total_records = int(row["total_records"] or 0)
    total_attack = int(row["total_attack"] or 0)
    total_benign = int(row["total_benign"] or 0)
    auto_records = int(row["auto_records"] or 0)
    review_records = int(row["review_records"] or 0)
    review_attack = int(row["review_actual_attack"] or 0)
    review_benign = int(row["review_actual_benign"] or 0)
    base_fp = int(row["baseline_total_fp"] or 0)
    base_fn = int(row["baseline_total_fn"] or 0)
    fp_to_rev = int(row["baseline_fp_to_review"] or 0)
    fn_to_rev = int(row["baseline_fn_to_review"] or 0)

    auto_metrics = confusion_metrics(
        int(row["auto_tp"] or 0),
        int(row["auto_fp"] or 0),
        int(row["auto_fn"] or 0),
        int(row["auto_tn"] or 0),
    )

    coverage = _safe_div(auto_records, total_records)
    review_rate = _safe_div(review_records, total_records)
    attack_to_review_rate = _safe_div(review_attack, total_attack)
    benign_to_review_rate = _safe_div(review_benign, total_benign)

    return {
        "flag_columns": list(flag_columns),
        "tau_low": float(tau_low),
        "tau_high": float(tau_high),
        "decision_threshold": float(decision_threshold),
        "total_records": total_records,
        "auto_records": auto_records,
        "review_records": review_records,
        "coverage": coverage,
        "review_rate": review_rate,
        "attack_to_review_rate": attack_to_review_rate,
        "benign_to_review_rate": benign_to_review_rate,
        "review_composition": {
            "actual_attack": review_attack,
            "actual_benign": review_benign,
            "flag_only": int(row["review_flag_only"] or 0),
            "uncertainty_only": int(row["review_uncertainty_only"] or 0),
            "both_flag_and_uncertainty": int(row["review_both_flag_and_uncertainty"] or 0),
        },
        "error_interception": {
            "baseline_fp_total": base_fp,
            "baseline_fp_routed_to_review": fp_to_rev,
            "baseline_fp_capture_rate": _safe_div(fp_to_rev, base_fp),
            "baseline_fn_total": base_fn,
            "baseline_fn_routed_to_review": fn_to_rev,
            "baseline_fn_capture_rate": _safe_div(fn_to_rev, base_fn),
            "baseline_errors_total": base_fp + base_fn,
            "baseline_errors_routed_to_review": fp_to_rev + fn_to_rev,
            "baseline_error_capture_rate": _safe_div(fp_to_rev + fn_to_rev, base_fp + base_fn),
            "review_error_density": _safe_div(fp_to_rev + fn_to_rev, review_records),
        },
        "auto_metrics": auto_metrics,
    }


def evaluate_per_attack_class_df(
    scored_df: DataFrame,
    *,
    flag_columns: Sequence[str],
    tau_low: float,
    tau_high: float,
    decision_threshold: float = 0.50,
) -> List[Dict[str, Any]]:
    """Compute per-original-label recall/detection and review routing breakdown."""
    flag_trig = build_flag_trigger_expr(flag_columns)
    prob_unc = (F.col("p_attack") >= F.lit(float(tau_low))) & (
        F.col("p_attack") <= F.lit(float(tau_high))
    )
    is_review = flag_trig | prob_unc
    is_auto = ~is_review
    pred_pos = F.col("p_attack") >= F.lit(float(decision_threshold))
    actual_pos = F.col("label_binary") == F.lit(1.0)

    rows = (
        scored_df.groupBy("label", "label_binary")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(pred_pos == actual_pos, 1).otherwise(0)).alias("baseline_correct"),
            F.sum(F.when(is_review, 1).otherwise(0)).alias("routed_to_review"),
            F.sum(F.when(is_auto, 1).otherwise(0)).alias("auto_total"),
            F.sum(F.when(is_auto & (pred_pos == actual_pos), 1).otherwise(0)).alias("auto_correct"),
        )
        .orderBy(F.desc("total"), F.asc("label"))
        .collect()
    )

    result = []
    for r in rows:
        total = int(r["total"])
        base_ok = int(r["baseline_correct"])
        rev = int(r["routed_to_review"])
        auto_tot = int(r["auto_total"])
        auto_ok = int(r["auto_correct"])
        result.append(
            {
                "label": r["label"],
                "class_name": "BENIGN" if float(r["label_binary"]) == 0.0 else "ATTACK",
                "total": total,
                "baseline_correct": base_ok,
                "baseline_class_accuracy": _safe_div(base_ok, total),
                "routed_to_review": rev,
                "review_rate": _safe_div(rev, total),
                "auto_total": auto_tot,
                "auto_correct": auto_ok,
                "auto_class_accuracy": _safe_div(auto_ok, auto_tot),
            }
        )
    return result


def select_best_threshold(
    sweep_results: Sequence[Dict[str, Any]],
    *,
    max_review_rate: float = 0.05,
    review_cost_weight: float = 0.05,
) -> Dict[str, Any]:
    """Select the optimal [tau_low, tau_high] purely on Validation results.

    Objective:
      1. Prefer candidates satisfying `review_rate <= max_review_rate` (operational budget).
         If none satisfy the cap, fall back to all candidates and minimize review_rate.
      2. Maximize `balance_score = auto_f1 - review_cost_weight * review_rate`,
         balancing AUTO classification quality against human review workload.
         Tie-break by higher `auto_macro_f1`, lower `auto_fpr`, higher `coverage`.
    """
    if not sweep_results:
        raise ValueError("Empty Validation threshold sweep results")

    annotated = []
    for item in sweep_results:
        auto_m = item["auto_metrics"]
        rev_rate = float(item["review_rate"])
        balance_score = float(auto_m["f1"]) - float(review_cost_weight) * rev_rate
        enriched = dict(item)
        enriched["balance_score"] = balance_score
        enriched["within_review_budget"] = rev_rate <= max_review_rate
        annotated.append(enriched)

    eligible = [x for x in annotated if x["within_review_budget"]]
    pool = eligible if eligible else annotated

    if eligible:
        best = max(
            pool,
            key=lambda x: (
                round(x["balance_score"], 8),
                round(x["auto_metrics"]["macro_f1"], 8),
                -round(x["auto_metrics"]["fpr"], 8),
                round(x["coverage"], 8),
            ),
        )
    else:
        best = min(
            pool,
            key=lambda x: (
                round(x["review_rate"], 8),
                -round(x["auto_metrics"]["f1"], 8),
            ),
        )

    return {
        "selection_policy": {
            "dataset": "validation_only",
            "max_review_rate_budget": float(max_review_rate),
            "review_cost_weight": float(review_cost_weight),
            "formula": "balance_score = auto_f1 - review_cost_weight * review_rate (subject to review_rate <= max_review_rate_budget)",
        },
        "selected": {
            "tau_low": best["tau_low"],
            "tau_high": best["tau_high"],
            "balance_score": best["balance_score"],
            "validation_auto_f1": best["auto_metrics"]["f1"],
            "validation_auto_macro_f1": best["auto_metrics"]["macro_f1"],
            "validation_auto_fpr": best["auto_metrics"]["fpr"],
            "validation_coverage": best["coverage"],
            "validation_review_rate": best["review_rate"],
            "validation_attack_to_review_rate": best["attack_to_review_rate"],
        },
        "candidates": annotated,
    }

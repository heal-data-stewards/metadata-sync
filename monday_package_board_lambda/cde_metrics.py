"""
Per-HDPID CDE mapping metrics from the CDE mapping metrics report.

The report's `file-summary` sheet has one row per source workbook. A study can
have several workbooks, so metrics are rolled up per HDPID by summing counts
and recomputing rates from those sums. Averaging per-file rates would give a
small workbook the same weight as a large one.
"""

import pandas as pd

REPORT_SHEET = "file-summary"

# Counts that can be summed across a study's workbooks.
SUM_COLUMNS = [
    "total_variables",
    "accepted_cde_mapping_count",
    "high_confidence_accepted_count",
    "needs_human_review_count",
    "no_match_count",
    "accepted_score_not_aligned_count",
    "encoding_scorable_high_confidence_count",
    "cde_metadata_alignment_scorable_count",
    "high_cde_metadata_alignment_count",
]


# Both scores in the report are on a 0-100 scale.
# Re-encoding is flagged when the study's average encoding fidelity is at or below 50.
ENCODING_FIDELITY_REENCODE_THRESHOLD = 50.0


def add_re_encoding_flag(per_study: pd.DataFrame) -> pd.DataFrame:
    """Yes/No per study; blank when the average fidelity is unavailable."""
    avg = per_study["avg_recomputed_encoding_fidelity"]
    flag = pd.Series("", index=per_study.index, dtype=object)
    flag[avg.notna()] = [
        "Yes" if v <= ENCODING_FIDELITY_REENCODE_THRESHOLD else "No" for v in avg[avg.notna()]
    ]
    per_study["re_encoding_required"] = flag
    return per_study


# Averages from file-summary-audit, each weighted by the count of rows it was computed over.
# Weighting by total_variables would be wrong: the averages only cover scored rows.
AVERAGE_WEIGHTS = {
    "avg_recomputed_concept_similarity": "recomputed_concept_score_count",
    "avg_recomputed_encoding_fidelity": "encoding_scorable_high_confidence_count",
}


def rollup_by_hdpid(
    file_summary: pd.DataFrame,
    file_summary_audit: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns (per_study, excluded).

    per_study: one row per HDPID with summed counts, recomputed rates, and
               weighted averages when file_summary_audit is provided.
    excluded:  rows with no HDPID, kept so they can be reported rather than dropped silently.
    """
    missing = [c for c in ["HDPID", "source_workbook", *SUM_COLUMNS] if c not in file_summary.columns]
    if missing:
        raise ValueError(f"Report is missing expected columns: {missing}")

    has_id = file_summary["HDPID"].notna() & (file_summary["HDPID"].astype(str).str.strip() != "")
    excluded = file_summary.loc[~has_id, ["source_workbook", *SUM_COLUMNS]].copy()
    rows = file_summary.loc[has_id].copy()

    grouped = rows.groupby("HDPID", as_index=False)[SUM_COLUMNS].sum()
    grouped["workbook_count"] = rows.groupby("HDPID").size().values

    if file_summary_audit is not None:
        grouped = _add_weighted_averages(grouped, file_summary_audit, rows)

    total = grouped["total_variables"]
    accepted = grouped["accepted_cde_mapping_count"]
    high_conf = grouped["high_confidence_accepted_count"]
    scorable = grouped["cde_metadata_alignment_scorable_count"]
    high_align = grouped["high_cde_metadata_alignment_count"]

    grouped["accepted_cde_mapping_rate"] = _safe_div(accepted, total)
    grouped["high_confidence_share_of_accepted"] = _safe_div(high_conf, accepted)
    grouped["high_cde_metadata_alignment_rate_scorable"] = _safe_div(high_align, scorable)
    grouped["high_cde_metadata_alignment_rate_total_variables"] = _safe_div(high_align, total)

    return grouped, excluded


def _add_weighted_averages(
    grouped: pd.DataFrame, audit: pd.DataFrame, rows: pd.DataFrame
) -> pd.DataFrame:
    """Weighted mean per HDPID: sum(avg * n) / sum(n) over that study's workbooks."""
    keyed = audit.set_index("source_workbook")
    for avg_col, weight_col in AVERAGE_WEIGHTS.items():
        if avg_col not in keyed.columns or weight_col not in keyed.columns:
            raise ValueError(f"Audit sheet is missing {avg_col} or {weight_col}")
        # Match each study row to its audit row by workbook name.
        per_row = rows[["HDPID", "source_workbook"]].copy()
        per_row["avg"] = per_row["source_workbook"].map(keyed[avg_col])
        per_row["n"] = per_row["source_workbook"].map(keyed[weight_col]).fillna(0)
        # Only workbooks that actually have an average and scored rows contribute.
        valid = per_row["avg"].notna() & (per_row["n"] > 0)
        per_row["weighted"] = (per_row["avg"] * per_row["n"]).where(valid, 0)
        per_row["n_valid"] = per_row["n"].where(valid, 0)
        sums = per_row.groupby("HDPID")[["weighted", "n_valid"]].sum()
        sums.columns = ["weighted_sum", "n_sum"]
        grouped[avg_col] = grouped["HDPID"].map(
            (sums["weighted_sum"] / sums["n_sum"].where(sums["n_sum"] > 0)).round(4)
        )
    return grouped


def _safe_div(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    out = numerator / denominator.where(denominator != 0)
    return out.round(4)

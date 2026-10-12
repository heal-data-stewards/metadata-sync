"""
Assembles the per-study CDE metrics table, then narrows it to each board's
own columns and names. Keep one wide table as the source of truth; the two
boards are just different views over it.
"""

import pandas as pd

from blanks import fill_blanks
from cde_metrics import add_re_encoding_flag, rollup_by_hdpid
from hss_cde_mappings import counts_by_study


def build_cde_board_metrics(
    report_file_summary: pd.DataFrame,
    report_file_summary_audit: pd.DataFrame,
    hss_mapped_variables: list[dict],
    hss_section_counts: dict[str, int],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (per_study, excluded_report_rows)."""
    per_study, excluded = rollup_by_hdpid(report_file_summary, report_file_summary_audit)
    per_study = add_re_encoding_flag(per_study)

    hss_counts = counts_by_study(hss_mapped_variables)
    hss_df = pd.DataFrame.from_dict(hss_counts, orient="index").reset_index(names="HDPID")

    # Outer join: a study can have HSS mappings without report rows, or vice versa.
    merged = pd.merge(per_study, hss_df, how="outer", on="HDPID")
    for col in ["num_variable_cde_mappings", "num_distinct_cde_measures", "num_cde_forms_from_mapping"]:
        merged[col] = merged[col].fillna(0).astype(int)

    merged["num_sections"] = merged["HDPID"].map(hss_section_counts).fillna(0).astype(int)

    return merged, excluded


DATA_PACKAGE_COLUMNS = {
    "HDPID": "HDPID",
    "high_confidence_share_of_accepted": "high_confidence_share_of_accepted",
    "needs_human_review_count": "needs_human_review_count",
    "no_match_count": "no_match_count",
    "high_cde_metadata_alignment_rate_scorable": "high_cde_metadata_alignment_rate_scorable",
    "avg_recomputed_encoding_fidelity": "avg_recomputed_encoding_fidelity",
    "num_sections": "num_sections",
    "num_variable_cde_mappings": "num_variable_cde_mappings",
    "num_distinct_cde_measures": "num_distinct_cde_measures",
}

DATA_REUSE_COLUMNS = {
    "HDPID": "HDPID",
    "num_sections": "Number of CDE Forms Mapped to Study",
    "num_cde_forms_from_mapping": "Number of CDE Forms found from Variable Mapping",
    "avg_recomputed_concept_similarity": "Average Quality of CDE to Variable Mapping",
    "re_encoding_required": "Re-encoding required for any CDE",
    "num_variable_cde_mappings": "Number of Variables Mapped to a CDE Measure",
}


def to_data_package_columns(per_study: pd.DataFrame) -> pd.DataFrame:
    return fill_blanks(_select_and_rename(per_study, DATA_PACKAGE_COLUMNS))


def to_data_reuse_columns(per_study: pd.DataFrame) -> pd.DataFrame:
    return fill_blanks(_select_and_rename(per_study, DATA_REUSE_COLUMNS))


def _select_and_rename(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    missing = [c for c in mapping if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")
    return df[list(mapping.keys())].rename(columns=mapping)

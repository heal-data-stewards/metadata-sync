"""
Final per-board assembly. Each board is one Excel sheet, ready to upload to
Monday as-is:

- Data Package board: one row per MDS repository link (data_package_links.py),
  with the per-study CDE mapping metrics (board_metrics.py) joined on, left
  to right since a study can have the links without the metrics or vice versa.
- Data Reuse board: one row per study, purely the CDE mapping metrics --
  board_metrics.to_data_reuse_columns() output is already in this shape.
"""

import pandas as pd

from blanks import fill_blanks
from board_metrics import to_data_package_columns, to_data_reuse_columns


def build_data_package_board(
    links: pd.DataFrame,
    cde_metrics_per_study: pd.DataFrame,
    hss_section_counts: dict[str, int],
) -> pd.DataFrame:
    """
    links: data_package_links.build_data_package_links() output.
    cde_metrics_per_study: board_metrics.build_cde_board_metrics()'s per_study output.
    hss_section_counts: same dict passed to build_data_package_links() -- used
        here to re-derive num_sections directly, rather than trust whatever
        survived cde_metrics_per_study's own report/variable-mapping outer
        join. That join only unions HDPIDs from the CDE report and HSS's
        CDE-mapped-variables index, so a study with sections in HSS but
        neither of those never gets a row there at all, and num_sections
        would silently come back "-" despite "CDE mappings on HSS for
        Study?" (computed independently, straight from this same dict in
        data_package_links.py) correctly saying "Yes" for it.
    """
    metrics = to_data_package_columns(cde_metrics_per_study)
    merged = pd.merge(links, metrics, how="left", on="HDPID")
    merged["num_sections"] = merged["HDPID"].map(hss_section_counts).fillna(0).astype(int)
    return fill_blanks(merged)


def build_data_reuse_board(cde_metrics_per_study: pd.DataFrame) -> pd.DataFrame:
    return to_data_reuse_columns(cde_metrics_per_study)

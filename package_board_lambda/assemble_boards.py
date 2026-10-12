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


def build_data_package_board(links: pd.DataFrame, cde_metrics_per_study: pd.DataFrame) -> pd.DataFrame:
    """
    links: data_package_links.build_data_package_links() output.
    cde_metrics_per_study: board_metrics.build_cde_board_metrics()'s per_study output.
    """
    metrics = to_data_package_columns(cde_metrics_per_study)
    merged = pd.merge(links, metrics, how="left", on="HDPID")
    return fill_blanks(merged)


def build_data_reuse_board(cde_metrics_per_study: pd.DataFrame) -> pd.DataFrame:
    return to_data_reuse_columns(cde_metrics_per_study)

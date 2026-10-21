"""
Final per-board assembly:

- Data Package board: one row per MDS repository link (data_package_links.py),
  with EVERY computed per-study CDE metric column (board_metrics.py) joined
  on, left to right since a study can have the links without the metrics or
  vice versa. Not narrowed to board_metrics.DATA_PACKAGE_COLUMNS's original
  9-column guess -- which of these actually belong on the real Monday board
  isn't decided yet, so this hands over everything and leaves that as a
  column-mapping choice made at upload time, not a decision baked into this
  code. Column names are left as the working snake_case names from
  cde_metrics.py/board_metrics.py (meanings are in the CDE report's own
  metric-definitions sheet) rather than invented Monday labels, since
  there's no agreed display name for most of these yet.
- Data Reuse board: one row per study, same full set of columns -- see
  build_data_reuse_board()'s docstring for why this one might not need to
  exist as its own generated file going forward.
"""

import pandas as pd

from blanks import fill_blanks


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
    merged = pd.merge(links, cde_metrics_per_study, how="left", on="HDPID")
    merged["num_sections"] = merged["HDPID"].map(hss_section_counts).fillna(0).astype(int)
    return fill_blanks(merged)


def build_data_reuse_board(cde_metrics_per_study: pd.DataFrame) -> pd.DataFrame:
    """
    Now that Data Package carries every CDE metric column too, this is a
    strict subset of it (same columns, minus the link-level ones, deduped to
    one row per study) -- Hina flagged this and is right. Left in place
    rather than removed pending her answer on whether lambda_function.py
    should keep writing a separate DataReuseBoard_<date>.xlsx at all, since
    dropping it outright is an output-contract change worth confirming
    rather than assuming.
    """
    return fill_blanks(cde_metrics_per_study)

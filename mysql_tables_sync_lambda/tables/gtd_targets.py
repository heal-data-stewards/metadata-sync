"""
Builds the GTD (Get the Data) Targets report -- two PM-facing target lists
(studies whose data is due this year, and early awards starting last year
that aren't already on the main list) written as a two-sheet .xlsx to S3.

Mirrors mysql_code/python_scripts/HEAL_08_GTDTargets.py, with one change:
the original script filtered on hardcoded literals ("latest end year ==
2026", "earliest start year == 2025"). Both are now relative to the
current calendar year (this year / last year) so the report never goes
stale as the calendar rolls over.

alldata (already built by this lambda) is the only input -- no manual
input, no new MySQL table (this is a report, not a table).
"""

import io
import logging
import os
from datetime import datetime, timezone

import pandas as pd

from db import clean_mysql_value

logger = logging.getLogger(__name__)

GTD_REPORT_FILE = "gtd_targets.xlsx"

# Column order/selection requested by PM parameters -- unchanged from the
# original script.
COLUMN_ORDER = [
    "appl_id",
    "proj_num",
    "ctc_pi_nm",
    "study_pi_email",
    "proj_strt_date",
    "proj_end_date",
    "fund_mech",
    "heal_funded",
    "nih_core_cde",
    "hdp_id",
    "xstudy_id",
    "study_hdp_id",
    "study_hdp_id_appl",
    "study_first_appl",
    "study_most_recent_appl",
    "do_not_engage",
    "checklist_exempt_all",
    "study_res_net",
    "project_title_reporter",
    "project_title_platform",
]

_SORT_COLS = ["study_res_net", "checklist_exempt_all", "proj_end_date"]


def _read_table(conn, table: str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM `{table}`")
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    for col in df.columns:
        df[col] = df[col].map(clean_mysql_value)
    if "appl_id" in df.columns:
        df["appl_id"] = df["appl_id"].astype(str)
    return df


# ---------------------------------------------------------------------------
# Build logic
# ---------------------------------------------------------------------------

def build_gtd_targets(alldata_df: pd.DataFrame, gtd_year: int, early_award_year: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (gtd_final, early_final)."""
    df_master = alldata_df.copy()
    df_master["proj_end_date_date"] = pd.to_datetime(df_master["proj_end_date_date"], errors="coerce")
    df_master["proj_strt_date_date"] = pd.to_datetime(df_master["proj_strt_date_date"], errors="coerce")

    # --- 1. GTD target list ---
    gtd = df_master.copy()
    gtd["latest_end_date"] = gtd.groupby("xstudy_id")["proj_end_date_date"].transform("max")
    gtd["latest_end_yr"] = gtd["latest_end_date"].dt.year
    gtd = gtd[gtd["latest_end_yr"] == gtd_year]
    gtd = gtd[gtd["appl_id"] == gtd["study_most_recent_appl"]]
    gtd = gtd[gtd["do_not_engage"] != 1]
    gtd = gtd[gtd["study_hdp_status"] != "archived"]

    gtd_final = gtd[COLUMN_ORDER].copy()
    gtd_final = gtd_final.sort_values(by=_SORT_COLS)
    logger.info("gtd_final (%d): %d rows", gtd_year, len(gtd_final))

    on_gtd_list = gtd_final[["xstudy_id"]].drop_duplicates().copy()
    on_gtd_list["on_gtd_list"] = 1

    # --- 2. Early awards target list ---
    early = df_master.copy()
    early["first_strt_date"] = early.groupby("xstudy_id")["proj_strt_date_date"].transform("min")
    early["first_strt_yr"] = early["first_strt_date"].dt.year
    early = early[early["first_strt_yr"] == early_award_year]
    early = early[early["do_not_engage"] != 1]
    early = early[early["fund_mech"] != "SBIR/STTR"]
    early = early[early["study_hdp_status"] != "archived"]

    early = pd.merge(early, on_gtd_list, on="xstudy_id", how="left")
    early = early[early["on_gtd_list"].isna()].drop(columns=["on_gtd_list"])

    # Exactly one row per study -- keep the most recent appl row if duplicates remain.
    early = early.sort_values(by=["xstudy_id", "proj_strt_date_date"])
    early = early.drop_duplicates(subset=["xstudy_id"], keep="last")

    early_final = early[COLUMN_ORDER].copy()
    early_final = early_final.sort_values(by=_SORT_COLS)
    logger.info("early_final (%d): %d rows", early_award_year, len(early_final))

    return gtd_final, early_final


def _to_xlsx_bytes(gtd_final: pd.DataFrame, early_final: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        gtd_final.to_excel(writer, sheet_name="gtd", index=False)
        early_final.to_excel(writer, sheet_name="earlyawards", index=False)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def update_gtd_targets(
    alldata_conn,
    ref_source,
    alldata_table: str | None = None,
    report_file: str | None = None,
) -> dict:
    """
    alldata_conn: connection for reading `alldata`, which follows
          DB_ENVIRONMENT like every other table this lambda builds.
    ref_source: a sources.FileSource pointed at the directory/prefix to
          write the report into.
    """
    if alldata_table is None:
        alldata_table = os.getenv("ALLDATA_TABLE_NAME", "alldata")
    if report_file is None:
        report_file = os.getenv("GTD_REPORT_FILE", GTD_REPORT_FILE)

    alldata_df = _read_table(alldata_conn, alldata_table)

    now = datetime.now(timezone.utc)
    gtd_year = now.year
    early_award_year = now.year - 1

    gtd_final, early_final = build_gtd_targets(alldata_df, gtd_year, early_award_year)
    ref_source.write_bytes(report_file, _to_xlsx_bytes(gtd_final, early_final))

    return {
        "report": report_file,
        "gtd_year": gtd_year,
        "gtd_rows": len(gtd_final),
        "early_award_year": early_award_year,
        "early_rows": len(early_final),
    }

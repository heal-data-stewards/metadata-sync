"""
Builds `alldata` -- the big, per-(appl_id, hdp_id) compiled dataset that
joins study_lookup_table against mysql_merged + reporter_dqaudit, plus
study-level derived values (PI email, research network, HDP live/archived
status).

Mirrors mysql_code/python_scripts/HEAL_06_CompilebyStudy.py, whose own
notes describe this output as "analogous, but not identical" to the data
extract manually prepared for the Monday Studies Board.

All seven inputs are either built by this lambda already (mysql_merged,
reporter_dqaudit, study_lookup_table, engagement_flags) or pre-existing
real MySQL tables (pi_emails, progress_tracker, research_networks) --
nothing external, no manual input.

Dropped vs. the original script: every Stata-style column reorder and
row sort (moving study_hdp_status after study_hdp_id, sorting by
xstudy_id/fisc_yr) -- same call as mysql_merge.py/study_lookup_table.py,
since none of it affects a MySQL table's actual content. The
livearchkey.csv/xalldata_*.csv intermediate writes are dropped too --
internal staging frames, never read back by anything outside this one
script.

The original script's own pi_emails_key/res_net_key derivations are also
dropped, in favor of reusing tables/study_summary.py's _build_pi_emails/
_build_resnet directly: study_summary.py already computes the exact same
study-level PI-email/research-network values, and a side-by-side check of
the two independent implementations found each one had fixed a real edge
case the other hadn't (study_summary's version safely resolves an
ambiguous multi-email study to no email, where the original alldata logic
could silently produce duplicate rows per study; alldata's version
filtered "NULL"/"None" string sentinels and guaranteed one row per study,
where study_summary's didn't). Rather than keep two independently-drifting
copies, _build_resnet was updated in study_summary.py to pick up alldata's
two fixes, and both modules now share the one function.
"""

import logging
import os
import re

import pandas as pd

from db import clean_mysql_value, format_mysql_write_value
from tables.study_summary import _build_pi_emails, _build_resnet

logger = logging.getLogger(__name__)

ALLDATA_TABLE = "alldata"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _mysql_colname(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(name))
    return re.sub(r"_+", "_", s).strip("_").lower() or "col"


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


def _write_table(conn, df: pd.DataFrame, table: str) -> None:
    col_map = {c: _mysql_colname(c) for c in df.columns}
    mysql_cols = list(col_map.values())

    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    col_defs = ", ".join(f"`{c}` TEXT" for c in mysql_cols)
    cur.execute(f"CREATE TABLE `{table}` ({col_defs})")

    placeholders = ", ".join(["%s"] * len(mysql_cols))
    col_list = ", ".join(f"`{c}`" for c in mysql_cols)
    rows = [
        tuple(format_mysql_write_value(v) for v in row)
        for _, row in df.iterrows()
    ]
    cur.executemany(f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})", rows)
    conn.commit()
    cur.close()
    logger.info("Wrote %d rows to %s", len(rows), table)


# ---------------------------------------------------------------------------
# Build logic
# ---------------------------------------------------------------------------

def build_alldata(
    mysql_merged_df: pd.DataFrame,
    reporter_dqaudit_df: pd.DataFrame,
    study_lookup_df: pd.DataFrame,
    engagement_flags_df: pd.DataFrame,
    pi_emails_df: pd.DataFrame,
    progress_tracker_df: pd.DataFrame,
    research_networks_df: pd.DataFrame,
) -> pd.DataFrame:
    # --- 1. Merge source data ---
    xalldata = pd.concat([mysql_merged_df, reporter_dqaudit_df], ignore_index=True, sort=False)
    xalldata = xalldata.dropna(subset=["appl_id"])
    xalldata = xalldata[xalldata["appl_id"] != ""]

    # reporter_dqaudit has no compound_key of its own (no hdp_id concept
    # yet) -- drop whatever's there and recompute fresh across the
    # combined set so every row gets one uniformly.
    xalldata = xalldata.drop(columns=["compound_key"], errors="ignore")
    xalldata["compound_key"] = (
        xalldata["appl_id"].astype(str).str.replace(r"\.0$", "", regex=True)
        + "_" + xalldata["hdp_id"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    )
    logger.info("xalldata: %d rows", len(xalldata))

    xalldata_merge = xalldata.drop(columns=["appl_id"], errors="ignore")
    alldata = pd.merge(study_lookup_df, xalldata_merge, on="compound_key", how="left", indicator=True)
    alldata = alldata[alldata["_merge"] != "right_only"].drop(columns=["_merge"])

    eng = engagement_flags_df[["appl_id", "do_not_engage", "checklist_exempt_all"]]
    alldata = pd.merge(alldata, eng, on="appl_id", how="left", indicator=True)
    alldata = alldata[alldata["_merge"] != "right_only"].drop(columns=["_merge"])

    pi_emails = pi_emails_df[["appl_id", "pi_email"]]
    alldata = pd.merge(alldata, pi_emails, on="appl_id", how="left", indicator=True)
    alldata = alldata[alldata["_merge"] != "right_only"].drop(columns=["_merge"])
    logger.info("alldata (post source merges): %d rows", len(alldata))

    # --- 2. Derive study-level values ---
    # PI email and research network are shared with study_summary.py's own
    # study-level derivations (tables/study_summary.py's _build_pi_emails/
    # _build_resnet) -- same logic, not a second, divergent copy of it.
    # Both are keyed by study_most_recent_appl, matching study_lookup_df's
    # own convention (alldata has no independent notion of "the" study key
    # beyond what study_lookup_table already assigns).
    pi_emails_key = _build_pi_emails(study_lookup_df, pi_emails_df).rename(columns={"pi_email": "study_pi_email"})
    res_net_key = _build_resnet(study_lookup_df, research_networks_df).rename(columns={"res_net": "study_res_net"})
    live_arch_key = (
        progress_tracker_df[["hdp_id", "archived"]]
        .rename(columns={"hdp_id": "study_hdp_id", "archived": "study_hdp_status"})
    )

    # --- 3. Merge derived values ---
    final_df = alldata.copy()
    final_df = pd.merge(final_df, pi_emails_key, on="study_most_recent_appl", how="left")
    final_df = pd.merge(final_df, res_net_key, on="study_most_recent_appl", how="left")
    final_df = pd.merge(final_df, live_arch_key, on="study_hdp_id", how="left", indicator=True)
    final_df = final_df[final_df["_merge"] != "right_only"].drop(columns=["_merge"])

    final_df = final_df.rename(columns={
        "proj_title": "project_title_reporter",
        "project_title": "project_title_platform",
    })

    logger.info("alldata: %d rows", len(final_df))
    return final_df


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def update_alldata(
    conn,
    write_conn=None,
    mysql_merged_conn=None,
    reporter_dqaudit_conn=None,
    study_lookup_table_conn=None,
    engagement_flags_conn=None,
    research_networks_conn=None,
    target_table: str | None = None,
) -> dict:
    """
    conn: connection for reading pi_emails/progress_tracker -- genuinely
          external inputs this lambda doesn't build, always on DB_NAME.
    write_conn: connection for writing alldata itself. Defaults to `conn`.
    mysql_merged_conn: connection for reading `mysql_merged` -- ALWAYS
          tempspace regardless of DB_ENVIRONMENT. Defaults to `conn`, but
          callers should normally pass a tempspace-specific connection.
    reporter_dqaudit_conn / study_lookup_table_conn / engagement_flags_conn /
    research_networks_conn: connections for reading those four tables,
          which (like alldata itself) follow DB_ENVIRONMENT. Default to
          `conn` if not given.
    """
    write_conn = write_conn or conn
    mysql_merged_conn = mysql_merged_conn or conn
    reporter_dqaudit_conn = reporter_dqaudit_conn or conn
    study_lookup_table_conn = study_lookup_table_conn or conn
    engagement_flags_conn = engagement_flags_conn or conn
    research_networks_conn = research_networks_conn or conn

    if target_table is None:
        target_table = os.getenv("ALLDATA_TABLE_NAME", ALLDATA_TABLE)

    mysql_merged_table = os.getenv("MYSQL_MERGED_TABLE_NAME", "mysql_merged_test")
    reporter_dqaudit_table = os.getenv("REPORTER_DQAUDIT_TABLE_NAME", "reporter_dqaudit")
    study_lookup_table_name = os.getenv("STUDY_LOOKUP_TABLE_NAME", "study_lookup_table")
    engagement_flags_table = os.getenv("ENGAGEMENT_FLAGS_TABLE_NAME", "engagement_flags")
    research_networks_table = os.getenv("RESEARCH_NETWORKS_TABLE_NAME", "research_networks")

    mysql_merged_df = _read_table(mysql_merged_conn, mysql_merged_table)
    reporter_dqaudit_df = _read_table(reporter_dqaudit_conn, reporter_dqaudit_table)
    study_lookup_df = _read_table(study_lookup_table_conn, study_lookup_table_name)
    engagement_flags_df = _read_table(engagement_flags_conn, engagement_flags_table)
    pi_emails_df = _read_table(conn, "pi_emails")
    progress_tracker_df = _read_table(conn, "progress_tracker")
    research_networks_df = _read_table(research_networks_conn, research_networks_table)

    df = build_alldata(
        mysql_merged_df, reporter_dqaudit_df, study_lookup_df,
        engagement_flags_df, pi_emails_df, progress_tracker_df,
        research_networks_df,
    )
    _write_table(write_conn, df, target_table)

    return {"table": target_table, "rows_written": len(df)}

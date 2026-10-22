"""
Builds `reporter_dqaudit` -- new HEAL-related awards that share a project
serial number with something already tracked in MySQL, but aren't yet in
MySQL themselves.

Mirrors mysql_code/python_scripts/HEAL_03_DQAudit.py, with these
differences:
  - The "NIH Reporter serial-number extract" that script read from a
    manually-generated local CSV (heal_awards_reporter_sn_file) is now the
    `heal_awards_reporter_sn` table this same lambda already builds --
    that table's whole purpose (per its own module docstring) is to be
    this script's automated replacement for that manual extract.
  - The "old reporter_dqaudit export" it also read (reporter_dqaudit_file,
    producing df_combined) is dropped entirely: df_combined is written to
    an intermediate CSV and never used again anywhere in the original
    script -- the real pipeline continues from heal_by_sn_01 directly, the
    same kind of dead-code read as HEAL_02's unused pi_emails.
  - dqauditwave is dropped along with it; it only existed to support the
    df_combined merge.
  - Every Stata-style column/row reordering ("order ... after(...)", the
    sort_values calls) is dropped, same as mysql_merge.py -- it only
    mattered for CSV/Stata display, never for a MySQL table.
"""

import logging
import os

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

REPORTER_DQAUDIT_TABLE = "reporter_dqaudit"

# Award-serial-number grouping key used to isolate "newer, related" awards --
# unchanged from the original script's $stewards_id_vars.
STEWARDS_ID_VARS = ["proj_ser_num", "subproj_id", "proj_num_spl_sfx_code"]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _mysql_colname(name: str) -> str:
    import re
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(name))
    return re.sub(r"_+", "_", s).strip("_").lower() or "col"


def _read_table(conn, table: str) -> pd.DataFrame:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM `{table}`")
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
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
        tuple(None if pd.isna(v) else str(v) for v in row)
        for _, row in df.iterrows()
    ]
    cur.executemany(f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})", rows)
    conn.commit()
    cur.close()
    logger.info("Wrote %d rows to %s", len(rows), table)


def _build_mysql_applkey(nihtables_df: pd.DataFrame, research_networks_df: pd.DataFrame) -> pd.DataFrame:
    nih = nihtables_df[["appl_id", "heal_funded", "proj_ser_num"]]
    merged = pd.merge(nih, research_networks_df, on="appl_id", how="left")
    merged = merged.drop_duplicates()

    res_net_trimmed = merged["res_net"].fillna("").str.strip()
    merged["in_resnet"] = (res_net_trimmed != "").astype(int)

    merged = merged.rename(columns={"proj_ser_num": "in_rep_table_sernum"})
    keep_cols = ["appl_id", "heal_funded", "res_net", "in_resnet", "in_rep_table_sernum"]
    return merged[keep_cols]


def _clean_heal_sn(heal_sn_df: pd.DataFrame) -> pd.DataFrame:
    d = heal_sn_df.copy()
    if "heal_funded" in d.columns:
        d = d.drop(columns=["heal_funded"])
    d = d.drop_duplicates()

    date_cols = ["proj_end_date", "proj_strt_date", "bgt_strt", "bgt_end"]
    for col in date_cols:
        if col in d.columns:
            dt_series = pd.to_datetime(d[col], errors="coerce")
            d[col] = dt_series.dt.strftime("%Y-%m-%d").fillna(d[col])
    return d


# ---------------------------------------------------------------------------
# Public build function
# ---------------------------------------------------------------------------

def build_reporter_dqaudit(
    nihtables_df: pd.DataFrame,
    research_networks_df: pd.DataFrame,
    heal_sn_df: pd.DataFrame,
) -> pd.DataFrame:
    mysql_applkey = _build_mysql_applkey(nihtables_df, research_networks_df)
    heal_clean = _clean_heal_sn(heal_sn_df)

    # --- flag which serial-number-matched awards are already in MySQL ---
    df_merged = pd.merge(heal_clean, mysql_applkey, on="appl_id", how="outer", indicator=True)
    df_merged = df_merged[df_merged["_merge"] != "right_only"]
    df_merged["in_mysql"] = np.where(df_merged["_merge"] == "both", 1, 0)
    df_merged = df_merged.drop(columns=["_merge"])

    date_cols = ["awd_not_date", "bgt_end", "bgt_strt", "proj_end_date", "proj_strt_date"]
    for col in date_cols:
        if col in df_merged.columns:
            clean_date_str = df_merged[col].astype(str).str.slice(0, 10)
            df_merged[f"{col}_date"] = pd.to_datetime(clean_date_str, format="%Y-%m-%d", errors="coerce")

    df_merged["fisc_yr"] = pd.to_numeric(df_merged["fisc_yr"], errors="coerce")
    df_merged["award_year"] = df_merged["awd_not_date_date"].dt.year
    df_merged["award_year"] = df_merged["award_year"].fillna(df_merged["fisc_yr"])

    drop_cols = ["in_rep_table_sernum", "heal_funded", "res_net", "in_resnet"]
    df_merged = df_merged.drop(columns=[c for c in drop_cols if c in df_merged.columns])
    logger.info("df_merged (post serial-number match): %d rows", len(df_merged))

    # --- isolate newer awards for studies HEAL already partially tracks ---
    df_filtered = df_merged[df_merged["award_year"] >= 2018].copy()

    df_filtered["study"] = df_filtered.groupby(STEWARDS_ID_VARS, dropna=False).ngroup() + 1

    df_filtered["ever_in_mysql"] = df_filtered.groupby("study")["in_mysql"].transform("max")
    df_filtered["all_in_mysql"] = df_filtered.groupby("study")["in_mysql"].transform("min")
    # Drop studies never in MySQL (nothing to relate these to) or always in
    # MySQL (nothing new to surface).
    df_filtered = df_filtered[(df_filtered["ever_in_mysql"] != 0) & (df_filtered["all_in_mysql"] != 1)]
    df_filtered = df_filtered.drop(columns=["ever_in_mysql", "all_in_mysql"])

    # Drop awards for a study predating that study's earliest award already in MySQL.
    df_filtered["xearliest"] = df_filtered["award_year"].where(df_filtered["in_mysql"] == 1)
    df_filtered["earliest_award_inmysql"] = df_filtered.groupby("study")["xearliest"].transform("min")
    drop_condition = (df_filtered["in_mysql"] == 0) & (
        df_filtered["award_year"] < df_filtered["earliest_award_inmysql"]
    )
    df_filtered = df_filtered[~drop_condition]
    df_filtered = df_filtered.drop(columns=["xearliest", "earliest_award_inmysql"])
    logger.info("df_filtered (newer-awards-related-to-studies): %d rows", len(df_filtered))

    # --- final table: only the not-yet-in-MySQL awards ---
    df = df_filtered[df_filtered["in_mysql"] != 1].copy()
    df = df.drop(columns=["study", "award_year", "in_mysql"])

    date_drop_cols = [
        "awd_not_date_date", "bgt_end_date", "bgt_strt_date", "proj_end_date_date", "proj_strt_date_date",
    ]
    df = df.drop(columns=[c for c in date_drop_cols if c in df.columns])
    logger.info("reporter_dqaudit: %d rows", len(df))

    return df


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def update_reporter_dqaudit(
    conn,
    write_conn=None,
    nihtables_conn=None,
    research_networks_conn=None,
    heal_awards_reporter_sn_conn=None,
    target_table: str | None = None,
) -> dict:
    """
    conn: fallback connection for any param not given its own -- this table
          has no genuinely external inputs (everything it reads is itself
          built by this lambda), so this is only ever actually used if a
          caller omits one of the specific conn params below.
    write_conn: connection for writing reporter_dqaudit itself. Defaults to
          `conn`.
    nihtables_conn: connection for reading `nihtables` -- built by
          update_mysql_merge(), which ALWAYS writes it to tempspace
          regardless of DB_ENVIRONMENT (see tables/mysql_merge.py).
          Defaults to `conn`, but callers should normally pass a
          tempspace-specific connection here.
    research_networks_conn / heal_awards_reporter_sn_conn: connections for
          reading those two tables, which (like reporter_dqaudit itself)
          follow DB_ENVIRONMENT. Default to `conn` if not given.
    """
    write_conn = write_conn or conn
    nihtables_conn = nihtables_conn or conn
    research_networks_conn = research_networks_conn or conn
    heal_awards_reporter_sn_conn = heal_awards_reporter_sn_conn or conn

    if target_table is None:
        target_table = os.getenv("REPORTER_DQAUDIT_TABLE_NAME", REPORTER_DQAUDIT_TABLE)

    nihtables_table = os.getenv("NIHTABLES_TABLE_NAME", "nihtables_test")
    research_networks_table = os.getenv("RESEARCH_NETWORKS_TABLE_NAME", "research_networks")
    heal_sn_table = os.getenv("HEAL_AWARDS_REPORTER_SN_TABLE_NAME", "heal_awards_reporter_sn")

    nihtables_df = _read_table(nihtables_conn, nihtables_table)
    research_networks_df = _read_table(research_networks_conn, research_networks_table)
    heal_sn_df = _read_table(heal_awards_reporter_sn_conn, heal_sn_table)

    df = build_reporter_dqaudit(nihtables_df, research_networks_df, heal_sn_df)
    _write_table(write_conn, df, target_table)

    return {"table": target_table, "rows_written": len(df)}

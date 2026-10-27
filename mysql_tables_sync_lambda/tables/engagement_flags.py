"""
Builds `engagement_flags` -- per-appl_id "do not engage" and "checklist
exempt" indicators, propagated to every appl_id sharing a study.

Mirrors mysql_code/python_scripts/HEAL_05_EngagementTable.py exactly. All
three inputs (nihtables, research_networks, study_lookup_table) are
already built by this same lambda -- nothing external, no manual input.

Dropped vs. the original script: the pm_flags.csv intermediate write --
it's an internal staging frame, never read back by anything outside this
one script.
"""

import logging
import os
import re

import numpy as np
import pandas as pd

from db import clean_mysql_value, format_mysql_write_value

logger = logging.getLogger(__name__)

ENGAGEMENT_FLAGS_TABLE = "engagement_flags"

_AWARD_FIELDS = [
    "appl_id", "res_net", "res_net_override_flag", "act_code",
    "nih_noa_heal_lang", "nih_foa_heal_lang", "nih_aian", "proj_title",
    "fund_mech", "nih_noa_notes",
]


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

def build_engagement_flags(
    nihtables_df: pd.DataFrame,
    research_networks_df: pd.DataFrame,
    study_lookup_df: pd.DataFrame,
) -> pd.DataFrame:
    # --- 1. Create flags ---
    merged = pd.merge(nihtables_df, research_networks_df, on="appl_id", how="outer", indicator=True)
    merged = merged[merged["_merge"] != "right_only"].drop(columns=["_merge"])

    flags = merged[[c for c in _AWARD_FIELDS if c in merged.columns]].copy()

    flags["res_net"] = flags["res_net"].astype(str).str.upper().replace("NAN", np.nan)
    flags = flags.drop(columns=["res_net_override_flag"], errors="ignore")

    flags["do_not_engage"] = 0
    flags.loc[flags["act_code"].isin(["T90", "R90", "K99"]), "do_not_engage"] = 1

    heal_zero = flags["nih_foa_heal_lang"].isin(["0", 0]) | flags["nih_noa_heal_lang"].isin(["0", 0])
    flags.loc[heal_zero, "do_not_engage"] = 1

    flags.loc[flags["nih_aian"] == "1", "do_not_engage"] = 1

    is_medtech = flags["res_net"] == "MEDTECH"
    has_seedling = flags["proj_title"].astype(str).str.contains("Seedling", case=True, na=False)
    flags.loc[is_medtech & has_seedling, "do_not_engage"] = 1

    flags["checklist_exempt_all"] = 0
    flags.loc[flags["do_not_engage"] == 1, "checklist_exempt_all"] = 1
    flags.loc[flags["fund_mech"] == "SBIR/STTR", "checklist_exempt_all"] = 1
    flags.loc[flags["nih_noa_notes"] == "but encouraged to comply", "checklist_exempt_all"] = 1

    pm_flags = flags[["appl_id", "do_not_engage", "checklist_exempt_all"]].copy()
    logger.info("pm_flags: %d rows", len(pm_flags))

    # --- 2. Generate engagement table ---
    eng = pd.merge(study_lookup_df, pm_flags, on="appl_id", how="outer", indicator=True)
    eng = eng[eng["_merge"] != "right_only"].drop(columns=["_merge"])

    # Propagate: if any appl_id in a study is flagged, the whole study is flagged.
    for var in ["do_not_engage", "checklist_exempt_all"]:
        eng[var] = eng.groupby("xstudy_id")[var].transform("max")
        eng[var] = eng[var].fillna(0).astype(int)

    final = eng[["appl_id", "do_not_engage", "checklist_exempt_all"]].drop_duplicates()
    if not final["appl_id"].is_unique:
        logger.warning("engagement_flags: appl_id is NOT unique in the final table")
    logger.info("engagement_flags: %d rows", len(final))
    return final


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def update_engagement_flags(
    conn,
    write_conn=None,
    nihtables_conn=None,
    research_networks_conn=None,
    study_lookup_table_conn=None,
    target_table: str | None = None,
) -> dict:
    """
    conn: fallback connection for any param not given its own -- every
          source here is itself built by this lambda, so this is only
          ever actually used if a caller omits one of the specific conn
          params below.
    write_conn: connection for writing engagement_flags itself. Defaults
          to `conn`.
    nihtables_conn: connection for reading `nihtables` -- built by
          update_mysql_merge(), which ALWAYS writes it to tempspace
          regardless of DB_ENVIRONMENT. Defaults to `conn`, but callers
          should normally pass a tempspace-specific connection.
    research_networks_conn / study_lookup_table_conn: connections for
          reading those two tables, which (like engagement_flags itself)
          follow DB_ENVIRONMENT. Default to `conn` if not given.
    """
    write_conn = write_conn or conn
    nihtables_conn = nihtables_conn or conn
    research_networks_conn = research_networks_conn or conn
    study_lookup_table_conn = study_lookup_table_conn or conn

    if target_table is None:
        target_table = os.getenv("ENGAGEMENT_FLAGS_TABLE_NAME", ENGAGEMENT_FLAGS_TABLE)

    nihtables_table = os.getenv("NIHTABLES_TABLE_NAME", "nihtables_test")
    research_networks_table = os.getenv("RESEARCH_NETWORKS_TABLE_NAME", "research_networks")
    study_lookup_table_name = os.getenv("STUDY_LOOKUP_TABLE_NAME", "study_lookup_table")

    nihtables_df = _read_table(nihtables_conn, nihtables_table)
    research_networks_df = _read_table(research_networks_conn, research_networks_table)
    study_lookup_df = _read_table(study_lookup_table_conn, study_lookup_table_name)

    df = build_engagement_flags(nihtables_df, research_networks_df, study_lookup_df)
    _write_table(write_conn, df, target_table)

    return {"table": target_table, "rows_written": len(df)}

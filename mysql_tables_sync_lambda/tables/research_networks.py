"""
Rebuilds the `research_networks` table: appl_id -> res_net (research
network code).

Mirrors mysql_code/python_scripts/HEAL_01_ResNetDocTables.py's second half
(the "update_research_networks.sql" section) exactly, with two differences:
  - The Google-Drive-sourced reference workbook is read from S3 now,
    instead of a local Input/ copy a person placed there by hand.
  - `awards` is read directly from MySQL, instead of a DBeaver-exported
    "awards_<date>.csv".
HEAL_01's first half (reformatting the workbook's tabs into intermediate
temp_res_net_*.csv files) isn't needed at all -- that existed only to get
the workbook into a shape pd.read_csv could consume; reading the workbook
directly with pandas skips that step entirely.

study_summary.py's _build_resnet() only ever selects appl_id/res_net from
this table, so res_net_override_flag being along for the ride is harmless.
"""

import logging
import os
import re

import pandas as pd

logger = logging.getLogger(__name__)

REF_WORKBOOK_NAME = "HEAL_research_networks_ref_table_for_MySQL.xlsx"
REF_SHEET = "ref_table"
OVERRIDE_SHEET = "value_overrides_byappl"


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
    if "appl_id" in df.columns:
        df["appl_id"] = df["appl_id"].astype(str)
    return df


def _clean_text(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse line breaks/extra whitespace in every cell; drop empty rows/cols."""
    def clean_cell(val):
        if pd.isna(val):
            return val
        return " ".join(str(val).replace("\n", " ").replace("\r", " ").split())

    df = df.apply(lambda s: s.map(clean_cell))
    if len(df) == 0:
        # dropna(axis=1, how="all") is vacuously true over zero rows, which
        # would strip every column -- including appl_id/res_net that
        # downstream code requires -- if a sheet (e.g. the overrides sheet,
        # when there happen to be no active overrides) has headers but no
        # data rows. Nothing meaningful to drop either way with zero rows.
        return df
    return df.dropna(axis=1, how="all").dropna(axis=0, how="all")


def _write_research_networks(conn, df: pd.DataFrame, table: str) -> None:
    col_map = {c: _mysql_colname(c) for c in df.columns}
    mysql_cols = list(col_map.values())

    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    col_defs = ", ".join(f"`{c}` TEXT" for c in mysql_cols)
    cur.execute(f"CREATE TABLE `{table}` ({col_defs})")

    placeholders = ", ".join(["%s"] * len(mysql_cols))
    col_list = ", ".join(f"`{c}`" for c in mysql_cols)
    rows = [
        tuple(None if (pd.isna(v) or str(v).strip() == "") else str(v) for v in row)
        for _, row in df.iterrows()
    ]
    cur.executemany(f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})", rows)
    conn.commit()
    cur.close()
    logger.info("Wrote %d rows to %s", len(rows), table)


# ---------------------------------------------------------------------------
# Build logic
# ---------------------------------------------------------------------------

def build_research_networks(awards_df: pd.DataFrame, ref_source) -> pd.DataFrame:
    """
    awards_df: the `awards` MySQL table, read with at least appl_id/res_prg.
    ref_source: a sources.FileSource (S3ExcelSource/LocalExcelSource) pointed
        at the directory/prefix holding REF_WORKBOOK_NAME.

    Returns one row per appl_id: appl_id, res_net, res_net_override_flag.
    """
    ref_table = _clean_text(ref_source.read_excel(REF_WORKBOOK_NAME, sheet_name=REF_SHEET))
    override = _clean_text(ref_source.read_excel(REF_WORKBOOK_NAME, sheet_name=OVERRIDE_SHEET))

    # Excel reads numeric appl_ids as float -> "10684372.0"; strip the .0 so
    # they match awards' string-typed appl_id.
    override["appl_id"] = override["appl_id"].astype(str).str.replace(r"\.0$", "", regex=True).str.strip()

    result = awards_df[["appl_id", "res_prg"]].copy()
    result = result.merge(ref_table[["res_prg", "res_net"]], on="res_prg", how="left")
    result = result.drop(columns=["res_prg"])

    result = result.merge(
        override[["appl_id", "res_net"]], on="appl_id", how="left", suffixes=("", "_override")
    )
    result["res_net"] = result["res_net_override"].combine_first(result["res_net"])
    result["res_net_override_flag"] = result["appl_id"].isin(override["appl_id"]).astype(int)
    result = result.drop(columns=["res_net_override"]).drop_duplicates()

    logger.info("research_networks: %d rows (%d overridden)", len(result), result["res_net_override_flag"].sum())
    return result


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def update_research_networks(
    read_conn, write_conn, ref_source, target_table: str | None = None
) -> dict:
    """
    read_conn: connection for reading `awards` (whatever database it's in today).
    write_conn: connection for writing the result (its own database, per
        RESEARCH_NETWORKS_DB_NAME -- may be the same connection as read_conn,
        or a different one if the two tables live in different databases).
    ref_source: see build_research_networks().
    """
    target_table = target_table or os.getenv("RESEARCH_NETWORKS_TABLE_NAME", "research_networks_test")

    awards_df = _read_table(read_conn, "awards")
    result = build_research_networks(awards_df, ref_source)
    _write_research_networks(write_conn, result, target_table)

    return {"table": target_table, "rows": len(result)}

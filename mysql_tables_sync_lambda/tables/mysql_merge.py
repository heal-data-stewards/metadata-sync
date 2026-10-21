"""
Builds `nihtables` and `mysql_merged` -- the core cross-source merge this
whole pipeline is built on.

Mirrors mysql_code/python_scripts/HEAL_02_ImportMerge.py, with these
differences:
  - Reads reporter/awards/progress_tracker/research_networks directly from
    MySQL instead of DBeaver-exported, date-stamped CSVs.
  - correct_foanoa_values.csv (a standing manual correction file) comes
    from S3 instead of a local Input/ copy.
  - pi_emails is read by the original script but never actually used
    anywhere in it (checked: no reference to it after the initial
    read/log) -- dropped here, not a real dependency.
  - Column-order reshuffling (Stata-style "order" statements) is dropped;
    it only mattered for CSV/Stata display, not for a MySQL table.
  - Both tables are deliberately *not* in TABLE_PRODUCTION_DB -- they're
    pure pipeline-internal staging data nothing outside this exact chain
    reads (confirmed: no SQL/_read_table reference to either name anywhere
    else in the codebase), so they always resolve to tempspace regardless
    of DB_ENVIRONMENT, in every environment including production.
  - progress_tracker is the mirror case: it's built and written by a
    separate lambda directly to healstudies, with no tempspace copy ever,
    so reading it always targets healstudies regardless of DB_ENVIRONMENT
    too -- it never goes through TABLE_PRODUCTION_DB/_resolve_db_name at
    all, since those only choose between tempspace and healstudies for
    tables *this* lambda owns.

Produced together in one build() call rather than as two independent
registry entries, since mysql_merged needs nihtables as an intermediate
step anyway -- two separate entries would each redundantly recompute the
whole pipeline (including re-reading every source table) under
{"table": "all"}.
"""

import logging
import re

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Both tables always resolve to tempspace regardless of DB_ENVIRONMENT (see
# module docstring), so unlike every other table here, the database itself
# never distinguishes a test run from a production one. The table-NAME axis
# still needs to, same as everywhere else, or a test invocation would
# clobber whatever a production invocation last wrote (or vice versa) --
# both landing in the exact same database under the exact same name.
NIHTABLES_TABLE = "nihtables_test"
MYSQL_MERGED_TABLE = "mysql_merged_test"


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


def _clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse embedded line breaks to a space and strip whitespace, every
    object column -- matches the cleaning HEAL_02 applies to every table it
    reads, before any of the merge logic runs."""
    df = df.copy()
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).where(df[col].notna(), df[col])
        df[col] = df[col].str.replace(r"[\r\n]+", " ", regex=True).str.strip()
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


# ---------------------------------------------------------------------------
# Step 2: clean progress_tracker -- CTN/bad-projnum/entity_type classification
# ---------------------------------------------------------------------------

def _build_progress_tracker_clean(df_prog_trkr: pd.DataFrame) -> pd.DataFrame:
    d = df_prog_trkr[df_prog_trkr["appl_id"].astype(str).str.strip() != ""].dropna(subset=["appl_id"]).copy()
    d["mds_flag_bad_projnum"] = 0

    d["mds_ctn_flag"] = d["project_num"].str.contains("^CTN", regex=True, na=False).astype(int)
    d["mds_ctn_number"] = np.where(d["mds_ctn_flag"] == 1, d["project_num"], None)
    # CTN protocol number lives in project_num/appl_id in MDS; once captured
    # into mds_ctn_number above, both get blanked for CTN rows.
    d.loc[d["mds_ctn_flag"] == 1, ["project_num", "appl_id"]] = ""

    num_dashes = d["project_num"].str.count("-")
    d.loc[num_dashes > 1, "mds_flag_bad_projnum"] = 1
    d["mds_bad_projnum"] = np.where(num_dashes > 1, d["project_num"], None)
    d.loc[d["project_num"].str.startswith("ICPSR", na=False), "mds_flag_bad_projnum"] = 1

    d["num_hdp_by_appl"] = d.groupby("appl_id")["hdp_id"].transform("count")
    d.loc[d["appl_id"] == "0", "num_hdp_by_appl"] = 0
    d.loc[d["appl_id"] == "", "num_hdp_by_appl"] = np.nan

    d["entity_type"] = "Study"
    d.loc[d["mds_ctn_flag"] == 1, "entity_type"] = "CTN"
    d.loc[d["mds_flag_bad_projnum"] == 1, "entity_type"] = "Other"
    d.loc[(d["appl_id"] == "0") & (d["mds_ctn_flag"] != 1), "entity_type"] = "Other"

    # Refined after the catch-all "Other"/"CTN" above, so these win.
    pn = d["project_num"]
    d.loc[pn.str.startswith("ZIA", na=False), "entity_type"] = "Intramural"
    d.loc[pn.str.startswith("ICPSR", na=False), "entity_type"] = "ICPSR"
    d.loc[pn.str.startswith(("75N", "2712"), na=False), "entity_type"] = "Contract"

    d.loc[d["appl_id"] == "0", "appl_id"] = ""
    logger.info("progress_tracker cleaned: %d rows", len(d))
    return d


# ---------------------------------------------------------------------------
# Step 3: clean awards -- foa/noa correction merge
# ---------------------------------------------------------------------------

def _build_awards_clean(df_awards: pd.DataFrame, correct_foanoa: pd.DataFrame) -> pd.DataFrame:
    d = df_awards[df_awards["appl_id"].astype(str).str.strip() != ""].copy()

    # "NULL" is a literal sentinel string here, not a real SQL/pandas NULL --
    # matches HEAL_02 exactly, since downstream consumers may check for it.
    for acr in ["foa", "noa"]:
        col = f"nih_{acr}_heal_lang"
        d.loc[d[col].isna() | (d[col] == ""), col] = "NULL"
        d = d.rename(columns={col: f"x{col}"})

    d = pd.merge(d, correct_foanoa, on="appl_id", how="left", indicator=True)
    d = d[d["_merge"] != "right_only"]  # correction-file-only rows don't apply to any award
    d = d.drop(columns=["_merge"] + [c for c in d.columns if c.startswith("xnih_")])

    for acr in ["foa", "noa"]:
        col = f"nih_{acr}_heal_lang"
        d.loc[d[col].isna() | (d[col] == ""), col] = "NULL"

    logger.info("awards cleaned: %d rows", len(d))
    return d


# ---------------------------------------------------------------------------
# Steps 4-5: merge everything, final cleanup
# ---------------------------------------------------------------------------

def build_mysql_merge(
    reporter_df: pd.DataFrame,
    awards_df: pd.DataFrame,
    progress_tracker_df: pd.DataFrame,
    research_networks_df: pd.DataFrame,
    correct_foanoa_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (nihtables, mysql_merged)."""
    reporter_df = _clean_text_columns(reporter_df)
    awards_df = _clean_text_columns(awards_df)
    progress_tracker_df = _clean_text_columns(progress_tracker_df)
    research_networks_df = _clean_text_columns(research_networks_df)

    prog_trkr_clean = _build_progress_tracker_clean(progress_tracker_df)
    awards_clean = _build_awards_clean(awards_df, correct_foanoa_df)

    # --- reporter <-> awards -> nihtables ---
    reporter_nonblank = reporter_df[reporter_df["appl_id"].astype(str).str.strip() != ""]
    nihtables = pd.merge(
        reporter_nonblank, awards_clean, on="appl_id", how="outer", indicator="merge_reporter_awards"
    )
    nihtables = nihtables[nihtables["appl_id"].astype(str).str.strip() != ""]
    nihtables["merge_reporter_awards"] = nihtables["merge_reporter_awards"].map({
        "left_only": "In reporter only",
        "right_only": "In awards only",
        "both": "In both tables",
    })
    logger.info("nihtables: %d rows", len(nihtables))

    # --- nihtables <-> progress_tracker ---
    dataset = pd.merge(nihtables, prog_trkr_clean, on="appl_id", how="outer", indicator="merge_awards_mds")
    dataset["merge_awards_mds"] = dataset["merge_awards_mds"].map({
        "left_only": "In MySQL only",
        "right_only": "In MDS only",
        "both": "In both databases",
    })

    # --- dataset <-> research_networks ---
    res_net = research_networks_df.dropna(subset=["appl_id"]) if "appl_id" in research_networks_df.columns else research_networks_df
    dataset2 = pd.merge(dataset, res_net, on="appl_id", how="left", indicator="_merge")
    dataset2 = dataset2[dataset2["_merge"] != "right_only"]
    dataset2 = dataset2.drop(columns=["_merge", "res_net_override_flag"], errors="ignore")

    if "res_net" in dataset2.columns:
        dataset2["res_net"] = dataset2["res_net"].str.upper()
        dataset2.loc[dataset2.get("act_code", pd.Series(dtype=object)).isin(["N01", "N02"]), "entity_type"] = "Contract"
        dataset2.loc[dataset2["res_net"] == "PSPP", "entity_type"] = "PSPP"
        dataset2.loc[dataset2["res_net"] == "CTN", "entity_type"] = "CTN"
        dataset2["entity_type"] = dataset2["entity_type"].replace("", "Study").fillna("Study")
    logger.info("dataset2 (post research_networks merge): %d rows", len(dataset2))

    # --- final cleanup -> mysql_merged ---
    merged = dataset2.copy()
    merged["supplement_flag"] = np.where(merged["proj_num"].str[-2:-1] == "S", 1, np.nan)
    merged["fisc_yr"] = pd.to_numeric(merged["fisc_yr"], errors="coerce")

    for var in ["awd_not_date", "bgt_end", "bgt_strt", "proj_end_date", "proj_strt_date"]:
        if var in merged.columns:
            merged[f"{var}_date"] = pd.to_datetime(merged[var].astype(str).str[:10], errors="coerce")

    merged["compound_key"] = (
        merged["appl_id"].astype(str) + "_"
        + merged["hdp_id"].fillna("").astype(str).str.replace(r"\.0$", "", regex=True)
    )

    merged = merged.rename(columns={
        "Registering user": "registeringuser",
        "clinical_trials_study_ID": "clinical_trials_study_id",
    })
    logger.info("mysql_merged: %d rows", len(merged))

    return nihtables, merged


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def update_mysql_merge(
    conn,
    ref_source,
    write_conn=None,
    reporter_conn=None,
    research_networks_conn=None,
    progress_tracker_conn=None,
    nihtables_table: str | None = None,
    mysql_merged_table: str | None = None,
) -> dict:
    """
    conn: connection for reading awards -- a genuinely external input this
          lambda doesn't build, always on DB_NAME.
    write_conn: connection for writing nihtables/mysql_merged themselves.
          Defaults to `conn` -- but these two always resolve to tempspace
          (see module docstring), so callers should normally pass a
          tempspace-specific connection here rather than rely on the
          default, unless DB_NAME already *is* tempspace.
    reporter_conn / research_networks_conn: connections for reading those
          two tables, which this same lambda builds and which can live in
          a different database than `conn` under DB_ENVIRONMENT. Default
          to `conn` if not given, same pattern as study_summary.py.
    progress_tracker_conn: connection for reading progress_tracker, which a
          separate lambda builds and writes to healstudies only -- there is
          no tempspace copy, in test or production, so this never follows
          DB_ENVIRONMENT. Defaults to `conn` if not given, but callers
          should normally pass a healstudies-specific connection.
    ref_source: a sources.FileSource pointed at the directory/prefix
          holding correct_foanoa_values.csv.
    """
    import os
    write_conn = write_conn or conn
    reporter_conn = reporter_conn or conn
    research_networks_conn = research_networks_conn or conn
    progress_tracker_conn = progress_tracker_conn or conn

    if nihtables_table is None:
        nihtables_table = os.getenv("NIHTABLES_TABLE_NAME", NIHTABLES_TABLE)
    if mysql_merged_table is None:
        mysql_merged_table = os.getenv("MYSQL_MERGED_TABLE_NAME", MYSQL_MERGED_TABLE)

    reporter_df = _read_table(reporter_conn, "reporter")
    awards_df = _read_table(conn, "awards")
    progress_tracker_df = _read_table(progress_tracker_conn, "progress_tracker")
    research_networks_df = _read_table(research_networks_conn, "research_networks")
    correct_foanoa_df = ref_source.read_csv(
        "correct_foanoa_values.csv", dtype=str, usecols=["appl_id", "nih_foa_heal_lang", "nih_noa_heal_lang"]
    )

    nihtables, mysql_merged = build_mysql_merge(
        reporter_df, awards_df, progress_tracker_df, research_networks_df, correct_foanoa_df
    )

    _write_table(write_conn, nihtables, nihtables_table)
    _write_table(write_conn, mysql_merged, mysql_merged_table)

    return {
        "nihtables_table": nihtables_table,
        "nihtables_rows": len(nihtables),
        "mysql_merged_table": mysql_merged_table,
        "mysql_merged_rows": len(mysql_merged),
    }

"""
Builds `study_lookup_table` -- the per-award-to-study grouping table -- plus
a human-review workflow for the handful of genuinely ambiguous pairings
("manual matches") that automatic heuristics can't resolve on their own.

Mirrors mysql_code/python_scripts/HEAL_04_StudyTable.py (already a Python
port of the original Stata script). That script's workflow -- run once,
dump a template of ambiguous pairs, stop and wait for a human to fill it
in, re-run with --manual-matches -- doesn't fit a one-shot Lambda
invocation, for a specific reason:

xstudy_id_stewards/zstudy_id are NOT stable identifiers across runs --
they come from .groupby(...).ngroup(), which numbers groups by first-seen
order in that run's data, so adding or removing even one row anywhere
upstream can renumber everything. A manual-matches file that recorded
decisions by those numbers would only be valid for the exact run that
produced it.

So instead, every decision is keyed by the one thing that IS stable
across runs: the (appl_id, zappl_id, zhdp_id) triple actually being
compared. A persistent, Lambda-managed ledger (in S3, alongside the other
manual-input files) remembers every decision ever made by that key, in
both directions -- match=1 AND match=0 ("these are not the same study" is
just as permanent a decision as "yes", and its match_notes explaining why
is the record of that decision, not an absence of one). Verified against
six historical dated sheets (Jan-Sept 2026): that key was unique and
stable in every one, and of 113 reviewed pairs from one run to the next,
zero decisions changed and zero were dropped -- only 6 pairs were
genuinely new.

Two entry points, split because finalizing study_lookup_table reshuffles
every xstudy_id in it -- that should happen when a human decides to run
it, not silently on every scheduled sync:
  - generate_match_candidates(): runs the automatic classification (steps
    1-5d below), folds any newly-decided rows from the current pending
    file into the ledger, archives that pending file, then computes a
    fresh one containing only genuinely new, undecided pairs.
  - update_study_lookup_table(): runs the same steps 1-5d, applies the
    ENTIRE ledger (not just what's new), and finishes steps 5e-10 to
    produce the real table. Anything never decided (not in the ledger,
    never resolved automatically) is simply left out -- exactly like the
    original script's `.notna()` filter in step 10 -- never an error,
    just not grouped yet.

Both entry points share _build_candidates() for steps 1-5d so that logic
isn't duplicated between them.

Also fixed vs. the original script: the z-side (bad_nonmiss, the
hdp-bearing comparison candidates) never carried the study's own
study_name/project_title -- a reviewer saw the appl_id side's study
identity but nothing to compare it against on the zhdp_id side. Both are
now z-prefixed through like every other z-column.

Dropped vs. the original script: study_table_dd, a static 7-row
column-documentation table it also wrote. Nothing downstream reads it as
a MySQL table -- it's pure documentation, out of scope here.
"""

import logging
import os
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from db import clean_mysql_value, format_mysql_write_value

logger = logging.getLogger(__name__)

STEWARDS_ID_VARS = ["proj_ser_num", "subproj_id", "proj_num_spl_sfx_code"]
_STANDARD_ENTITY_TYPES = {"Study", "CTN"}

PENDING_FILE = "study_manual_matches_pending.csv"
LEDGER_FILE = "study_manual_matches_history/resolved_matches_ledger.csv"

_LEDGER_KEY = ["appl_id", "zappl_id", "zhdp_id"]
_LEDGER_COLUMNS = [
    "appl_id", "zappl_id", "zhdp_id", "hdp_id", "match", "match_notes",
    "proj_title", "zproj_title", "study_name", "zstudy_name",
    "project_title", "zproject_title", "act_code", "zact_code", "decided_date",
]

# The set of columns shown to a human reviewer and carried through step 5e.
# Mirrors HEAL_04_StudyTable.py's _template_cols, with zstudy_name/
# zproject_title added (see module docstring).
_TEMPLATE_COLS = [
    "study_id", "xstudy_id_stewards", "appl_id", "hdp_id", "archived",
    "num_appl_by_xstudyidstewards", "num_hdp_by_appl", "num_hdp_by_xstudyidstewards", "compound_key",
    "proj_ser_num", "subproj_id", "proj_num_spl_sfx_code", "proj_abs", "act_code",
    "fund_ic", "ic_fund_code", "ic_fund_yr", "awd_not_date",
    "bgt_end", "bgt_strt", "fisc_yr", "fund_mech", "phr_text",
    "proj_end_date", "proj_num", "proj_strt_date", "proj_title", "res_prg",
    "guid_type", "study_name", "project_num", "project_title",
    "zstudy_id", "zappl_id", "zhdp_id", "zarchived", "zproj_ser_num", "zproj_num_spl_sfx_code",
    "zact_code", "zbgt_end_date", "zbgt_strt_date", "zproj_num_spl_ty_code", "zproj_title", "zin_mysql",
    "zstudy_name", "zproject_title",
    "act_code_match", "typ_match", "title_match",
    "count_actcode_matches", "count_typ_matches", "xappl_pairs", "tag", "num_appl_pairs",
]


# ---------------------------------------------------------------------------
# Small helpers (ported from HEAL_04_StudyTable.py)
# ---------------------------------------------------------------------------

def normalize_id_series(series: pd.Series) -> pd.Series:
    series = series.astype(str).str.strip()
    series = series.replace({"nan": "", "NaN": "", ".": ""})
    series = series.str.replace(r"^(\d+)\.0$", r"\1", regex=True)
    return series


def parse_ymd_date(value):
    if pd.isna(value):
        return pd.NaT
    if isinstance(value, (int, np.integer)):
        value = str(value).zfill(8)
    value = str(value).strip()
    if value == "" or value in {"0", "nan", "NaN", "."}:
        return pd.NaT
    for fmt in ["%Y%m%d", "%Y-%m-%d", "%Y/%m/%d", "%Y%m%d%H%M%S"]:
        try:
            return pd.to_datetime(value, format=fmt, errors="raise")
        except (ValueError, TypeError):
            continue
    return pd.to_datetime(value, errors="coerce")


def is_empty_series(series: pd.Series) -> pd.Series:
    return series.isin(["0", ""]) | series.isna()


def clean_string_columns(df: pd.DataFrame, columns) -> pd.DataFrame:
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({"nan": "", "NaN": ""})
    return df


def group_ids(df: pd.DataFrame, cols, name: str) -> pd.Series:
    return df.groupby(cols, dropna=False).ngroup().add(1).astype("Int64")


def process_other_entity_types(other_df: pd.DataFrame, next_study_id: int) -> pd.DataFrame:
    """Build study_lookup_table rows for non-Study/non-CTN entity types.

    Contract/PSPP rows that have an appl_id are treated as single-appl studies.
    Intramural/ICPSR/Other rows without appl_ids are keyed by hdp_id alone.
    """
    rows = []
    for _, row in other_df.iterrows():
        appl = str(row.get("appl_id", "") or "").strip()
        if appl in ("", "nan", "NaN", "0"):
            appl = ""
        hdp = str(row.get("hdp_id", "") or "").strip()
        if hdp in ("nan", "NaN"):
            hdp = ""
        rows.append({
            "appl_id": appl,
            "xstudy_id": str(next_study_id),
            "study_hdp_id": hdp,
            "study_hdp_id_appl": appl if hdp else "",
            "study_most_recent_appl": appl,
            "study_first_appl": appl,
            "compound_key": f"{appl}_{hdp}",
        })
        next_study_id += 1

    if not rows:
        return pd.DataFrame(columns=[
            "appl_id", "xstudy_id", "study_hdp_id", "study_hdp_id_appl",
            "study_most_recent_appl", "study_first_appl", "compound_key",
        ])
    return pd.DataFrame(rows)


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
        df["appl_id"] = normalize_id_series(df["appl_id"])
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


def _str_eq(a: pd.Series, b: pd.Series) -> pd.Series:
    """String equality that treats NaN==NaN as True, matching Stata's ''=='' behavior."""
    return (a == b) | (a.isna() & b.isna())


def _norm_title(s: pd.Series) -> pd.Series:
    return s.str.strip().str.replace(r"\s+", " ", regex=True)


# ---------------------------------------------------------------------------
# Steps 1-5d: shared by both entry points
# ---------------------------------------------------------------------------

def _build_candidates(mysql_merged_df: pd.DataFrame, reporter_dqaudit_df: pd.DataFrame) -> dict:
    mysql_df = mysql_merged_df.copy()
    mysql_df.columns = [c.strip() for c in mysql_df.columns]
    mysql_df = mysql_df.drop(columns=[c for c in mysql_df.columns if c == "Unnamed: 0"], errors="ignore")

    # Non-standard entity types (Contract/PSPP/Intramural/ICPSR/Other) are
    # handled separately -- pulled out before the appl_id filter so rows
    # with no appl_id (Intramural/ZIA, ICPSR, Other) aren't lost.
    other_df = mysql_df[~mysql_df["entity_type"].fillna("Study").isin(_STANDARD_ENTITY_TYPES)].copy()
    mysql_df = mysql_df[mysql_df["entity_type"].fillna("Study").isin(_STANDARD_ENTITY_TYPES)].copy()

    mysql_df = mysql_df[~is_empty_series(mysql_df.get("appl_id", pd.Series(dtype=str)))].copy()
    mysql_df["appl_id"] = mysql_df["appl_id"].astype(str)

    appls_in_reporter = mysql_df[["appl_id"]].drop_duplicates().assign(in_reporter_table=1)

    reporter_dqaudit_df = reporter_dqaudit_df.copy()
    reporter_dqaudit_df["appl_id"] = reporter_dqaudit_df["appl_id"].astype(str)
    reporter_dqaudit_df = reporter_dqaudit_df[~reporter_dqaudit_df["appl_id"].isin(appls_in_reporter["appl_id"])]
    if "fisc_yr" in reporter_dqaudit_df.columns:
        reporter_dqaudit_df["fisc_yr"] = pd.to_numeric(reporter_dqaudit_df["fisc_yr"], errors="coerce").astype("Int64")

    mysql_df["in_mysql"] = 1
    mysql_df["xhas_subproj_num"] = mysql_df["subproj_id"].apply(lambda v: 1 if pd.notna(v) and str(v) != "" else np.nan)
    mysql_df["has_subproj_num_by_sernum"] = mysql_df.groupby("proj_ser_num")["xhas_subproj_num"].transform("max")
    mysql_df = mysql_df.drop(columns=["xhas_subproj_num"])

    combined_df = pd.concat([mysql_df, reporter_dqaudit_df], ignore_index=True, sort=False)
    combined_df["in_mysql"] = combined_df["in_mysql"].fillna(0).astype(int)
    combined_df["appl_id"] = combined_df["appl_id"].astype(str)

    combined_df = clean_string_columns(combined_df, STEWARDS_ID_VARS)
    combined_df["xstudy_id_stewards"] = group_ids(combined_df, STEWARDS_ID_VARS, "xstudy_id_stewards")

    has_hdp = combined_df["hdp_id"].notna() & (combined_df["hdp_id"].astype(str) != "")
    combined_df["study_id"] = group_ids(combined_df[has_hdp], ["xstudy_id_stewards", "hdp_id"], "study_id").reindex(combined_df.index)
    combined_df["study_id"] = combined_df["study_id"].astype("Int64")

    sis_count = (
        combined_df[["xstudy_id_stewards", "appl_id"]].drop_duplicates()
        .groupby("xstudy_id_stewards", dropna=False).size().reset_index(name="num_appl_by_xstudyidstewards")
    )
    hdp_cols = combined_df[["xstudy_id_stewards", "hdp_id", "archived"]].copy().drop_duplicates()
    hdp_cols["hdp_id_cnt"] = hdp_cols["hdp_id"].replace({"": pd.NA}).notna().astype(int)
    hdpid_count = (
        hdp_cols.groupby("xstudy_id_stewards", dropna=False)
        .agg(
            num_hdp_by_xstudyidstewards=("hdp_id_cnt", "sum"),
            num_live_hdps=("archived", lambda x: (x == "live").sum()),
            num_arch_hdps=("archived", lambda x: (x == "archived").sum()),
        ).reset_index()
    )
    combined_df = combined_df.merge(sis_count, on="xstudy_id_stewards", how="left")
    combined_df = combined_df.merge(hdpid_count, on="xstudy_id_stewards", how="left")
    combined_df["compound_key"] = combined_df["appl_id"].fillna("").astype(str) + "_" + combined_df["hdp_id"].fillna("").astype(str)

    if "num_hdp_by_appl" not in combined_df.columns:
        combined_df["num_hdp_by_appl"] = 0
    combined_df["num_hdp_by_appl"] = pd.to_numeric(combined_df["num_hdp_by_appl"], errors="coerce").astype("Int64").fillna(0)

    mysql_hasappls_df = combined_df.copy()
    logger.info("mysql_hasappls_df: %d rows", len(mysql_hasappls_df))

    ### STEP 2: split by hdp count per xstudy_id_stewards group
    hdpid0 = mysql_hasappls_df[mysql_hasappls_df["num_hdp_by_xstudyidstewards"] == 0].copy()
    hdpid1 = mysql_hasappls_df[mysql_hasappls_df["num_hdp_by_xstudyidstewards"] == 1].copy()

    studyidgood1 = mysql_hasappls_df[
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 0) &
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 1) &
        (mysql_hasappls_df["num_appl_by_xstudyidstewards"] == mysql_hasappls_df["num_hdp_by_xstudyidstewards"]) &
        (mysql_hasappls_df["num_hdp_by_appl"] == 1)
    ][["study_id", "appl_id", "hdp_id", "compound_key"]].copy()

    studyidgood2 = mysql_hasappls_df[
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 0) &
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 1) &
        (mysql_hasappls_df["num_appl_by_xstudyidstewards"] == 1)
    ][["study_id", "appl_id", "hdp_id", "compound_key"]].copy()

    studyidbad = mysql_hasappls_df[
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 0) &
        (mysql_hasappls_df["num_hdp_by_xstudyidstewards"] != 1) &
        ~(
            (mysql_hasappls_df["num_appl_by_xstudyidstewards"] == mysql_hasappls_df["num_hdp_by_xstudyidstewards"]) &
            (mysql_hasappls_df["num_hdp_by_appl"] == 1)
        ) &
        (mysql_hasappls_df["num_appl_by_xstudyidstewards"] != 1)
    ].copy()

    ### STEP 3: assign new study IDs for hdpid0 (no HDP at all)
    maxid = mysql_hasappls_df["study_id"].max()
    maxid = 0 if pd.isna(maxid) else int(maxid)

    hdpid0 = hdpid0.sort_values("xstudy_id_stewards").copy()
    hdpid0["tempn"] = group_ids(hdpid0, ["xstudy_id_stewards"], "tempn")
    hdpid0["study_id"] = hdpid0["tempn"].add(maxid)
    studyidgood3 = hdpid0[["study_id", "appl_id", "hdp_id", "compound_key", "in_mysql"]].copy()

    ### STEP 4: update hdpid1 (exactly one HDP in the group)
    studyid_sis_key = (
        hdpid1[["study_id", "xstudy_id_stewards"]].dropna(subset=["study_id"])
        .drop_duplicates(subset=["xstudy_id_stewards"])
        .rename(columns={"study_id": "xstudy_id"}).copy()
    )
    hdpid1 = hdpid1.merge(studyid_sis_key, on="xstudy_id_stewards", how="left")
    hdpid1.loc[hdpid1["study_id"].isna(), "study_id"] = hdpid1.loc[hdpid1["study_id"].isna(), "xstudy_id"]
    studyidgood4 = hdpid1[["study_id", "appl_id", "hdp_id", "compound_key", "in_mysql"]].copy()

    ### STEP 5: resolve studyidbad (multiple HDPs, ambiguous)
    bad_nonmiss = studyidbad[studyidbad["hdp_id"].notna() & (studyidbad["hdp_id"].astype(str) != "")].copy()
    bad_nonmiss = bad_nonmiss.rename(columns={
        "study_id": "zstudy_id",
        "appl_id": "zappl_id",
        "proj_ser_num": "zproj_ser_num",
        "proj_num_spl_ty_code": "zproj_num_spl_ty_code",
        "proj_num_spl_sfx_code": "zproj_num_spl_sfx_code",
        "act_code": "zact_code",
        "hdp_id": "zhdp_id",
        "archived": "zarchived",
        "proj_title": "zproj_title",
        "bgt_end_date": "zbgt_end_date",
        "bgt_strt_date": "zbgt_strt_date",
        "in_mysql": "zin_mysql",
        # Fix vs. the original script: these two were never z-prefixed, so a
        # reviewer never saw the zhdp_id side's own study identity.
        "study_name": "zstudy_name",
        "project_title": "zproject_title",
    })

    bad_missing = studyidbad[~(studyidbad["hdp_id"].notna() & (studyidbad["hdp_id"].astype(str) != ""))].copy()

    nonmiss_merge_cols = ["xstudy_id_stewards"] + [c for c in bad_nonmiss.columns if c.startswith("z")]
    if not bad_missing.empty:
        bad_joined = bad_missing.merge(bad_nonmiss[nonmiss_merge_cols], on="xstudy_id_stewards", how="inner")
    else:
        bad_joined = pd.DataFrame(columns=list(bad_missing.columns) + [c for c in nonmiss_merge_cols if c != "xstudy_id_stewards"])

    bad_joined["act_code_match"] = _str_eq(bad_joined["act_code"], bad_joined["zact_code"]).astype(int)
    bad_joined["typ_match"] = _str_eq(bad_joined["proj_num_spl_ty_code"], bad_joined["zproj_num_spl_ty_code"]).astype(int)
    bad_joined["title_match"] = _str_eq(_norm_title(bad_joined["proj_title"]), _norm_title(bad_joined["zproj_title"])).astype(int)

    bad_joined["count_actcode_matches"] = bad_joined.groupby("appl_id", dropna=False)["act_code_match"].transform("sum")
    bad_joined["count_typ_matches"] = bad_joined.groupby("appl_id", dropna=False)["typ_match"].transform("sum")
    bad_joined["xappl_pairs"] = bad_joined.groupby(["appl_id", "zappl_id"], dropna=False).ngroup().add(1)
    bad_joined["tag"] = (~bad_joined.duplicated(subset=["xstudy_id_stewards", "xappl_pairs"])).astype(int)
    bad_joined["num_appl_pairs"] = bad_joined.groupby("xstudy_id_stewards", dropna=False)["tag"].transform("sum")
    bad_joined = bad_joined.sort_values(["xstudy_id_stewards", "zarchived", "zhdp_id", "appl_id"])

    xstudyidgood5a = bad_joined[(bad_joined["count_actcode_matches"] == 1) & (bad_joined["act_code_match"] == 1)].copy()
    xstudyidgood5a["study_id"] = xstudyidgood5a["zstudy_id"].astype("Int64")
    xstudyidgood5a = xstudyidgood5a[["study_id", "appl_id", "hdp_id", "compound_key"]]

    xstudyidgood5b = bad_joined[(bad_joined["count_actcode_matches"] != 1) & (bad_joined["num_appl_pairs"] == 1)].copy()
    xstudyidgood5b["study_id"] = xstudyidgood5b["zstudy_id"].astype("Int64")
    xstudyidgood5b = xstudyidgood5b[["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"]]

    xstudyidgood5c = bad_joined[
        (bad_joined["count_actcode_matches"] != 1) &
        (bad_joined["num_appl_pairs"] != 1) &
        (bad_joined["num_live_hdps"] == 1) &
        (bad_joined["zarchived"] == "live")
    ].copy()
    xstudyidgood5c["study_id"] = xstudyidgood5c["zstudy_id"].astype("Int64")
    xstudyidgood5c = xstudyidgood5c[["study_id", "appl_id", "hdp_id", "compound_key"]]

    xstudyidgood5d = bad_joined[
        (bad_joined["count_actcode_matches"] != 1) &
        (bad_joined["num_appl_pairs"] != 1) &
        (bad_joined["num_live_hdps"] == 0)
    ].copy()
    if not xstudyidgood5d.empty:
        xstudyidgood5d["gap"] = xstudyidgood5d["bgt_strt_date"].apply(parse_ymd_date) - xstudyidgood5d["zbgt_end_date"].apply(parse_ymd_date)
        xstudyidgood5d["gap"] = xstudyidgood5d["gap"].where(xstudyidgood5d["gap"] >= pd.Timedelta(0), pd.NaT)
        xstudyidgood5d["group"] = xstudyidgood5d.groupby(["xstudy_id_stewards", "appl_id"], dropna=False).ngroup().add(1)
        xstudyidgood5d["min_gap"] = xstudyidgood5d.groupby("group")["gap"].transform("min")
        both_nat = xstudyidgood5d["gap"].isna() & xstudyidgood5d["min_gap"].isna()
        xstudyidgood5d = xstudyidgood5d[(xstudyidgood5d["gap"] == xstudyidgood5d["min_gap"]) | both_nat].copy()
        xstudyidgood5d["match"] = ((~xstudyidgood5d["min_gap"].isna()) | (xstudyidgood5d["title_match"] == 1)).astype(int)
        xstudyidgood5d["any_match"] = xstudyidgood5d.groupby("appl_id")["match"].transform("max")
        xstudyidgood5d = xstudyidgood5d[~((xstudyidgood5d["match"] == 0) & (xstudyidgood5d["any_match"] == 1))].copy()
        xstudyidgood5d["rowcount"] = xstudyidgood5d.groupby("appl_id")["appl_id"].transform("size")

        studyidgood5d_1 = xstudyidgood5d[xstudyidgood5d["rowcount"] == 1].copy()
        studyidgood5d_1["study_id"] = studyidgood5d_1["zstudy_id"].astype("Int64")
        studyidgood5d_1 = studyidgood5d_1[["study_id", "appl_id", "hdp_id", "compound_key"]]

        studyidgood5d_2 = xstudyidgood5d[(xstudyidgood5d["rowcount"] > 1) & (xstudyidgood5d["any_match"] == 1)].copy()
        studyidgood5d_2["study_id"] = studyidgood5d_2["zstudy_id"].astype("Int64")
        studyidgood5d_2 = studyidgood5d_2[["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"]]

        nomatches5d = xstudyidgood5d[(xstudyidgood5d["rowcount"] > 1) & (xstudyidgood5d["any_match"] == 0)].copy()
    else:
        studyidgood5d_1 = pd.DataFrame(columns=["study_id", "appl_id", "hdp_id", "compound_key"])
        studyidgood5d_2 = pd.DataFrame(columns=["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"])
        nomatches5d = pd.DataFrame(columns=bad_joined.columns)

    # Step 5e candidates: rows bad_joined couldn't resolve on its own
    # (num_live_hdps outside {0,1}) plus 5d's multi-row, zero-match leftovers.
    unresolved_5e = bad_joined[
        (bad_joined["count_actcode_matches"] != 1) &
        (bad_joined["num_appl_pairs"] != 1) &
        ~bad_joined["num_live_hdps"].isin([0, 1])
    ].copy()

    candidate_rows = pd.concat([unresolved_5e, nomatches5d], ignore_index=True, sort=False)
    candidate_rows = candidate_rows.sort_values(
        ["xstudy_id_stewards", "bgt_strt_date", "appl_id", "zappl_id"], na_position="last"
    )
    candidate_rows = candidate_rows[[c for c in _TEMPLATE_COLS if c in candidate_rows.columns]]
    logger.info("candidate_rows (need ledger lookup / human review): %d rows", len(candidate_rows))

    studyidgood6 = bad_nonmiss[["zstudy_id", "zappl_id", "zhdp_id", "compound_key"]].copy()
    studyidgood6.columns = ["study_id", "appl_id", "hdp_id", "compound_key"]

    return {
        "mysql_df": mysql_df,
        "other_df": other_df,
        "mysql_hasappls_df": mysql_hasappls_df,
        "studyidgood1": studyidgood1,
        "studyidgood2": studyidgood2,
        "studyidgood3": studyidgood3,
        "studyidgood4": studyidgood4,
        "studyidgood6": studyidgood6,
        "xstudyidgood5a": xstudyidgood5a,
        "xstudyidgood5b": xstudyidgood5b,
        "xstudyidgood5c": xstudyidgood5c,
        "studyidgood5d_1": studyidgood5d_1,
        "studyidgood5d_2": studyidgood5d_2,
        "candidate_rows": candidate_rows,
    }


# ---------------------------------------------------------------------------
# Step 5e, ledger-backed: shared by both entry points
# ---------------------------------------------------------------------------

def _apply_ledger(candidate_rows: pd.DataFrame, ledger_df: pd.DataFrame, maxid2: int):
    """Resolve candidate_rows using the ledger's decisions.

    Returns (studyidgood5e_1, studyidgood5e_2, studyidgood5e_3, still_pending):
    the first three mirror HEAL_04_StudyTable.py's step 5e output buckets;
    still_pending is candidate_rows filtered to keys the ledger has no
    decision for at all -- the thing a human still needs to review.
    """
    empty_single = pd.DataFrame(columns=["study_id", "appl_id", "hdp_id", "compound_key"])
    empty_multi = pd.DataFrame(columns=["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"])
    if candidate_rows.empty:
        return empty_single, empty_single.copy(), empty_multi, candidate_rows.copy()

    if not ledger_df.empty:
        ledger_lookup = ledger_df[_LEDGER_KEY + ["match", "match_notes"]].drop_duplicates(subset=_LEDGER_KEY, keep="last")
    else:
        ledger_lookup = pd.DataFrame(columns=_LEDGER_KEY + ["match", "match_notes"])

    manual = candidate_rows.merge(ledger_lookup, on=_LEDGER_KEY, how="left", indicator="_in_ledger")
    still_pending = manual[manual["_in_ledger"] == "left_only"].drop(columns=["_in_ledger", "match", "match_notes"]).copy()

    # Any pair the ledger has no decision for defaults to "not matched" for
    # THIS run's table build -- same graceful-degradation rule the original
    # script used for a blank `match` cell.
    manual["match"] = pd.to_numeric(manual["match"], errors="coerce").fillna(0).astype(int)
    manual = manual.drop(columns=["_in_ledger"])

    # hdp_id for these records is empty (they came from bad_missing with no hdp_id)
    manual["hdp_id"] = manual["zhdp_id"]
    manual = manual[manual["appl_id"].notna() & (manual["appl_id"] != "")].copy()
    manual["any_match"] = manual.groupby("appl_id")["match"].transform("max")
    manual = manual[~((manual["match"] == 0) & (manual["any_match"] == 1))].copy()
    manual["rowcount"] = manual.groupby("appl_id")["appl_id"].transform("size")

    studyidgood5e_1 = manual[manual["rowcount"] == 1].copy()
    studyidgood5e_1["study_id"] = studyidgood5e_1["zstudy_id"].astype("Int64")
    studyidgood5e_1 = studyidgood5e_1[["study_id", "appl_id", "hdp_id", "compound_key"]].copy()

    studyidgood5e_2 = (
        manual[manual["any_match"] == 0][["xstudy_id_stewards", "appl_id"]]
        .drop_duplicates().sort_values("xstudy_id_stewards").copy()
    )
    studyidgood5e_2["hdp_id"] = ""
    studyidgood5e_2["compound_key"] = studyidgood5e_2["appl_id"].astype(str) + "_"
    studyidgood5e_2["tempn"] = group_ids(studyidgood5e_2, ["xstudy_id_stewards"], "tempn")
    studyidgood5e_2["study_id"] = studyidgood5e_2["tempn"].add(maxid2)
    studyidgood5e_2 = studyidgood5e_2[["study_id", "appl_id", "hdp_id", "compound_key"]].copy()

    studyidgood5e_3 = manual[(manual["rowcount"] > 1) & (manual["any_match"] == 1)].copy()
    studyidgood5e_3["study_id"] = studyidgood5e_3["zstudy_id"].astype("Int64")
    studyidgood5e_3 = studyidgood5e_3[["study_id", "appl_id", "hdp_id", "compound_key", "xstudy_id_stewards"]].copy()

    return studyidgood5e_1, studyidgood5e_2, studyidgood5e_3, still_pending


# ---------------------------------------------------------------------------
# Steps 6-10: finalize study_lookup_table (update_study_lookup_table only)
# ---------------------------------------------------------------------------

def _finalize_study_table(candidates: dict, good5e_1: pd.DataFrame, good5e_2: pd.DataFrame, good5e_3: pd.DataFrame) -> pd.DataFrame:
    studyidgood5 = pd.concat(
        [candidates["xstudyidgood5a"], candidates["xstudyidgood5c"], candidates["studyidgood5d_1"], good5e_1, good5e_2],
        ignore_index=True, sort=False,
    )
    studyidmulti = pd.concat(
        [candidates["xstudyidgood5b"], good5e_3, candidates["studyidgood5d_2"]],
        ignore_index=True, sort=False,
    )

    studyidkey = pd.concat(
        [
            candidates["studyidgood1"],
            candidates["studyidgood2"][["study_id", "appl_id", "hdp_id", "compound_key"]],
            candidates["studyidgood3"][["study_id", "appl_id", "hdp_id", "compound_key"]],
            candidates["studyidgood4"][["study_id", "appl_id", "hdp_id", "compound_key"]],
            candidates["studyidgood6"],
            studyidgood5,
        ],
        ignore_index=True, sort=False,
    )
    studyidkey = studyidkey.drop_duplicates(subset=["compound_key"], keep="first")
    studyidkey = studyidkey.rename(columns={"study_id": "study_id_final"})
    logger.info("studyidkey: %d rows", len(studyidkey))

    mysql_studyid = candidates["mysql_hasappls_df"].copy()
    mysql_studyid = mysql_studyid.merge(studyidkey[["compound_key", "study_id_final"]], on="compound_key", how="left")
    mysql_studyid = mysql_studyid.sort_values(["study_id_final", "appl_id", "hdp_id"])
    mysql_studyid = mysql_studyid.drop(columns=["study_id", "xstudy_id_stewards"], errors="ignore")

    if not studyidmulti.empty:
        studyidmulti = studyidmulti.rename(columns={"study_id": "xstudy_id_final"})
        mysql_studyid = mysql_studyid.merge(studyidmulti[["compound_key", "xstudy_id_final"]], on="compound_key", how="left")
        missing_final = mysql_studyid["study_id_final"].isna() & mysql_studyid["xstudy_id_final"].notna()
        mysql_studyid.loc[missing_final, "study_id_final"] = mysql_studyid.loc[missing_final, "xstudy_id_final"]
        mysql_studyid = mysql_studyid.drop(columns=["xstudy_id_final"])

    # Step 7: most recent appl_id for each study
    recent = mysql_studyid[mysql_studyid["study_id_final"].notna()].copy()
    recent["fisc_yr"] = pd.to_numeric(recent["fisc_yr"], errors="coerce").astype("Int64")
    recent["latest_fy"] = recent.groupby("study_id_final")["fisc_yr"].transform("max")
    recent = recent[(recent["latest_fy"] == recent["fisc_yr"]) | (recent["fisc_yr"].isna() & recent["latest_fy"].isna())].copy()
    # Use bgt_end (not bgt_end_date): reporter_dqaudit drops the _date
    # columns, so bgt_end is the only column populated for both sources.
    recent["bgt_end_date_parsed"] = recent["bgt_end"].apply(parse_ymd_date)
    recent["latest_bgt_end"] = recent.groupby("study_id_final")["bgt_end_date_parsed"].transform("max")
    recent = recent[(recent["latest_bgt_end"] == recent["bgt_end_date_parsed"]) | (recent["bgt_end_date_parsed"].isna() & recent["latest_bgt_end"].isna())].copy()
    recent["awd_not_date_date"] = recent["awd_not_date"].apply(parse_ymd_date)
    recent["latest_awd_not_date"] = recent.groupby("study_id_final")["awd_not_date_date"].transform("max")
    recent = recent[(recent["latest_awd_not_date"] == recent["awd_not_date_date"]) | (recent["awd_not_date_date"].isna() & recent["latest_awd_not_date"].isna())].copy()
    mostrecentapplid = recent[["study_id_final", "appl_id"]].rename(columns={"appl_id": "study_most_recent_appl"}).copy()

    # Step 8: first appl_id for each study
    first = mysql_studyid[mysql_studyid["study_id_final"].notna()].copy()
    first["fisc_yr"] = pd.to_numeric(first["fisc_yr"], errors="coerce").astype("Int64")
    first["first_fy"] = first.groupby("study_id_final")["fisc_yr"].transform("min")
    first = first[(first["first_fy"] == first["fisc_yr"]) | (first["fisc_yr"].isna() & first["first_fy"].isna())].copy()
    first["bgt_strt_date_parsed"] = first["bgt_strt"].apply(parse_ymd_date)
    first["earliest_bgt_strt"] = first.groupby("study_id_final")["bgt_strt_date_parsed"].transform("min")
    first = first[(first["earliest_bgt_strt"] == first["bgt_strt_date_parsed"]) | (first["bgt_strt_date_parsed"].isna() & first["earliest_bgt_strt"].isna())].copy()
    first["awd_not_date_date"] = first["awd_not_date"].apply(parse_ymd_date)
    first["earliest_awd_not_date"] = first.groupby("study_id_final")["awd_not_date_date"].transform("min")
    first = first[(first["earliest_awd_not_date"] == first["awd_not_date_date"]) | (first["awd_not_date_date"].isna() & first["earliest_awd_not_date"].isna())].copy()
    firstapplid = first[["study_id_final", "appl_id"]].rename(columns={"appl_id": "study_first_appl"}).copy()

    # Step 9: HDP/application mapping
    hdpapplid = mysql_studyid[
        mysql_studyid["study_id_final"].notna() &
        mysql_studyid["hdp_id"].notna() &
        (mysql_studyid["hdp_id"].astype(str) != "")
    ][["study_id_final", "hdp_id", "appl_id"]].rename(columns={"appl_id": "study_hdp_id_appl", "hdp_id": "study_hdp_id"}).copy()

    # Step 10: create study_lookup_table
    study_table = mysql_studyid[mysql_studyid["study_id_final"].notna()][
        ["study_id_final", "appl_id", "in_mysql", "compound_key"]
    ].copy()
    study_table = study_table.rename(columns={"in_mysql": "appl_id_in_repawd"})
    study_table = study_table.merge(hdpapplid, on="study_id_final", how="left")
    study_table = study_table.merge(mostrecentapplid, on="study_id_final", how="left")
    study_table = study_table.merge(firstapplid, on="study_id_final", how="left")
    study_table = study_table.merge(
        candidates["mysql_df"][["compound_key", "entity_type", "res_net"]],
        on="compound_key", how="left", suffixes=(None, "_orig"),
    )

    study_table["study"] = (study_table["entity_type"] == "Study").astype(int)
    study_table["ctn"] = (study_table["entity_type"] == "CTN").astype(int)
    study_table["any_study"] = study_table.groupby("study_id_final")["study"].transform("max")
    study_table["any_ctn"] = study_table.groupby("study_id_final")["ctn"].transform("max")

    # Drop groups where the study is CTN-only
    study_table = study_table[~((study_table["any_ctn"] == 1) & (study_table["any_study"] != 1))].copy()

    # Drop studies where every appl_id is from DQ audit only
    study_table["max_flag"] = study_table.groupby("study_id_final")["appl_id_in_repawd"].transform("max")
    study_table = study_table[study_table["max_flag"] != 0].copy()
    study_table = study_table.drop(columns=["max_flag"])

    max_study_id = int(study_table["study_id_final"].dropna().astype(int).max()) if not study_table.empty else 0

    study_table = study_table.rename(columns={"study_id_final": "xstudy_id"})
    study_table["xstudy_id"] = study_table["xstudy_id"].astype("Int64").astype(str)
    study_table = study_table[[
        "appl_id", "xstudy_id", "study_hdp_id", "study_hdp_id_appl",
        "study_most_recent_appl", "study_first_appl", "compound_key",
    ]]

    # Append non-standard entity types (Contract, PSPP, Intramural, ICPSR, Other)
    if not candidates["other_df"].empty:
        other_rows = process_other_entity_types(candidates["other_df"], next_study_id=max_study_id + 1)
        study_table = pd.concat([study_table, other_rows], ignore_index=True, sort=False)

    logger.info("study_lookup_table: %d rows", len(study_table))
    return study_table


# ---------------------------------------------------------------------------
# Ledger helpers
# ---------------------------------------------------------------------------

def _read_pending_decisions(pending_df: pd.DataFrame) -> pd.DataFrame:
    """Rows from the pending file where a human filled in an explicit 0/1
    match decision. Blank/unreviewed rows are never folded into the ledger
    -- that would permanently record "no match" for something nobody has
    actually decided yet."""
    if pending_df is None or pending_df.empty or "match" not in pending_df.columns:
        return pd.DataFrame(columns=_LEDGER_COLUMNS)
    decided = pending_df.copy()
    decided["match"] = pd.to_numeric(decided["match"], errors="coerce")
    decided = decided[decided["match"].isin([0, 1])].copy()
    decided["match"] = decided["match"].astype(int)
    return decided


def _upsert_ledger(ledger_df: pd.DataFrame, decided_rows: pd.DataFrame) -> pd.DataFrame:
    """Fold newly-decided rows into the ledger, keyed by (appl_id, zappl_id,
    zhdp_id). A newer decision for the same key overwrites an older one --
    corrections are allowed, the ledger isn't strictly append-only."""
    if decided_rows.empty:
        return ledger_df
    decided_rows = decided_rows.copy()
    decided_rows["decided_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    keep_cols = [c for c in _LEDGER_COLUMNS if c in decided_rows.columns]
    decided_rows = decided_rows[keep_cols]
    combined = pd.concat([ledger_df, decided_rows], ignore_index=True, sort=False)
    combined = combined.drop_duplicates(subset=_LEDGER_KEY, keep="last")
    return combined


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def generate_match_candidates(
    mysql_merged_conn,
    reporter_dqaudit_conn,
    ref_source,
    mysql_merged_table: str | None = None,
    reporter_dqaudit_table: str | None = None,
) -> dict:
    """
    Run the automatic classification, apply the ledger, archive the
    current pending file, and write a fresh one containing only genuinely
    new, undecided pairs. Writes no MySQL table -- S3 only.
    """
    if mysql_merged_table is None:
        mysql_merged_table = os.getenv("MYSQL_MERGED_TABLE_NAME", "mysql_merged_test")
    if reporter_dqaudit_table is None:
        reporter_dqaudit_table = os.getenv("REPORTER_DQAUDIT_TABLE_NAME", "reporter_dqaudit")

    mysql_merged_df = _read_table(mysql_merged_conn, mysql_merged_table)
    reporter_dqaudit_df = _read_table(reporter_dqaudit_conn, reporter_dqaudit_table)
    candidates = _build_candidates(mysql_merged_df, reporter_dqaudit_df)

    try:
        old_pending = ref_source.read_csv(PENDING_FILE, dtype=str)
    except Exception:
        old_pending = pd.DataFrame()

    try:
        ledger_df = ref_source.read_csv(LEDGER_FILE, dtype=str)
    except Exception:
        ledger_df = pd.DataFrame(columns=_LEDGER_COLUMNS)

    decided_rows = _read_pending_decisions(old_pending)
    ledger_df = _upsert_ledger(ledger_df, decided_rows)

    if not old_pending.empty:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
        ref_source.write_csv(f"study_manual_matches_history/pending_archive_{ts}.csv", old_pending)

    maxid2 = int(candidates["studyidgood3"]["study_id"].max()) if not candidates["studyidgood3"].empty else 0
    _, _, _, still_pending = _apply_ledger(candidates["candidate_rows"], ledger_df, maxid2)

    still_pending = still_pending.sort_values(
        ["xstudy_id_stewards", "bgt_strt", "appl_id", "zappl_id"], na_position="last"
    )
    still_pending["match"] = ""
    still_pending["match_notes"] = ""

    ref_source.write_csv(PENDING_FILE, still_pending)
    ref_source.write_csv(LEDGER_FILE, ledger_df)

    logger.info(
        "study_manual_match_candidates: %d newly decided, %d still pending, %d ledger rows total",
        len(decided_rows), len(still_pending), len(ledger_df),
    )
    return {
        "pending_rows": len(still_pending),
        "newly_decided": len(decided_rows),
        "ledger_rows": len(ledger_df),
    }


def update_study_lookup_table(
    mysql_merged_conn,
    reporter_dqaudit_conn,
    ref_source,
    write_conn=None,
    mysql_merged_table: str | None = None,
    reporter_dqaudit_table: str | None = None,
    target_table: str | None = None,
) -> dict:
    """
    Run the automatic classification, apply the ENTIRE ledger, and finish
    steps 5e-10 to produce study_lookup_table. Anything never decided is
    left out of the table, not an error.
    """
    write_conn = write_conn or mysql_merged_conn
    if mysql_merged_table is None:
        mysql_merged_table = os.getenv("MYSQL_MERGED_TABLE_NAME", "mysql_merged_test")
    if reporter_dqaudit_table is None:
        reporter_dqaudit_table = os.getenv("REPORTER_DQAUDIT_TABLE_NAME", "reporter_dqaudit")
    if target_table is None:
        target_table = os.getenv("STUDY_LOOKUP_TABLE_NAME", "study_lookup_table")

    mysql_merged_df = _read_table(mysql_merged_conn, mysql_merged_table)
    reporter_dqaudit_df = _read_table(reporter_dqaudit_conn, reporter_dqaudit_table)
    candidates = _build_candidates(mysql_merged_df, reporter_dqaudit_df)

    try:
        ledger_df = ref_source.read_csv(LEDGER_FILE, dtype=str)
    except Exception:
        ledger_df = pd.DataFrame(columns=_LEDGER_COLUMNS)

    maxid2 = int(candidates["studyidgood3"]["study_id"].max()) if not candidates["studyidgood3"].empty else 0
    good5e_1, good5e_2, good5e_3, _ = _apply_ledger(candidates["candidate_rows"], ledger_df, maxid2)

    study_table = _finalize_study_table(candidates, good5e_1, good5e_2, good5e_3)
    _write_table(write_conn, study_table, target_table)

    return {"table": target_table, "rows_written": len(study_table)}

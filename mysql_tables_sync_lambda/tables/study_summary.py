"""
Update the `study_summary` MySQL table.

Joins study_lookup_table, awards, reporter, progress_tracker, pi_emails,
research_networks, engagement_flags, and po_emails into one denormalized
row per study (keyed by HDPID / appl_id).

This replaces the old monday_studies_mysql table. The table name is
configurable via STUDY_SUMMARY_TABLE_NAME env var (default: study_summary_test).
"""

import logging
import re
from datetime import datetime

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column rename maps  (MySQL source column → display/output name)
# ---------------------------------------------------------------------------

RENAME_DICT = {
    "proj_num":               "Project #",
    "proj_title":             "Title",
    "rfa":                    "Research Focus",
    "res_prg":                "Research Program",
    "ctc_pi_nm":              "Contact PI",
    "pi_email":               "Contact Email",
    "adm_ic":                 "Administering IC",
    "prg_ofc":                "NIH PO",
    "org_nm":                 "Institution(s)",
    "pi":                     "PI(s)",
    "org_cy":                 "City",
    "org_st":                 "State",
    "act_code":               "Activity Code",
    "awd_ty":                 "Award Type",
    "fisc_yr":                "Award Year",
    "tot_fund":               "Total Funded",
    "proj_abs":               "Summary",
    "fund_mech":              "SBIR/STTR",
    "proj_strt_date":         "Project Start",
    "proj_end_date":          "Project End",
    "proj_url":               "Reporter Link",
    "res_net":                "Research Network",
    "time_of_registration":   "Platform Reg Time",
    "overall_percent_complete": "CEDAR Form %",
    "repository_name":        "Repo per Platform",
    "repository_study_link":  "Repo Study Link",
    "repository_metadata":    "Repo List",
    "repository_selected":    "Repo Selected",
    "data_linked_on_platform": "Data Linked",
    "guid_type":              "GUID Type",
    "archived":               "Archived",
    "heal_funded":            "HEAL-Related",
    "do_not_engage":          "Do not Engage",
    "data_type":              "Data Type",
    "checklist_exempt_all":   "Checklist Exempt",
    "po_email":               "NIH PO Email",
}

RENAME_DICT_MDS = {
    "project_num":            "Project #",
    "project_title":          "Title",
    "investigators_name":     "PI(s)",
    "award_type":             "Award Type",
    "year_awarded":           "Award Year",
    "award_amount":           "Total Funded",
    "study_name":             "Summary",
    "project_end_date":       "Project End",
    "nih_reporter_link":      "Reporter Link",
    "time_of_registration":   "Platform Reg Time",
    "overall_percent_complete": "CEDAR Form %",
    "repository_name":        "Repo per Platform",
    "repository_study_link":  "Repo Study Link",
    "repository_metadata":    "Repo List",
    "repository_selected":    "Repo Selected",
    "data_linked_on_platform": "Data Linked",
    "guid_type":              "GUID Type",
    "archived":               "Archived",
}

RENAME_DICT_CTN = {
    "project_num":            "Project #",
    "project_title":          "Title",
    "investigators_name":     "PI(s)",
    "award_type":             "Award Type",
    "year_awarded":           "Award Year",
    "award_amount":           "Total Funded",
    "study_name":             "Summary",
    "proj_end_date":          "Project End",
    "nih_reporter_link":      "Reporter Link",
    "time_of_registration":   "Platform Reg Time",
    "overall_percent_complete": "CEDAR Form %",
    "repository_name":        "Repo per Platform",
    "repository_study_link":  "Repo Study Link",
    "repository_metadata":    "Repo List",
    "repository_selected":    "Repo Selected",
    "data_linked_on_platform": "Data Linked",
    "guid_type":              "GUID Type",
    "archived":               "Archived",
}


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


def _create_subset(df: pd.DataFrame, extra_fields: list, rename_dict: dict) -> pd.DataFrame:
    cols = [k for k in rename_dict if k in df.columns] + [f for f in extra_fields if f in df.columns]
    sub = df[cols].copy()
    sub.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns}, inplace=True)
    return sub


def _build_pi_emails(gt_file: pd.DataFrame, pi_emails_df: pd.DataFrame) -> pd.DataFrame:
    appl_ids = gt_file[["appl_id", "study_most_recent_appl"]].drop_duplicates()
    merged = pd.merge(appl_ids, pi_emails_df, how="left", on="appl_id")
    merged["pi_email"] = merged["pi_email"].fillna("")

    email_counts = merged[merged["pi_email"] != ""].groupby("study_most_recent_appl").size()
    appl_counts  = merged.groupby("study_most_recent_appl").size()

    merged["email_count"]  = [email_counts.get(k, 0) for k in merged["study_most_recent_appl"]]
    merged["applid_count"] = [appl_counts.get(k, 0)  for k in merged["study_most_recent_appl"]]
    merged["keep"] = [
        1 if (c == 0 or (c == 1 and len(e) > 0) or (c > 1 and a == m)) else 0
        for c, a, m, e in merged[["email_count", "appl_id", "study_most_recent_appl", "pi_email"]].values
    ]
    result = merged[merged["keep"] == 1][["study_most_recent_appl", "pi_email"]].drop_duplicates()
    result["pi_email"] = result["pi_email"].str.strip()
    return result


def _build_resnet(gt_file: pd.DataFrame, resnet_df: pd.DataFrame) -> pd.DataFrame:
    appl_ids = gt_file[["appl_id", "study_most_recent_appl"]].drop_duplicates()
    added    = pd.merge(appl_ids, resnet_df[["appl_id", "res_net"]], how="left", on="appl_id")
    has_net  = added[~pd.isna(added["res_net"])][["study_most_recent_appl", "res_net"]]
    updated  = pd.merge(appl_ids, has_net, how="left", on="study_most_recent_appl")
    return updated[["study_most_recent_appl", "res_net"]].drop_duplicates()


# ---------------------------------------------------------------------------
# Build function
# ---------------------------------------------------------------------------

def _build_study_summary(conn) -> pd.DataFrame:
    """Read all source tables and return the denormalized study summary DataFrame."""
    logger.info("Reading source tables from MySQL")
    gt_file = _read_table(conn, "study_lookup_table")
    gt_file.replace("0", np.nan, inplace=True)
    logger.info("  study_lookup_table: %d rows", len(gt_file))

    # Unique key per study
    lf = gt_file[["study_hdp_id", "study_most_recent_appl", "study_hdp_id_appl"]].drop_duplicates()
    lf["key"] = [m if pd.isna(h) else h for h, m in lf[["study_hdp_id", "study_most_recent_appl"]].values]
    logger.info("Unique study keys: %d", len(lf))

    awards_df   = _read_table(conn, "awards").dropna(how="all")
    reporter_df = _read_table(conn, "reporter").dropna(how="all")
    try:
        rpt_dq = _read_table(conn, "reporter_dqaudit").dropna(how="all")
        reporter_df = pd.concat([reporter_df, rpt_dq], ignore_index=True)
        logger.info("  reporter + reporter_dqaudit combined: %d rows", len(reporter_df))
    except Exception:
        logger.info("  reporter_dqaudit not found, using reporter only")

    progress_tracker_df = _read_table(conn, "progress_tracker")
    pi_emails_df        = _read_table(conn, "pi_emails")
    resnet_df           = _read_table(conn, "research_networks")
    engagement_df       = _read_table(conn, "engagement_flags")
    po_emails_df        = _read_table(conn, "po_emails")
    logger.info("All source tables loaded")

    pi_emails_clean = _build_pi_emails(gt_file, pi_emails_df)
    resnet_clean    = _build_resnet(gt_file, resnet_df)

    f_awards     = _create_subset(awards_df,          ["appl_id"],               RENAME_DICT)
    f_reporter   = _create_subset(reporter_df,        ["appl_id"],               RENAME_DICT)
    f_platform   = _create_subset(progress_tracker_df,["hdp_id"],                RENAME_DICT)
    f_pi_emails  = _create_subset(pi_emails_clean,    ["study_most_recent_appl"], RENAME_DICT)
    f_resnet     = _create_subset(resnet_clean,       ["study_most_recent_appl"], RENAME_DICT)
    f_engagement = _create_subset(engagement_df,      ["appl_id"],               RENAME_DICT)
    f_po_emails  = _create_subset(po_emails_df,       ["appl_id"],               RENAME_DICT)

    if "Research Network" in f_resnet.columns:
        f_resnet["Research Network"] = [
            k.upper() if not pd.isna(k) else "" for k in f_resnet["Research Network"]
        ]

    logger.info("Merging tables")
    d = pd.merge(lf,  f_awards,     how="left", left_on="study_most_recent_appl", right_on="appl_id").drop(columns="appl_id", errors="ignore")
    d = pd.merge(d,   f_reporter,   how="left", left_on="study_most_recent_appl", right_on="appl_id").drop(columns="appl_id", errors="ignore")
    d = pd.merge(d,   f_platform,   how="left", left_on="study_hdp_id",           right_on="hdp_id")
    d = pd.merge(d,   f_resnet,     how="left", on="study_most_recent_appl")
    d = pd.merge(d,   f_engagement, how="left", left_on="study_most_recent_appl", right_on="appl_id").drop(columns="appl_id", errors="ignore")
    d = pd.merge(d,   f_po_emails,  how="left", left_on="study_most_recent_appl", right_on="appl_id").drop(columns="appl_id", errors="ignore")
    d = pd.merge(d,   f_pi_emails,  how="left", on="study_most_recent_appl")
    logger.info("After core merge: %d rows", len(d))

    # Fill holes from progress_tracker MDS fields
    pt = progress_tracker_df.copy()
    pt["project_title"] = pt["project_title"].replace("0", "")
    pt_fields = _create_subset(pt, ["hdp_id"], RENAME_DICT_MDS)
    pt_fields["PI(s)"] = pt_fields["PI(s)"].fillna("").apply(
        lambda k: k.translate(str.maketrans(",", ";", "[]\'"))
    )
    pt_fields["key"]             = pt_fields["hdp_id"]
    pt_fields["study_hdp_id"]    = pt_fields["hdp_id"]
    pt_fields["Research Network"] = ["CTN" if str(k).startswith("CTN") else "" for k in pt_fields["Project #"]]

    filled = pd.merge(d, pt_fields, how="left", on="study_hdp_id")
    for col in list(RENAME_DICT_MDS.values()) + ["key", "Research Network"]:
        cx, cy = col + "_x", col + "_y"
        if cx in filled.columns and cy in filled.columns:
            filled[col] = [vy if pd.isna(vx) else vx for vx, vy in filled[[cx, cy]].values]
            filled.drop(columns=[cx, cy], inplace=True)

    # Add CTN studies from progress_tracker that are NOT already in the lookup table.
    # Studies in the lookup table are already in `filled` via the main merge; appending
    # them again would create duplicate rows.
    ctn_rows = pt[pt["project_num"].str.startswith("CTN", na=False)].copy()
    logger.info("CTN studies from progress_tracker: %d", len(ctn_rows))
    ctn_fields = _create_subset(ctn_rows, ["hdp_id"], RENAME_DICT_CTN)
    ctn_fields["PI(s)"] = ctn_fields["PI(s)"].fillna("").apply(
        lambda k: k.translate(str.maketrans(",", ";", "[]\'"))
    )
    ctn_fields["key"]              = ctn_fields["hdp_id"]
    ctn_fields["study_hdp_id"]     = ctn_fields["hdp_id"]
    ctn_fields["Research Network"] = "CTN"

    existing_keys  = set(filled["key"].dropna())
    ctn_fields_new = ctn_fields[~ctn_fields["key"].isin(existing_keys)]
    logger.info("New CTN studies not in lookup table: %d", len(ctn_fields_new))

    result = pd.concat([filled, ctn_fields_new], ignore_index=True)
    logger.info("Combined before encoding: %d rows", len(result))
    return result


def _parse_date(val) -> str | None:
    """Parse a date string in common reporter formats; return YYYY-MM-DD or None."""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(str(val), fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue
    return None


def _prepare_for_mysql(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply the same column encodings that prepare_for_monday() does, adapted for MySQL
    storage (NULL instead of '-', snake_case column names retained).

    Excludes any Monday board comparison or email merge logic.
    """
    d = df.copy()

    # study_type: 'CTN' / 'APPLIDONLY' / 'HDP'
    d["study_type"] = [
        "CTN" if str(p).startswith("CTN")
        else ("APPLIDONLY" if pd.isna(k) else "HDP")
        for p, k in d[["Project #", "study_hdp_id_appl"]].values
    ]

    # Location: City, State
    empty = pd.Series([""] * len(d), index=d.index)
    city  = d["City"].fillna("") if "City" in d.columns else empty
    state = d["State"].fillna("") if "State" in d.columns else empty
    d["Location"] = [f"{c},{s}" for c, s in zip(city, state)]

    # Date normalisation
    if "Project Start" in d.columns:
        d["Project Start"] = d["Project Start"].apply(_parse_date)
    if "Project End" in d.columns:
        d["Project End"] = d["Project End"].apply(_parse_date)
    if "Platform Reg Time" in d.columns:
        d["Platform Reg Time"] = (
            pd.to_datetime(d["Platform Reg Time"], utc=True, errors="coerce")
            .dt.strftime("%Y-%m-%d")
        )

    # Boolean / categorical encodings
    if "Archived" in d.columns:
        d["Archived"] = ["archived" if a == "archived" else "n" for a in d["Archived"]]

    if "HEAL-Related" in d.columns:
        d["HEAL-Related"] = [
            "Y" if (st != "CTN" and pd.notna(a) and float(a) == 1) else "N"
            for st, a in d[["study_type", "HEAL-Related"]].values
        ]

    if "SBIR/STTR" in d.columns:
        d["SBIR/STTR"] = ["Y" if t == "SBIR/STTR" else "N" for t in d["SBIR/STTR"]]

    if "Checklist Exempt" in d.columns:
        d["Checklist Exempt"] = ["Y" if str(t) == "1" else "N" for t in d["Checklist Exempt"]]

    if "Do not Engage" in d.columns:
        d["Do not Engage"] = ["Y" if str(t) == "1" else "N" for t in d["Do not Engage"]]

    # Drop join-artifact columns; keep study_hdp_id (useful for queries)
    d.drop(columns=["hdp_id", "hdp_id_x", "hdp_id_y"], errors="ignore", inplace=True)

    logger.info("After encoding: %d rows, %d columns", len(d), len(d.columns))
    return d


def _write_study_summary(conn, df: pd.DataFrame, table: str) -> None:
    col_map    = {c: _mysql_colname(c) for c in df.columns}
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
# Public entry point
# ---------------------------------------------------------------------------

def update_study_summary(conn, target_table: str | None = None) -> dict:
    """
    Rebuild the study_summary table from all source MySQL tables.

    Args:
        conn:         mysql.connector connection (from db.connect_mysql()).
        target_table: Table name to write; defaults to STUDY_SUMMARY_TABLE_NAME
                      env var or 'study_summary_test'.

    Returns:
        dict with keys: table, rows_written
    """
    import os
    if target_table is None:
        target_table = os.getenv("STUDY_SUMMARY_TABLE_NAME", "study_summary_test")
    df = _build_study_summary(conn)
    df = _prepare_for_mysql(df)
    _write_study_summary(conn, df, target_table)
    return {"table": target_table, "rows_written": len(df)}

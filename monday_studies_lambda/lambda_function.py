"""
AWS Lambda: builds/refreshes the monday_studies_mysql table.

Reads source MySQL tables (study_lookup_table, awards, reporter, progress_tracker,
pi_emails, research_networks, engagement_flags, po_emails), joins them into one
denormalized study record per HDPID/appl_id, and writes to monday_studies_mysql.

Trigger: EventBridge schedule (weekly) or invoke manually.
Can also be run as a script: python lambda_function.py
"""

import json
import logging
import os
import re
import sys

import mysql.connector
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    stream=sys.stdout)

# ---------------------------------------------------------------------------
# Column-name dictionaries  (MySQL field → Monday Board display name)
# These are shared with monday_board_update.py.
# ---------------------------------------------------------------------------

RENAME_DICT = {
    'proj_num':               'Project #',
    'proj_title':             'Title',
    'rfa':                    'Research Focus',
    'res_prg':                'Research Program',
    'ctc_pi_nm':              'Contact PI',
    'pi_email':               'Contact Email',
    'adm_ic':                 'Administering IC',
    'prg_ofc':                'NIH PO',
    'org_nm':                 'Institution(s)',
    'pi':                     'PI(s)',
    'org_cy':                 'City',
    'org_st':                 'State',
    'act_code':               'Activity Code',
    'awd_ty':                 'Award Type',
    'fisc_yr':                'Award Year',
    'tot_fund':               'Total Funded',
    'proj_abs':               'Summary',
    'fund_mech':              'SBIR/STTR',
    'proj_strt_date':         'Project Start',
    'proj_end_date':          'Project End',
    'proj_url':               'Reporter Link',
    'res_net':                'Research Network',
    'time_of_registration':   'Platform Reg Time',
    'overall_percent_complete': 'CEDAR Form %',
    'repository_name':        'Repo per Platform',
    'archived':               'Archived',
    'heal_funded':            'HEAL-Related',
    'do_not_engage':          'Do not Engage',
    'data_type':              'Data Type',
    'checklist_exempt_all':   'Checklist Exempt',
    'po_email':               'NIH PO Email',
}

RENAME_DICT_MDS = {
    'project_num':            'Project #',
    'project_title':          'Title',
    'investigators_name':     'PI(s)',
    'award_type':             'Award Type',
    'year_awarded':           'Award Year',
    'award_amount':           'Total Funded',
    'study_name':             'Summary',
    'project_end_date':       'Project End',
    'nih_reporter_link':      'Reporter Link',
    'time_of_registration':   'Platform Reg Time',
    'overall_percent_complete': 'CEDAR Form %',
    'repository_name':        'Repo per Platform',
    'archived':               'Archived',
}

RENAME_DICT_CTN = {
    'project_num':            'Project #',
    '   project_title':       'Title',
    'investigators_name':     'PI(s)',
    'award_type':             'Award Type',
    'year_awarded':           'Award Year',
    'award_amount':           'Total Funded',
    'study_name':             'Summary',
    'proj_end_date':          'Project End',
    'nih_reporter_link':      'Reporter Link',
    'time_of_registration':   'Platform Reg Time',
    'overall_percent_complete': 'CEDAR Form %',
    'repository_name':        'Repo per Platform',
    'archived':               'Archived',
}

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _connect():
    return mysql.connector.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
    )


def _read_table(conn, table: str) -> pd.DataFrame:
    """Read a full MySQL table into a DataFrame."""
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM `{table}`")
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()
    if 'appl_id' in df.columns:
        df['appl_id'] = df['appl_id'].astype(str)
    return df


def _mysql_colname(name: str) -> str:
    """Convert a display name to a MySQL-safe snake_case identifier."""
    s = re.sub(r'[^a-zA-Z0-9]+', '_', str(name))
    return re.sub(r'_+', '_', s).strip('_').lower() or 'col'


def _table_exists(conn, table: str) -> bool:
    cur = conn.cursor()
    cur.execute("SHOW TABLES LIKE %s", (table,))
    result = cur.fetchone()
    cur.close()
    return result is not None


# ---------------------------------------------------------------------------
# Merge helpers  (ported from scripts/monday_board_update.py)
# ---------------------------------------------------------------------------

def _create_subset(df: pd.DataFrame, extra_fields: list, rename_dict: dict) -> pd.DataFrame:
    """Select and rename columns from df using rename_dict; keep extra_fields as-is."""
    cols = [k for k in rename_dict if k in df.columns] + [f for f in extra_fields if f in df.columns]
    sub = df[cols].copy()
    sub.rename(columns={k: v for k, v in rename_dict.items() if k in df.columns}, inplace=True)
    return sub


def _get_lookup_fields(gt_file: pd.DataFrame) -> pd.DataFrame:
    """Derive per-study lookup fields (one row per HDPID/appl_id key)."""
    lf = gt_file[['study_hdp_id', 'study_most_recent_appl', 'study_hdp_id_appl']].copy().drop_duplicates()
    lf['key'] = [m if pd.isna(h) else h for h, m in lf[['study_hdp_id', 'study_most_recent_appl']].values]
    logging.info(f"Unique study keys: {len(lf)}")
    return lf


def _build_pi_emails(gt_file: pd.DataFrame, pi_emails_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map PI emails to most-recent appl_id.
    When a project has multiple appl_ids, carry forward the email from whichever
    appl_id has it; prefer the most-recent appl_id's email when available.
    Returns a DataFrame with columns [study_most_recent_appl, pi_email].
    """
    appl_ids = gt_file[['appl_id', 'study_most_recent_appl']].drop_duplicates()
    merged = pd.merge(appl_ids, pi_emails_df, how='left', on='appl_id')
    merged['pi_email'] = merged['pi_email'].fillna('')

    email_counts = merged[merged['pi_email'] != ''].groupby('study_most_recent_appl').size()
    appl_counts  = merged.groupby('study_most_recent_appl').size()

    merged['email_count'] = [email_counts.get(k, 0) for k in merged['study_most_recent_appl']]
    merged['applid_count'] = [appl_counts.get(k, 0)  for k in merged['study_most_recent_appl']]
    merged['keep'] = [
        1 if (c == 0 or (c == 1 and len(e) > 0) or (c > 1 and a == m)) else 0
        for c, a, m, e in merged[['email_count', 'appl_id', 'study_most_recent_appl', 'pi_email']].values
    ]
    result = merged[merged['keep'] == 1][['study_most_recent_appl', 'pi_email']].drop_duplicates()
    result['pi_email'] = result['pi_email'].str.strip()
    return result


def _build_resnet(gt_file: pd.DataFrame, resnet_df: pd.DataFrame) -> pd.DataFrame:
    """
    Map research network to most-recent appl_id.
    If any earlier appl_id for a study has a res_net, propagate it to the
    most_recent_appl_id.
    Returns a DataFrame with columns [study_most_recent_appl, res_net].
    """
    appl_ids = gt_file[['appl_id', 'study_most_recent_appl']].drop_duplicates()
    added = pd.merge(appl_ids, resnet_df[['appl_id', 'res_net']], how='left', on='appl_id')
    has_net = added[~pd.isna(added['res_net'])][['study_most_recent_appl', 'res_net']]
    updated = pd.merge(appl_ids, has_net, how='left', on='study_most_recent_appl')
    return updated[['study_most_recent_appl', 'res_net']].drop_duplicates()


# ---------------------------------------------------------------------------
# Main build function
# ---------------------------------------------------------------------------

def build_monday_studies(conn) -> pd.DataFrame:
    """
    Read all source tables and produce a single denormalized DataFrame,
    one row per HDPID/appl_id, with Monday Board display-name columns.
    Mirrors the logic of create_monday_update_file() in scripts/monday_board_update.py
    but without any file I/O or Monday Board comparison.
    """
    logging.info("Reading source tables from MySQL")
    gt_file = _read_table(conn, 'study_lookup_table')
    gt_file.replace("0", np.nan, inplace=True)
    logging.info(f"  study_lookup_table: {len(gt_file)} rows")

    lookup_fields = _get_lookup_fields(gt_file)

    awards_df   = _read_table(conn, 'awards').dropna(how='all')
    reporter_df = _read_table(conn, 'reporter').dropna(how='all')
    try:
        rpt_dq = _read_table(conn, 'reporter_dqaudit').dropna(how='all')
        reporter_df = pd.concat([reporter_df, rpt_dq], ignore_index=True)
        logging.info(f"  reporter + reporter_dqaudit combined: {len(reporter_df)} rows")
    except Exception:
        logging.info("  reporter_dqaudit not found, using reporter only")

    progress_tracker_df  = _read_table(conn, 'progress_tracker')
    pi_emails_df         = _read_table(conn, 'pi_emails')
    resnet_df            = _read_table(conn, 'research_networks')
    engagement_flags_df  = _read_table(conn, 'engagement_flags')
    po_emails_df         = _read_table(conn, 'po_emails')
    logging.info("All source tables loaded")

    # ── PI emails and research networks ─────────────────────────────────────
    pi_emails_clean = _build_pi_emails(gt_file, pi_emails_df)
    resnet_clean    = _build_resnet(gt_file, resnet_df)

    # ── Subset each table to Monday-relevant columns ─────────────────────────
    f_awards      = _create_subset(awards_df,          ['appl_id'],              RENAME_DICT)
    f_reporter    = _create_subset(reporter_df,        ['appl_id'],              RENAME_DICT)
    f_platform    = _create_subset(progress_tracker_df,['hdp_id'],               RENAME_DICT)
    f_pi_emails   = _create_subset(pi_emails_clean,    ['study_most_recent_appl'], RENAME_DICT)
    f_resnet      = _create_subset(resnet_clean,       ['study_most_recent_appl'], RENAME_DICT)
    f_engagement  = _create_subset(engagement_flags_df,['appl_id'],              RENAME_DICT)
    f_po_emails   = _create_subset(po_emails_df,       ['appl_id'],              RENAME_DICT)

    # Uppercase research network values
    if 'Research Network' in f_resnet.columns:
        f_resnet['Research Network'] = [
            k.upper() if not pd.isna(k) else '' for k in f_resnet['Research Network']
        ]

    # ── Sequential left-joins ────────────────────────────────────────────────
    logging.info("Merging tables")
    d = pd.merge(lookup_fields, f_awards,     how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id', errors='ignore')
    d = pd.merge(d,             f_reporter,   how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id', errors='ignore')
    d = pd.merge(d,             f_platform,   how='left', left_on='study_hdp_id',           right_on='hdp_id')
    d = pd.merge(d,             f_resnet,     how='left', on='study_most_recent_appl')
    d = pd.merge(d,             f_engagement, how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id', errors='ignore')
    d = pd.merge(d,             f_po_emails,  how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id', errors='ignore')
    d = pd.merge(d,             f_pi_emails,  how='left', on='study_most_recent_appl')
    logging.info(f"After core merge: {len(d)} rows")

    # ── Fill holes from progress_tracker (MDS data) ──────────────────────────
    pt = progress_tracker_df.copy()
    pt['project_title'] = pt['project_title'].replace('0', '')
    pt_fields = _create_subset(pt, ['hdp_id'], RENAME_DICT_MDS)
    pt_fields['PI(s)'] = pt_fields['PI(s)'].fillna('').apply(
        lambda k: k.translate(str.maketrans(',', ';', "[]\'"))
    )
    pt_fields['key']          = pt_fields['hdp_id']
    pt_fields['study_hdp_id'] = pt_fields['hdp_id']
    pt_fields['Research Network'] = [
        'CTN' if str(k).startswith('CTN') else '' for k in pt_fields['Project #']
    ]

    filled = pd.merge(d, pt_fields, how='left', on='study_hdp_id')
    for col in list(RENAME_DICT_MDS.values()) + ['key', 'Research Network']:
        cx, cy = col + '_x', col + '_y'
        if cx in filled.columns and cy in filled.columns:
            filled[col] = [vy if pd.isna(vx) else vx for vx, vy in filled[[cx, cy]].values]
            filled.drop(columns=[cx, cy], inplace=True)

    # ── CTN studies from progress_tracker ────────────────────────────────────
    ctn_rows = pt[pt['project_num'].str.startswith('CTN', na=False)].copy()
    logging.info(f"CTN studies from progress_tracker: {len(ctn_rows)}")
    ctn_fields = _create_subset(ctn_rows, ['hdp_id'], RENAME_DICT_CTN)
    ctn_fields['PI(s)'] = ctn_fields['PI(s)'].fillna('').apply(
        lambda k: k.translate(str.maketrans(',', ';', "[]\'"))
    )
    ctn_fields['key']             = ctn_fields['hdp_id']
    ctn_fields['study_hdp_id']    = ctn_fields['hdp_id']
    ctn_fields['Research Network'] = 'CTN'

    all_data = pd.concat([filled, ctn_fields], ignore_index=True)
    logging.info(f"Final combined dataset: {len(all_data)} rows")
    logging.info("Research network distribution:\n" + str(all_data.get('Research Network', pd.Series()).value_counts()))

    return all_data


# ---------------------------------------------------------------------------
# Write to monday_studies_mysql
# ---------------------------------------------------------------------------

def write_monday_studies(conn, df: pd.DataFrame, table: str = 'monday_studies_mysql'):
    """
    Write the DataFrame to a MySQL table, replacing any existing data.
    Column names are converted to MySQL-safe snake_case identifiers.
    All values stored as TEXT for maximum compatibility.
    """
    col_map   = {c: _mysql_colname(c) for c in df.columns}
    mysql_cols = list(col_map.values())

    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    col_defs = ', '.join(f"`{c}` TEXT" for c in mysql_cols)
    cur.execute(f"CREATE TABLE `{table}` ({col_defs})")

    placeholders = ', '.join(['%s'] * len(mysql_cols))
    col_list = ', '.join(f'`{c}`' for c in mysql_cols)
    sql = f"INSERT INTO `{table}` ({col_list}) VALUES ({placeholders})"

    rows = [
        tuple(None if pd.isna(v) else str(v) for v in row)
        for _, row in df.iterrows()
    ]
    cur.executemany(sql, rows)
    conn.commit()
    cur.close()
    logging.info(f"Wrote {len(rows)} rows to `{table}`")


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

_HEADERS = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


def lambda_handler(event, context):
    try:
        conn = _connect()
        df   = build_monday_studies(conn)
        write_monday_studies(conn, df)
        conn.close()
        return {
            "statusCode": 200,
            "headers":    _HEADERS,
            "body":       json.dumps({"status": "ok", "rows": len(df)}),
        }
    except Exception as e:
        logging.exception("Lambda failed")
        return {
            "statusCode": 500,
            "headers":    _HEADERS,
            "body":       json.dumps({"error": str(e)}),
        }


# ---------------------------------------------------------------------------
# Script entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    conn = _connect()
    df   = build_monday_studies(conn)
    write_monday_studies(conn, df)
    conn.close()
    print(f"Done. {len(df)} rows written to monday_studies_mysql.")

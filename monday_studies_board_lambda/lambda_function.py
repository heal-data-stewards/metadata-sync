"""
Monday Studies Board builder — monthly/on-demand Lambda entry point.

Reads the study_summary MySQL table (built by mysql_tables_sync_lambda),
restores Monday's display column names, and writes the ready-to-upload
xlsx (full file + Monday's row-limited batches) to S3.

Deliberately simple, by design: study_summary already applies every
encoding Monday needs (study_type, Location, date normalisation, Y/N
flags -- see mysql_tables_sync_lambda/tables/study_summary.py
_prepare_for_mysql) before it's ever written to MySQL, so there is no
transformation logic left to redo here. No comparison against the live
Monday board and no email backfill happen in this Lambda -- both need the
*current* live board (which can have human edits since the last sync), so
both stay in the local pre-upload step (see compare_monday_studies_board.py
in scripts/), not here.

Output (s3://<S3_BUCKET>/<OUTPUT_PREFIX>/)
--------------------------------------------
- MondayStudiesBoard_<today>.xlsx                                  (full file)
- MondayStudiesBoard_<today>_batch_<n>_records_<start>_to_<end>.xlsx (Monday import batches)

Lambda packaging notes
-----------------------
Include these files alongside lambda_function.py when deploying (flat, no
package wrapper -- same convention as mysql_tables_sync_lambda):
  column_mapping.py  db.py  notify.py

Slack notifications
--------------------
Set SLACK_WEBHOOK_URL to a Slack incoming webhook URL to get a message when
the run finishes (success or failure). Unset = notifications disabled.
"""

import io
import json
import logging
import os
from datetime import date

import boto3
import pandas as pd
from dotenv import load_dotenv

from column_mapping import DROP_COLUMNS, KEY_COLUMN, MYSQL_TO_DISPLAY, PASSTHROUGH_RENAME
from db import connect_mysql
from notify import notify_slack

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv("S3_BUCKET", "heal-dashboard-snapshots")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "monday_studies_board/outputs")
STUDY_SUMMARY_TABLE = os.getenv("STUDY_SUMMARY_TABLE", "study_summary")

# Monday's import row limit, per batch file. Matches the already-in-use
# monday_studies_lambda/monday_board_update.py ("v2") precedent.
BATCH_SIZE = 1400


def read_study_summary(conn, table: str = STUDY_SUMMARY_TABLE) -> pd.DataFrame:
    """Read study_summary and restore Monday Board display column names."""
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM `{table}`")
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()

    df.rename(columns=MYSQL_TO_DISPLAY, inplace=True)
    logger.info("%s: %d rows, %d columns", table, len(df), len(df.columns))
    return df


def prepare_for_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Final column renames + the Monday-upload blank failsafe. No re-derivation
    of study_type/Location/dates/flags -- study_summary already did that.
    """
    d = df.rename(columns=PASSTHROUGH_RENAME).drop(columns=DROP_COLUMNS, errors="ignore")

    value_columns = [c for c in d.columns if c != KEY_COLUMN]
    d[value_columns] = d[value_columns].fillna("-")
    d[value_columns] = d[value_columns].replace("", "-")

    d.reset_index(drop=True, inplace=True)
    d.index.name = "index"
    return d


def write_batches_to_s3(s3_client, df: pd.DataFrame, bucket: str, prefix: str, today: str) -> list[str]:
    """Writes the full file plus Monday-import-sized batches; returns the keys written."""
    written = []

    def _put(name: str, frame: pd.DataFrame) -> None:
        buf = io.BytesIO()
        frame.to_excel(buf, engine="xlsxwriter", index=True)
        key = f"{prefix}/{name}"
        s3_client.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
        written.append(key)

    _put(f"MondayStudiesBoard_{today}.xlsx", df)

    n_batches = (len(df) - 1) // BATCH_SIZE + 1
    for i in range(n_batches):
        start, end = i * BATCH_SIZE, min((i + 1) * BATCH_SIZE, len(df))
        _put(f"MondayStudiesBoard_{today}_batch_{i + 1}_records_{start + 1}_to_{end}.xlsx", df.iloc[start:end])

    return written


def run() -> dict:
    conn = connect_mysql()
    try:
        raw = read_study_summary(conn)
    finally:
        conn.close()

    final = prepare_for_export(raw)

    key_counts = final.groupby(KEY_COLUMN).size()
    duplicate_keys = int((key_counts > 1).sum())
    if duplicate_keys:
        logger.warning("%d key(s) appear more than once in study_summary -- investigate", duplicate_keys)

    today = date.today().strftime("%Y-%m-%d")
    written = write_batches_to_s3(boto3.client("s3"), final, S3_BUCKET, OUTPUT_PREFIX, today)

    return {
        "rows": len(final),
        "duplicate_keys": duplicate_keys,
        "files_written": written,
    }


def lambda_handler(event, context):
    try:
        result = run()
        logger.info("Monday Studies Board build complete: %s", result)
        notify_slack(
            f":white_check_mark: Monday Studies Board build complete — "
            f"{result['rows']} rows, {len(result['files_written'])} file(s) written"
        )
        return {"statusCode": 200, "body": json.dumps(result)}
    except Exception as e:
        logger.exception("Monday Studies Board build failed")
        notify_slack(f":x: Monday Studies Board build failed — {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


# ---------------------------------------------------------------------------
# Script entry point (local testing — needs DB_HOST etc. reachable directly,
# e.g. via VPN, not an SSH tunnel)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(json.dumps(run(), indent=2))

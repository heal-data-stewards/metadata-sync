"""
HEAL Monday Board Update — MySQL source edition

Reads the assembled study_summary table from MySQL (built by mysql_tables_sync_lambda),
compares it with the current Monday board xlsx export, and exports batched xlsx files
ready for import to the HEAL Studies Monday board.

Key difference from monday_board_update.py:
  - No CSV imports needed — data comes from the study_summary MySQL table,
    which is already denormalized and encoded (Y/N flags, dates normalised, CTN included).
  - Monday board comparison and all reporting logic is fully preserved.
  - Email backfill from Monday board is still applied on top of MySQL data.

Usage:
    python monday_board_update_from_mysql.py \\
        --input-dir /path/to/dir/with/Monday/xlsx/export \\
        --output-dir /path/to/output         \\
        [--table study_summary]              \\
        [--debug]

The --input-dir must contain the Monday board xlsx export (HEAL_Studies_*.xlsx).
The --output-dir receives MondayBoard_Update.xlsx, batch files, and log/debug CSVs.
If --output-dir is omitted, --input-dir is used for output as well.
"""

import logging
import os
import sys
from pathlib import Path

import click
import mysql.connector
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sshtunnel import SSHTunnelForwarder

# ---------------------------------------------------------------------------
# Column-name mapping  (re-used from study_summary to avoid duplication)
# ---------------------------------------------------------------------------

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "mysql_tables_sync_lambda"))
from tables.study_summary import RENAME_DICT, _mysql_colname  # noqa: E402

load_dotenv()

# Reverse map: MySQL snake_case column → Monday Board display name
MYSQL_TO_DISPLAY = {_mysql_colname(v): v for v in RENAME_DICT.values()}

# Passthrough columns that exist in study_summary but aren't in RENAME_DICT
_PASSTHROUGH_COLS = {
    "study_hdp_id", "study_most_recent_appl", "study_hdp_id_appl",
    "key", "study_type", "location",
}


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

RDS_PORT = 3306


def _connect(local_port: int = None) -> mysql.connector.MySQLConnection:
    host = "127.0.0.1" if local_port else os.getenv("DB_HOST")
    port = local_port or RDS_PORT
    return mysql.connector.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=host,
        port=port,
        database=os.getenv("DB_NAME"),
    )


def read_study_summary(conn, table: str = "study_summary") -> pd.DataFrame:
    """
    Read the study_summary MySQL table and restore Monday Board display column names.
    Passthrough columns (key, study_most_recent_appl, etc.) are left as-is.
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM `{table}`")
    cols = [d[0] for d in cur.description]
    df = pd.DataFrame(cur.fetchall(), columns=cols)
    cur.close()

    # Restore display names for all RENAME_DICT-derived columns
    df.rename(columns=MYSQL_TO_DISPLAY, inplace=True)

    logging.info("study_summary: %d rows, %d columns", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Monday board import  (preserved from scripts/monday_board_update.py)
# ---------------------------------------------------------------------------

def import_monday_board(
    input_dir: Path,
    file_name: str = "HEAL_Studies_*.xlsx",
    rows_to_skip: int = 4,
) -> pd.DataFrame:
    board_files = list(input_dir.glob(file_name))
    if not board_files:
        raise FileNotFoundError(f"No Monday board export found matching {input_dir / file_name}")
    logging.info("Loading Monday board export: %s", board_files[0])
    df = pd.read_excel(board_files[0], skiprows=rows_to_skip)
    logging.info("Monday board raw columns: %s", list(df.columns))
    monday_board = df[~df["Name"].isin([
        "Studies under investigation",
        "Name",
        "Studies Not Added to Platform",
        "CTN Protocols",
        "Pending assessment results",
    ])]
    monday_board = monday_board[~pd.isna(monday_board["Name"])]
    logging.info("Number of records on Monday Board: %d", len(monday_board))
    return monday_board


# ---------------------------------------------------------------------------
# Comparison  (preserved from scripts/monday_board_update.py)
# ---------------------------------------------------------------------------

def compare_study_lookup_monday(
    study_summary_df: pd.DataFrame,
    monday_board: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compare study_summary keys against Monday board Names.

    Returns:
        (mondayboard_missing_in_lookup, lookup_fields)
        mondayboard_missing_in_lookup — rows on Monday that are NOT in study_summary (investigate)
        lookup_fields                 — study key info extracted from study_summary
    """
    lookup_fields = (
        study_summary_df[["study_hdp_id", "study_most_recent_appl", "study_hdp_id_appl", "key"]]
        .drop_duplicates()
    )

    in_lookup = monday_board[monday_board["Name"].isin(lookup_fields["key"])]
    missing_from_lookup = monday_board[~monday_board["Name"].isin(lookup_fields["key"])]
    missing_from_monday = lookup_fields[~lookup_fields["key"].isin(monday_board["Name"])]

    logging.info("Number of records from Monday already in study_summary: %d", len(in_lookup))
    logging.info(
        "Number of records from Monday NOT in study_summary "
        "(discrepancies — investigate): %d",
        len(missing_from_lookup),
    )
    logging.info(
        "Number of records in study_summary NOT on Monday (potentially new entries): %d",
        len(missing_from_monday),
    )

    if len(missing_from_lookup) > 0:
        logging.warning(
            "Entries on Monday that are NOT in study_summary:\n%s",
            missing_from_lookup[["Name"]].to_string(index=False),
        )

    return missing_from_lookup, lookup_fields


# ---------------------------------------------------------------------------
# Research Focus comparison against previous Monday board
# ---------------------------------------------------------------------------

def compare_research_focus(
    study_summary_df: pd.DataFrame,
    monday_board: pd.DataFrame,
    output_dir: Path,
) -> None:
    """
    For studies present in both study_summary and the Monday board export,
    compare the Research Focus value and log/export any differences.
    """
    if "Research Focus" not in study_summary_df.columns:
        logging.warning("Research Focus column not found in study_summary — skipping comparison")
        return
    if "Research Focus" not in monday_board.columns:
        logging.warning("Research Focus column not found in Monday board export — skipping comparison")
        return

    new_rf = (
        study_summary_df[["key", "Research Focus"]]
        .drop_duplicates(subset="key")
        .rename(columns={"Research Focus": "Research Focus (new)"})
    )
    old_rf = (
        monday_board[["Name", "Research Focus"]]
        .drop_duplicates(subset="Name")
        .rename(columns={"Research Focus": "Research Focus (Monday)"})
    )

    merged = pd.merge(new_rf, old_rf, left_on="key", right_on="Name", how="inner").drop(columns="Name")

    def _norm(v):
        return str(v).strip() if not pd.isna(v) else ""

    merged["changed"] = merged.apply(
        lambda r: _norm(r["Research Focus (new)"]) != _norm(r["Research Focus (Monday)"]),
        axis=1,
    )

    changed = merged[merged["changed"]].drop(columns="changed")
    unchanged_count = (~merged["changed"]).sum()

    logging.info(
        "Research Focus comparison: %d unchanged, %d changed",
        unchanged_count, len(changed),
    )
    if len(changed) > 0:
        logging.warning(
            "Research Focus values that changed (key | new | Monday):\n%s",
            changed.to_string(index=False),
        )
        changed.to_csv(output_dir / "research_focus_changes.csv", index=False)
        logging.info("Wrote research_focus_changes.csv")


# ---------------------------------------------------------------------------
# Email backfill from Monday board  (preserved from scripts/monday_board_update.py)
# ---------------------------------------------------------------------------

def backfill_emails_from_monday(
    df: pd.DataFrame,
    monday_board: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    """
    Where study_summary has no Contact Email, fill in from the Monday board export.
    Mirrors the pi_email backfill logic in import_mysql_data().
    Writes email_updates.csv for audit.
    """
    monday_emails = (
        monday_board[["Most Recent Appl_ID", "Contact Email"]]
        .drop_duplicates()
        .copy()
    )
    monday_emails["Contact Email"] = (
        monday_emails["Contact Email"].replace("-", "").fillna("")
    )
    # Guarantee one row per Most Recent Appl_ID before the join; prefer non-empty email.
    monday_emails = (
        monday_emails
        .sort_values("Contact Email", ascending=False)
        .drop_duplicates(subset="Most Recent Appl_ID", keep="first")
    )

    merged = pd.merge(
        df,
        monday_emails,
        how="left",
        left_on="study_most_recent_appl",
        right_on="Most Recent Appl_ID",
        suffixes=("", "_monday"),
    )

    def _pick_email(row):
        mysql_email = str(row.get("Contact Email", "")).strip()
        monday_email = str(row.get("Contact Email_monday", "")).strip()
        if mysql_email in ("", "-", "nan", "None") and len(monday_email) > 1:
            return monday_email
        return mysql_email

    merged["Contact Email"] = merged.apply(_pick_email, axis=1)
    merged.drop(
        columns=["Most Recent Appl_ID", "Contact Email_monday"],
        errors="ignore",
        inplace=True,
    )

    # Audit export
    audit = merged[["study_most_recent_appl", "Contact Email"]].copy()
    audit.to_csv(output_dir / "email_updates.csv", index=False)
    logging.info("Email backfill complete — audit written to email_updates.csv")
    logging.debug("Email backfill result:\n%s", audit.to_string())

    return merged


# ---------------------------------------------------------------------------
# Final prep for export  (column renames + drop internals)
# ---------------------------------------------------------------------------

def _prep_for_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename internal columns to Monday Board display names and drop join artifacts.
    Mirrors the tail of prepare_for_monday() in scripts/monday_board_update.py.
    """
    d = df.copy()
    d.rename(
        columns={
            "study_most_recent_appl": "Most Recent Appl_ID",
            "study_hdp_id_appl":      "HDP appl_ID",
            "Project End":            "Project End date",
        },
        inplace=True,
    )
    d.drop(
        columns=["study_hdp_id", "hdp_id", "hdp_id_x", "hdp_id_y"],
        errors="ignore",
        inplace=True,
    )
    # Fill remaining string NaN with '-'
    handled = {
        "study_type", "City", "State", "Location", "location",
        "Project Start", "Project End", "Project End date", "Platform Reg Time",
        "Archived", "HEAL-Related", "SBIR/STTR", "Checklist Exempt", "Do not Engage",
    }
    for col in d.columns:
        if col not in handled and d[col].dtype == object:
            d[col] = ["-" if (v is np.nan or str(v).strip() in ("", "nan", "None")) else v for v in d[col]]
    return d


# ---------------------------------------------------------------------------
# Export  (preserved from scripts/monday_board_update.py — reporting intact)
# ---------------------------------------------------------------------------

def export_finaldata(
    output_dir: Path,
    final_dataset: pd.DataFrame,
    mondayboard_missing_in_data: pd.DataFrame,
    monday_board: pd.DataFrame,
) -> None:
    logging.info("******************* MONDAY COMPARISON ******************************************")

    in_final = monday_board[monday_board["Name"].isin(final_dataset["key"])]
    mondayboard_not_in_final = monday_board[~monday_board["Name"].isin(final_dataset["key"])]
    data_not_in_monday = final_dataset[~final_dataset["key"].isin(monday_board["Name"])]

    logging.info(
        "Number of records from Monday already in final dataset: %d", len(in_final)
    )
    logging.info(
        "Number of records from Monday NOT in final dataset "
        "(discrepancies — investigate): %d",
        len(mondayboard_not_in_final),
    )
    logging.info(
        "Number of records in final dataset NOT on Monday (potentially new entries): %d",
        len(data_not_in_monday),
    )

    logging.warning(
        "****** Investigate/Delete the following entries on Monday that are not in the final dataset:\n%s",
        mondayboard_not_in_final[["Name"]].to_string(index=False)
        if len(mondayboard_not_in_final) > 0 else "(none)",
    )

    # Index column is integral to QA — see SOP
    final_dataset.reset_index(drop=True, inplace=True)
    final_dataset.index.name = "index"

    key_counts = final_dataset.groupby("key").size()
    t = key_counts.describe()
    logging.info("******************* FINAL DATASET NUMBERS ******************************************")
    logging.info("Number of records in the final dataset: %d", len(final_dataset))
    logging.info(
        "One row per key (HDPID/APPLID)? %s",
        bool(t["min"] == 1 and t["max"] == 1),
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    outfile = output_dir / "MondayBoard_Update.xlsx"
    logging.info("******************* EXPORTING ******************************************")
    logging.info("Exporting full file → %s", outfile)
    final_dataset.to_excel(outfile, engine="xlsxwriter", index=True)

    batch_size = 1000
    num_batches = (len(final_dataset) - 1) // batch_size + 1
    for batch_num in range(num_batches):
        start = batch_num * batch_size
        end = min(start + batch_size, len(final_dataset))
        batch_file = output_dir / f"MondayBoard_Update_batch_{batch_num + 1}_records_{start + 1}_to_{end}.xlsx"
        logging.info(
            "Exporting batch %d (%d records) → %s", batch_num + 1, end - start, batch_file
        )
        final_dataset.iloc[start:end].to_excel(batch_file, engine="xlsxwriter", index=True)

    logging.info("******************* DONE! ******************************************")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

@click.command()
@click.option("--input-dir",  required=True,
              help="Directory containing the Monday board xlsx export (HEAL_Studies_*.xlsx).")
@click.option("--output-dir", default=None,
              help="Directory for xlsx files and logs. Defaults to --input-dir.")
@click.option("--table",      default="study_summary", show_default=True,
              help="MySQL table name to read from.")
@click.option("--ssh-host",   default=lambda: os.getenv("SSH_HOST", ""),
              required=True,  help="Bastion/EC2 host. Env: SSH_HOST.")
@click.option("--ssh-user",   default=lambda: os.getenv("SSH_USER", "ec2-user"),
              show_default=True, help="SSH username. Env: SSH_USER.")
@click.option("--ssh-key",    default=lambda: os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa"),
              show_default=True, help="Path to SSH private key (.pem). Env: SSH_KEY_PATH.")
@click.option("--debug",      is_flag=True, default=False, help="Enable debug logging.")
def create_monday_update_file(
    input_dir: str, output_dir: str, table: str,
    ssh_host: str, ssh_user: str, ssh_key: str, debug: bool,
):
    input_dir  = Path(input_dir)
    output_dir = Path(output_dir) if output_dir else input_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(output_dir / "report-log.txt"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    rds_host = os.getenv("DB_HOST")
    logging.info("Opening SSH tunnel  %s  →  %s:%d", ssh_host, rds_host, RDS_PORT)

    with SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_pkey=str(Path(ssh_key).expanduser()),
        remote_bind_address=(rds_host, RDS_PORT),
    ) as tunnel:
        local_port = tunnel.local_bind_port
        logging.info("Tunnel open on local port %d", local_port)

        # ---- STEP 1: Read study_summary from MySQL ----
        logging.info("---- STEP 1: Reading study_summary from MySQL (%s)", table)
        conn = _connect(local_port)
        df = read_study_summary(conn, table=table)
        conn.close()

    # ---- STEP 2: Import Monday board xlsx ----
    logging.info("---- STEP 2: Importing Monday Studies Board export")
    monday_board = import_monday_board(input_dir)

    # ---- STEP 3: Compare study_summary keys vs Monday board ----
    logging.info("---- STEP 3: Comparing study_summary with Monday Board")
    mondayboard_missing_in_lookup, lookup_fields = compare_study_lookup_monday(df, monday_board)
    compare_research_focus(df, monday_board, output_dir)

    # ---- STEP 4: Backfill emails from Monday board ----
    logging.info("---- STEP 4: Backfilling any missing emails from Monday Board")
    df = backfill_emails_from_monday(df, monday_board, output_dir)

    # ---- STEP 5: Final column renames + cleanup ----
    logging.info("---- STEP 5: Final column renames for Monday Board")
    final_dataset = _prep_for_export(df)

    # ---- STEP 6: Distribution summary ----
    logging.info("---- STEP 6: Distribution summary")
    st_col = "study_type" if "study_type" in final_dataset.columns else None
    if st_col:
        logging.info(
            "Counts for study types in the final dataset:\n%s",
            final_dataset[st_col].value_counts().to_string(),
        )
    resnet_col = "Research Network" if "Research Network" in final_dataset.columns else "research_network"
    if resnet_col in final_dataset.columns:
        logging.info(
            "Research network distribution:\n%s",
            final_dataset[resnet_col].value_counts().to_string(),
        )

    # ---- STEP 7: QC — NA counts ----
    logging.info("---- STEP 7: Fields with empty values in final dataset")
    na_counts = {k: int(pd.isna(final_dataset[k]).sum()) for k in final_dataset.columns}
    na_nonzero = {k: v for k, v in na_counts.items() if v > 0}
    import pprint
    logging.info("Fields and NA counts:\n%s", pprint.pformat(na_nonzero))

    # ---- STEP 8: Final numbers and export ----
    logging.info("---- STEP 8: Final numbers and Export")
    export_finaldata(output_dir, final_dataset, mondayboard_missing_in_lookup, monday_board)


if __name__ == "__main__":
    create_monday_update_file()

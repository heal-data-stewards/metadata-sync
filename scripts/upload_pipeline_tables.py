"""
Upload HEAL pipeline output CSVs to MySQL and rebuild study_summary — via SSH tunnel.

Steps (all in one tunnel session):
  1. Upload pipeline CSV outputs to their MySQL tables
  2. Rebuild the study_summary table from all MySQL source tables

Tables uploaded from pipeline (all from Output/ unless noted):
  study_lookup_table  — HEAL_04 output
  engagement_flags    — HEAL_05 output
  research_networks   — HEAL_01 export
  reporter_dqaudit    — HEAL_03 output (new awards not yet in MySQL)
  pi_emails           — Input/ (manually maintained)

Then study_summary is rebuilt by reading all of the above plus awards, reporter,
progress_tracker, and po_emails (already in MySQL) — same logic as the Lambda.

Usage:
    python upload_pipeline_tables.py \\
        --run-dir /path/to/PythonMySQLRun_Aug2026 \\
        --date 20260827 \\
        --ssh-host <bastion-ip-or-hostname> \\
        --ssh-user ec2-user \\
        --ssh-key  ~/.ssh/your-key.pem

Env vars (same .env used by the lambdas):
    DB_USER, DB_PASSWORD, DB_HOST, DB_NAME

SSH options can also be set via env vars:
    SSH_HOST, SSH_USER, SSH_KEY_PATH
"""

import logging
import os
import sys
from pathlib import Path

import click
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sshtunnel import SSHTunnelForwarder

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "mysql_tables_sync_lambda"))
from tables.study_summary import update_study_summary  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)

RDS_PORT = 3306


def _alchemy_engine(local_port: int):
    u  = os.getenv("DB_USER")
    p  = os.getenv("DB_PASSWORD")
    db = os.getenv("DB_NAME")
    return create_engine(f"mysql+pymysql://{u}:{p}@127.0.0.1:{local_port}/{db}")


def _connector_conn(local_port: int):
    return mysql.connector.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host="127.0.0.1",
        port=local_port,
        database=os.getenv("DB_NAME"),
    )


def _upload_csv(engine, csv_path: Path, table: str) -> None:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    df = df.replace("", None)
    df.to_sql(table, con=engine, if_exists="replace", index=False)
    logging.info("  %-30s → %d rows", table, len(df))


def run(run_dir: Path, today: str, ssh_host: str, ssh_user: str, ssh_key: str,
        study_summary_table: str, skip_upload: bool) -> None:

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

        # ── Step 1: Upload pipeline CSVs ────────────────────────────────────
        if not skip_upload:
            logging.info("---- Step 1: Uploading pipeline tables")
            engine = _alchemy_engine(local_port)
            out = run_dir / "Output"
            inp = run_dir / "Input"

            pipeline_tables = [
                # (csv_path,                              mysql_table,          required)
                (out / "study_lookup_table.csv",          "study_lookup_table", True),
                (out / "engagement_flags.csv",            "engagement_flags",   True),
                (out / f"research_networks_{today}.csv",  "research_networks",  True),
                (out / "reporter_dqaudit.csv",            "reporter_dqaudit",   True),
                (inp / f"pi_emails_{today}.csv",          "pi_emails",          True),
            ]

            for csv_path, table_name, required in pipeline_tables:
                if not csv_path.exists():
                    if required:
                        logging.error("REQUIRED file missing: %s", csv_path)
                        sys.exit(1)
                    logging.warning("Optional file not found, skipping: %s", csv_path)
                    continue
                logging.info("  Uploading %s", csv_path.name)
                _upload_csv(engine, csv_path, table_name)
        else:
            logging.info("---- Step 1: Skipped (--skip-upload)")

        # ── Step 2: Rebuild study_summary ────────────────────────────────────
        logging.info("---- Step 2: Rebuilding %s from all MySQL source tables", study_summary_table)
        conn = _connector_conn(local_port)
        try:
            result = update_study_summary(conn, target_table=study_summary_table)
            logging.info(
                "study_summary rebuilt: %d rows written to %s",
                result["rows_written"], result["table"],
            )
        finally:
            conn.close()

    logging.info("Tunnel closed. Done.")


@click.command()
@click.option("--run-dir",  required=True,
              help="Pipeline run directory (e.g. PythonMySQLRun_Aug2026).")
@click.option("--date",     required=True,
              help="Run date string used in filenames (e.g. 20260827).")
@click.option("--ssh-host", default=lambda: os.getenv("SSH_HOST", ""),
              required=True, help="Bastion/EC2 host. Env: SSH_HOST.")
@click.option("--ssh-user", default=lambda: os.getenv("SSH_USER", "ec2-user"),
              show_default=True, help="SSH username. Env: SSH_USER.")
@click.option("--ssh-key",  default=lambda: os.getenv("SSH_KEY_PATH", "~/.ssh/id_rsa"),
              show_default=True, help="Path to SSH private key (.pem). Env: SSH_KEY_PATH.")
@click.option("--study-summary-table", default="study_summary",
              show_default=True, help="MySQL table name to write study_summary to.")
@click.option("--skip-upload", is_flag=True, default=False,
              help="Skip CSV upload and only rebuild study_summary (useful if tables are already current).")
def main(run_dir, date, ssh_host, ssh_user, ssh_key, study_summary_table, skip_upload):
    run(Path(run_dir), date, ssh_host, ssh_user, ssh_key, study_summary_table, skip_upload)


if __name__ == "__main__":
    main()

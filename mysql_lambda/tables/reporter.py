"""
Update the `reporter` MySQL table.

Reads appl_ids from the `awards` table (plus any already in `reporter`),
fetches current NIH Reporter data for each, and replaces the table.

Depends on reporter_lib — at Lambda deploy time, include
metadata-sync/mysql_lambda/reporter_lib/ in the package.
"""

import logging
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError

from reporter_lib.heal_award_segmenter_lib import prepare_for_ingest, process_awards

logger = logging.getLogger(__name__)

_DEFAULT_DD = os.path.join(os.path.dirname(__file__), "..", "reporter_lib", "reporter_dd.csv")


def _build_sql_dtype_map(dd_path: str) -> dict | None:
    """Build a SQLAlchemy dtype map from reporter_dd.csv. Returns None if file missing."""
    import re
    from sqlalchemy import types as sqltypes

    _TYPE_MAP = {
        "varchar": sqltypes.VARCHAR,
        "text": sqltypes.Text,
        "longtext": sqltypes.Text,
        "mediumtext": sqltypes.Text,
        "int": sqltypes.Integer,
        "integer": sqltypes.Integer,
        "bigint": sqltypes.BigInteger,
        "float": sqltypes.Float,
        "double": sqltypes.Float,
        "decimal": sqltypes.Numeric,
        "date": sqltypes.Date,
        "datetime": sqltypes.DateTime,
        "boolean": sqltypes.Boolean,
        "bool": sqltypes.Boolean,
    }
    if not os.path.exists(dd_path):
        return None
    dd = pd.read_csv(dd_path)
    dtype_map = {}
    for _, row in dd.iterrows():
        col_name = str(row["var_name"]).strip()
        col_type = str(row["var_fmt_proposed"]).strip().lower()
        if col_type.startswith("varchar"):
            match = re.search(r'\((\d+)\)', col_type)
            dtype_map[col_name] = sqltypes.VARCHAR(int(match.group(1)) if match else 255)
        else:
            base = re.sub(r'\(.*\)', '', col_type).strip()
            sa_type = _TYPE_MAP.get(base)
            if sa_type:
                dtype_map[col_name] = sa_type()
    return dtype_map


def update_reporter(engine, target_table: str | None = None, sns_topic_arn: str | None = None) -> dict:
    """
    Refresh the reporter MySQL table.

    Args:
        engine:         SQLAlchemy engine (from db.create_alchemy_engine()).
        target_table:   Table name to write; defaults to TABLE_NAME env var or 'reporter'.
        sns_topic_arn:  Optional SNS topic for success/failure notifications.

    Returns:
        dict with keys: table, rows_written, added, removed
    """
    if target_table is None:
        target_table = os.getenv("REPORTER_TABLE_NAME", "reporter_test")
    dd_path = os.getenv("REPORTER_DD_PATH", _DEFAULT_DD)
    run_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    # Collect appl_ids: union of awards table and existing reporter rows
    awards_db = pd.read_sql_table("awards", con=engine)
    awards_db["appl_id"] = awards_db["appl_id"].astype(str)
    logger.info("Read %d rows from awards", len(awards_db))

    try:
        existing = pd.read_sql_table(target_table, con=engine)
        existing["appl_id"] = existing["appl_id"].astype(str)
        old_ids = set(existing["appl_id"])
        logger.info("Read %d rows from existing %s", len(existing), target_table)
    except (SQLAlchemyError, NoSuchTableError, ValueError):
        logger.info("%s does not exist yet — first run", target_table)
        old_ids = set()

    combined_ids = (
        pd.concat([awards_db[awards_db["appl_id"] != ""]["appl_id"], pd.Series(list(old_ids))])
        .dropna()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    logger.info("Total unique appl_ids to query: %d", len(combined_ids))

    id_df = pd.DataFrame({"Appl ID": combined_ids, "Title": ""})
    awards_df, _ = process_awards(id_df, "appl_id", "Appl ID", "Title")
    reporter_df = prepare_for_ingest(awards_df)

    dtype_map = _build_sql_dtype_map(dd_path)
    reporter_df.to_sql(target_table, con=engine, if_exists="replace", index=False, dtype=dtype_map)
    logger.info("Wrote %d rows to %s", len(reporter_df), target_table)

    new_ids = set(reporter_df["appl_id"].astype(str))
    added   = sorted(new_ids - old_ids)
    removed = sorted(old_ids - new_ids)

    summary = (
        f"Reporter table update succeeded\n"
        f"Run time: {run_time}  Table: {target_table}\n"
        f"Rows: {len(reporter_df)}  Added: {len(added)}  Removed: {len(removed)}"
    )
    logger.info(summary)
    if sns_topic_arn:
        boto3.client("sns").publish(
            TopicArn=sns_topic_arn,
            Subject=f"Reporter update succeeded — {run_time}",
            Message=summary,
        )

    return {"table": target_table, "rows_written": len(reporter_df), "added": len(added), "removed": len(removed)}

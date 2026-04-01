import os
import re
import json
from datetime import datetime, timezone
import boto3
from sqlalchemy import create_engine
from sqlalchemy import types as sqltypes
from sqlalchemy.exc import SQLAlchemyError, NoSuchTableError
from dotenv import load_dotenv
import pandas as pd
import logging
from heal_award_segmenter_lib import process_awards, prepare_for_ingest


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _notify(topic_arn: str, subject: str, message: str):
    """Publish a plain-text notification to SNS. No-op if no topic ARN is configured."""
    if not topic_arn:
        return
    boto3.client('sns').publish(TopicArn=topic_arn, Subject=subject, Message=message)

# Maps lowercase MySQL type base names to SQLAlchemy types
_MYSQL_TYPE_MAP = {
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

def build_sql_dtype_map(dd_path: str, name_col: str = "name", type_col: str = "type") -> dict:
    """
    Build a SQLAlchemy dtype dict from a data dictionary CSV for use with to_sql(dtype=...).

    The CSV must have at minimum a column for the variable name and one for the
    MySQL type string (e.g. "VARCHAR(255)", "INT", "TEXT", "DATETIME").

    Args:
        dd_path:  Path to the data dictionary CSV file.
        name_col: Column in the CSV that holds variable/column names (default: "name").
        type_col: Column in the CSV that holds MySQL type strings (default: "type").

    Returns:
        Dict mapping column names to SQLAlchemy type instances.
    """
    dd = pd.read_csv(dd_path)
    dtype_map = {}
    for _, row in dd.iterrows():
        col_name = str(row[name_col]).strip()
        col_type = str(row[type_col]).strip().lower()

        if col_type.startswith("varchar"):
            match = re.search(r'\((\d+)\)', col_type)
            length = int(match.group(1)) if match else 255
            dtype_map[col_name] = sqltypes.VARCHAR(length)
        else:
            base_type = re.sub(r'\(.*\)', '', col_type).strip()
            sa_type = _MYSQL_TYPE_MAP.get(base_type)
            if sa_type:
                dtype_map[col_name] = sa_type()
            else:
                logger.warning("Unknown MySQL type '%s' for column '%s' — skipping", col_type, col_name)

    return dtype_map

def lambda_handler(event, context):
    load_dotenv()

    db_username = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_database = os.getenv('DB_NAME')
    db_target_tablename = os.getenv('TABLE_NAME', 'reporter_test')
    sns_topic_arn = os.getenv('REPORTER_SNS_TOPIC_ARN')
    _default_dd = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'reporter_dd.csv')
    reporter_dd_path = os.getenv('REPORTER_DD_PATH', _default_dd)

    run_time = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    engine_url = f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_database}'
    engine = create_engine(engine_url)

    try:
        # Read awards table (required)
        awards_db = pd.read_sql_table("awards", con=engine)
        awards_db['appl_id'] = awards_db['appl_id'].astype(str)
        logger.info("Read %d rows from awards", len(awards_db))

        # Read existing reporter table to track changes (optional — may not exist on first run)
        try:
            reporter_db = pd.read_sql_table(db_target_tablename, con=engine)
            reporter_db['appl_id'] = reporter_db['appl_id'].astype(str)
            old_ids = set(reporter_db['appl_id'])
            logger.info("Read %d rows from existing reporter table", len(reporter_db))
        except (SQLAlchemyError, NoSuchTableError, ValueError):
            logger.info("Reporter table does not exist yet — treating as first run")
            old_ids = set()

        # Combine appl_ids from both tables and remove duplicates
        combined_ids = (
            pd.concat([awards_db['appl_id'], pd.Series(list(old_ids))])
            .dropna()
            .drop_duplicates()
            .reset_index(drop=True)
        )
        logger.info("Total unique appl_ids to query: %d", len(combined_ids))

        # Build a minimal DataFrame for process_awards (only project_id_col is used)
        id_df = pd.DataFrame({'Appl ID': combined_ids, 'Title': ''})

        id_type = "appl_id"
        project_id_col = "Appl ID"
        project_title_col = "Title"
        awards_df, _ = process_awards(id_df, id_type, project_id_col, project_title_col)
        reporter_df = prepare_for_ingest(awards_df)

        # Write to MySQL
        dtype_map = build_sql_dtype_map(reporter_dd_path, name_col="var_name", type_col="var_fmt_proposed") if os.path.exists(reporter_dd_path) else None
        reporter_df.to_sql(db_target_tablename, con=engine, if_exists="replace", index=False, dtype=dtype_map)
        logger.info("Wrote %d rows to reporter table", len(reporter_df))

        # Compute what changed
        new_ids = set(reporter_df['appl_id'].astype(str))
        added = sorted(new_ids - old_ids)
        removed = sorted(old_ids - new_ids)

        def _format_ids(ids, limit=50):
            preview = ', '.join(ids[:limit])
            return preview + (f'\n  ... and {len(ids) - limit} more' if len(ids) > limit else '')

        summary_lines = [
            f"Reporter table update succeeded",
            f"Run time:   {run_time}",
            f"Table:      {db_target_tablename}",
            f"Total rows: {len(reporter_df)}",
            f"Added:      {len(added)} rows",
            f"Removed:    {len(removed)} rows",
        ]
        if added:
            summary_lines += ["\nAdded appl_ids:", f"  {_format_ids(added)}"]
        if removed:
            summary_lines += ["\nRemoved appl_ids:", f"  {_format_ids(removed)}"]

        summary = '\n'.join(summary_lines)
        logger.info(summary)
        _notify(sns_topic_arn, f"Reporter update succeeded - {run_time}", summary)

    except Exception as e:
        error_msg = f"Reporter table update FAILED\nRun time: {run_time}\n\nError:\n{e}"
        logger.error(error_msg)
        _notify(sns_topic_arn, f"Reporter update FAILED - {run_time}", error_msg)
        raise

    return {
        'statusCode': 200,
        'body': json.dumps('Updated the reporter table!!')
    }


def main():
    """
    Local testing function.
    Reads from a CSV file, processes, and prints results.
    """
    # Example CSV path, adjust as needed
    csv_path = "/Users/hinashah/Documents/HEAL/ReporterCode/awards_01212026.csv"
    df = pd.read_csv(csv_path, dtype={'Appl ID':str})

    id_type = "appl_id"  # or "project_num"
    project_id_col = "Appl ID"
    project_title_col = "Title"

    awards_df, pubs_df = process_awards(df, id_type, project_id_col, project_title_col, clean_non_utf=True)

    print("Awards DataFrame:")
    print(awards_df.head())
    print("\nPublications DataFrame:")
    print(pubs_df.head())

    # Optionally save to CSV for testing
    awards_df.to_csv("/Users/hinashah/Documents/HEAL/ReporterCode/Output_032026/awards_test.csv", index=False)
    pubs_df.to_csv("/Users/hinashah/Documents/HEAL/ReporterCode/Output_032026/pubs_test.csv", index=False)

    reporter_df = prepare_for_ingest(awards_df)
    reporter_df.to_csv("/Users/hinashah/Documents/HEAL/ReporterCode/Output_032026/reporter_test.csv", index=False)

if __name__ == "__main__":
    main()
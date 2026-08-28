"""
Update the `heal_awards_reporter_sn` MySQL table.

Collects project serial numbers from both the `reporter` table (proj_ser_num)
and the `awards` table (derived from proj_num), then queries NIH Reporter for
all awards associated with those serial numbers. The result is broader than the
reporter table — it surfaces awards that share a grant serial number with a
known HEAL award but aren't yet in the awards/reporter tables.

This table is consumed by HEAL_03_DQAudit as its primary DQ audit input.
Depends on the `reporter` table being current (run after update_reporter).
"""

import logging
import os

import pandas as pd
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError

from reporter_lib.heal_award_segmenter_lib import (
    flatten_json,
    post_request,
    prepare_for_ingest,
)

logger = logging.getLogger(__name__)


def update_heal_awards_reporter_sn(engine, target_table: str | None = None) -> dict:
    """
    Refresh the heal_awards_reporter_sn MySQL table.

    Args:
        engine:       SQLAlchemy engine (from db.create_alchemy_engine()).
        target_table: Table name to write; defaults to HEAL_AWARDS_REPORTER_SN_TABLE_NAME
                      env var or 'heal_awards_reporter_sn_test'.

    Returns:
        dict with keys: table, rows_written, serial_nums_queried
    """
    if target_table is None:
        target_table = os.getenv(
            "HEAL_AWARDS_REPORTER_SN_TABLE_NAME", "heal_awards_reporter_sn_test"
        )

    # Pull proj_ser_num from reporter table (which is built from awards, so covers both)
    try:
        reporter_db = pd.read_sql_table("reporter", con=engine)
    except (SQLAlchemyError, NoSuchTableError, ValueError) as e:
        logger.error("Could not read reporter table: %s", e)
        return {"table": target_table, "rows_written": 0, "serial_nums_queried": 0}

    if "proj_ser_num" not in reporter_db.columns:
        logger.error("reporter table has no proj_ser_num column — aborting")
        return {"table": target_table, "rows_written": 0, "serial_nums_queried": 0}

    serial_list = (
        reporter_db["proj_ser_num"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s != ""]
        .unique()
        .tolist()
    )
    logger.info("Collected %d unique serial nums from reporter table", len(serial_list))
    logger.info("Total unique serial numbers to query: %d", len(serial_list))

    if not serial_list:
        logger.error("No serial numbers collected — aborting")
        return {"table": target_table, "rows_written": 0, "serial_nums_queried": 0}

    # --- Query NIH Reporter by project_serial_num ---
    results = post_request(True, "project_serial_num", serial_list)
    results_flat = [flatten_json(r) for r in results]
    awards_df = pd.DataFrame(results_flat)
    logger.info("NIH Reporter returned %d rows for %d serial numbers", len(awards_df), len(serial_list))

    if awards_df.empty:
        logger.warning("No results returned from NIH Reporter — writing empty table")
        awards_df.to_sql(target_table, con=engine, if_exists="replace", index=False)
        return {"table": target_table, "rows_written": 0, "serial_nums_queried": len(serial_list)}

    # --- Rename/drop columns using the same mapping as the reporter table ---
    reporter_sn_df = prepare_for_ingest(awards_df)
    reporter_sn_df.to_sql(target_table, con=engine, if_exists="replace", index=False)
    logger.info("Wrote %d rows to %s", len(reporter_sn_df), target_table)

    return {
        "table": target_table,
        "rows_written": len(reporter_sn_df),
        "serial_nums_queried": len(serial_list),
    }

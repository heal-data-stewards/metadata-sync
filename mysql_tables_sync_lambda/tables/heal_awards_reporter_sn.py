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
import re

import pandas as pd
from sqlalchemy.exc import NoSuchTableError, SQLAlchemyError

from reporter_lib.heal_award_segmenter_lib import (
    flatten_json,
    post_request,
    prepare_for_ingest,
    utfy_dict,
)

logger = logging.getLogger(__name__)

_SERIAL_RE = re.compile(r'[A-Z]{2}\d{6}')


def _serial_from_proj_num(proj_num: str) -> str | None:
    """Extract IC+6-digit serial number from a full NIH project number."""
    m = _SERIAL_RE.search(str(proj_num))
    return m.group(0) if m else None


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

    serial_nums: set[str] = set()

    # --- Source 1: proj_ser_num from reporter table ---
    try:
        reporter_db = pd.read_sql_table("reporter", con=engine)
        sn_col = "proj_ser_num"
        if sn_col in reporter_db.columns:
            from_reporter = (
                reporter_db[sn_col]
                .dropna()
                .astype(str)
                .str.strip()
                .loc[lambda s: s != ""]
                .unique()
                .tolist()
            )
            serial_nums.update(from_reporter)
            logger.info("Collected %d serial nums from reporter table", len(from_reporter))
        else:
            logger.warning("reporter table has no proj_ser_num column — skipping")
    except (SQLAlchemyError, NoSuchTableError, ValueError) as e:
        logger.warning("Could not read reporter table: %s", e)

    # --- Source 2: derive serial numbers from proj_num in awards table ---
    try:
        awards_db = pd.read_sql_table("awards", con=engine)
        if "proj_num" in awards_db.columns:
            from_awards = (
                awards_db["proj_num"]
                .dropna()
                .astype(str)
                .map(_serial_from_proj_num)
                .dropna()
                .unique()
                .tolist()
            )
            serial_nums.update(from_awards)
            logger.info("Collected %d serial nums from awards table", len(from_awards))
        else:
            logger.warning("awards table has no proj_num column — skipping")
    except (SQLAlchemyError, NoSuchTableError, ValueError) as e:
        logger.warning("Could not read awards table: %s", e)

    serial_list = sorted(serial_nums)
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

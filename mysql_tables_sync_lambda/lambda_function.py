"""
HEAL MySQL Table Updater — single Lambda entry point.

Dispatches to individual table-update functions based on the `table` key
in the Lambda event. New tables are registered in TABLE_REGISTRY below.

Event format
------------
{"table": "reporter"}        → update reporter table only
{"table": "study_summary"}   → update study_summary table only
{"table": "all"}             → update all tables in dependency order
{}                           → defaults to "all"

EventBridge / scheduled triggers
---------------------------------
Use separate EventBridge rules with different event payloads to run
individual tables on different cadences:

  Weekly (reporter):       {"table": "reporter"}
  Weekly (study_summary):  {"table": "study_summary"}
  Full refresh (all):      {"table": "all"}   (or omit key)

Adding a new table
------------------
1. Create metadata-sync/mysql_tables_sync_lambda/tables/<your_table>.py
2. Implement update_<your_table>(conn_or_engine, ...) -> dict
3. Add an entry to TABLE_REGISTRY below.

Lambda packaging notes
----------------------
Include these files alongside lambda_function.py when deploying:
  - db.py
  - notify.py
  - tables/__init__.py
  - tables/reporter.py
  - tables/study_summary.py
  - reporter_lib/heal_award_segmenter_lib.py   (required by tables/reporter.py)
  - reporter_lib/reporter_dd.csv               (optional; controls column types)

Slack notifications
--------------------
Set SLACK_WEBHOOK_URL to a Slack incoming webhook URL to get a message per
table when it finishes (success or failure). Unset = notifications disabled.
"""

import json
import logging

from dotenv import load_dotenv

from db import connect_mysql, create_alchemy_engine
from notify import notify_slack
from tables.reporter import update_reporter
from tables.study_summary import update_study_summary

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Table registry
#
# Each entry: table_key -> callable that performs the update and returns a dict.
# Order matters for "all" — tables listed earlier run first.
# reporter must precede study_summary (study_summary reads from reporter).
# ---------------------------------------------------------------------------

def _run_reporter():
    engine = create_alchemy_engine()
    return update_reporter(engine)


def _run_study_summary():
    conn = connect_mysql()
    result = update_study_summary(conn)
    conn.close()
    return result


TABLE_REGISTRY = {
    "reporter":      _run_reporter,
    "study_summary": _run_study_summary,
    # Future tables — add entries here, e.g.:
    # "engagement_flags": _run_engagement_flags,
    # "awards":           _run_awards,
}

_HEADERS = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    table = event.get("table", "all").lower().strip()
    logger.info("Invoked with table=%s", table)

    if table == "all":
        tables_to_run = list(TABLE_REGISTRY.keys())
    elif table in TABLE_REGISTRY:
        tables_to_run = [table]
    else:
        return {
            "statusCode": 400,
            "headers": _HEADERS,
            "body": json.dumps({
                "error": f"Unknown table '{table}'. Valid values: {list(TABLE_REGISTRY)} or 'all'."
            }),
        }

    results = {}
    errors  = {}
    for t in tables_to_run:
        try:
            logger.info("Starting update: %s", t)
            results[t] = TABLE_REGISTRY[t]()
            logger.info("Finished update: %s → %s", t, results[t])
            details = "  ".join(f"{k}={v}" for k, v in results[t].items() if k != "table")
            notify_slack(f":white_check_mark: `{t}` update complete — {details}")
        except Exception as e:
            logger.exception("Failed to update %s", t)
            errors[t] = str(e)
            notify_slack(f":x: `{t}` update failed — {e}")

    status = 500 if errors else 200
    return {
        "statusCode": status,
        "headers": _HEADERS,
        "body": json.dumps({"results": results, "errors": errors}),
    }


# ---------------------------------------------------------------------------
# Script entry point (local testing)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    table_arg = sys.argv[1] if len(sys.argv) > 1 else "all"
    response = lambda_handler({"table": table_arg}, None)
    print(json.dumps(json.loads(response["body"]), indent=2))

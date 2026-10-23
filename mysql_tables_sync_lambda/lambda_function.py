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
  - sources.py
  - notify.py
  - tables/__init__.py
  - tables/reporter.py
  - tables/study_summary.py
  - tables/research_networks.py
  - tables/mysql_merge.py
  - tables/reporter_dqaudit.py
  - tables/study_lookup_table.py
  - tables/engagement_flags.py
  - tables/alldata.py
  - reporter_lib/heal_award_segmenter_lib.py   (required by tables/reporter.py)
  - reporter_lib/reporter_dd.csv               (optional; controls column types)

tempspace/healstudies database split
--------------------------------------
Each table's *output* database is independent of its table-name env var
(<TABLE>_TABLE_NAME, default "<table>" -- the real/production name in every
environment, since tempspace vs. healstudies is what actually distinguishes
test from production now; a "_test" suffix on top of that was redundant and
confusing). Which database
is a single shared choice, not a per-table env var: TEMPSPACE_DB_NAME and
HEALSTUDIES_DB_NAME name the two real databases (defaults "tempspace" and
"healstudies"), and DB_ENVIRONMENT ("test"/"production", default "test" --
the safe choice) decides, once per deployed function, whether every table
writes to tempspace regardless (test) or each table writes to whatever
TABLE_PRODUCTION_DB below says (production). Adding a table's production
home is one line in that dict, not a new env var. A table's *inputs*
(e.g. research_networks reading `awards`) still come from wherever DB_NAME
already points, unaffected by any of this -- only each table's own
written-to database moves.

Slack notifications
--------------------
Set SLACK_WEBHOOK_URL to a Slack incoming webhook URL to get a message per
table when it finishes (success or failure). Unset = notifications disabled.
"""

import json
import logging
import os

from dotenv import load_dotenv

from db import connect_mysql, create_alchemy_engine
from notify import notify_slack
from sources import LocalExcelSource, S3ExcelSource
from tables.reporter import update_reporter
from tables.study_summary import update_study_summary
from tables.heal_awards_reporter_sn import update_heal_awards_reporter_sn
from tables.research_networks import update_research_networks
from tables.mysql_merge import update_mysql_merge
from tables.reporter_dqaudit import update_reporter_dqaudit
from tables.study_lookup_table import generate_match_candidates, update_study_lookup_table
from tables.engagement_flags import update_engagement_flags
from tables.alldata import update_alldata

load_dotenv()

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s", force=True)
logger = logging.getLogger(__name__)

# Manual inputs (hand-maintained reference files a person uploads, rather
# than anything synced automatically) live here. Same bucket convention as
# package_board_lambda/monday_package_board_lambda's inputs.
S3_BUCKET = os.getenv("S3_BUCKET", "heal-dashboard-snapshots")
MANUAL_INPUTS_PREFIX = os.getenv("MANUAL_INPUTS_PREFIX", "mysql_tables_sync/inputs")

# ---------------------------------------------------------------------------
# tempspace/healstudies database routing -- one shared mechanism, not a
# per-table env var. See the module docstring's "tempspace/healstudies
# database split" section above.
# ---------------------------------------------------------------------------

TEMPSPACE_DB_NAME = os.getenv("TEMPSPACE_DB_NAME", "tempspace")
HEALSTUDIES_DB_NAME = os.getenv("HEALSTUDIES_DB_NAME", "healstudies")
DB_ENVIRONMENT = os.getenv("DB_ENVIRONMENT", "test").lower().strip()

# Each table's home database when DB_ENVIRONMENT=production. Add a line
# here, not a new env var, as each table's real home gets decided -- tables
# not listed default to tempspace even in production (the safe choice when
# nobody's explicitly decided otherwise yet).
TABLE_PRODUCTION_DB = {
    "research_networks": "healstudies",
    "reporter": "healstudies",
    "heal_awards_reporter_sn": "healstudies",
    "study_summary": "healstudies",
    "reporter_dqaudit": "healstudies",
    "study_lookup_table": "healstudies",
    "engagement_flags": "healstudies",
    "alldata": "healstudies",
}


def _resolve_db_name(table_key: str) -> str:
    """Which database table_key writes to, given DB_ENVIRONMENT."""
    if DB_ENVIRONMENT != "production":
        return TEMPSPACE_DB_NAME
    return HEALSTUDIES_DB_NAME if TABLE_PRODUCTION_DB.get(table_key) == "healstudies" else TEMPSPACE_DB_NAME

# ---------------------------------------------------------------------------
# Table registry
#
# Each entry: table_key -> callable that performs the update and returns a dict.
# Order matters for "all" — tables listed earlier run first.
# reporter must precede study_summary (study_summary reads from reporter).
# research_networks must also precede study_summary (same reason).
# ---------------------------------------------------------------------------

def _run_reporter():
    # awards is an external input, not built by this lambda -- stays on
    # whatever DB_NAME already points to, regardless of DB_ENVIRONMENT.
    write_engine = create_alchemy_engine(database=_resolve_db_name("reporter"))
    read_engine = create_alchemy_engine()
    return update_reporter(write_engine, read_engine=read_engine)


def _run_study_summary():
    # study_summary's own write + the 4 source tables not built by this
    # lambda (awards, progress_tracker, pi_emails, po_emails) all use
    # DB_NAME, unchanged. reporter/research_networks/reporter_dqaudit/
    # study_lookup_table/engagement_flags are built by this lambda and may
    # live elsewhere under DB_ENVIRONMENT -- separate connections, opened
    # once per distinct database and reused (same _conn_for() pattern as
    # _run_mysql_merge, now that there are 6 instead of 5).
    db_name = os.getenv("DB_NAME")
    open_conns = {}

    def _conn_for(database):
        if database not in open_conns:
            open_conns[database] = connect_mysql() if database == db_name else connect_mysql(database=database)
        return open_conns[database]

    conn = _conn_for(db_name)
    reporter_conn = _conn_for(_resolve_db_name("reporter"))
    resnet_conn = _conn_for(_resolve_db_name("research_networks"))
    dqaudit_conn = _conn_for(_resolve_db_name("reporter_dqaudit"))
    study_lookup_conn = _conn_for(_resolve_db_name("study_lookup_table"))
    engagement_flags_conn = _conn_for(_resolve_db_name("engagement_flags"))

    try:
        result = update_study_summary(
            conn,
            reporter_conn=reporter_conn,
            research_networks_conn=resnet_conn,
            reporter_dqaudit_conn=dqaudit_conn,
            study_lookup_table_conn=study_lookup_conn,
            engagement_flags_conn=engagement_flags_conn,
        )
    finally:
        for c in open_conns.values():
            c.close()
    return result


def _run_heal_awards_reporter_sn():
    # reporter is built by this same lambda, so read it from wherever
    # _resolve_db_name("reporter") says it actually lives (moves together
    # with it in test mode), not necessarily this table's own database.
    write_engine = create_alchemy_engine(database=_resolve_db_name("heal_awards_reporter_sn"))
    read_engine = create_alchemy_engine(database=_resolve_db_name("reporter"))
    return update_heal_awards_reporter_sn(write_engine, read_engine=read_engine)


def _run_research_networks():
    # awards lives wherever DB_NAME already points (unchanged, existing
    # behavior) -- only the *output* table's database is the
    # tempspace/healstudies choice, via _resolve_db_name().
    read_conn = connect_mysql()
    write_db = _resolve_db_name("research_networks")
    write_conn = read_conn if write_db == os.getenv("DB_NAME") else connect_mysql(database=write_db)
    ref_source = S3ExcelSource(f"s3://{S3_BUCKET}/{MANUAL_INPUTS_PREFIX}")
    try:
        result = update_research_networks(read_conn, write_conn, ref_source)
    finally:
        read_conn.close()
        if write_conn is not read_conn:
            write_conn.close()
    result["database"] = write_db
    return result


def _run_mysql_merge():
    # awards: external, always DB_NAME. reporter/research_networks: built
    # by this lambda, follow DB_ENVIRONMENT. progress_tracker: built by a
    # separate lambda that writes to healstudies only -- no tempspace copy
    # ever, so it's ALWAYS healstudies regardless of DB_ENVIRONMENT, same
    # as nihtables/mysql_merged (the write target) are ALWAYS tempspace.
    # Neither is in TABLE_PRODUCTION_DB (see tables/mysql_merge.py).
    #
    # Up to 5 distinct databases could be in play at once here, so connections
    # are opened once per distinct database and reused, rather than the
    # nested-reuse-or-not ternary _run_study_summary() uses for its 3.
    db_name = os.getenv("DB_NAME")
    open_conns = {}

    def _conn_for(database):
        if database not in open_conns:
            open_conns[database] = connect_mysql() if database == db_name else connect_mysql(database=database)
        return open_conns[database]

    conn = _conn_for(db_name)
    reporter_conn = _conn_for(_resolve_db_name("reporter"))
    resnet_conn = _conn_for(_resolve_db_name("research_networks"))
    progress_tracker_conn = _conn_for(HEALSTUDIES_DB_NAME)
    write_conn = _conn_for(TEMPSPACE_DB_NAME)

    ref_source = S3ExcelSource(f"s3://{S3_BUCKET}/{MANUAL_INPUTS_PREFIX}")
    try:
        result = update_mysql_merge(
            conn, ref_source,
            write_conn=write_conn, reporter_conn=reporter_conn, research_networks_conn=resnet_conn,
            progress_tracker_conn=progress_tracker_conn,
        )
    finally:
        for c in open_conns.values():
            c.close()
    return result


def _run_reporter_dqaudit():
    # No genuinely external inputs here -- nihtables, research_networks,
    # and heal_awards_reporter_sn are all themselves built by this lambda,
    # so nothing needs DB_NAME directly. nihtables is ALWAYS tempspace (see
    # tables/mysql_merge.py); the other two follow DB_ENVIRONMENT like
    # reporter_dqaudit itself. Same _conn_for() reuse pattern as
    # _run_mysql_merge/_run_study_summary.
    open_conns = {}

    def _conn_for(database):
        if database not in open_conns:
            open_conns[database] = connect_mysql(database=database)
        return open_conns[database]

    nihtables_conn = _conn_for(TEMPSPACE_DB_NAME)
    resnet_conn = _conn_for(_resolve_db_name("research_networks"))
    heal_sn_conn = _conn_for(_resolve_db_name("heal_awards_reporter_sn"))
    write_conn = _conn_for(_resolve_db_name("reporter_dqaudit"))

    try:
        result = update_reporter_dqaudit(
            write_conn,
            write_conn=write_conn,
            nihtables_conn=nihtables_conn,
            research_networks_conn=resnet_conn,
            heal_awards_reporter_sn_conn=heal_sn_conn,
        )
    finally:
        for c in open_conns.values():
            c.close()
    return result


def _run_study_manual_match_candidates():
    # Writes no MySQL table -- S3 only (the pending file + ledger +
    # archive). mysql_merged is ALWAYS tempspace; reporter_dqaudit follows
    # DB_ENVIRONMENT. Same _conn_for() reuse pattern as _run_reporter_dqaudit.
    open_conns = {}

    def _conn_for(database):
        if database not in open_conns:
            open_conns[database] = connect_mysql(database=database)
        return open_conns[database]

    mysql_merged_conn = _conn_for(TEMPSPACE_DB_NAME)
    reporter_dqaudit_conn = _conn_for(_resolve_db_name("reporter_dqaudit"))
    ref_source = S3ExcelSource(f"s3://{S3_BUCKET}/{MANUAL_INPUTS_PREFIX}")

    try:
        result = generate_match_candidates(mysql_merged_conn, reporter_dqaudit_conn, ref_source)
    finally:
        for c in open_conns.values():
            c.close()
    return result


def _run_study_lookup_table():
    # Finalizes study_lookup_table -- reshuffles every xstudy_id in it, so
    # this is deliberately a separate, independently-triggered registry
    # entry rather than something that runs automatically on every "all".
    # mysql_merged is ALWAYS tempspace; reporter_dqaudit/study_lookup_table
    # (the write target) follow DB_ENVIRONMENT.
    open_conns = {}

    def _conn_for(database):
        if database not in open_conns:
            open_conns[database] = connect_mysql(database=database)
        return open_conns[database]

    mysql_merged_conn = _conn_for(TEMPSPACE_DB_NAME)
    reporter_dqaudit_conn = _conn_for(_resolve_db_name("reporter_dqaudit"))
    write_conn = _conn_for(_resolve_db_name("study_lookup_table"))
    ref_source = S3ExcelSource(f"s3://{S3_BUCKET}/{MANUAL_INPUTS_PREFIX}")

    try:
        result = update_study_lookup_table(
            mysql_merged_conn, reporter_dqaudit_conn, ref_source, write_conn=write_conn,
        )
    finally:
        for c in open_conns.values():
            c.close()
    return result


def _run_engagement_flags():
    # No genuinely external inputs -- nihtables, research_networks, and
    # study_lookup_table are all themselves built by this lambda. nihtables
    # is ALWAYS tempspace; the other two follow DB_ENVIRONMENT like
    # engagement_flags itself.
    open_conns = {}

    def _conn_for(database):
        if database not in open_conns:
            open_conns[database] = connect_mysql(database=database)
        return open_conns[database]

    nihtables_conn = _conn_for(TEMPSPACE_DB_NAME)
    resnet_conn = _conn_for(_resolve_db_name("research_networks"))
    study_lookup_conn = _conn_for(_resolve_db_name("study_lookup_table"))
    write_conn = _conn_for(_resolve_db_name("engagement_flags"))

    try:
        result = update_engagement_flags(
            write_conn,
            write_conn=write_conn,
            nihtables_conn=nihtables_conn,
            research_networks_conn=resnet_conn,
            study_lookup_table_conn=study_lookup_conn,
        )
    finally:
        for c in open_conns.values():
            c.close()
    return result


def _run_alldata():
    # pi_emails/progress_tracker: external, always DB_NAME. mysql_merged:
    # ALWAYS tempspace. reporter_dqaudit/study_lookup_table/
    # engagement_flags/research_networks/alldata (the write target) follow
    # DB_ENVIRONMENT.
    db_name = os.getenv("DB_NAME")
    open_conns = {}

    def _conn_for(database):
        if database not in open_conns:
            open_conns[database] = connect_mysql() if database == db_name else connect_mysql(database=database)
        return open_conns[database]

    conn = _conn_for(db_name)
    mysql_merged_conn = _conn_for(TEMPSPACE_DB_NAME)
    reporter_dqaudit_conn = _conn_for(_resolve_db_name("reporter_dqaudit"))
    study_lookup_conn = _conn_for(_resolve_db_name("study_lookup_table"))
    engagement_flags_conn = _conn_for(_resolve_db_name("engagement_flags"))
    research_networks_conn = _conn_for(_resolve_db_name("research_networks"))
    write_conn = _conn_for(_resolve_db_name("alldata"))

    try:
        result = update_alldata(
            conn,
            write_conn=write_conn,
            mysql_merged_conn=mysql_merged_conn,
            reporter_dqaudit_conn=reporter_dqaudit_conn,
            study_lookup_table_conn=study_lookup_conn,
            engagement_flags_conn=engagement_flags_conn,
            research_networks_conn=research_networks_conn,
        )
    finally:
        for c in open_conns.values():
            c.close()
    return result


TABLE_REGISTRY = {
    "reporter":               _run_reporter,
    "heal_awards_reporter_sn": _run_heal_awards_reporter_sn,  # must run after reporter
    "research_networks":     _run_research_networks,          # must run before study_summary
    "mysql_merge":            _run_mysql_merge,                # must run after reporter + research_networks
    "reporter_dqaudit":       _run_reporter_dqaudit,           # must run after mysql_merge + research_networks + heal_awards_reporter_sn
    "study_manual_match_candidates": _run_study_manual_match_candidates,  # must run after mysql_merge + reporter_dqaudit; not part of "all" (see note below)
    "study_lookup_table":     _run_study_lookup_table,         # must run after mysql_merge + reporter_dqaudit; not part of "all" (see note below)
    "engagement_flags":       _run_engagement_flags,           # must run after mysql_merge + research_networks + study_lookup_table
    "alldata":                _run_alldata,                    # must run after mysql_merge + reporter_dqaudit + study_lookup_table + engagement_flags + research_networks
    "study_summary":          _run_study_summary,              # must run after reporter_dqaudit + engagement_flags (+ study_lookup_table, once finalized)
    # Future tables — add entries here, e.g.:
    # "awards":           _run_awards,
}

# study_manual_match_candidates and study_lookup_table are both registered
# above but deliberately excluded from "all" (see _ALL_TABLES below):
# finalizing study_lookup_table reshuffles every xstudy_id in it, which
# should happen when a human decides to run it -- after reviewing whatever
# the candidates step surfaced -- not silently on every scheduled sync.
_ALL_TABLES = [k for k in TABLE_REGISTRY if k not in ("study_manual_match_candidates", "study_lookup_table")]

# Registry key -> (env var controlling the real table name, default table name).
# Mirrors the resolution each table module does internally — kept here too so
# Slack notifications can show the real table name even on failure, before
# the table function itself ever gets a chance to resolve/return it.
#
# mysql_merge produces two tables, not one -- it isn't in this dict at all,
# since its own result dict already carries both real table names directly
# (nihtables_table/mysql_merged_table), and _resolve_table_name()'s fallback
# (the bare registry key) is only used for the Slack "starting" log line,
# not anything that needs to be precise ahead of time.
_TABLE_NAME_ENV = {
    "reporter":               ("REPORTER_TABLE_NAME", "reporter"),
    "heal_awards_reporter_sn": ("HEAL_AWARDS_REPORTER_SN_TABLE_NAME", "heal_awards_reporter_sn"),
    "research_networks":     ("RESEARCH_NETWORKS_TABLE_NAME", "research_networks"),
    "reporter_dqaudit":       ("REPORTER_DQAUDIT_TABLE_NAME", "reporter_dqaudit"),
    "study_lookup_table":     ("STUDY_LOOKUP_TABLE_NAME", "study_lookup_table"),
    "engagement_flags":       ("ENGAGEMENT_FLAGS_TABLE_NAME", "engagement_flags"),
    "alldata":                ("ALLDATA_TABLE_NAME", "alldata"),
    "study_summary":          ("STUDY_SUMMARY_TABLE_NAME", "study_summary"),
}


def _resolve_table_name(key: str) -> str:
    env_var, default = _TABLE_NAME_ENV.get(key, (None, key))
    return os.getenv(env_var, default) if env_var else key

_HEADERS = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    table = event.get("table", "all").lower().strip()
    logger.info("Invoked with table=%s", table)

    if table == "all":
        tables_to_run = _ALL_TABLES
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
        table_name = _resolve_table_name(t)
        try:
            logger.info("Starting update: %s (table=%s)", t, table_name)
            results[t] = TABLE_REGISTRY[t]()
            logger.info("Finished update: %s → %s", t, results[t])
            details = "  ".join(f"{k}={v}" for k, v in results[t].items() if k != "table")
            notify_slack(f":white_check_mark: `{table_name}` update complete — {details}")
        except Exception as e:
            logger.exception("Failed to update %s", t)
            errors[t] = str(e)
            notify_slack(f":x: `{table_name}` update failed — {e}")

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

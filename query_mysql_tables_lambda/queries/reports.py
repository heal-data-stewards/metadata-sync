"""
Download/trigger support for the Excel reports mysql_tables_sync_lambda
writes to S3 (study_metrics, qc_report, gtd_targets). None of these are
MySQL tables -- they don't fit the SQL-query shape of the other
queries/*.py modules. Both functions below still take `conn` for
QUERY_REGISTRY's uniform calling convention, but neither touches MySQL.

mysql_tables_sync_lambda itself stays invoke-only (no API Gateway): its
runs can be long (several MySQL tables read + an Excel workbook built),
which risks API Gateway's hard synchronous timeout. This lambda is the
one externally-callable surface, so "trigger" here means an async,
fire-and-forget boto3 lambda.invoke -- the caller polls get_download_url
afterward and compares its last_modified to when they triggered.

DB_ENVIRONMENT here mirrors mysql_tables_sync_lambda's own env var of the
same name (default "test", the safe choice) -- this is a SEPARATE Lambda
deployment, so it can't share that module's Python state, but the
derived REPORTS_PREFIX/SYNC_FUNCTION_NAME below must still land on
exactly the S3 path and sync-function pair that THAT deployment's own
DB_ENVIRONMENT would produce, or get_download_url/trigger_regenerate
silently talk to the wrong environment's copy. Set DB_ENVIRONMENT on
this lambda's own config to match the mysql_tables_sync_lambda it's
meant to pair with (query_mysql_tables_test <-> mysql_tables_sync_test,
both default "test"; query_mysql_tables <-> mysql_tables_sync both need
DB_ENVIRONMENT=production explicitly set).
"""

import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

DB_ENVIRONMENT = os.getenv("DB_ENVIRONMENT", "test").lower().strip()

S3_BUCKET = os.getenv("S3_BUCKET", "heal-dashboard-snapshots")
REPORTS_PREFIX = os.getenv("REPORTS_PREFIX", f"mysql_tables_sync/reports/{DB_ENVIRONMENT}")
SYNC_FUNCTION_NAME = os.getenv(
    "MYSQL_TABLES_SYNC_FUNCTION_NAME",
    "mysql_tables_sync" if DB_ENVIRONMENT == "production" else "mysql_tables_sync_test",
)

_DEFAULT_EXPIRES_IN = 3600

# report key -> (S3 object filename, env var that can override it). The env
# var name must match the one mysql_tables_sync_lambda's own report module
# reads (QC_REPORT_FILE / GTD_REPORT_FILE / STUDY_METRICS_REPORT_FILE), so
# overriding the filename in one place keeps both lambdas in sync.
_REPORT_FILES = {
    "qc_report":     ("QCReport.xlsx", "QC_REPORT_FILE"),
    "gtd_targets":   ("gtd_targets.xlsx", "GTD_REPORT_FILE"),
    "study_metrics": ("StudyMetrics.xlsx", "STUDY_METRICS_REPORT_FILE"),
}

# report key -> table key in mysql_tables_sync_lambda's TABLE_REGISTRY.
# Same strings as _REPORT_FILES' keys today, kept as its own mapping so
# the two can diverge later without this file silently drifting.
_REPORT_TABLE_KEYS = {
    "qc_report":     "qc_report",
    "gtd_targets":   "gtd_targets",
    "study_metrics": "study_metrics",
}


def _report_key(report: str) -> str:
    filename_default, env_var = _REPORT_FILES[report]
    filename = os.getenv(env_var, filename_default)
    return f"{REPORTS_PREFIX}/{filename}" if REPORTS_PREFIX else filename


def get_download_url(conn, report=None, expires_in=_DEFAULT_EXPIRES_IN, **kwargs):
    """
    Return a presigned S3 GET URL for the given report's current .xlsx,
    plus when it was last generated. Does not trigger generation -- see
    trigger_regenerate for that.
    """
    if report not in _REPORT_FILES:
        return {
            "query": "get_download_url",
            "error": f"Unknown report '{report}'.",
            "valid_reports": list(_REPORT_FILES),
        }

    try:
        expires_in = int(expires_in)
    except (TypeError, ValueError):
        expires_in = _DEFAULT_EXPIRES_IN

    import boto3
    from botocore.exceptions import ClientError

    key = _report_key(report)
    client = boto3.client("s3")

    try:
        head = client.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        return {
            "query": "get_download_url",
            "report": report,
            "error": f"Report not found at s3://{S3_BUCKET}/{key} -- has it ever been generated? ({e})",
        }

    url = client.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires_in,
    )
    return {
        "query": "get_download_url",
        "report": report,
        "url": url,
        "expires_in": expires_in,
        "last_modified": head["LastModified"].isoformat(),
    }


def trigger_regenerate(conn, report=None, **kwargs):
    """
    Fire an async invoke of mysql_tables_sync_lambda ({"table": report})
    to regenerate the given report. Returns immediately, before the new
    copy exists -- poll get_download_url's last_modified against
    triggered_at to know when it's ready.
    """
    if report not in _REPORT_TABLE_KEYS:
        return {
            "query": "trigger_regenerate",
            "error": f"Unknown report '{report}'.",
            "valid_reports": list(_REPORT_TABLE_KEYS),
        }

    import boto3

    table_key = _REPORT_TABLE_KEYS[report]
    triggered_at = datetime.now(timezone.utc).isoformat()

    client = boto3.client("lambda")
    response = client.invoke(
        FunctionName=SYNC_FUNCTION_NAME,
        InvocationType="Event",  # async/fire-and-forget
        Payload=json.dumps({"table": table_key}).encode("utf-8"),
    )

    return {
        "query": "trigger_regenerate",
        "report": report,
        "function": SYNC_FUNCTION_NAME,
        "status_code": response.get("StatusCode"),
        "triggered_at": triggered_at,
    }

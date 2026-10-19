"""
Data Package / Data Reuse Monday board builder — monthly Lambda entry point.

Decouples UpdateMondayPackageBoard.ipynb into something that can run on a
schedule: reads the two monthly inputs from S3, combines them with live
MDS/HSS data, and writes the two board Excel files back to S3 for manual
upload to Monday. No comparison report and no manual-review merge — those
stay manual, per the original plan.

Inputs (s3://<S3_BUCKET>/<INPUT_PREFIX>/)
------------------------------------------
- CDE mapping metrics report: an Excel file matching CDE_REPORT_PATTERN
  (keeps its own dated name each month, e.g.
  cde_mapping_metrics_report_2026-09-29_v2.xlsx — the latest-modified match
  is used, so the Lambda doesn't need the exact name updated every month).
- Platform determination sheet: PLATFORM_SHEET_NAME (fixed name, re-uploaded
  in place each month) — Hina's hand-cleaned export of the platform's
  Google Sheet asset tracker.

Outputs (s3://<S3_BUCKET>/<OUTPUT_PREFIX>/)
--------------------------------------------
- DataPackageBoard_<today>.xlsx
- DataReuseBoard_<today>.xlsx

Lambda packaging notes
-----------------------
Include these files alongside lambda_function.py when deploying (flat, no
package wrapper — same convention as mysql_tables_sync_lambda):
  assemble_boards.py  blanks.py  board_metrics.py  cde_metrics.py
  data_package_links.py  hss_cde_mappings.py  hss_sections.py
  link_matching.py  mds_repo_links.py  notify.py
  platform_determinations.py  sources.py

Slack notifications
--------------------
Set SLACK_WEBHOOK_URL to a Slack incoming webhook URL to get a message when
the run finishes (success or failure). Unset = notifications disabled.
"""

import json
import logging
import os
from datetime import date

import httpx
import pandas as pd

from assemble_boards import build_data_package_board, build_data_reuse_board
from board_metrics import build_cde_board_metrics
from hss_cde_mappings import fetch_mapped_variables
from hss_sections import fetch_study_index_counts
from mds_repo_links import fetch_mds_records, extract_links
from notify import notify_slack
from platform_determinations import load_platform_assessments
from data_package_links import build_data_package_links
from sources import S3ExcelSource

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)
logger = logging.getLogger(__name__)

S3_BUCKET = os.getenv("S3_BUCKET", "heal-dashboard-snapshots")
INPUT_PREFIX = os.getenv("INPUT_PREFIX", "package_board/inputs")
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "package_board/outputs")
CDE_REPORT_PATTERN = os.getenv("CDE_REPORT_PATTERN", "cde_mapping_metrics_report_*.xlsx")
PLATFORM_SHEET_NAME = os.getenv(
    "PLATFORM_SHEET_NAME", "HEAL Data & Digital Assets Tracking - All HEAL Assets.csv"
)
MDS_BASE = os.getenv("MDS_BASE", "https://healdata.org/mds")
HSS_BASE = os.getenv("HSS_BASE", "https://heal.renci.org/search-api")


def run(input_source, output_source) -> dict:
    """Source-agnostic pipeline body — same code path for S3 and local test runs."""
    report_file = input_source.find_latest(CDE_REPORT_PATTERN)
    logger.info("Using CDE mapping metrics report: %s", report_file)
    file_summary = input_source.read_excel(report_file, sheet_name="file-summary")
    file_summary_audit = input_source.read_excel(report_file, sheet_name="file-summary-audit")

    platform_sheet = input_source.read_csv(PLATFORM_SHEET_NAME, dtype=str)
    platform_assessments = load_platform_assessments(platform_sheet)

    with httpx.Client() as client:
        hss_mapped_variables = fetch_mapped_variables(client, HSS_BASE)
        hss_study_counts = fetch_study_index_counts(client, HSS_BASE)
        mds_records = fetch_mds_records(client, MDS_BASE)

    mds_links = pd.DataFrame(extract_links(mds_records))
    hss_section_counts = {k: v["num_sections"] for k, v in hss_study_counts.items()}

    per_study, excluded_report_rows = build_cde_board_metrics(
        file_summary, file_summary_audit, hss_mapped_variables, hss_section_counts
    )
    links = build_data_package_links(mds_links, platform_assessments, hss_study_counts)

    data_package_board = build_data_package_board(links, per_study, hss_section_counts)
    data_reuse_board = build_data_reuse_board(per_study)

    today = date.today().strftime("%Y-%m-%d")
    data_package_name = f"DataPackageBoard_{today}.xlsx"
    data_reuse_name = f"DataReuseBoard_{today}.xlsx"
    output_source.write_excel(data_package_name, data_package_board)
    output_source.write_excel(data_reuse_name, data_reuse_board)

    if len(excluded_report_rows):
        logger.warning(
            "CDE report had %d row(s) with no HDPID (dropped): %s",
            len(excluded_report_rows),
            excluded_report_rows["source_workbook"].tolist(),
        )

    return {
        "cde_report_file": report_file,
        "data_package_board": data_package_name,
        "data_package_rows": len(data_package_board),
        "data_reuse_board": data_reuse_name,
        "data_reuse_rows": len(data_reuse_board),
        "excluded_report_rows": len(excluded_report_rows),
    }


def lambda_handler(event, context):
    input_uri = f"s3://{S3_BUCKET}/{INPUT_PREFIX}"
    output_uri = f"s3://{S3_BUCKET}/{OUTPUT_PREFIX}"
    try:
        result = run(S3ExcelSource(input_uri), S3ExcelSource(output_uri))
        logger.info("Package board update complete: %s", result)
        notify_slack(
            ":white_check_mark: Package board update complete — "
            f"{result['data_package_board']} ({result['data_package_rows']} rows), "
            f"{result['data_reuse_board']} ({result['data_reuse_rows']} rows)"
        )
        return {"statusCode": 200, "body": json.dumps(result)}
    except Exception as e:
        logger.exception("Package board update failed")
        notify_slack(f":x: Package board update failed — {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}


# ---------------------------------------------------------------------------
# Script entry point (local testing)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    from sources import LocalExcelSource

    in_dir = sys.argv[1] if len(sys.argv) > 1 else "."
    out_dir = sys.argv[2] if len(sys.argv) > 2 else in_dir
    result = run(LocalExcelSource(in_dir), LocalExcelSource(out_dir))
    print(json.dumps(result, indent=2))

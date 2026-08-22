"""
Slack notifications for the mysql_tables_sync Lambda.

Set SLACK_WEBHOOK_URL to a Slack incoming webhook URL to enable notifications.
If it's unset, notify_slack() is a silent no-op — a missing/broken Slack
integration must never fail the actual table sync.
"""

import logging
import os

import requests

logger = logging.getLogger(__name__)


def notify_slack(message: str) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL not set — skipping Slack notification")
        return

    try:
        resp = requests.post(webhook_url, json={"text": message}, timeout=10)
        if not resp.ok:
            logger.warning("Slack notification failed (%s): %s", resp.status_code, resp.text)
    except requests.exceptions.RequestException as e:
        logger.warning("Slack notification failed: %s", e)

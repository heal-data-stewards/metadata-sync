"""
HEAL MySQL Query Lambda — read-only endpoint.

Dispatches named queries against the MySQL database. Designed to sit behind
API Gateway (HTTP GET) or be invoked directly.

Invocation formats
------------------
API Gateway:    GET /query?name=research_network_freq
Direct Lambda:  {"query": "research_network_freq"}

Available queries
-----------------
  study_summary.research_network_freq  — research network frequencies
  study_summary.ended_studies          — studies past their project end date
  study_summary.funding_ic_freq        — administering IC frequencies

Adding a new query
------------------
1. Add a function to queries/<table>.py returning {"query": ..., "results": [...]}
2. Register it in QUERY_REGISTRY below.
"""

import json
import logging
import os

from dotenv import load_dotenv

from db import connect_mysql
from queries.study_summary import ended_studies, funding_ic_freq, research_network_freq

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

QUERY_REGISTRY = {
    "research_network_freq": research_network_freq,
    "ended_studies":         ended_studies,
    "funding_ic_freq":       funding_ic_freq,
}

_HEADERS = {
    "Content-Type": "application/json",
}


def _resolve_query_name(event):
    """Extract query name from either API Gateway or direct invocation event."""
    # API Gateway proxy event
    if "queryStringParameters" in event:
        params = event.get("queryStringParameters") or {}
        return params.get("name")
    # Direct invocation
    return event.get("query")


def lambda_handler(event, context):
    name = _resolve_query_name(event)
    logger.info("Invoked with query=%s", name)

    if not name:
        return {
            "statusCode": 400,
            "headers": _HEADERS,
            "body": json.dumps({
                "error": "Missing query name.",
                "valid_queries": list(QUERY_REGISTRY),
            }),
        }

    if name not in QUERY_REGISTRY:
        return {
            "statusCode": 404,
            "headers": _HEADERS,
            "body": json.dumps({
                "error": f"Unknown query '{name}'.",
                "valid_queries": list(QUERY_REGISTRY),
            }),
        }

    conn = connect_mysql()
    try:
        result = QUERY_REGISTRY[name](conn)
    except Exception as e:
        logger.exception("Query '%s' failed", name)
        return {
            "statusCode": 500,
            "headers": _HEADERS,
            "body": json.dumps({"error": str(e)}),
        }
    finally:
        conn.close()

    return {
        "statusCode": 200,
        "headers": _HEADERS,
        "body": json.dumps(result, default=str),
    }


if __name__ == "__main__":
    import sys
    query_arg = sys.argv[1] if len(sys.argv) > 1 else None
    if not query_arg:
        print(f"Usage: python lambda_handler.py <query_name>")
        print(f"Available: {list(QUERY_REGISTRY)}")
        sys.exit(1)
    response = lambda_handler({"query": query_arg}, None)
    print(json.dumps(json.loads(response["body"]), indent=2, default=str))

import json
import os
import mysql.connector
from decimal import Decimal
from datetime import datetime
from dotenv import load_dotenv

class EnhancedEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super(EnhancedEncoder, self).default(obj)

def parse_json_fields(data):
    json_fields = ['investigators_name', 'repository_metadata', 'dmp_plan', 'heal_cde_used', 'vlmd_metadata']
    for field in json_fields:
        if field in data and isinstance(data[field], str):
            try:
                data[field] = json.loads(data[field].replace("'", "\""))
            except json.JSONDecodeError:
                pass
    return data

load_dotenv()

db_username = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_host     = os.getenv('DB_HOST')
db_database = os.getenv('DB_NAME')
table_name  = os.getenv('TABLE_NAME')

_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
}

def _connect():
    return mysql.connector.connect(
        user=db_username,
        password=db_password,
        host=db_host,
        database=db_database,
    )

def _count(cursor, table, where):
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}")
    return cursor.fetchone()[0]


# ---------------------------------------------------------------------------
# Dashboard metrics query
# ---------------------------------------------------------------------------

def get_dashboard_metrics(conn, table):
    """
    Returns aggregate counts from progress_tracker for the HEAL dashboard.

    Study categories:
      - Live HEAL studies: guid_type IN (discovery_metadata,
          unregistered_discovery_metadata) AND archived = 'live'
      - In HEAL metrics: above AND appl_id is a real value (not null/empty/'0')
      - Not in HEAL metrics: live HEAL guid_type but no valid appl_id

    Pipeline stages (within HEAL-metrics-eligible studies):
      - Repository selected, data not yet linked
      - Data linked on platform
      - VLMD (variable-level metadata) available
      - Not sharing data (by choice)

    CEDAR completion: studies with overall_percent_complete > 50 are
    considered to have fulfilled the metadata completeness requirement.
    """
    cur = conn.cursor()
    t = table

    # Base WHERE clauses
    heal_live = (
        "guid_type IN ('discovery_metadata', 'unregistered_discovery_metadata') "
    )
    in_metrics = (
        f"{heal_live} "
        "AND appl_id IS NOT NULL AND appl_id != '' AND appl_id != '0'"
    )
    not_in_metrics = (
        f"{heal_live} "
        "AND (appl_id IS NULL OR appl_id = '' OR appl_id = '0')"
    )

    # ── Study counts ─────────────────────────────────────────────────────────
    total_live          = _count(cur, t, heal_live)
    in_metrics_count    = _count(cur, t, in_metrics)
    not_in_metrics_count= _count(cur, t, not_in_metrics)
    registered          = _count(cur, t, f"{heal_live} AND guid_type = 'discovery_metadata'")
    unregistered        = _count(cur, t, f"{heal_live} AND guid_type = 'unregistered_discovery_metadata'")
    archived_count      = _count(cur, t, "guid_type = 'discovery_metadata_archive'")

    # ── Study origin breakdown ───────────────────────────────────────────────
    # Categories are mutually exclusive, determined by project_num prefix first.
    # Studies without a special prefix are bucketed by appl_id validity.
    not_special = (
        "COALESCE(project_num, '') NOT LIKE 'CTN%' "
        "AND COALESCE(project_num, '') NOT LIKE 'ZIA%' "
        "AND COALESCE(project_num, '') NOT LIKE 'ICPSR%'"
    )
    ctn_count   = _count(cur, t, f"{heal_live} AND project_num LIKE 'CTN%'")
    zia_count   = _count(cur, t, f"{heal_live} AND project_num LIKE 'ZIA%'")
    icpsr_count = _count(cur, t, f"{heal_live} AND project_num LIKE 'ICPSR%'")
    hdp_count   = _count(cur, t,
        f"{heal_live} AND {not_special} "
        "AND appl_id IS NOT NULL AND appl_id != '' AND appl_id != '0'"
    )
    other_count = _count(cur, t,
        f"{heal_live} AND {not_special} "
        "AND (appl_id IS NULL OR appl_id = '' OR appl_id = '0')"
    )

    # ── Data sharing pipeline (HEAL-metrics-eligible studies only) ───────────
    repo_selected_not_linked = _count(
        cur, t,
        f"{heal_live} AND repository_selected = 'Yes' AND data_linked_on_platform = 'No'"
    )
    data_linked = _count(cur, t, f"{heal_live} AND data_linked_on_platform = 'Yes'")
    vlmd        = _count(cur, t, f"{heal_live} AND num_data_dictionaries > 0")
    not_sharing = _count(cur, t, f"is_producing_data_not_sharing = 'Yes'")

    # ── CEDAR metadata completion ─────────────────────────────────────────────
    cedar_complete = _count(cur, t, f"{heal_live} AND overall_percent_complete > 50")

    # ── CDE usage (studies with at least one common data element) ─────────────
    studies_with_cdes = _count(cur, t, f"{heal_live} AND num_common_data_elements > 0")
    
    # ── Repository breakdown (HEAL-metrics-eligible, repository selected) ────
    cur.execute(f"""
        SELECT repository_name, COUNT(*) AS cnt
        FROM {t}
        WHERE {in_metrics}
          AND repository_selected = 'Yes'
          AND repository_name IS NOT NULL
          AND repository_name != ''
        GROUP BY repository_name
        ORDER BY cnt DESC
    """)
    repositories = [{"name": r[0], "count": r[1]} for r in cur.fetchall()]

    # ── Research network breakdown (from monday_studies_mysql) ───────────────
    # Populated by monday_studies_lambda/lambda_function.py.
    # Gracefully returns [] if the table doesn't exist yet.
    try:
        cur.execute("""
            SELECT research_network, COUNT(*) AS cnt
            FROM monday_studies_mysql
            WHERE research_network IS NOT NULL
              AND research_network != ''
              AND research_network != '-'
            GROUP BY research_network
            ORDER BY cnt DESC
        """)
        research_networks = [{"name": r[0], "count": int(r[1])} for r in cur.fetchall()]
    except Exception:
        research_networks = []

    cur.close()

    return {
        "studies": {
            "total_live":               total_live,
            "in_heal_metrics":          in_metrics_count,
            "not_in_heal_metrics":      not_in_metrics_count,
            "registered":               registered,
            "unregistered":             unregistered,
            "archived":                 archived_count,
            "breakdown": {
                "ctn":   ctn_count,
                "zia":   zia_count,
                "icpsr": icpsr_count,
                "hdp":   hdp_count,
                "other": other_count,
            },
        },
        "pipeline": {
            "repo_selected_not_linked": repo_selected_not_linked,
            "data_linked_on_platform":  data_linked,
            "vlmd_available":           vlmd,
            "not_sharing_data":         not_sharing,
        },
        "cedar_complete":       cedar_complete,
        "cde_studies":          studies_with_cdes,
        "repositories":         repositories,
        "research_networks":    research_networks,
    }


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    path = (
        event.get("rawPath")
        or event.get("path")
        or ""
    )

    # ── Route: GET /dashboard/metrics ────────────────────────────────────────
    params = event.get("queryStringParameters") or {}
    if path.rstrip("/").endswith("/dashboard/metrics") or params.get("action") == "dashboard_metrics":
        try:
            conn = _connect()
            metrics = get_dashboard_metrics(conn, table_name)
            conn.close()
            return {
                "statusCode": 200,
                "headers": _HEADERS,
                "body": json.dumps(metrics, cls=EnhancedEncoder),
            }
        except mysql.connector.Error as e:
            return {
                "statusCode": 500,
                "headers": _HEADERS,
                "body": json.dumps({"error": f"Database error: {e}"}),
            }
        except Exception as e:
            return {
                "statusCode": 500,
                "headers": _HEADERS,
                "body": json.dumps({"error": str(e)}),
            }

    # ── Route: GET /study  (existing single-study lookup) ────────────────────
    appl_id = proj_num = hdp_id = ""
    results = []

    try:
        appl_id  = params.get("appl_id", "")
        proj_num = params.get("proj_num", "")
        hdp_id   = params.get("hdp_id", "").upper()

        normalized_appl_id  = appl_id.replace("-", "") if appl_id.startswith("CTN") else appl_id
        normalized_proj_num = proj_num.replace("-", "")

        conn   = _connect()
        cursor = conn.cursor()

        query = (
            f"SELECT * FROM {table_name} "
            "WHERE REPLACE(appl_id, '-', '')=%s "
            "   OR REPLACE(project_num, '-', '')=%s "
            "   OR REPLACE(hdp_id, '-', '')=%s;"
        )
        cursor.execute(query, (normalized_appl_id, normalized_proj_num, hdp_id))

        if cursor.description is not None:
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                results.append(parse_json_fields(dict(zip(columns, row))))
        else:
            results = "No results returned or query did not execute successfully."

        cursor.close()
        conn.close()

    except mysql.connector.Error as e:
        results = f"Database error: {e}"
    except Exception as e:
        results = f"Execution error: {e}"

    return {
        "statusCode": 200,
        "headers": _HEADERS,
        "body": json.dumps(results, cls=EnhancedEncoder),
    }

# test = lambda_handler({"rawPath": "/dashboard/metrics", "queryStringParameters": {}}, None)
# print(test)

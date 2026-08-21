"""
Read-only queries against the study_summary table.

Column names in study_summary are derived from display names via _mysql_colname()
in tables/study_summary.py. Key mappings relevant here:
  "Research Network"  → research_network
  "Administering IC"  → administering_ic
  "Project #"         → project
  "Project Start"     → project_start
  "Project End"       → project_end
  study_hdp_id, study_most_recent_appl are passed through unchanged.
"""

import logging
import os

logger = logging.getLogger(__name__)

_TABLE = os.getenv("STUDY_SUMMARY_TABLE_NAME", "study_summary")


def _fetchall(conn, sql, params=None):
    cur = conn.cursor(dictionary=True)
    cur.execute(sql, params or ())
    rows = cur.fetchall()
    cur.close()
    return rows


def research_network_freq(conn, table=_TABLE, **kwargs):
    """Return research network frequencies, sorted descending by count."""
    rows = _fetchall(conn, f"""
        SELECT research_network, COUNT(*) AS count
        FROM `{table}`
        WHERE research_network IS NOT NULL AND research_network != ''
        GROUP BY research_network
        ORDER BY count DESC
    """)
    return {"query": "research_network_freq", "results": rows}


def ended_studies(conn, table=_TABLE, before=None, **kwargs):
    """
    Return studies whose project end date is before `before` (YYYY-MM-DD).
    Defaults to NOW() if `before` isn't given.
    """
    if before:
        rows = _fetchall(conn, f"""
            SELECT
                study_hdp_id,
                title,
                study_most_recent_appl,
                project          AS project_num,
                project_start,
                project_end
            FROM `{table}`
            WHERE project_end IS NOT NULL
              AND project_end != ''
              AND project_end < %s
            ORDER BY project_end
        """, (before,))
    else:
        rows = _fetchall(conn, f"""
            SELECT
                study_hdp_id,
                title,
                study_most_recent_appl,
                project          AS project_num,
                project_start,
                project_end
            FROM `{table}`
            WHERE project_end IS NOT NULL
              AND project_end != ''
              AND project_end < DATE_FORMAT(NOW(), '%Y-%m-%d')
            ORDER BY project_end
        """)
    return {"query": "ended_studies", "results": rows}


def funding_ic_freq(conn, table=_TABLE, **kwargs):
    """Return administering IC frequencies, sorted descending by count."""
    rows = _fetchall(conn, f"""
        SELECT administering_ic, COUNT(*) AS count
        FROM `{table}`
        WHERE administering_ic IS NOT NULL AND administering_ic != ''
        GROUP BY administering_ic
        ORDER BY count DESC
    """)
    return {"query": "funding_ic_freq", "results": rows}

def get_resnet_resprog(conn, table=_TABLE, **kwargs):
    """Return Research Program and Research Network assignments for each HDPID."""
    rows = _fetchall(conn, f"""
                     SELECT  study_hdp_id, research_program, research_network
                     FROM `{table}`
                     """)
    return {"query": "get_resnet_resprog", "results": rows}

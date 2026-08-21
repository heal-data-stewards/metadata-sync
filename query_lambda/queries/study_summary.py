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

import ast
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


def _parse_repo_list(raw):
    """
    Parse the `repo_list` column: a Python-repr'd list of repository dicts
    (single-quoted, not JSON) written by mds_data_prep.py's `repository_metadata`
    field. Uses ast.literal_eval rather than a quote-replacement hack, since
    repository names can themselves contain apostrophes.
    """
    if not raw:
        return []
    try:
        parsed = ast.literal_eval(raw)
    except (ValueError, SyntaxError):
        return []
    return parsed if isinstance(parsed, list) else []


def studies_by_repository(conn, table=_TABLE, repository=None, **kwargs):
    """
    Return studies that use the given repository anywhere in their repository
    list (`repo_list`), not just the first/primary one MDS happens to report
    in `repo_per_platform`. Falls back to `repo_per_platform` for rows where
    `repo_list` is missing or unparseable.
    """
    if not repository:
        return {"query": "studies_by_repository", "results": [], "error": "repository param required"}

    rows = _fetchall(conn, f"""
        SELECT
            study_hdp_id,
            title,
            study_most_recent_appl,
            project             AS project_num,
            repo_per_platform,
            repo_study_link,
            repo_list,
            project_start,
            project_end
        FROM `{table}`
    """)

    results = []
    for row in rows:
        match = next(
            (r for r in _parse_repo_list(row.get("repo_list")) if r.get("repository_name") == repository),
            None,
        )
        if match is None and row.get("repo_per_platform") == repository:
            match = {
                "repository_name": row.get("repo_per_platform"),
                "repository_study_link": row.get("repo_study_link"),
            }
        if match is None:
            continue
        results.append({
            "study_hdp_id":           row.get("study_hdp_id"),
            "title":                  row.get("title"),
            "study_most_recent_appl": row.get("study_most_recent_appl"),
            "project_num":            row.get("project_num"),
            "repository":             match.get("repository_name"),
            "repository_study_link":  match.get("repository_study_link"),
            "project_start":          row.get("project_start"),
            "project_end":            row.get("project_end"),
        })

    results.sort(key=lambda r: r["title"] or "")
    return {"query": "studies_by_repository", "results": results}


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

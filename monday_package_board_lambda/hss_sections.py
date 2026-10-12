"""
Per-study section (CDE) and variable counts from the HEAL Semantic Search
studies index.

Mirrors heal-dashboard/collector/collect.py's definition: each ingested CDE is
indexed as a "section", so a study's CDE count is len(section_list) on its
studies-index record, not anything from the variables index. The same record
also carries variable_list, which the notebook used as the VLMD indicator
("does this study have any indexed variables at all") -- fetched here in the
same pass since it costs nothing extra.
"""

import httpx

HSS_BASE = "https://heal.renci.org/search-api"
PAGE_SIZE = 500
TIMEOUT = httpx.Timeout(60.0)


def fetch_study_index_counts(client: httpx.Client, hss_base: str = HSS_BASE) -> dict[str, dict[str, int]]:
    """Returns {HDPID: {"num_sections": int, "num_variables": int}} for every HSS study."""
    counts: dict[str, dict[str, int]] = {}
    offset = 0
    while True:
        r = client.post(
            f"{hss_base}/studies",
            json={"query": "*", "size": PAGE_SIZE, "offset": offset},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        batch = data.get("results", [])
        total = data.get("metadata", {}).get("total_count", 0)
        for study in batch:
            study_id = study.get("id")
            if study_id:
                counts[study_id] = {
                    "num_sections": len(study.get("section_list", []) or []),
                    "num_variables": len(study.get("variable_list", []) or []),
                }
        offset += PAGE_SIZE
        if not batch or offset >= total:
            break
    return counts


def fetch_section_counts(client: httpx.Client, hss_base: str = HSS_BASE) -> dict[str, int]:
    """Returns {HDPID: num_sections}. Kept for callers that only need the one count."""
    return {k: v["num_sections"] for k, v in fetch_study_index_counts(client, hss_base).items()}

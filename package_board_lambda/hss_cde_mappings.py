"""
Per-study CDE mapping counts from HEAL Semantic Search variables.

Each variable with metadata.cde_mapping carries two fields:
  - measure: the CDE measure it was mapped to
  - cde:     the CDE form that measure belongs to (e.g. "adult-demographics")

Counts per study are taken from the variable records themselves, so no extra
source is needed for the forms found from variable mapping.
"""

import httpx

HSS_BASE = "https://heal.renci.org/search-api"
PAGE_SIZE = 1000
TIMEOUT = httpx.Timeout(60.0)


def fetch_mapped_variables(client: httpx.Client, hss_base: str = HSS_BASE) -> list[dict]:
    """All variables with a non-empty metadata.cde_mapping, paginated."""
    variables: list[dict] = []
    offset = 0
    while True:
        r = client.post(
            f"{hss_base}/variables",
            json={
                "query": "*",
                "size": PAGE_SIZE,
                "offset": offset,
                "filters": [{"field": "metadata.cde_mapping", "operator": "size_gt", "value": 0}],
            },
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        batch = data.get("results", [])
        total = data.get("metadata", {}).get("total_count", 0)
        variables.extend(batch)
        offset += PAGE_SIZE
        if not batch or offset >= total:
            break
    return variables


def counts_by_study(variables: list[dict]):
    """
    Returns a dict keyed by HDPID with:
      num_variable_cde_mappings: mapped variable count
      num_distinct_cde_measures: distinct measures mapped
      num_cde_forms_from_mapping: distinct CDE forms those measures belong to
    """
    per_study: dict[str, dict] = {}
    for v in variables:
        hdp_ids = [p for p in v.get("parents", []) if p.startswith("HDP")]
        mapping = (v.get("metadata") or {}).get("cde_mapping") or {}
        for hdp in hdp_ids:
            s = per_study.setdefault(hdp, {"variables": 0, "measures": set(), "forms": set()})
            s["variables"] += 1
            if mapping.get("measure"):
                s["measures"].add(mapping["measure"])
            if mapping.get("cde"):
                s["forms"].add(mapping["cde"])
    return {
        hdp: {
            "num_variable_cde_mappings": s["variables"],
            "num_distinct_cde_measures": len(s["measures"]),
            "num_cde_forms_from_mapping": len(s["forms"]),
        }
        for hdp, s in per_study.items()
    }

"""
Study repository and other-website links from MDS.

UpdateMondayPackageBoard.ipynb fetches these with one GET per study (~1,200
sequential calls) after first fetching the bare study ID list. The bulk MDS
endpoint already carries data_repositories and other_study_websites inline
when called with data=True — the same parameter heal-dashboard's collector
uses — so this is two calls total, not ~1,200.
"""

import httpx

MDS_BASE = "https://healdata.org/mds"
HEAL_STUDY_GUID_TYPES = ["discovery_metadata", "unregistered_discovery_metadata"]
TIMEOUT = httpx.Timeout(60.0)


def fetch_mds_records(client: httpx.Client, mds_base: str = MDS_BASE) -> dict[str, dict]:
    """Returns {hdp_id: study_record} for every registered and unregistered HEAL study."""
    records: dict[str, dict] = {}
    for guid_type in HEAL_STUDY_GUID_TYPES:
        r = client.get(
            f"{mds_base}/metadata",
            params={"_guid_type": guid_type, "limit": 10000, "data": "True"},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        records.update(r.json())
    return records


def extract_links(records: dict[str, dict]) -> list[dict]:
    """
    One row per (study, link). Mirrors the notebook's link logic exactly:
    other-website links are deduped against repo link strings, not objects.
    A study with no links at all still gets one row, with an empty link.
    """
    rows: list[dict] = []
    for hdp_id, study in records.items():
        disc = study.get("gen3_discovery") or {}
        loc = (disc.get("study_metadata") or {}).get("metadata_location") or {}
        repositories = loc.get("data_repositories") or []

        repo_entries = [r for r in repositories if r.get("repository_study_link")]
        repo_link_strings = {r["repository_study_link"] for r in repo_entries}
        other_links = [
            w for w in (loc.get("other_study_websites") or []) if w not in repo_link_strings
        ]

        num_repos = len(repo_entries) + len(other_links)
        num_data_repos = len(repo_entries)
        num_other_repos = len(other_links)

        for r in repo_entries:
            rows.append({
                "id": hdp_id,
                "repo_link": r["repository_study_link"],
                "repo_name": r.get("repository_name", ""),
                "num_repositories": num_repos,
                "num_data_repositories": num_data_repos,
                "num_other_links": num_other_repos,
                "is_other_website": False,
            })
        for link in other_links:
            rows.append({
                "id": hdp_id,
                "repo_link": link,
                "repo_name": "",
                "num_repositories": num_repos,
                "num_data_repositories": num_data_repos,
                "num_other_links": num_other_repos,
                "is_other_website": True,
            })
        if not repo_entries and not other_links:
            rows.append({
                "id": hdp_id,
                "repo_link": "",
                "repo_name": "",
                "num_repositories": 0,
                "num_data_repositories": 0,
                "num_other_links": 0,
                "is_other_website": None,
            })
    return rows

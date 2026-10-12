"""
Matches MDS-known repository links (mds_repo_links.py) against the platform's
manually-curated link assessments (platform_determinations.py).

Mirrors UpdateMondayPackageBoard.ipynb cell 10: the same real-world link can
be written differently in MDS vs. the platform sheet, so an exact string
match is tried first, then a loosened substring match as a fallback.

One deliberate change from the notebook: there, MDS links and platform
assessments are joined on HDP_ID alone (not on the link too), so a study with
more than one link on each side gets every combination carried through as a
separate row -- a cartesian product, not real pairs. That was fine for the
notebook's ad hoc stats, but corrected_match exists precisely to tell the
real pairs from the spurious ones, so data_package_board.py uses it to keep
only the best match per link rather than every combination.
"""

import pandas as pd

from platform_determinations import VALID_ASSESSMENTS

_STRIP = ("https://", "doi.org", "www.")


def match_links(mds_links: pd.DataFrame, platform_assessments: pd.DataFrame) -> pd.DataFrame:
    """
    mds_links: mds_repo_links.extract_links() output, as a DataFrame. Rows
               with no real link (num_repositories == 0) are dropped first,
               same as the notebook's `all_studies_pd_links` filter.
    platform_assessments: platform_determinations.load_platform_assessments() output.

    Returns the outer-joined per-(study, link) table with a `corrected_match`
    column: 'True Exact Match', 'Potential Match', 'True New Data' (platform
    knows a link MDS doesn't have yet), or 'False'.
    """
    links = mds_links[mds_links["num_repositories"] > 0]

    merged = pd.merge(
        links, platform_assessments,
        how="outer", left_on="id", right_on="hdp_id",
    )
    merged.rename(columns={"id": "index_hdp_id", "hdp_id": "platform_hdp_id"}, inplace=True)

    merged["match"] = [
        "True New Data" if (pd.isna(prl) and pa in VALID_ASSESSMENTS)
        else ("True Exact Match" if prl == irl else "False")
        for prl, irl, pa in merged[["repo_link", "repository_study_link", "platform_assessment"]].values
    ]

    exact_counts = merged.loc[merged["match"] == "True Exact Match", "index_hdp_id"].value_counts()
    merged["match_count"] = merged["index_hdp_id"].map(exact_counts).fillna(0)

    merged["corrected_match"] = [_correct(row) for row in merged.itertuples()]
    return merged


def _correct(row) -> str:
    if pd.isna(row.repo_link):
        return row.match  # platform-only link; not yet in MDS
    if pd.isna(row.repository_study_link):
        return row.match  # MDS-only link; platform has no assessment for it
    if row.match_count == row.platform_num_repositories:
        return row.match  # every platform link for this study already matched exactly
    if row.num_repositories == 1 and row.platform_num_repositories == 1:
        return "Potential Match"  # exactly one link on each side; very likely the same

    index_url = _loosen(row.repo_link)
    platform_url = _loosen(row.repository_study_link)
    if index_url in platform_url or platform_url in index_url:
        return "Potential Match"
    return row.match


def _loosen(url: str) -> str:
    for token in _STRIP:
        url = url.replace(token, "")
    return url

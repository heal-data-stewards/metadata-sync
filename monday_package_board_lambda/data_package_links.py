"""
Assembles the Data Package board's per-link rows: one row per MDS
repository link, with the platform's determination of whether that link is
real data, plus whether HSS/VLMD has anything indexed for the study.

This is the board's original shape (mirrors UpdateMondayPackageBoard.ipynb
cell 13), from before CDE mapping metrics were added. board_metrics.py is
the per-study CDE metrics layered on top; merge both on HDPID for the full
Data Package board upload.

Only MDS-known links are kept (matches the notebook's cell 13 filter) --
platform-only "True New Data" links aren't on this board; they belong to the
platform sheet's own review, not the one scheduled here. A study with more
than one link on each side is resolved by corrected_match (see
link_matching.py) rather than carried through as every combination.
"""

import pandas as pd

from blanks import fill_blanks
from link_matching import match_links

# Best match to worst; a link keeps the assessment from its highest-ranked match.
_MATCH_PRIORITY = {"True Exact Match": 0, "Potential Match": 1, "True New Data": 2, "False": 3}


def build_data_package_links(
    mds_links: pd.DataFrame,
    platform_assessments: pd.DataFrame,
    hss_study_counts: dict[str, dict[str, int]],
) -> pd.DataFrame:
    """
    hss_study_counts: {HDPID: {"num_variables": int, "num_sections": int}},
    from hss_sections.fetch_study_index_counts().

    Returns one row per (HDPID, repository link): HDPID, Repository Link,
    Repository Name, Is Other Website Link?, Platform assessment of Data
    Link, VLMD on Platform for Study?, CDE mappings on HSS for Study? --
    blanks filled with "-".
    """
    matched = match_links(mds_links, platform_assessments)

    has_mds_link = matched["repo_link"].notna() & (matched["repo_link"] != "")
    rows = matched.loc[has_mds_link].copy()

    # A "False" pairing is cartesian noise, not a real association between this
    # link and that platform row -- its platform_assessment belongs to some
    # other link in the same study, not this one. Blank it before ranking so a
    # link with no real match (exact or fuzzy) ends up with no assessment at
    # all, instead of silently inheriting an unrelated link's determination.
    rows.loc[rows["corrected_match"] == "False", "platform_assessment"] = pd.NA

    rows["_priority"] = rows["corrected_match"].map(_MATCH_PRIORITY).fillna(3)
    rows = rows.sort_values("_priority").drop_duplicates(subset=["index_hdp_id", "repo_link"], keep="first")

    rows["num_variables"] = rows["index_hdp_id"].map(
        lambda hdp: hss_study_counts.get(hdp, {}).get("num_variables", 0)
    )
    rows["num_sections"] = rows["index_hdp_id"].map(
        lambda hdp: hss_study_counts.get(hdp, {}).get("num_sections", 0)
    )
    rows["VLMD on Platform for Study?"] = ["Yes" if n > 0 else "No" for n in rows["num_variables"]]
    rows["CDE mappings on HSS for Study?"] = ["Yes" if n > 0 else "No" for n in rows["num_sections"]]
    # is_other_website is only ever True/False here -- the True/False/None
    # tri-state from mds_repo_links.py's "no links at all" sentinel rows was
    # already excluded by the has_mds_link filter above.
    rows["Is Other Website Link?"] = ["Yes" if v else "No" for v in rows["is_other_website"]]

    out = rows[[
        "index_hdp_id", "repo_link", "repo_name", "Is Other Website Link?", "platform_assessment",
        "VLMD on Platform for Study?", "CDE mappings on HSS for Study?",
    ]].rename(columns={
        "index_hdp_id": "HDPID",
        "repo_link": "Repository Link",
        "repo_name": "Repository Name",
        "platform_assessment": "Platform assessment of Data Link",
    }).reset_index(drop=True)

    return fill_blanks(out)

"""
Platform-curated digital asset determinations, from the manually-cleaned
"HEAL Data & Digital Assets Tracking - All HEAL Assets.csv" platform sheet
(a Google Sheet Hina cleans up by hand before each monthly upload).

Mirrors UpdateMondayPackageBoard.ipynb cell 9. Each study row has up to 5
(URL / DOI, Determination, Action) triples -- with a blank helper column
after each one left over from Google Sheets formulas (read back by pandas as
"Unnamed: N", holding "#REF!" or nothing; harmless noise, just not selected
here) -- plus ~150 trailing placeholder rows with no HDP_ID at all. This
flattens the real rows to one row per (study, url) with only the
determinations the board trusts.

"Awaiting data" determinations are intentionally excluded here, matching the
notebook's existing behavior ("As of now, we will not be incorporating
'Awaiting Data' assessments on the Package Board"). Confirm with Hina before
changing this, since it is common in the real data (~70 of the links as of
the 2026-10-06 sheet).
"""

import pandas as pd

VALID_ASSESSMENTS = ["Data", "Non-data asset", "Peer-reviewed publication", "Code"]
URL_SLOTS = range(1, 6)


def load_platform_assessments(platform_sheet: pd.DataFrame) -> pd.DataFrame:
    """
    platform_sheet: the raw platform sheet, as read from CSV. Rows with no
    HDP_ID (the trailing placeholder rows) are dropped.

    Returns one row per (hdp_id, repository_study_link) that has a
    determination the board trusts, with platform_num_repositories (how many
    such links that study has) alongside.
    """
    has_id = platform_sheet["HDP_ID"].notna() & (platform_sheet["HDP_ID"].astype(str).str.strip() != "")
    rows = platform_sheet.loc[has_id]

    records = []
    for _, row in rows.iterrows():
        hdp_id = str(row["HDP_ID"]).strip()
        for slot in URL_SLOTS:
            url = row.get(f"URL / DOI {slot}")
            if pd.isna(url):
                continue
            assessment = row.get(f"Determination {slot}")
            assessment = str(assessment).strip() if not pd.isna(assessment) else ""
            records.append({
                "hdp_id": hdp_id,
                "repository_study_link": str(url).strip(),
                "platform_assessment": assessment if assessment in VALID_ASSESSMENTS else "",
            })

    assessments = pd.DataFrame(
        records, columns=["hdp_id", "repository_study_link", "platform_assessment"]
    )
    assessments = assessments[assessments["platform_assessment"].isin(VALID_ASSESSMENTS)].reset_index(drop=True)

    counts = assessments["hdp_id"].value_counts().rename("platform_num_repositories")
    assessments = pd.merge(assessments, counts, how="left", left_on="hdp_id", right_index=True)
    return assessments

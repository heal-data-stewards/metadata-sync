"""
Monday display-name column mapping for study_summary.

study_summary stores Monday Board display names run through _mysql_colname()
as its MySQL column names (see mysql_tables_sync_lambda/tables/study_summary.py
_write_study_summary). To get display names back, reverse that same
transformation.

RENAME_DICT is vendored from mysql_tables_sync_lambda/tables/study_summary.py
rather than imported across Lambda directories -- each Lambda's deploy zip is
flat and self-contained, so cross-Lambda imports don't survive deployment.
Keep this in sync if that RENAME_DICT changes.
"""

import re

# MySQL source column -> Monday Board display name.
RENAME_DICT = {
    "proj_num":               "Project #",
    "proj_title":             "Title",
    "rfa":                    "Research Focus",
    "res_prg":                "Research Program",
    "ctc_pi_nm":              "Contact PI",
    "pi_email":               "Contact Email",
    "adm_ic":                 "Administering IC",
    "prg_ofc":                "NIH PO",
    "org_nm":                 "Institution(s)",
    "pi":                     "PI(s)",
    "org_cy":                 "City",
    "org_st":                 "State",
    "act_code":               "Activity Code",
    "awd_ty":                 "Award Type",
    "fisc_yr":                "Award Year",
    "tot_fund":               "Total Funded",
    "proj_abs":               "Summary",
    "fund_mech":              "SBIR/STTR",
    "proj_strt_date":         "Project Start",
    "proj_end_date":          "Project End",
    "proj_url":               "Reporter Link",
    "res_net":                "Research Network",
    "time_of_registration":   "Platform Reg Time",
    "overall_percent_complete": "CEDAR Form %",
    "repository_name":        "Repo per Platform",
    "repository_study_link":  "Repo Study Link",
    "repository_metadata":    "Repo List",
    "repository_selected":    "Repo Selected",
    "data_linked_on_platform": "Data Linked",
    "guid_type":              "GUID Type",
    "archived":               "Archived",
    "heal_funded":            "HEAL-Related",
    "do_not_engage":          "Do not Engage",
    "data_type":              "Data Type",
    "checklist_exempt_all":   "Checklist Exempt",
    "po_email":               "NIH PO Email",
}

# Columns stored under their own snake_case name already (not derived from
# RENAME_DICT) that still need a Monday-facing rename or are kept as-is.
PASSTHROUGH_RENAME = {
    "study_most_recent_appl": "Most Recent Appl_ID",
    "study_hdp_id_appl":      "HDP appl_ID",
    "location":               "Location",
}

# Internal join artifacts -- never shown on the board.
DROP_COLUMNS = ["study_hdp_id", "hdp_id", "hdp_id_x", "hdp_id_y"]

# The board's unique-row key; never blanked out to "-".
KEY_COLUMN = "key"


def _mysql_colname(name: str) -> str:
    """Convert a display name to a MySQL-safe snake_case identifier."""
    s = re.sub(r"[^a-zA-Z0-9]+", "_", str(name))
    return re.sub(r"_+", "_", s).strip("_").lower() or "col"


# MySQL column name -> Monday Board display name (reverse of the write-time map).
MYSQL_TO_DISPLAY = {_mysql_colname(v): v for v in RENAME_DICT.values()}

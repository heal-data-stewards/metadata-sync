# /* -------------------------------------------------------------------------------- */
# /* Project: HEAL 																	*/
# /* RTI PI: Kira Bradford															*/
# /* Program: HEAL_08_GTDTargets														*/
# /* Programmer: Sabrina McCutchan (CDMS)												*/
# /* Date Created: 2026/02/03															*/
# /* Date Last Updated: 2026/02/12													*/
# /* Description:	This program prepares Get the Data target lists for PMs.			*/
# /*																					*/
# /* Notes:  																			*/
# /*																					*/
# /* -------------------------------------------------------------------------------- */

# ----- Boiler Plate Code -----------------------------------------------------*/


# Import Python Modules Here
import os
import pandas as pd
from datetime import date
import subprocess
import sys
from pathlib import Path
import re
import numpy as np
import openpyxl
from openpyxl import load_workbook  
# from docx import Document
# from docx.shared import Pt

# TEMPORARY UNTIL INCLUDED in MASTER
# ==============================================================================
# 0. SET GLOBAL MACROS
# ==============================================================================

# ----- SET MACROS -----*/

# ----- 1. Dates ----- */
today = os.environ.get("today")

# today = date.today().strftime("%Y-%m-%d")
print(today)
# ----- 2. Filepaths ----- */
dir = Path(os.environ.get("dir"))
inp = dir / "Input"
out = dir / "Output"
log = dir / "Log"

# Logging
log_path = os.path.join(log, f"HEAL_08_GTDTarget_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/StudyMetrics_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{log}/HEAL_08_GTDTarget_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_08_GTDTarget Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/




# Target column structure requested by PM parameters
column_order = [
    "appl_id",
    "proj_num",
    "ctc_pi_nm",
    "study_pi_email",
    "proj_strt_date",
    "proj_end_date",
    "fund_mech",
    "heal_funded",
    "nih_core_cde",
    "hdp_id",
    "xstudy_id",
    "study_hdp_id",
    "study_hdp_id_appl",
    "study_first_appl",
    "study_most_recent_appl",
    "do_not_engage",
    "checklist_exempt_all",
    "study_res_net",
    "project_title_reporter",
    "project_title_platform",
]


# Define the master excel path
(out / "GTD_Targets").mkdir(parents=True, exist_ok=True)
excel_output_path = os.path.join(
    out, "GTD_Targets", f"gtd_targets_2026_{today}.xlsx"
)

# Load the base dataset
# Note: If your date columns aren't automatically parsed, ensure they are datetime types
df_master = pd.read_csv(os.path.join(out, f"alldata_{today}.csv"))

# Force structural date parsing for calculations if necessary
df_master["proj_end_date_date"] = pd.to_datetime(
    df_master["proj_end_date_date"]
)
df_master["proj_strt_date_date"] = pd.to_datetime(
    df_master["proj_strt_date_date"]
)


# ==============================================================================
# 1. Get the Data (GTD) Target List (Fixed & Cleaned)
# ==============================================================================

# Create baseline copy
gtd_00 = df_master.copy()

# Calculate the maximum (latest) project end date grouped by study
gtd_00["latest_end_date"] = gtd_00.groupby("xstudy_id")["proj_end_date_date"].transform("max")
print(gtd_00.shape)
# Extract the year component
gtd_00["latest_end_yr"] = gtd_00["latest_end_date"].dt.year
print(gtd_00.shape)

# Filter: Keep if latest end year is 2026
gtd_00 = gtd_00[gtd_00["latest_end_yr"] == 2026]
print(gtd_00.shape)

# Filter: Keep the row matching the most recent application id for the study
gtd_00 = gtd_00[gtd_00["appl_id"] == gtd_00["study_most_recent_appl"]]
print(gtd_00.shape)

# Filter: Exclude "do not engage" flags
gtd_00 = gtd_00[gtd_00["do_not_engage"] != 1]
print(gtd_00.shape)

# Filter: Drop records where the study HDP ID status is archived
gtd_00 = gtd_00[gtd_00["study_hdp_status"] != "archived"]
print(gtd_00.shape)

# Restructure, drop unneeded columns, and order cleanly
gtd_final = gtd_00[column_order].copy()
print(gtd_final.shape)

# Sort by network, checklist exemption, and project end date
gtd_final = gtd_final.sort_values(
    by=["study_res_net", "checklist_exempt_all", "proj_end_date"]
)

# Save Stata-equivalent intermediate Parquet/DTA and build cross-reference key
gtd_final.to_csv(os.path.join(out, "GTD_Targets", "gtd_general.csv"))

# Isolate the list of study IDs on the GTD list to prevent overlap
on_gtd_list = gtd_final[["xstudy_id"]].drop_duplicates().copy()
on_gtd_list["on_gtd_list"] = 1


# ==============================================================================
# 2. Early Awards Target List
# ==============================================================================
early = df_master.copy()

# Calculate the minimum (earliest) project start date grouped by study
early["first_strt_date"] = early.groupby("xstudy_id")[
    "proj_strt_date_date"
].transform("min")
early["first_strt_yr"] = early["first_strt_date"].dt.year

# Filter: Keep if earliest start year is 2025
early = early[early["first_strt_yr"] == 2025]

# Filter: Exclude "do not engage" flags
early = early[early["do_not_engage"] != 1]

# Filter: Exclude SBIR/STTR funding mechanisms
early = early[early["fund_mech"] != "SBIR/STTR"]

# Filter: Drop records where the study HDP ID status is archived
early = early[early["study_hdp_status"] != "archived"]

# Exclude targets that exist on the GTD Target list
early = pd.merge(early, on_gtd_list, on="xstudy_id", how="left")
early = early[early["on_gtd_list"].isna()].drop(columns=["on_gtd_list"])

# Confirm strict 1-record-per-study constraint requested by PM notes
# If a study has multiple remaining rows, keep the most recent application row
early = early.sort_values(by=["xstudy_id", "proj_strt_date_date"])
early = early.drop_duplicates(subset=["xstudy_id"], keep="last")

# Restructure, drop unneeded columns, and order cleanly
early_final = early[column_order].copy()

# Sort identical to GTD tab format
early_final = early_final.sort_values(
    by=["study_res_net", "checklist_exempt_all", "proj_end_date"]
)

# Save intermediate file
early_final.to_csv(
    os.path.join(out, "GTD_Targets", "gtd_earlyaward.csv"), index=False
)


# ==============================================================================
# 3. Export Consolidated Output to Excel Sheets
# ==============================================================================
with pd.ExcelWriter(excel_output_path, engine="openpyxl") as writer:
    gtd_final.to_excel(writer, sheet_name="gtd", index=False)
    early_final.to_excel(writer, sheet_name="earlyawards", index=False)

print(
    f"Target sheets generated successfully. Saved workbook to: {excel_output_path}"
)

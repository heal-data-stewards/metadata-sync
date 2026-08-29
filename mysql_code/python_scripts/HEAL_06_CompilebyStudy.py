# /* -------------------------------------------------------------------------------- */
# /* Project: HEAL 																	*/
# /* RTI PI: Kira Bradford															*/
# /* Program: HEAL_06_CompilebyStudy													*/
# /* Programmer: Sabrina McCutchan (CDMS)												*/
# /* Date Created: 2026/02/13															*/
# /* Date Last Updated: 2026/02/13													*/
# /* Description:	This program merges together data from multiple MySQL DB tables with*/
# /*	the study_lookup_table, producing a large compiled dataset that provides key	*/
# /*	values by study. 																*/
# /*		1. Merge source data 														*/
# /*		2. Derive study-level values for data that exists at appl_id level  		*/
# /*		3. Merge derived study-level values 										*/
# /*																					*/
# /* Notes:  																			*/
# /*	-The output dataset is analogous, but not identical, to the data extract that is*/
# /*		prepared by Hina Shah for upload to monday.com HEAL Studies Board.			*/
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

# TEMPORARY UNTIL INCLUDED in MASTER
# ==============================================================================
# 0. SET GLOBAL MACROS
# ==============================================================================
# today = "20260615"
# heal_dir = Path(
# )
# # Inject into environment variables for child processes (convert Path to string)
# os.environ["today"] = today
# os.environ["dir"] = str(heal_dir)
# END TEMPORARY UNTIL INCLUDED in MASTER

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
log_path = os.path.join(log, f"HEAL_06_CompilebyStudy_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/StudyMetrics_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{log}/HEAL_06_CompilebyStudy_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_06_CompilebyStudy Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/


# ==========================================
# 1. Merge source data
# ==========================================

# 1. Combine the two datasets
mysql_df = pd.read_csv(os.path.join(out, f"mysql_{today}.csv"), dtype={"appl_id": str})
print("mysql_df shape: {}".format(mysql_df.shape))

# mysql_df = pd.read_csv(os.path.join(out, f"mysql_{today}.csv"))
# dqaudit_df = pd.read_stata(os.path.join(out, "reporter_dqaudit.dta"))
dqaudit_df = pd.read_csv(os.path.join(out, "reporter_dqaudit.csv"), dtype={"appl_id": str})
print("dqaudit_df shape: {}".format(dqaudit_df.shape))

xalldata = pd.concat([mysql_df, dqaudit_df], ignore_index=True)
print("xalldata shape: {}".format(xalldata.shape))


# 2. Filter out missing or empty application IDs
xalldata = xalldata.dropna(subset=["appl_id"])
xalldata = xalldata[xalldata["appl_id"] != ""]
print("xalldata shape after dropping missing appl_ids: {}".format(xalldata.shape))

# 3. Drop old key if it exists to avoid duplication errors
if "compound_key" in xalldata.columns:
    xalldata = xalldata.drop(columns=["compound_key"])
print("xalldata shape after dropping existing compound key: {}".format(xalldata.shape))

# 4. Create the new concatenated compound_key safely
# xalldata["compound_key"] = (
#     xalldata["appl_id"].astype(str) + "_" + xalldata["hdp_id"].astype(str)
# )
xalldata["compound_key"] = (
    xalldata["appl_id"].astype(str).str.replace(r'\.0$', '', regex=True) + "_" + 
    xalldata["hdp_id"].fillna("").astype(str).str.replace(r'\.0$', '', regex=True)
)
print("xalldata shape after creating new compound key: {}".format(xalldata.shape))

# 5. Sort and export the final dataset
xalldata = xalldata.sort_values(by=["appl_id", "hdp_id"])
xalldata.to_csv(os.path.join(out, f"xalldata_{today}.csv"), index=False)
print("xalldata shape after sorting by appl_id and hdp_id: {}".format(xalldata.shape))


# Merge with study lookup table
study_lookup = pd.read_csv(os.path.join(out, "study_lookup_table.csv"), dtype=str)
# Stata: merge m:1 compound_key using xalldata. Keep master (1) and matched (3). Drop using-only (2).
xalldata_merge = xalldata.drop(columns=["appl_id"], errors="ignore")
alldata = pd.merge(
    study_lookup, xalldata_merge, on=["compound_key"], how="left", indicator=True
    # study_lookup, xalldata, on=["compound_key", "appl_id"], how="left", indicator=True
)

alldata = alldata[alldata["_merge"] != "right_only"].drop(columns=["_merge"])

# Merge engagement flags
eng_flags = pd.read_csv(os.path.join(out, "engagement_flags.csv"), dtype={"appl_id": str})[
    ["appl_id", "do_not_engage", "checklist_exempt_all"]
]
alldata = pd.merge(alldata, eng_flags, on="appl_id", how="left", indicator=True)
alldata = alldata[alldata["_merge"] != "right_only"].drop(columns=["_merge"])

# Merge PI Emails
pi_emails_raw = pd.read_csv(os.path.join(inp, f"pi_emails_{today}.csv"), dtype={"appl_id": str})[
    ["appl_id", "pi_email"]
]
alldata = pd.merge(
    alldata, pi_emails_raw, on="appl_id", how="left", indicator=True
)
alldata = alldata[alldata["_merge"] != "right_only"].drop(columns=["_merge"])

alldata = alldata.sort_values(by="xstudy_id")
# NOT NEEDED
alldata.to_csv(os.path.join(out, f"alldata_{today}.csv"), index=False)


# ==========================================
# 2. Derive study-level values
# ==========================================

# --- PI Emails Key ---
pi_df = alldata.dropna(subset=["pi_email"]).copy()
pi_df = pi_df[pi_df["pi_email"] != ""]

# Assign email if it's the most recent application
pi_df["study_pi_email"] = ""
mask_recent = pi_df["appl_id"] == pi_df["study_most_recent_appl"]
pi_df.loc[mask_recent, "study_pi_email"] = pi_df["pi_email"]

# Handle single row exceptions per study
pi_df["num_rows"] = pi_df.groupby("xstudy_id")["xstudy_id"].transform("count")
mask_single = (pi_df["study_pi_email"] == "") & (pi_df["num_rows"] == 1)
pi_df.loc[mask_single, "study_pi_email"] = pi_df["pi_email"]

# Drop temporary duplicate flag structures
pi_df["xstudy_has_email"] = (
    (pi_df["study_pi_email"] != "") & (pi_df["study_pi_email"].notna())
).astype(int)
pi_df["indic"] = pi_df.groupby("xstudy_id")["xstudy_has_email"].transform("max")
pi_df = pi_df[~((pi_df["indic"] == 1) & (pi_df["xstudy_has_email"] == 0))]

# Finalize key
pi_df.loc[pi_df["study_pi_email"] == "", "study_pi_email"] = pi_df["pi_email"]
pi_emails_key = (
    pi_df[["xstudy_id", "study_pi_email"]]
    .drop_duplicates()
    .sort_values(by="xstudy_id")
)
# NOT NEEDED
pi_emails_key.to_csv(os.path.join(out, "pi_emails_key.csv"), index=False)

# --- Research Network Key ---
# Stata: keep if res_net!="" AND keep if res_net!="NULL"
# "NULL" strings come through from MySQL CSV exports and must be filtered explicitly.
res_df = alldata[~alldata["res_net"].astype(str).str.strip().isin(['', 'nan', 'NULL', 'None'])].copy()
res_df = (
    res_df[["xstudy_id", "res_net"]]
    .drop_duplicates()
    .rename(columns={"res_net": "study_res_net"})
    .drop_duplicates(subset="xstudy_id")  # one row per study — matches Stata's keep xstudy_id study_res_net
)
# NOT NEEDED
res_df.to_csv(os.path.join(out, "res_net_key.csv"), index=False)

# --- Live/Archived Status Key ---
# progress = pd.read_stata(os.path.join(inp, f"progress_tracker_04242026.dta"))
progress = pd.read_csv(os.path.join(inp, f"progress_tracker_{today}.csv"))
live_arch_key = (
    progress[["hdp_id", "archived"]]
    .rename(columns={"hdp_id": "study_hdp_id", "archived": "study_hdp_status"})
    .sort_values(by="study_hdp_id")
)
# NOT NEEDED
live_arch_key.to_csv(os.path.join(out, "livearchkey.csv"), index=False)


# ==========================================
# 3. Merge derived values & Export
# ==========================================

# Reload main file
# final_df = pd.read_parquet(os.path.join(temp, f"alldata_{today}.dta"))
final_df = alldata.copy()

# Merge PI key
final_df = pd.merge(final_df, pi_emails_key, on="xstudy_id", how="left")

# Merge Network key
final_df = pd.merge(final_df, res_df, on="xstudy_id", how="left")

# Merge Status key (keep master only, drop rows found only in the archive key)
final_df = pd.merge(
    final_df, live_arch_key, on="study_hdp_id", how="left", indicator=True
)
final_df = final_df[final_df["_merge"] != "right_only"].drop(columns=["_merge"])

# Rename columns
final_df = final_df.rename(
    columns={
        "proj_title": "project_title_reporter",
        "project_title": "project_title_platform",
    }
)

# Reorder columns (moves 'study_hdp_status' to right after 'study_hdp_id')
cols = list(final_df.columns)
if "study_hdp_status" in cols and "study_hdp_id" in cols:
    cols.remove("study_hdp_status")
    idx = cols.index("study_hdp_id") + 1
    cols.insert(idx, "study_hdp_status")
    final_df = final_df[cols]

# Final sorting
final_df = final_df.sort_values(by=["xstudy_id", "fisc_yr"])

# Save output
final_df.to_csv(os.path.join(out, f"alldata_{today}.csv"), index=False, quotechar='"')


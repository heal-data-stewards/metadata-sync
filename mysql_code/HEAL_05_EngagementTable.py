# /* -------------------------------------------------------------------------------- */
# /* Project: HEAL 																	*/
# /* PI: Kira Bradford																*/
# /* Program: HEAL_05_EngagementTable													*/
# /* Programmer: Sabrina McCutchan (CDMS)												*/
# /* Date Created: 2024/12/03															*/
# /* Date Last Updated: 2026/02/03													*/
# /* Description:	This program creates the engagement_flags table, which contains 	*/
# /*	 indicators for "do not engage" & "checklist exempt" statuses.					*/
# /*		1. Create flags																*/
# /*		2. Generate Engagement Table 												*/
# /*																					*/
# /* Notes:  																			*/
# /*	- 2026/02/03 reporter_dqaudit appl_ids are now included in the output table		*/
# /*	- 2026/02/02 Carolyn Conlin in a 2026/01/07 email gave the instruction: 		*/
# /*	  "Stewards should engage with the MedTech Optimizer studies and should not 	*/
# /*	  engage with the MedTech Seedlings."											*/
# /*	- 2025/12/22 NIH gave us information about NOAs instead of FOAs in 2025. The 	*/
# /*	  logic for flagging do not engage was updated to include the new variable for	*/
# /*	  nih_noa_heal_lang.															*/
# /*	- Awards table fields changed in 2025 as NIH provided some data points in a form*/
# /*	  not backwards compatible with prior data structures. Flag creation logic was	*/
# /*	  updated to include relevant new awards table fields.							*/
# /*	- This table was added during the FY24 awards cycle.							*/
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
 
# ----- SET MACROS -----*/

# ----- 1. Dates ----- */
today = "20260612"
# today = date.today().strftime("%Y-%m-%d")
print(today)

# ----- 2. Filepaths ----- */
dir = Path(r"C:\Users\berman\OneDrive - Research Triangle Institute\Python Environment\HEAL") #WINDOwS
inp = dir / "Input"
out = dir / "Output"
log = dir / "Log"
templ = dir / "Template"

# Logging
log_path = os.path.join(log, f"HEAL_05_EngagementTable_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/HEAL_05_EngagementTable_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{log}/HEAL_05_EngagementTable_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_05_EngagementTable Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/
 



# /* Program: HEAL_05_EngagementTable													*/

log_out(f"HEAL_05_EngagementTable")


# -----------------------------------------------------------------------------
# 1. Create flags
# -----------------------------------------------------------------------------

# 1. Import latest MySQL data ----- */
datasets = [
    "nihtables",
    "research_networks"
]

dfs = {}  # dictionary to hold your dataframes

for name in datasets:
    csv_file = out / f"{name}_{today}.csv"

    # read CSV with all columns as strings
    # df = pd.read_csv(csv_file, dtype=str)
    df = pd.read_csv(
    csv_file,
    # sep=';',                # Semicolon delimiter
    engine='python',        # Use Python engine for complex parsing
    # quoting=3,              # QUOTE_NONE, avoids treating quotes specially
    encoding='cp1252',
    dtype=str,
    on_bad_lines='warn'   # Skip problematic lines (optional)
    )
    # replace line breaks inside cells with a space
    # and trim whitespace on all string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = (
            df[col]
            .str.replace(r"[\r\n]+", " ", regex=True)
            .str.strip()
        )

    # sort by appl_id (if that column exists)
    if "appl_id" in df.columns:
        df = df.sort_values("appl_id")

   # store clean DataFrame under a key
    dfs[f"df_{name}"] = df.copy()

   
    
# Load dfs
df_nih = dfs["df_nihtables"]
print("df_nih: " + str(df_nih.shape))
log_out(f"Import NiHTables: {str(df_nih.shape)}")

df_net = dfs["df_research_networks"]
print("df_net: " + str(df_net.shape))
log_out(f"Import Research Networks: {str(df_net.shape)}")


# Merge 1:1 on appl_id
df_flags = pd.merge(df_nih, df_net, on="appl_id", how="outer", indicator=True)

# Drop rows that are only in the using dataset (Stata _merge == 2)
df_flags = df_flags[df_flags["_merge"] != "right_only"].drop(columns=["_merge"])

# Clean res_net column
df_flags["res_net"] = df_flags["res_net"].astype(str).str.upper()

# Drop res_net_override_flag if it exists
if "res_net_override_flag" in df_flags.columns:
    df_flags = df_flags.drop(columns=["res_net_override_flag"])

# Initialize do_not_engage flag
df_flags["do_not_engage"] = 0

# Condition 1: act_code in T90, R90, K99
df_flags.loc[df_flags["act_code"].isin(["T90", "R90", "K99"]), "do_not_engage"] = 1

# Condition 2: HEAL language equals "0" (ignoring nulls)
df_flags.loc[
    (df_flags["nih_foa_heal_lang"] == "0") | (df_flags["nih_noa_heal_lang"] == "0"),
    "do_not_engage",
] = 1

# Condition 3: nih_aian equals "1"
df_flags.loc[df_flags["nih_aian"] == "1", "do_not_engage"] = 1

# Condition 4: MEDTECH containing "Seedling" in title
# case=True matches Stata's ustrpos behavior
is_medtech = df_flags["res_net"] == "MEDTECH"
has_seedling = df_flags["proj_title"].astype(str).str.contains("Seedling", case=True, na=False)
df_flags.loc[is_medtech & has_seedling, "do_not_engage"] = 1

# Initialize checklist_exempt_all flag
df_flags["checklist_exempt_all"] = 0

# Apply checklist exemptions
df_flags.loc[df_flags["do_not_engage"] == 1, "checklist_exempt_all"] = 1
df_flags.loc[df_flags["fund_mech"] == "SBIR/STTR", "checklist_exempt_all"] = 1
df_flags.loc[
    df_flags["nih_noa_notes"] == "but encouraged to comply", "checklist_exempt_all"
] = 1

# Keep specific columns and save intermediate file
df_pm_flags = df_flags[["appl_id", "do_not_engage", "checklist_exempt_all"]].copy()
pm_flags_path = os.path.join(out, "pm_flags.csv")
df_pm_flags.to_csv(pm_flags_path, index=False)


# -----------------------------------------------------------------------------
# 2. Generate Engagement Table
# -----------------------------------------------------------------------------

# Load and clean study lookup table
lookup_path = os.path.join(out, "study_lookup_table.csv")
df_lookup = pd.read_csv(lookup_path).sort_values("appl_id")
df_lookup["appl_id"] = df_lookup["appl_id"].astype(str).str.strip()

# Merge m:1 with pm_flags
df_eng = pd.merge(df_lookup, df_pm_flags, on="appl_id", how="outer", indicator=True)
df_eng = df_eng[df_eng["_merge"] != "right_only"].drop(columns=["_merge"])

# Apply max flags to all appl_ids within the same xstudy_id (Stata egen max)
for var in ["do_not_engage", "checklist_exempt_all"]:
    df_eng[var] = df_eng.groupby("xstudy_id")[var].transform("max")
    df_eng[var] = df_eng[var].fillna(0).astype(int)

# Finalize table output
df_final = df_eng[["appl_id", "do_not_engage", "checklist_exempt_all"]].copy()
df_final = df_final.drop_duplicates().sort_values("appl_id")

# Verify uniqueness of appl_id (Stata duplicates list warning check)
if not df_final["appl_id"].is_unique:
    print("Warning: appl_id is NOT unique in the final table!")

# Export final files
# eng_flags_dta = os.path.join(der_dir, "engagement_flags.dta")
eng_flags_csv = os.path.join(out, "engagement_flags.csv")

# df_final.to_stata(eng_flags_dta, write_index=False)
df_final.to_csv(eng_flags_csv, index=False, quotechar='"')

# /* -------------------------------------------------------------------------------- */
# /* Project: HEAL 																	*/
# /* PI: Kira Bradford																*/
# /* Program: HEAL_02_ImportMerge														*/
# /* Programmer: Sabrina McCutchan (CDMS)												*/
# /* Date Created: 2024/02/29															*/
# /* Date Last Updated: 2026/02/03													*/
# /* Description:	This program imports the latest data from MySQL, merges it, and 	*/
# /*  cleans it.																		*/
# /*		1. Import data																*/
# /*		2. Clean progress_tracker data												*/
# /*		3. Clean awards table data													*/
# /*		4. Merge data 																*/
# /*		5. Clean merged data														*/
# /*																					*/
# /* Notes:  																			*/
# /*		- This program is a necessary first step to all Stata processing. It must 	*/
# /*		  be run before any other Stata HEAL programs.								*/	
# /*		- Both project_num and appl_id fields in MDS are populated with the CTN 	*/
# /*		  protocol number if the HDP_ID is for a CTN protocol						*/
# /*		- progress_tracker only includes records hosted on Platform's MDS. Records	*/
# /*		  hosted somewhere else, such as PDAPS or the AggMDA, are not included.		*/
# /*																					*/
# /* Version changes																	*/
# /*		- 2025/09/02 Platform now contains some records that do not match any NIH	*/
# /*		  appl_id or NIH study.	These were originally in the AggMDS system, but have*/
# /*		  moved to Platform MDS. They are often links to repository data deposits. 	*/
# /*		  They have appl_id="0".													*/
# /*		- 2024/04/29 The reporter table may contain records for appl_ids not present*/
# /*		  in the awards table. This occurs when Platform adds a record for a study	*/
# /*		  that isn't HEAL-funded, but is related to HEAL-funded work ("HEAL-adjacent*/
# /*		  studies"). Such records appear in NIH Reporter but they don't appear in	*/
# /*		  the HEAL-funded specific data sources used to populate the awards table.	*/ 
# /*		- 2024/05/15 Platform has performed QC on appl_id to fix format errors; the */
# /*		  code block that fixed these errors has been archived at end of program, 	*/
# /*		  in case it's ever needed again.											*/
# /*																					*/
# /* -------------------------------------------------------------------------------- */


# CURRENTLY IMPORTS CSV files exported from MySQL Heal Studies database
# Requires these _today.csv files to exist:
# pi_emails_yyyy-mm-dd.csv
# reporter_yyyy-mm-dd.csv
# progress_tracker_yyyy-mm-dd.csv
# awards_yyyy-mm-dd.csv
# research_networks_yyyy-mm-dd.csv 

 
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
today = "2026-04-24"
# today = date.today().strftime("%Y-%m-%d")
print(today)

# ----- 2. Filepaths ----- */
# dir = Path(r"/rtpnfil03/rtpnfil03_vol4/NPTB2/EDC/Migrated/HEAL") #UNIX
dir = Path(r"\\rtpnfil03\NPTB2\EDC\Migrated\HEAL") #WINDOwS
raw = dir / "Extracts"
der = dir / "Derived"
prog = dir / "Programs"
doc = dir / "Documentation"
temp = dir / "temp"
out = dir / "Output"
qc = out / "QC"
backups = dir / "Backups"

# Logging
log_path = os.path.join(out, f"HEAL_02_ImportMerge_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/StudyMetrics_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{out}/HEAL_02_ImportMerge_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_02_ImportMerge Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/



# 1. Import latest MySQL data ----- */
datasets = [
    "reporter",
    "awards",
    "progress_tracker"
]

dfs = {}  # dictionary to hold your dataframes

for name in datasets:
    csv_file = raw / f"{name}_{today}.csv"

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
df_reporter_00 = dfs["df_reporter"]
print("df_reporter_00: " + str(df_reporter_00.shape))
df_awards_00 = dfs["df_awards"]
print("df_awards_00: " + str(df_awards_00.shape))
df_prog_trkr_00 = dfs["df_progress_tracker"]
print("df_prog_trkr_00: " + str(df_prog_trkr_00.shape))
log_out(f"Import MySQL Reporter table: {str(df_reporter_00.shape)}")
log_out(f"Import MySQL Awards table: {str(df_awards_00.shape)}")
log_out(f"Import MySQL Progress Tracker table: {str(df_prog_trkr_00.shape)}")

# Write MySQL Tables to Worksheets
# wb_dataflow = f"{out}/Dataflow_{today}.xlsx"

# Use ExcelWriter in append mode ('a')
# Pandas will automatically handle loading the existing workbook
# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#     # 3. Write your DataFrame directly
#     df_awards_00.to_excel(writer, sheet_name='MySQL_Awards', index=False)
    
# df_reporter_00_noabs=df_reporter_00.drop(columns=['proj_abs'], inplace=False)
# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#     # 3. Write your DataFrame directly
#     df_reporter_00_noabs.to_excel(writer, sheet_name='MySQL_Reporter', index=False)
# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#     # 3. Write your DataFrame directly
#     df_prog_trkr_00.to_excel(writer, sheet_name='MySQL_Prog_Tracker', index=False)


# 2. Prepare progress_tracker to merge ----- */

df_prog_trkr_01 = df_prog_trkr_00.copy()
# reorder columns so appl_id comes first
if "appl_id" in df_prog_trkr_01.columns:
    cols = ["appl_id"] + [c for c in df_prog_trkr_01.columns if c != "appl_id"]
    df_prog_trkr_01 = df_prog_trkr_01[cols]

# drop rows where appl_id is blank (empty string) 
df_prog_trkr_01 = df_prog_trkr_01[
    (df_prog_trkr_01["appl_id"].astype(str).str.strip() != "") 
]

print("df_prog_trkr_01: " + str(df_prog_trkr_01.shape))
log_out(f"Progress Tracker table after dropping records where appl_id is blank: {str(df_prog_trkr_01.shape)}")


# * -- CTN Protocols -- *;
# * Create new CTN variables*;
# * Remove CTN values from appl_id and project_num fields *;
df_prog_trkr_02 = df_prog_trkr_01.copy()

# 1) Create CTN flag: True where project_num starts with "CTN"
df_prog_trkr_02["mds_ctn_flag"] = df_prog_trkr_02["project_num"].str.startswith("CTN", na=False)  # pandas str.startswith for prefix match :contentReference[oaicite:0]{index=0}

# mds_ctn_flag counts and normalized percentages
counts = df_prog_trkr_02['mds_ctn_flag'].value_counts()
percent = df_prog_trkr_02['mds_ctn_flag'].value_counts(normalize=True) * 100

# Combine into a single "Frequency" table
freq_table = pd.concat([counts, percent], axis=1, keys=['Frequency', 'Percent'])
print(freq_table)

log_out(f"Progress Tracker Frequency of 'mds_ctn_flag':")
log_out(f"{str(freq_table)}")
        



# 2) Store CTN project number only for flagged rows
df_prog_trkr_02["mds_ctn_number"] = df_prog_trkr_02["project_num"].where(df_prog_trkr_02["mds_ctn_flag"], None)

# 3) Blank out project_num and appl_id for flagged rows
df_prog_trkr_02.loc[df_prog_trkr_02["mds_ctn_flag"], "project_num"] = ""
df_prog_trkr_02.loc[df_prog_trkr_02["mds_ctn_flag"], "appl_id"] = ""

print("df_prog_trkr_02: " + str(df_prog_trkr_02.shape))

# Usage
log_out(f"df_prog_trkr_02: {df_prog_trkr_02.shape}")

# * -- Project numbers -- *;

# * Split project_num into components needed for xstudy_id *;
# foreach var in project_num {
# replace xproject_num="" if num_dashes>1 /*n=6 changes made*/ 
# 	* Identify and flag bad values of project_num*;
# 	* If an underscore was inserted, remove it and everything that follows it *;
df_prog_trkr_03 = df_prog_trkr_02.copy()

# ————————————————————————
# 1) Create working copy of project_num
# ————————————————————————
df_prog_trkr_03["xproject_num"] = df_prog_trkr_03["project_num"]

# ————————————————————————
# 2) Sieve out characters to count dashes
# (remove everything except dashes)
df_prog_trkr_03["sieved_project_num"] = df_prog_trkr_03["project_num"].str.replace(r"[^-]", "", regex=True)

# Count length of sieved string (trim whitespace)
df_prog_trkr_03["num_dashes"] = df_prog_trkr_03["sieved_project_num"].str.strip().str.len()

# ————————————————————————
# 3) Flag bad project numbers (num_dashes > 1)
df_prog_trkr_03["mds_flag_bad_projnum"] = (df_prog_trkr_03["num_dashes"] > 1).astype(int)
df_prog_trkr_03["mds_bad_projnum"] = df_prog_trkr_03["project_num"].where(df_prog_trkr_03["num_dashes"] > 1, None)

# If more than 1 dash, blank out original in working copy
df_prog_trkr_03.loc[df_prog_trkr_03["num_dashes"] > 1, "xproject_num"] = ""

# ————————————————————————
# 4) Remove underscores and everything after (if any)
df_prog_trkr_03["xproject_num"] = df_prog_trkr_03["xproject_num"].str.replace(r"\_.*", "", regex=True)

# ————————————————————————
# 5) Parse components
# proj_num_spl_ty_code = first character
df_prog_trkr_03["proj_num_spl_ty_code"] = df_prog_trkr_03["xproject_num"].str[0:1]

# proj_num_spl_act_code = next 3 characters
df_prog_trkr_03["proj_num_spl_act_code"] = df_prog_trkr_03["xproject_num"].str[1:4]

# proj_ser_num = next 4 characters
df_prog_trkr_03["proj_ser_num"] = df_prog_trkr_03["xproject_num"].str[4:8]

# ————————————————————————
# 6) Split xproject_num on dashes into parts
split_cols = df_prog_trkr_03["xproject_num"].str.split("-", expand=True)

# drop the first element of the split (like drop xproject_num1 in Stata)
# then take the second element of split as `proj_nm_spl_supp_yr`
# which is at index 1 in pandas because it’s 0-based
df_prog_trkr_03["proj_nm_spl_supp_yr"] = split_cols[1].fillna("")

# suffix code = substr from position 3 onward
df_prog_trkr_03["proj_num_spl_sfx_code"] = df_prog_trkr_03["proj_nm_spl_supp_yr"].str[2:]

# ————————————————————————
# 7) Add `mds_` prefix to match your Stata variable names
df_prog_trkr_03.rename(
    columns={
        "proj_num_spl_ty_code": "mds_proj_num_spl_ty_code",
        "proj_num_spl_act_code": "mds_proj_num_spl_act_code",
        "proj_ser_num": "mds_proj_ser_num",
        "proj_nm_spl_supp_yr": "mds_proj_nm_spl_supp_yr",
        "proj_num_spl_sfx_code": "mds_proj_num_spl_sfx_code",
    },
    inplace=True,
)

# Optional cleanup of intermediate columns
df_prog_trkr_03.drop(columns=["sieved_project_num", "num_dashes"], inplace=True)

print("df_prog_trkr_03: " + str(df_prog_trkr_03.shape))
log_out(f"df_prog_trkr_03: {df_prog_trkr_03.shape}")

# * Count number of hdp_ids for a given appl_id *;
# 	/*Note: 2024-10-09: there are only 8 appl_ids that have >1 HDP_ID associated, and the max number of HDP_IDs associated is 3. This excludes CTN records where appl_id==.*/
df_prog_trkr_04 = df_prog_trkr_03.copy()

# 1) count number of hdp_id values per appl_id
num_hdp = (
    df_prog_trkr_04[df_prog_trkr_04["appl_id"].astype(str).str.strip() != ""]  # ignore blank appl_ids
    .groupby("appl_id")["hdp_id"]
    .count()
    .rename("num_hdp_by_appl")
)

# 2) merge that count back into the main DataFrame
df_prog_trkr_04 = df_prog_trkr_04.merge(num_hdp, on="appl_id", how="left")

# 3) rows with blank appl_id → keep count as NaN
mask_blank_appl = df_prog_trkr_04["appl_id"].astype(str).str.strip() == ""
df_prog_trkr_04.loc[mask_blank_appl, "num_hdp_by_appl"] = pd.NA

# 4) rows not found in grouping get 0
df_prog_trkr_04["num_hdp_by_appl"] = df_prog_trkr_04["num_hdp_by_appl"].fillna(0).astype(int)

print("df_prog_trkr_04: " + str(df_prog_trkr_04.shape))
log_out(f"df_prog_trkr_04: Count number of hdp_ids for a given appl_id {df_prog_trkr_04.shape}")


# * Save prepped data *;
df_prog_trkr_05 = df_prog_trkr_04.copy()


# 1) Drop the helper columns
df_prog_trkr_05 = df_prog_trkr_05.drop(columns=["sievedproject_num", "num_dashes", "xproject_num"], errors="ignore")

# 2) Save to CSV
temp = Path(r"\\rtpnfil03\NPTB2\EDC\Migrated\HEAL\Temp")  # replace with your temp folder path
output_file = temp / f"progress_tracker_{today}.csv"

df_prog_trkr_05.to_csv(output_file, index=False)

print(f"Saved prepped data to {output_file}")

print("df_prog_trkr_05: " + str(df_prog_trkr_05.shape))
log_out(f"df_prog_trkr_05: Drops helper vars and saves to 'progress_tracker_today.csv' {df_prog_trkr_05.shape}")
# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#     # 3. Write your DataFrame directly
#     df_prog_trkr_05.to_excel(writer, sheet_name='progress_tracker_today', index=False)

# /* ----- 3. Merge data ----- */
# * Merge awards reporter *;

# Make local copies so we don’t modify originals
df_reporter_01 = df_reporter_00.copy()
df_awards_01 = df_awards_00.copy()

# —————————————————————————————
# 2) Drop rows where appl_id is blank
df_reporter_01 = df_reporter_01[
    (df_reporter_01["appl_id"].notna()) & 
    (df_reporter_01["appl_id"].astype(str).str.strip() != "")
].copy()


df_awards_01 = df_awards_01[
    (df_awards_01["appl_id"].notna()) & 
    (df_awards_01["appl_id"].astype(str).str.strip() != "")
].copy()
print("df_reporter_01: " + str(df_reporter_01.shape))
print("df_awards_01: " + str(df_awards_01.shape))

log_out(f"df_reporter_01: Drops records where appl_id is blank {df_reporter_01.shape}")
log_out(f"df_awards_01: Drops records where appl_id is blank {df_awards_01.shape}")


# —————————————————————————————
# 3) Merge 1:1 on appl_id with indicator
df_nihtables_00 = pd.merge(
    df_reporter_01,
    df_awards_01,
    how="outer",
    on="appl_id",
    indicator="merge_reporter_awards"
)

# —————————————————————————————
# 4) Drop rows where appl_id is blank after merge
df_nihtables_00 = df_nihtables_00[
    (df_nihtables_00["appl_id"].notna()) & 
    (df_nihtables_00["appl_id"].astype(str).str.strip() != "")
].copy()

print("df_nihtables_00: " + str(df_nihtables_00.shape))
log_out(f"df_nihtables_00: Outer Join on appl_id (df_reporter_01,df_awards_01) {df_nihtables_00.shape}")



# —————————————————————————————
# 5) Convert indicator text to numeric codes (1/2/3) like Stata
indicator_map = {
    "left_only": 1,   # In reporter only
    "right_only": 2,  # In awards only
    "both": 3         # In both
}

df_nihtables_00["merge_reporter_awards"] = df_nihtables_00["merge_reporter_awards"].map(indicator_map)

# merge_reporter_awards counts and normalized percentages
counts = df_nihtables_00['merge_reporter_awards'].value_counts()
percent = df_nihtables_00['merge_reporter_awards'].value_counts(normalize=True) * 100

# Combine into a single "Frequency" table
freq_table = pd.concat([counts, percent], axis=1, keys=['Frequency', 'Percent'])
print(freq_table)

log_out(f"df_nihtables_00 Frequency of 'merge_reporter_awards':")
log_out(f"{str(freq_table)}")



# —————————————————————————————
# 6) Save result (CSV or Stata)
df_nihtables_00.to_csv(temp / f"nihtables_{today}.csv", index=False)
log_out(f"df_reporter_01: ' {df_reporter_01.shape}")
log_out(f"df_awards_01: ' {df_awards_01.shape}")
log_out(f"df_nihtables_00: Saves to 'nihtables_today.csv' {df_nihtables_00.shape}")


print("df_reporter_01: " + str(df_reporter_01.shape))
print("df_awards_01: " + str(df_awards_01.shape))
print("df_nihtables_00: " + str(df_nihtables_00.shape))

df_nihtables_noabs=df_nihtables_00.drop(columns=['proj_abs'], inplace=False)

# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#     # 3. Write your DataFrame directly
#     df_nihtables_noabs.to_excel(writer, sheet_name='nihtables_today', index=False)



# * Merge MDS data (via progress_tracker) *;

# copy so we don’t modify originals
df_nihtables_01 = df_nihtables_00.copy()
df_prog_trkr_06 = df_prog_trkr_05.copy()

# merge 1:m on appl_id with merge indicator
df_dataset_00 = pd.merge(
    df_nihtables_01,
    df_prog_trkr_06,
    on="appl_id",
    how="outer",              # outer keeps all rows from both
    indicator="merge_awards_mds"
)

# map indicator to Stata numeric codes
indicator_map = {
    "left_only": "1. MySQL only",    # In MySQL only
    "right_only": "2. MDS only",   # In MDS only
    "both": "3. MySQL and MDS"          # In both databases
}

df_dataset_00["merge_awards_mds"] = df_dataset_00["merge_awards_mds"].map(indicator_map)

# merge_awards_mds counts and normalized percentages
counts = df_dataset_00['merge_awards_mds'].value_counts()
percent = df_dataset_00['merge_awards_mds'].value_counts(normalize=True) * 100

# Combine into a single "Frequency" table
freq_table = pd.concat([counts, percent], axis=1, keys=['Frequency', 'Percent'])
print(freq_table)

log_out(f"df_dataset_00 Frequency of 'merge_awards_mds':")
log_out(f"{str(freq_table)}")




# now df_merged matches Stata
# (you can also add labels or comments for documentation)

# save to Stata or CSV
df_dataset_00.to_csv(temp / f"dataset_{today}.csv", index=False)

log_out(f"df_prog_trkr_06: ' {df_prog_trkr_06.shape}")
log_out(f"df_nihtables_01: ' {df_nihtables_01.shape}")
log_out(f"df_dataset_00: Outer join on appl_id (df_prog_trkr_06,df_nihtables_01) and saves to 'dataset_today.csv' {df_dataset_00.shape}")

print("df_nihtables_01: " + str(df_nihtables_01.shape))
print("df_prog_trkr_06: " + str(df_prog_trkr_06.shape))
print("df_dataset_00: " + str(df_dataset_00.shape))

df_dataset_00_noabs=df_dataset_00.drop(columns=['proj_abs'], inplace=False)

# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#     # 3. Write your DataFrame directly
#     df_dataset_00_noabs.to_excel(writer, sheet_name='dataset_today', index=False)




# /* ----- 4. Clean merged data ----- */
# 	/* Note: subproj_id is not available in the MDS data */ /*n=2 and n=0 real changes made*/

# Make local copies so we don’t modify originals
df_dataset_01 = df_dataset_00.copy()

# Replace blank or whitespace only for proj_ser_num
df_dataset_01.loc[
    df_dataset_01["proj_ser_num"].str.strip().eq(""),
    "proj_ser_num"
] = df_dataset_01.loc[
    df_dataset_01["proj_ser_num"].str.strip().eq(""),
    "mds_proj_ser_num"
]

# Replace blank or whitespace only for proj_num_spl_sfx_code
df_dataset_01.loc[
    df_dataset_01["proj_num_spl_sfx_code"].str.strip().eq(""),
    "proj_num_spl_sfx_code"
] = df_dataset_01.loc[
    df_dataset_01["proj_num_spl_sfx_code"].str.strip().eq(""),
    "mds_proj_num_spl_sfx_code"
]


print("df_dataset_01: " + str(df_dataset_01.shape))
log_out(f"df_dataset_01: replaced blank/whitespace for proj_ser_num and proj_num_spl_sfx_code' {df_dataset_01.shape}")

# * Flag supplement awards *;

# Make local copies so we don’t modify originals
df_dataset_02 = df_dataset_01.copy()

# —————————————————————————————
# 1) Flag supplement awards
# —————————————————————————————

# get the second‑to‑last character of proj_num (like Stata substr at -2,1)
df_dataset_02["xsupp_flag"] = df_dataset_02["proj_num"].str[-2:-1]

# flag where that equals "S"
df_dataset_02["supplement_flag"] = (df_dataset_02["xsupp_flag"] == "S").astype(int)

# drop the helper
df_dataset_02.drop(columns=["xsupp_flag"], inplace=True)
print("df_dataset_02: " + str(df_dataset_02.shape))
log_out(f"df_dataset_02: create supplement flag where proj_num has S in second to last char' {df_dataset_02.shape}")

# merge_awards_mds counts and normalized percentages
counts = df_dataset_02['supplement_flag'].value_counts()
percent = df_dataset_02['supplement_flag'].value_counts(normalize=True) * 100

# Combine into a single "Frequency" table
freq_table = pd.concat([counts, percent], axis=1, keys=['Frequency', 'Percent'])
print(freq_table)
log_out(f"df_dataset_02 Frequency of 'supplement_flag':")
log_out(f"{str(freq_table)}")

# —————————————————————————————
# 2) Convert dates
# —————————————————————————————

# ensure fiscal year numeric
df_dataset_02["fisc_yr"] = pd.to_numeric(df_dataset_02["fisc_yr"], errors="coerce")

# parse the string date columns into real pandas datetime
for var in ["bgt_end", "proj_end_date"]:
    # extract the first 10 characters
    df_dataset_02[f"x{var}"] = df_dataset_02[var].astype(str).str[:10]

    # convert to datetime (pandas datetime)
    df_dataset_02[f"{var}_date"] = pd.to_datetime(df_dataset_02[f"x{var}"], format="%Y-%m-%d", errors="coerce")

    # drop the intermediate
    df_dataset_02.drop(columns=[f"x{var}"], inplace=True)

    # (Optional) reorder columns to come after the original
    cols = list(df_dataset_02.columns)
    if f"{var}_date" in cols:
        cols.remove(f"{var}_date")
        insert_at = cols.index(var) + 1 if var in cols else len(cols)
        cols.insert(insert_at, f"{var}_date")
        df_dataset_02 = df_dataset_02[cols]

    # label for clarity (not necessary in pandas storage)
    df_dataset_02[f"{var}_date"].attrs["label"] = "Python datetime format"

# —————————————————————————————
# 3) Entity type
# —————————————————————————————

# default
df_dataset_02["entity_type"] = "Study"

# override for CTN
df_dataset_02.loc[df_dataset_02["mds_ctn_flag"] == 1, "entity_type"] = "CTN"

# override for bad projnum
df_dataset_02.loc[df_dataset_02["mds_flag_bad_projnum"] == 1, "entity_type"] = "Other"

# —————————————————————————————
# 4) Save result
# —————————————————————————————

# Make local copies so we don’t modify originals
mysql_today = df_dataset_02.copy()

mysql_today.to_csv(der / f"mysql_{today}.csv", index=False)

print("mysql_today: " + str(mysql_today.shape))
log_out(f"df_dataset_02: Create Entity type (Study, CTN, Other) and save to mysql_today.csv' {df_dataset_02.shape}")

mysql_today_noabs=mysql_today.drop(columns=['proj_abs'], inplace=False)

# List of columns you want at the beginning
cols_to_front = ['merge_awards_mds', 'hdp_id', 'appl_id']

# Reorder: Target columns + [all other columns if they are not in the target list]
mysql_today_noabs = mysql_today_noabs[cols_to_front + [col for col in mysql_today_noabs.columns if col not in cols_to_front]]


log_out(f"mysql_today_noabs: Copy of mysql_today, but without proj_abs field b/c of export issues with special char' {mysql_today_noabs.shape}")

# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
#      # 2. Map the column to labels before writing
#     # This replaces the values in 'merge_awards_mds' for the output only
#     mysql_today_noabs['merge_awards_mds'] = mysql_today_noabs['merge_awards_mds'].map(indicator_map).fillna(mysql_today_noabs['merge_awards_mds'])
#     # 3. Write your DataFrame directly
#     mysql_today_noabs.to_excel(writer, sheet_name='mysql_today', index=False)

# # Deletes "Sheet1"
# # Open the file in append mode
# with pd.ExcelWriter(wb_dataflow, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
#     # Access the openpyxl workbook object
#     workbook = writer.book
    
#     # Check if the sheet exists and delete it
#     if 'Sheet1' in workbook.sheetnames:
#         del workbook['Sheet1']


# END HEAL_02_ImportMerge 
log_out(f"END HEAL_02_ImportMerge")


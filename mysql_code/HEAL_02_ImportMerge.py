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
today = os.environ.get("today")

# today = date.today().strftime("%Y-%m-%d")
print(today)
# ----- 2. Filepaths ----- */
dir = Path(os.environ.get("dir"))
inp = dir / "Input"
out = dir / "Output"
log = dir / "Log"

# Logging
log_path = os.path.join(log, f"HEAL_02_ImportMerge_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/StudyMetrics_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{log}/HEAL_02_ImportMerge_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_02_ImportMerge Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/



# 1. Import latest MySQL data ----- */
datasets = [
    "reporter",
    "awards",
    "progress_tracker",
    "pi_emails"
]

dfs = {}  # dictionary to hold your dataframes

for name in datasets:
    csv_file = inp / f"{name}_{today}.csv"

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
log_out(f"Import MySQL Reporter table: {str(df_reporter_00.shape)}")

# Drops record where appl_id is missing
df_awards_00 = dfs["df_awards"].dropna(subset=['appl_id'])
print("df_awards_00: " + str(df_awards_00.shape))
log_out(f"Import MySQL Awards table: {str(df_awards_00.shape)}")

df_prog_trkr_00 = dfs["df_progress_tracker"]
print("df_prog_trkr_00: " + str(df_prog_trkr_00.shape))
log_out(f"Import MySQL Progress Tracker table: {str(df_prog_trkr_00.shape)}")


df_pi_emails_00 = dfs["df_pi_emails"]
print("df_pi_emails_00: " + str(df_pi_emails_00.shape))
log_out(f"Import MySQL PI Emails table: {str(df_pi_emails_00.shape)}")


# 1. Import latest MySQL data ----- */
datasets = [
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

   
# Drops record where appl_id is missing
df_res_net_00 = dfs["df_research_networks"].dropna(subset=['appl_id'])
print("df_res_net_00: " + str(df_res_net_00.shape))
log_out(f"Import MySQL Research Network table: {str(df_res_net_00.shape)}")


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

# 2. Reorder columns (equivalent to 'order')
# This moves 'appl_id' to the first position
cols = ['appl_id'] + [c for c in df_prog_trkr_01.columns if c != 'appl_id']
df_prog_trkr_01 = df_prog_trkr_01[cols]

# 3. Drop rows with empty appl_id (equivalent to 'drop if')
# This handles both actual empty strings and NaN/None values
df_prog_trkr_01 = df_prog_trkr_01[df_prog_trkr_01['appl_id'].astype(str).str.strip() != ""]
df_prog_trkr_01 = df_prog_trkr_01.dropna(subset=['appl_id'])

# 4. Identify and flag bad values (equivalent to 'gen')
df_prog_trkr_01['mds_flag_bad_projnum'] = 0
print("df_prog_trkr_01: " + str(df_prog_trkr_01.shape))
log_out(f"Progress Tracker table after dropping records where appl_id is blank: {str(df_prog_trkr_01.shape)}")
# df_prog_trkr_01.to_csv(os.path.join(out, f'df_prog_trkr_01.csv'), index=False, quoting=1)



# * -- CTN Protocols -- *;
# * Create new CTN variables*;
# * Remove CTN values from appl_id and project_num fields *;
df_prog_trkr_02 = df_prog_trkr_01.copy()


# gen mds_ctn_flag = regexm(project_num, "^CTN")
df_prog_trkr_02['mds_ctn_flag'] = df_prog_trkr_02['project_num'].str.contains('^CTN', regex=True, na=False).astype(int)

# gen mds_ctn_number = project_num if mds_ctn_flag == 1
df_prog_trkr_02['mds_ctn_number'] = np.where(df_prog_trkr_02['mds_ctn_flag'] == 1, df_prog_trkr_02['project_num'], None)

# replace project_num="" and appl_id="" if mds_ctn_flag == 1
df_prog_trkr_02.loc[df_prog_trkr_02['mds_ctn_flag'] == 1, ['project_num', 'appl_id']] = ""


# --- Other entities (Bad project numbers) ---
# Stata 'sieve' logic: count dashes
df_prog_trkr_02['num_dashes'] = df_prog_trkr_02['project_num'].str.count('-')

# replace mds_flag_bad_projnum=1 if num_dashes > 1
df_prog_trkr_02.loc[df_prog_trkr_02['num_dashes'] > 1, 'mds_flag_bad_projnum'] = 1

# gen mds_bad_projnum = project_num if num_dashes > 1
df_prog_trkr_02['mds_bad_projnum'] = np.where(df_prog_trkr_02['num_dashes'] > 1, df_prog_trkr_02['project_num'], None)

# ICPSR data deposits
df_prog_trkr_02.loc[df_prog_trkr_02['project_num'].str.startswith('ICPSR', na=False), 'mds_flag_bad_projnum'] = 1

# Cleanup temp columns
df_prog_trkr_02 = df_prog_trkr_02.drop(columns=['num_dashes'])


# --- Count number of hdp_ids for a given appl_id ---
# by appl_id: egen num_hdp_by_appl = count(hdp_id)
# Note: In pandas, count() ignores NaNs automatically
df_prog_trkr_02['num_hdp_by_appl'] = df_prog_trkr_02.groupby('appl_id')['hdp_id'].transform('count')

# Handle specific replacements for num_hdp_by_appl
df_prog_trkr_02.loc[df_prog_trkr_02['appl_id'] == "0", 'num_hdp_by_appl'] = 0
df_prog_trkr_02.loc[df_prog_trkr_02['appl_id'] == "", 'num_hdp_by_appl'] = np.nan


# --- Entity type ---
df_prog_trkr_02['entity_type'] = "Study"
df_prog_trkr_02.loc[df_prog_trkr_02['mds_ctn_flag'] == 1, 'entity_type'] = "CTN"
df_prog_trkr_02.loc[df_prog_trkr_02['mds_flag_bad_projnum'] == 1, 'entity_type'] = "Other"
df_prog_trkr_02.loc[(df_prog_trkr_02['appl_id'] == "0") & (df_prog_trkr_02['mds_ctn_flag'] != 1), 'entity_type'] = "Other"


# --- Final Cleanup & Save ---
df_prog_trkr_02.loc[df_prog_trkr_02['appl_id'] == "0", 'appl_id'] = ""

df_prog_trkr_02.to_csv(os.path.join(out, f'progress_tracker_{today}.csv'), index=False, quoting=1)

print("df_prog_trkr_02: " + str(df_prog_trkr_02.shape))
log_out(f"df_prog_trkr_02: {df_prog_trkr_02.shape}")
# df_prog_trkr_02.to_csv(os.path.join(out, f'df_prog_trkr_02.csv'), index=False, quoting=1)




# /* ----- 3. Clean awards table data ----- */
# * Note: This step may not be needed if the awards table is not altered during export from MySQL. Sabrina had issues of NULL/missing values being set to 0 during export. As a quick check, note that the value of nih_foa_heal_lang and of nih_noa_heal_lang should be NULL in a majority of records, since NIH has only indicated values of these variables in the 2024 and 2025 new awards lists. *;
df_awards_01 = df_awards_00.copy()

# drop if appl_id==""
df_awards_01 = df_awards_01[df_awards_01['appl_id'].astype(str).str.strip() != ""]

# Standardize empty strings to "NULL" and rename (prep for merge)
# rename nih_`acr'_heal_lang xnih_`acr'_heal_lang
for acr in ['foa', 'noa']:
    col = f'nih_{acr}_heal_lang'
    df_awards_01.loc[df_awards_01[col].isna() | (df_awards_01[col] == ""), col] = "NULL"
    df_awards_01 = df_awards_01.rename(columns={col: f'x{col}'})

# Merge with correct values
#Look to improve Awards table to generate the correct foanoa values automatically
# merge 1:1 appl_id using ... keepusing(...)
df_correct = pd.read_csv(
    inp / "correct_foanoa_values.csv", 
    dtype=str,
    usecols=['appl_id', 'nih_foa_heal_lang', 'nih_noa_heal_lang']
)

# df_correct = pd.read_stata(inp / "correct_foanoa_values.csv", columns=['appl_id', 'nih_foa_heal_lang', 'nih_noa_heal_lang'])
df_awards_01 = pd.merge(df_awards_01, df_correct, on='appl_id', how='left', indicator=True)

# drop if _merge==2 (Keep only matches and master-only records)
df_awards_01 = df_awards_01[df_awards_01['_merge'] != 'right_only']

# Cleanup: drop _merge and the renamed old columns (x*)
df_awards_01 = df_awards_01.drop(columns=['_merge'] + [c for c in df_awards_01.columns if c.startswith('xnih_')])

# Ensure the newly merged columns also use "NULL" for missing values
for acr in ['foa', 'noa']:
    col = f'nih_{acr}_heal_lang'
    df_awards_01.loc[df_awards_01[col].isna() | (df_awards_01[col] == ""), col] = "NULL"

# order nih_noa_notes, last
cols = [c for c in df_awards_01.columns if c != 'nih_noa_notes'] + ['nih_noa_notes']
df_awards_01 = df_awards_01[cols]

# Save
# df_awards_01.to_csv(os.path.join(out, f'awards_{today}.csv'), index=False, quoting=1)

print("df_awards_01: " + str(df_awards_01.shape))
log_out(f"df_awards_01: {df_awards_01.shape}")
# df_awards_01.to_csv(os.path.join(out, f'df_awards_01.csv'), index=False, quoting=1)


# /* ----- 4. Merge data ----- */
# * Merge awards reporter *;
df_reporter_01 = df_reporter_00[df_reporter_00['appl_id'].astype(str).str.strip() != ""]
# df_awards_01 = df_awards_00.copy()
df_res_net_01 = df_res_net_00.copy()

# merge 1:1 appl_id
df_nihtables = pd.merge(df_reporter_01, df_awards_01, on='appl_id', how='outer', indicator='merge_reporter_awards')

# drop if appl_id=="" (already handled, but good for safety)
df_nihtables = df_nihtables[df_nihtables['appl_id'].astype(str).str.strip() != ""]

# Replicate Stata labels (for documentation/clarity)
merge_map_awrep = {
    'left_only': "In reporter only",   # _merge == 1
    'right_only': "In awards only",    # _merge == 2
    'both': "In both tables"           # _merge == 3
}
df_nihtables['merge_reporter_awards'] = df_nihtables['merge_reporter_awards'].map(merge_map_awrep)
df_nihtables.to_csv(os.path.join(out, f'nihtables_{today}.csv'), index=False, quoting=1)


# --- 2. Merge progress_tracker table ---
df_prog_trkr_03 = df_prog_trkr_02.copy()


# merge 1:m appl_id
df_dataset = pd.merge(df_nihtables, df_prog_trkr_03, on='appl_id', how='outer', indicator='merge_awards_mds')

merge_map_sqlmds = {
    'left_only': "In MySQL only",
    'right_only': "In MDS only",
    'both': "In both databases"
}
df_dataset['merge_awards_mds'] = df_dataset['merge_awards_mds'].map(merge_map_sqlmds)

print("df_dataset: " + str(df_dataset.shape))
log_out(f"df_dataset: {df_dataset.shape}")
# df_dataset.to_csv(os.path.join(out, f'df_dataset.csv'), index=False, quoting=1)


# --- 3. Merge research_networks table ---

# merge m:1 appl_id
df_dataset2 = pd.merge(df_dataset, df_res_net_01, on='appl_id', how='left', indicator='_merge')

# drop if _merge==2 (Keep only data from the main dataset)
df_dataset2 = df_dataset2[df_dataset2['_merge'] != 'right_only']

# Cleanup and logic updates
df_dataset2 = df_dataset2.drop(columns=['_merge', 'res_net_override_flag'])

# replace res_net=upper(res_net)
df_dataset2['res_net'] = df_dataset2['res_net'].str.upper()

# replace entity_type logic
df_dataset2.loc[df_dataset2['res_net'] == "CTN", 'entity_type'] = "CTN"
df_dataset2['entity_type'] = df_dataset2['entity_type'].replace("", "Study").fillna("Study")


# Save
# df_dataset2.to_csv(os.path.join(out, f'dataset2_{today}.csv'), index=False, quoting=1)

print("df_dataset2: " + str(df_dataset2.shape))
log_out(f"df_dataset2: {df_dataset2.shape}")
# df_dataset2.to_csv(os.path.join(out, f'df_dataset2.csv'), index=False, quoting=1)

# /* ----- 5. Clean merged data ----- */

mysql_today_00 = df_dataset2.copy()

# --- 1. Flag supplement awards ---
# gen xsupp_flag=substr(proj_num,-2,1)
# gen supplement_flag=1 if xsupp_flag=="S"
mysql_today_00['supplement_flag'] = np.where(mysql_today_00['proj_num'].str[-2:-1] == "S", 1, np.nan)

# --- 2. Dates ---
# destring fisc_yr, replace
mysql_today_00['fisc_yr'] = pd.to_numeric(mysql_today_00['fisc_yr'], errors='coerce')

date_vars = ['awd_not_date', 'bgt_end', 'bgt_strt', 'proj_end_date', 'proj_strt_date']

for var in date_vars:
    # gen x`var'=substr(`var',1,10) & gen `var'_date=date(x`var',"YMD")
    # pd.to_datetime handles the first 10 chars automatically if format is YYYY-MM-DD
    new_col = f'{var}_date'
    mysql_today_00[new_col] = pd.to_datetime(mysql_today_00[var].astype(str).str[:10], errors='coerce')
    
    # Reorder: order `var'_date, after(`var')
    current_cols = list(mysql_today_00.columns)
    var_idx = current_cols.index(var)
    current_cols.insert(var_idx + 1, current_cols.pop(current_cols.index(new_col)))
    mysql_today_00 = mysql_today_00[current_cols]

# --- 3. Compound Key ---
# egen compound_key=concat(appl_id hdp_id), punct(_)
# Convert to string first to ensure concatenation works correctly
# mysql_today_00['compound_key'] = mysql_today_00['appl_id'].astype(str) + "_" + mysql_today_00['hdp_id'].astype(str)
# This version avoids "nan" from appearing in missing hdp_ids
mysql_today_00['compound_key'] = (
    mysql_today_00['appl_id'].astype(str) + "_" + 
    mysql_today_00['hdp_id'].fillna('').astype(str).str.replace(r'\.0$', '', regex=True)
)


# Save
mysql_today_00.to_csv(os.path.join(out, f'mysql_{today}.csv'), index=False, quoting=1)

print("mysql_today_00: " + str(mysql_today_00.shape))
log_out(f"mysql_today_00: {mysql_today_00.shape}")


# END HEAL_02_ImportMerge 
log_out(f"END HEAL_02_ImportMerge")


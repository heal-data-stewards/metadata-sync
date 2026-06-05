# /* -------------------------------------------------------------------------------- */
# /* Project: HEAL 																	*/
# /* PI: Kira Bradford																*/
# /* Program: HEAL_03_DQAudit															*/
# /* Programmer: Sabrina McCutchan (CDMS)												*/
# /* Date Created: 2025/03/27															*/
# /* Date Last Updated: 2026/01/30													*/
# /* Description:	This program performs a data quality audit checking the completeness*/	
# /*	of data about NIH awards related to HEAL studies in the MySQL database.			*/			
# /*		1. Create key of appl_ids in MySQL (reporter, awards, progress-tracker)		*/
# /*		2. Query NIH Reporter for all awards sharing a project serial number with 	*/
# /*			an award in MySQL 														*/
# /*		3. Compile NIH Reporter query output & existing awards table				*/
# /*		4. Flag appls that are in the reporter table								*/
# /*		5. Isolate awards we should track that aren't yet in MySQL   				*/
# /*		6. Rename vars to same names used in reporter table							*/
# /*		7. Format and output table for MySQL										*/
# /*		8. Debug: split up large file into 2 for MySQL import						*/
# /*																					*/
# /* Notes:  																			*/
# /*		- Last run 2026/01/15														*/
# /*		- See https://docs.google.com/document/d/									*/
# /*		  11OOFOEzp_2ZDSM6kXE8G6Lpxs_qTmKDX92F0L5kh9z0/edit?tab=t.0  for a write-up */
# /*		  of why and how this DQ audit was performed.								*/
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
today = "2026-04-24"
# today = date.today().strftime("%Y-%m-%d")
print(today)

# ----- 2. Filepaths ----- */
dir = Path(r"C:\Users\berman\OneDrive - Research Triangle Institute\Python Environment\HEAL") #WINDOwS
inp = dir / "Input"
out = dir / "Output"
log = dir / "Log"

# Logging
log_path = os.path.join(log, f"HEAL_03_DQAudit_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/StudyMetrics_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{log}/HEAL_03_DQAudit_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_03_DQAudit Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/



# /* ----- 1. Create key of appl_ids in MySQL ----- */

# use "$temp/nihtables_$today.dta", clear
# keep appl_id heal_funded proj_ser_num
# merge 1:1 appl_id using "$raw/research_networks_$today.dta"
# drop if _merge==2
# duplicates drop
# gen in_resnet=0
# replace in_resnet=1 if strtrim(res_net)!=""
# keep appl_id heal_funded res_net in_resnet proj_ser_num
# rename proj_ser_num in_rep_table_sernum
# save "$temp/mysql_applkey.dta", replace 








# 1. Import CSV files ----- */
datasets = [
    "nihtables"
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
df_nihtables_00 = dfs["df_nihtables"]
print("df_nihtables_00: " + str(df_nihtables_00.shape))
log_out(f"Import {name}: {str(df_nihtables_00.shape)}")


# 1. Import CSV files ----- */
datasets = [
    "research_networks"
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
df_res_net_00 = dfs["df_research_networks"]
print("df_res_net_00: " + str(df_res_net_00.shape))
log_out(f"Import {name}: {str(df_res_net_00.shape)}")


df_nihtables_01 = df_nihtables_00[["appl_id", "heal_funded", "proj_ser_num"]]


# 3. Perform 1:1 merge (Left join mimics 'drop if _merge==2')
merged_df = pd.merge(df_nihtables_01, df_res_net_00, on="appl_id", how="left")

# 4. Remove exact duplicate rows
merged_df = merged_df.drop_duplicates()

# 5. Create conditional flag based on trimmed string content
# Handles missing values (NaN) safely by filling with empty string first
res_net_trimmed = merged_df["res_net"].fillna("").str.strip()
merged_df["in_resnet"] = (res_net_trimmed != "").astype(int)

# 6. Final column cleanup and renaming
merged_df = merged_df.rename(columns={"proj_ser_num": "in_rep_table_sernum"})
keep_cols = [
    "appl_id",
    "heal_funded",
    "res_net",
    "in_resnet",
    "in_rep_table_sernum",
]
mysql_applkey = merged_df[keep_cols]




# /* ----- 2. Query NIH Reporter for all awards sharing a project serial number with an award in MySQL ----- */
# /* Note: This process is done outside of MySQL. The file "$raw/xold/heal_awards_by_serial_number_02142025.csv" (called below) was generated by Maria Davila. She took every unique project serial number that existed in the MySQL reporter and awards tables and fetched every appl_id associated with those serial numbers from NIH Reporter. */
# /* Note: On 2026/01/26, re-ran this process with another extract file "$raw/raw source data/heal_awards_reporter_01152025_sn.csv" */



# /* ----- 3. Compile NIH Reporter query output & existing awards table ----- */

# * Import full list of candidates from NIH Reporter - Jan 2026 *;
# import delim using "$raw/raw source data/heal_awards_reporter_01152025_sn.csv", delimiters(",") varnames(1) bindquotes(strict) case(preserve) maxquotedrows(30) stringcols(_all) clear /*n=5079*/
# capture drop heal_funded
# duplicates drop /*n=0 duplicates*/
# gen dqauditwave=2

# 	* Use same date format *;
# 	/*e.g. change from 7/31/2025 to 2025-07-31*/
# 	/* Note: awd_not_date already correct format */
# 	foreach var in proj_end_date proj_strt_date bgt_strt bgt_end {
# 		split `var', p("/")
# 		rename `var' z`var'
# 			forv i=1/2 {
# 				gen z`var'`i'="0"+`var'`i'
# 				replace z`var'`i'=substr(z`var'`i',-2,2)
# 				drop `var'`i'
# 				rename z`var'`i' `var'`i'
# 				}
# 		egen `var'=concat(`var'3 `var'1 `var'2), punct("-")
# 		order `var', after(z`var')
# 		drop z`var' `var'1 `var'2 `var'3
# 		}

# save "$raw/heal_awards_by_serial_number_$today.dta", replace

# * Append to existing reporter_dqaudit table *;
# import delim using "$raw/xold/reporter_dqaudit_2026-01-26.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear /*n=448*/
# gen dqauditwave=1
# append using "$raw/heal_awards_by_serial_number_$today.dta" /*n=5527*/

# * Remove duplicate appl_ids *;
# duplicates drop /*n=0 dropped*/
# sort appl_id dqauditwave
# by appl_id: gen count=_n
# by appl_id: egen num_rows=max(count)
# drop if num_rows==2 & dqauditwave==1 /*n=448 dropped*/
# duplicates list appl_id /*n=0 duplicates*/

# drop count num_rows dqauditwave
# save "$temp/heal_awards_by_serial_number_$today.dta", replace /*n=5079*/

raw_csv_path = (inp / "heal_awards_reporter_sn_04242026_out.csv")

# 1. Load the raw awards reporter data
# stringcols(_all) translates to dtype=str
heal_by_sn_00 = pd.read_csv(raw_csv_path, sep=",", dtype=str, keep_default_na=False)

# capture drop heal_funded
if "heal_funded" in heal_by_sn_00.columns:
    heal_by_sn_00 = heal_by_sn_00.drop(columns=["heal_funded"])

# duplicates drop
heal_by_sn_01 = heal_by_sn_00.drop_duplicates()

# gen dqauditwave=2
heal_by_sn_01["dqauditwave"] = 2

# Convert date formats for the specified columns
date_cols = ["proj_end_date", "proj_strt_date", "bgt_strt", "bgt_end"]

for col in date_cols:
    if col in heal_by_sn_01.columns:
        # 1. Convert strings to datetime objects (handles M/D/YYYY and MM/DD/YYYY natively)
        dt_series = pd.to_datetime(heal_by_sn_01[col], format="%m/%d/%Y", errors="coerce")
        
        # 2. Convert back to YYYY-MM-DD string format, keeping missing values intact
        heal_by_sn_01[col] = dt_series.dt.strftime("%Y-%m-%d").fillna(heal_by_sn_01[col])


# Save intermediate file
heal_by_sn_01.to_csv(os.path.join(out, f'int_heal_awards_by_serial_number_{today}.csv'), index=False, quoting=1)



# 2. Load the old reporter_dqaudit table
# import delim using "$raw/xold/reporter_dqaudit_2026-01-26.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear /*n=448*/
# UPDATE to Pull directly from MySQL
old_dqaudit = pd.read_csv(inp / "reporter_dqaudit_04242026.csv", sep=",", dtype=str, keep_default_na=False)
old_dqaudit["dqauditwave"] = 1

# 3. Append datasets together
df_combined = pd.concat([old_dqaudit, heal_by_sn_01], ignore_index=True)

# duplicates drop
df_combined = df_combined.drop_duplicates()

# 4. Remove duplicate appl_ids favoring wave 2 over wave 1
# Sort by appl_id and dqauditwave to match Stata's ordering
df_combined = df_combined.sort_values(by=["appl_id", "dqauditwave"])

# Drop wave 1 rows if an appl_id exists in both waves
df_combined = df_combined.drop_duplicates(subset=["appl_id"], keep="last")

# Clean up structural tracking columns
df_combined = df_combined.drop(columns=["dqauditwave"])

# 5. Save final table
# df_combined.to_stata(final_output_path, write_index=False, version=118)
df_combined.to_csv(os.path.join(out, f'final_heal_awards_by_serial_number_{today}.csv'), index=False, quoting=1)





# /* ----- 4. Flag appls that are in the reporter table ----- */
# use "$temp/heal_awards_by_serial_number_$today.dta", clear
# sort appl_id
# merge 1:1 appl_id using "$temp/mysql_applkey.dta" /* Note : there are 6 _merge==2 appl_ids, in MySQL that don't appear in the NIH Reporter extract. These are the 6 known problematic appl_ids that don't have a correctly formatted project number. */
# drop if _merge==2

# * Gen vars *;
# gen in_mysql=0
# replace in_mysql=1 if _merge==3
# drop _merge

# 	* Dates *;
# 	foreach var in awd_not_date bgt_end bgt_strt proj_end_date proj_strt_date {
# 	gen x`var'=substr(`var',1,10)
# 	gen `var'_date=date(x`var',"YMD")
# 	format `var'_date %td
# 	drop x`var'
# 	order `var'_date,after(`var')
# 	label var `var'_date "Stata date format"
# 	}
	
# 	destring fisc_yr, replace
# 	gen award_year=year(awd_not_date_date)
# 		order award_year, after(awd_not_date_date)
# 		replace award_year=fisc_yr if award_year==. /*n=44 changes made*/

# drop in_rep_table_sernum heal_funded res_net in_resnet
		


# 1. Load dataframes
df_heal = heal_by_sn_01
df_mysql = mysql_applkey.copy()

# 2. Merge 1:1 on 'appl_id'
# indicator=True mimics Stata's _merge variable ('left_only', 'right_only', 'both')
df_merged = pd.merge(df_heal, df_mysql, on="appl_id", how="outer", indicator=True)

# drop if _merge==2 (right_only)
df_merged = df_merged[df_merged["_merge"] != "right_only"]

# gen in_mysql=0 -> replace in_mysql=1 if _merge==3 (both)
df_merged["in_mysql"] = np.where(df_merged["_merge"] == "both", 1, 0)

# drop _merge
df_merged = df_merged.drop(columns=["_merge"])

# 3. Process Dates
# Extract first 10 chars, convert to standard datetime objects
date_cols = ["awd_not_date", "bgt_end", "bgt_strt", "proj_end_date", "proj_strt_date"]

for col in date_cols:
    if col in df_merged.columns:
        # Replicates: gen x = substr(var, 1, 10) -> date(x, "YMD")
        clean_date_str = df_merged[col].astype(str).str.slice(0, 10)
        df_merged[f"{col}_date"] = pd.to_datetime(
            clean_date_str, format="%Y-%m-%d", errors="coerce"
        )

# 4. Destring and generate 'award_year'
# destring fisc_yr, replace
df_merged["fisc_yr"] = pd.to_numeric(df_merged["fisc_yr"], errors="coerce")

# gen award_year=year(awd_not_date_date)
df_merged["award_year"] = df_merged["awd_not_date_date"].dt.year

# replace award_year=fisc_yr if award_year==.
df_merged["award_year"] = df_merged["award_year"].fillna(df_merged["fisc_yr"])

# 5. Drop unnecessary columns
drop_cols = ["in_rep_table_sernum", "heal_funded", "res_net", "in_resnet"]
# Only drop columns if they exist in the dataframe to avoid errors
existing_drop_cols = [c for c in drop_cols if c in df_merged.columns]
df_merged = df_merged.drop(columns=existing_drop_cols)

df_merged.to_csv(os.path.join(out, f'final_heal_awards_by_serial_number_{today}_merged.csv'), index=False, quoting=1)







		
# /* ----- 5. Isolate awards we should track that aren't yet in MySQL ----- */
		
# * Exclude awards issued before the HEAL Initiative started giving awards *;
# drop if award_year<2018 /*n=900 deleted*/

# * Apply study ID creation rules *;
# sort $stewards_id_vars
# egen xstudy_id=group($stewards_id_vars), missing
# order xstudy_id appl_id in_mysql award_year $stewards_id_vars

# * Drop if awards associated with the study ID are *always* or *never* in MySQL *;
# bysort xstudy_id: egen ever_in_mysql=max(in_mysql)
# bysort xstudy_id: egen all_in_mysql=min(in_mysql)
# drop if ever_in_mysql==0 /*No appl"s associated with the study were in MySQL. n=1059 deleted*/
# drop if all_in_mysql==1 /*Every possible appl already in MySQL. n=1654 deleted*/
# drop ever_in_mysql all_in_mysql

# * Exclude all awards for the study predating an award already in MySQL *;
# sort xstudy_id in_mysql award_year
# by xstudy_id in_mysql: egen xearliest_award_inmysql=min(award_year)
# replace xearliest_award_inmysql=. if in_mysql==0
# by xstudy_id: egen earliest_award_inmysql=min(xearliest_award_inmysql)
# drop if in_mysql==0 & award_year<earliest_award_inmysql /*n=198*/
# drop xearliest_award_inmysql earliest_award_inmysql

# 	tab in_mysql /* 779 appls in MySQL ;  489 not in MySQL */

# rename xstudy_id study
# sort study award_year
# save "$temp/newer_appls_related_to_studies.dta", replace
# export excel using "$temp/newer_appls_related_to_studies.xlsx", firstrow(variables) replace /*n=1268*/ 


# Define paths and columns (replaces Stata globals)
# Replace these list elements with your actual column names from $stewards_id_vars
stewards_id_vars = ["cr_pro_num", "act_code"]  # Example variables
# newer_appls_dta_path = "$temp/newer_appls_related_to_studies.dta"
# newer_appls_xlsx_path = "$temp/newer_appls_related_to_studies.xlsx"

# Assuming 'df_merged' is the DataFrame from the previous step
df_filtered = df_merged.copy()

# 1. Exclude awards issued before the HEAL Initiative started
df_filtered = df_filtered[df_filtered["award_year"] >= 2018]

# 2. Apply study ID creation rules
# Replicates: egen xstudy_id=group($stewards_id_vars), missing
# ngroup() assigns a unique 1-based integer ID to each unique combination of variables
df_filtered["study"] = (
    df_filtered.groupby(stewards_id_vars, dropna=False).ngroup() + 1
)

# 3. Drop if awards associated with the study are *always* or *never* in MySQL
# Calculate min and max of 'in_mysql' grouped by the newly created 'study' ID
df_filtered["ever_in_mysql"] = df_filtered.groupby("study")[
    "in_mysql"
].transform("max")
df_filtered["all_in_mysql"] = df_filtered.groupby("study")[
    "in_mysql"
].transform("min")

# Drop if never in mysql (ever_in_mysql == 0) or always in mysql (all_in_mysql == 1)
df_filtered = df_filtered[
    (df_filtered["ever_in_mysql"] != 0) & (df_filtered["all_in_mysql"] != 1)
]

# Drop intermediate tracking flags
df_filtered = df_filtered.drop(columns=["ever_in_mysql", "all_in_mysql"])

# 4. Exclude all awards for the study predating an award already in MySQL
# Replicates: replace xearliest_award_inmysql=. if in_mysql==0
df_filtered["xearliest"] = df_filtered["award_year"].where(
    df_filtered["in_mysql"] == 1
)

# Replicates: by xstudy_id: egen earliest_award_inmysql=min(xearliest_award_inmysql)
df_filtered["earliest_award_inmysql"] = df_filtered.groupby("study")[
    "xearliest"
].transform("min")

# Replicates: drop if in_mysql==0 & award_year<earliest_award_inmysql
drop_condition = (df_filtered["in_mysql"] == 0) & (
    df_filtered["award_year"] < df_filtered["earliest_award_inmysql"]
)
df_filtered = df_filtered[~drop_condition]

# Clean up helper columns used for the logic window
df_filtered = df_filtered.drop(columns=["xearliest", "earliest_award_inmysql"])

# 5. Final sorting and column restructuring
# Order columns to match the 'order study appl_id...' command
ordered_cols = ["study", "appl_id", "in_mysql", "award_year"] + [
    c for c in df_filtered.columns if c not in ["study", "appl_id", "in_mysql", "award_year"]
]
df_filtered = df_filtered[ordered_cols]

# Replicates: sort study award_year
df_filtered = df_filtered.sort_values(by=["study", "award_year"])

# Print value counts to replicate the 'tab in_mysql' check
print(df_filtered["in_mysql"].value_counts())

# 6. Export results
# df_filtered.to_stata(newer_appls_dta_path, write_index=False, version=118)
# df_filtered.to_excel(newer_appls_xlsx_path, index=False)
df_filtered.to_csv(os.path.join(out, f'newer_appls_related_to_studies.csv'), index=False, quoting=1)







# /* ----- 6. Format and output table for MySQL ----- */
# use "$temp/newer_appls_related_to_studies.dta", clear

# * Drop records already in mysql *;
# drop if in_mysql==1 /*n=489 left */

# drop study award_year in_mysql

# order appl_id, after(fund_ic_tot_cst)
# order proj_ser_num, after(proj_nm_spl_yr)
# order proj_num_spl_sfx_code, after(proj_ser_nm_spl)
# order subproj_id, after(spd_cat_0)

# save "$der/reporter_dqaudit.dta", replace


# drop awd_not_date_date bgt_end_date bgt_strt_date proj_end_date_date proj_strt_date_date
# export delimited using "$der/reporter_dqaudit.csv", nolab /*quote */replace /*n=489*/
# export excel using "$der/reporter_dqaudit.xlsx", firstrow(var) nolabel replace



# Define paths (replaces Stata globals)

# 1. Load the dataset from the previous filtering step
df = df_filtered.copy()

# 2. Filter records (drop if in_mysql == 1)
df = df[df["in_mysql"] != 1]

# 3. Drop unneeded tracking columns
df = df.drop(columns=["study", "award_year", "in_mysql"])


# 4. Reorder columns exactly matching Stata's 'order ..., after(...)' commands
def reorder_after(columns_list, target_col, move_col):
    """Moves move_col immediately after target_col in columns_list."""
    if move_col in columns_list and target_col in columns_list:
        columns_list.remove(move_col)
        target_idx = columns_list.index(target_col)
        columns_list.insert(target_idx + 1, move_col)
    return columns_list


# Extract current columns as a list
cols = list(df.columns)

# order appl_id, after(fund_ic_tot_cst)
cols = reorder_after(cols, "fund_ic_tot_cst", "appl_id")

# order proj_ser_num, after(proj_nm_spl_yr)
cols = reorder_after(cols, "proj_nm_spl_yr", "proj_ser_num")

# order proj_num_spl_sfx_code, after(proj_ser_nm_spl)
cols = reorder_after(cols, "proj_ser_nm_spl", "proj_num_spl_sfx_code")

# order subproj_id, after(spd_cat_0)
cols = reorder_after(cols, "spd_cat_0", "subproj_id")

# Apply the structural reordering back to the DataFrame
df = df[cols]

# 5. Save the primary Stata format backup file
# output_dta_path = "$der/reporter_dqaudit.dta"
# output_xlsx_path = "$der/reporter_dqaudit.xlsx"

# df.to_stata(output_dta_path, write_index=False, version=118)

# 6. Drop specific date format columns for the flat text exports
date_drop_cols = [
    "awd_not_date_date",
    "bgt_end_date",
    "bgt_strt_date",
    "proj_end_date_date",
    "proj_strt_date_date",
]
existing_date_drops = [c for c in date_drop_cols if c in df.columns]
df_flat_exports = df.drop(columns=existing_date_drops)

# 7. Final exports (CSV and Excel)
# nolab/nolabel is handled naturally by pandas as it writes raw data values
df_flat_exports.to_csv(out / "reporter_dqaudit.csv", index=False, quoting=1)  # 1 = csv.QUOTE_ALL
df_flat_exports.to_excel(out / "reporter_dqaudit.xlsx", index=False)

print(f"Pipeline successfully written. Exported row count: {len(df_flat_exports)}")




# /* ----- 7. Debug: split up large file into 2 for MySQL import ----- */
# use "$der/reporter_dqaudit.dta", clear
# drop awd_not_date_date bgt_end_date bgt_strt_date proj_end_date_date proj_strt_date_date
# gen row=_n
# save "$temp/splits.dta", replace

# keep if row<=240
# drop row
# save "$der/reporter_dqaudit_pt1.dta", replace
# export delimited using "$der/reporter_dqaudit_pt1.csv", nolab quote replace

# use "$temp/splits.dta", clear
# keep if row>240
# drop row
# save "$der/reporter_dqaudit_pt2.dta", replace
# export delimited using "$der/reporter_dqaudit_pt2.csv", nolab quote replace


# * Correct date formats to match reporter table *;
# import delim using "$raw/reporter_dqaudit_2026-02-16.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear /*n=489*/
# foreach var in proj_strt_date proj_end_date {
# 	gen x`var'=`var'+"T00:00:00"
# 	order x`var', after(`var')
# 	drop `var'
# 	rename x`var' `var'
# 	}
# save "$der/reporter_dqaudit.dta", replace


# Define path constants (replaces Stata globals)
# source_dta_path = "$der/reporter_dqaudit.dta"
# splits_dta_path = "$temp/splits.dta"
# raw_csv_path = "$raw/reporter_dqaudit_2026-02-16.csv"

# output_dta_pt1 = "$der/reporter_dqaudit_pt1.dta"
# output_csv_pt1 = "$der/reporter_dqaudit_pt1.csv"
# output_dta_pt2 = "$der/reporter_dqaudit_pt2.dta"
# output_csv_pt2 = "$der/reporter_dqaudit_pt2.csv"

# ==========================================
# 1. Load, Drop Stata Date Formats, & Split
# ==========================================
df_07 = df_flat_exports.copy()
# Drop the display-formatted Stata date columns
date_drop_cols = [
    "awd_not_date_date",
    "bgt_end_date",
    "bgt_strt_date",
    "proj_end_date_date",
    "proj_strt_date_date",
]
existing_drops = [c for c in date_drop_cols if c in df_07.columns]
df_07 = df_07.drop(columns=existing_drops)

# Save intermediate backup file
df_07.to_csv(out / "splits.csv", index=False, quoting=1)  # 1 = csv.QUOTE_ALL

# Replicates: keep if row <= 240
# Python uses 0-indexed slicing (rows 0 up to 240 targets the first 240 items)
# df_pt1 = df_split.iloc[:240]
# df_pt1.to_stata(output_dta_pt1, write_index=False, version=118)
# df_pt1.to_csv(output_csv_pt1, index=False, quoting=1)  # quoting=1 wraps entries in quotes

# Replicates: keep if row > 240
# df_pt2 = df_split.iloc[240:]
# df_pt2.to_stata(output_dta_pt2, write_index=False, version=118)
# df_pt2.to_csv(output_csv_pt2, index=False, quoting=1)


# ==========================================
# 2. Format Dates to Match MySQL Requirements
# ==========================================
# Load the raw input file using explicit string dtypes
# raw_csv_path = "$raw/reporter_dqaudit_2026-02-16.csv"
df_final = old_dqaudit.copy()

# Replicates loop: gen x`var'=`var'+"T00:00:00" -> drop `var' -> rename x`var' `var'
target_date_cols = ["proj_strt_date", "proj_end_date"]

for col in target_date_cols:
    if col in df_final.columns:
        # Replicates string concatenation in Stata
        df_final[col] = df_final[col].astype(str) + "T00:00:00"

# Save the final table matching the production format requirements
df_final.to_csv(out / "reporter_dqaudit.csv", index=False, quoting=1)  # 1 = csv.QUOTE_ALL

print(
    f"Processing complete. No Splits ({len(df_07)})."
)














































# END HEAL_02_ImportMerge 
log_out(f"END HEAL_03_DQAudit")


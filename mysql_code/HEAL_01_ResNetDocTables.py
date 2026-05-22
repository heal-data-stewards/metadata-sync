# /* -------------------------------------------------------------------------------- */
# /* Project: HEAL 																	*/
# /* PI: Kira Bradford																*/
# /* Program: HEAL_01_ResNetDocTables													*/
# /* Programmer: Sabrina McCutchan (CDMS)												*/
# /* Date Created: 2024/05/13															*/
# /* Date Last Updated: 2026/01/23													*/
# /* Description:	This program prepares the key of research networks by appl_id from	*/
# /*  Google Drive for read into MySQL. A MySQL script creates the research_networks  */
# /*	table in MySQL. This program also imports the research_networks table from MySQL*/
# /*  for use in later code.															*/
# /*		1. Import documentation tables												*/
# /*																					*/
# /* Notes:  																			*/
# /*	This program simply formats the tabs in the Excel downloaded from Google Drive  */
# /*  into separate .csv files so they can be read into MySQL.						*/
# /*		- 2026/01/13 made this the first program in the tree						*/
# /*		- 2025/01/24 this program contains a subset of code from the retired 		*/
# /*	 	  HEAL_02_ResNetTable.do that creates .csv versions of the documentation	*/
# /*		  tables to read into MySQL.												*/
# /*		- 2024/09/24 this procedure migrated to a MySQL Script 						*/
# /*		- 2024/05/21 first generation of research_networks table 					*/
# /*																					*/
# /* -------------------------------------------------------------------------------- */



# /* ----- 1. Import documentation tables ----- */
# foreach tab in ref_table value_overrides_byappl {
# 	import excel using "$doc/HEAL_research_networks_ref_table_for_MySQL.xlsx", sheet("`tab'") firstrow /*case(upper)*/ allstring clear
# 	foreach x of varlist * {
# 		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
# 		replace `x'=strtrim(`x')
# 		replace `x'=stritrim(`x')
# 		replace `x'=ustrtrim(`x')
# 		}
# 	missings dropvars * , force /* drop columns with no data */
# 	missings dropobs * , force /* drop rows with no data */
# 	save "$temp/`tab'.dta", replace
# 	export delimited using "$doc/res_net_`tab'.csv", quote replace
# 	}
	
# /* Manual step: Run the SQL script file "create_res_net_doc_tables". Then run the SQL script file "update_research_networks." Then export research_networks table from MySQL as a .csv so it can be read in in program 02. */



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
log_path = os.path.join(log, f"HEAL_01_ResNetDoTables_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/StudyMetrics_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{log}/HEAL_01_ResNetDoTables_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_01_ResNetDoTables Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/



# /* Program: HEAL_01_ResNetDocTables													*/

# The tabs to process
dfs = {}
tabs = ['ref_table', 'value_overrides_byappl']

for tab in tabs:
    # 1. Import Excel sheet (allstring equivalent by setting dtype=str)
    df = pd.read_excel(inp / "HEAL_research_networks_ref_table_for_MySQL.xlsx", sheet_name=tab, dtype=str)
    
    # 2. Clean the data
    # Equivalent to: subinstr (linebreaks), strtrim, stritrim, and ustrtrim
    def clean_cell(val):
        if pd.isna(val):
            return val
        # Replace line breaks (char 10) with space
        val = val.replace('\n', ' ').replace('\r', ' ')
        # Strip leading/trailing whitespace and collapse internal double spaces
        return " ".join(val.split())

    df = df.applymap(clean_cell) if hasattr(df, 'applymap') else df.apply(lambda s: s.map(clean_cell))

    # 3. Drop empty columns and rows (missings dropvars/dropobs)
    df = df.dropna(axis=1, how='all') # Drop empty columns
    df = df.dropna(axis=0, how='all') # Drop empty rows

    # 4. Save outputs
    df.to_csv(os.path.join(out, f'temp_res_net_{tab}.csv'), index=False, quoting=1)
    log_out(f"Exported res_net_{tab} table: {str(df.shape)}")

    print(f"Processed: {tab}")
  # 5. Store in dictionary using the specific naming convention
    # This maps 'ref_table' -> 'res_net_ref_table'
    dfs[f"res_net_{tab}"] = df

# Assign the specific DataFrame to the variable name you requested
res_net_ref_table = dfs['res_net_ref_table']
res_net_override = dfs['res_net_value_overrides_byappl']

print(f"Loaded {len(res_net_ref_table)} rows into res_net_ref_table")



# Appending update_research_networks.sql

# 1. Import Awards MySQL Table ----- */
datasets = [
    "awards"
]

dfs = {}  # dictionary to hold your dataframes

for name in datasets:
    csv_file = inp / f"{name}_20260519.csv"

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
df_awards_00 = dfs["df_awards"]
print("df_awards_00: " + str(df_awards_00.shape))
log_out(f"Import MySQL Awards table: {str(df_awards_00.shape)}")



# Load your data sources (assuming CSVs or DB connections)
# research_networks = pd.read_sql('SELECT * FROM research_networks', conn)
# awards = pd.read_sql('SELECT appl_id, res_prg FROM awards', conn)
# res_net_ref_table = pd.read_sql('SELECT * FROM res_net_ref_table', conn)
# res_net_override = pd.read_sql('SELECT * FROM res_net_override', conn)

# 1. Clear old data and initialize (Simulating delete and alter)
# We start fresh with the awards data directly
research_networks_00 = df_awards_00[['appl_id', 'res_prg']].copy()

print("research_networks_00: " + str(research_networks_00.shape))
log_out(f"research_networks table_00: {str(research_networks_00.shape)}")
research_networks_00.to_csv(os.path.join(inp, f'research_networks_00.csv'), index=False, quoting=1)


# 2. Update res_net based on reference table (Left Join)
research_networks_01=research_networks_00.copy()
research_networks_01 = research_networks_01.merge(
    res_net_ref_table[['res_prg', 'res_net']], 
    on='res_prg', 
    how='left'
)
research_networks_01.to_csv(os.path.join(inp, f'research_networks_01.csv'), index=False, quoting=1)
print("research_networks_01: " + str(research_networks_01.shape))
log_out(f"research_networks table_01: {str(research_networks_01.shape)}")

# 3. Drop res_prg column
research_networks_01.drop(columns=['res_prg'], inplace=True)



# 4. Update the override values
# This step is not merging some records that actually do existin in res_net_01 and res_net_override dfs
# We merge the override data and update the res_net column where an override exists
research_networks_02 = research_networks_01.merge(
    res_net_override[['appl_id', 'res_net']], 
    on='appl_id', 
    how='left', 
    suffixes=('', '_override')
)

print("research_networks_02: " + str(research_networks_02.shape))
log_out(f"research_networks table_02: {str(research_networks_02.shape)}")
research_networks_02.to_csv(os.path.join(inp, f'research_networks_02.csv'), index=False, quoting=1)

research_networks_03=research_networks_02.copy()
research_networks_03['res_net'] = research_networks_03['res_net_override'].combine_first(research_networks_03['res_net'])

print("research_networks_03: " + str(research_networks_03.shape))
log_out(f"research_networks table_03: {str(research_networks_03.shape)}")
research_networks_03.to_csv(os.path.join(inp, f'research_networks_03.csv'), index=False, quoting=1)


# 5. Update the override flag (1 if ID exists in override table, else 0)

research_networks_04=research_networks_03.copy()
research_networks_04['res_net_override_flag'] = research_networks_04.appl_id.isin(res_net_override.appl_id).astype(int)


# Cleanup helper column
research_networks_04.drop(columns=['res_net_override'], inplace=True)

print("research_networks_04: " + str(research_networks_04.shape))
log_out(f"research_networks table_04: {str(research_networks_04.shape)}")
research_networks_04.to_csv(os.path.join(inp, f'research_networks_04.csv'), index=False, quoting=1)


research_networks_05=research_networks_04.copy()
research_networks_05.drop_duplicates(inplace=True)

print(research_networks_05.head())
print("research_networks_05: " + str(research_networks_05.shape))
log_out(f"research_networks table_05: {str(research_networks_05.shape)}")

# 6. Save outputs
research_networks_05.to_csv(os.path.join(inp, f'research_networks_{today}.csv'), index=False, quoting=1)

print(f"Processed: exported research_networks_today.csv to Input")
log_out(f"Exported research_networks table to Input\research_networks_{today}.csv")




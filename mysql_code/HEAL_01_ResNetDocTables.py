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
log_path = os.path.join(out, f"HEAL_01_ResNetDoTables_{today}_log.txt")
with open(log_path, 'w') as f:
    pass  # 'w' mode truncates existing file or creates new blank file
# open(f"{out}/StudyMetrics_{today}_log.txt", 'w').close() #Clears Log before running

def log_out(message):
    with open(f"{out}/HEAL_01_ResNetDoTables_{today}_log.txt", 'a') as f:
        print(message, file=f)

log_out(f"HEAL_01_ResNetDoTables Log Run Date: {today}")

# ----- END Boiler Plate Code -----------------------------------------------------*/





# /* Program: HEAL_01_ResNetDocTables													*/

# The tabs to process
tabs = ['ref_table', 'value_overrides_byappl']

for tab in tabs:
    # 1. Import Excel sheet (allstring equivalent by setting dtype=str)
    df = pd.read_excel(doc / "HEAL_research_networks_ref_table_for_MySQL.xlsx", sheet_name=tab, dtype=str)
    
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
    df.to_csv(os.path.join(doc, f'res_net_{tab}.csv'), index=False, quoting=1)

    print(f"Processed: {tab}")






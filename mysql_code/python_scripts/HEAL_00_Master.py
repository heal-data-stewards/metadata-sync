#  -------------------------------------------------------------------------------- 
#  Project: HEAL 																	    
#  PI: Kira Bradford															    	
#  Program: HEAL_00_Master												   	        
#  Programmer: Brian Erman (RTI)												        
#  Date Created: 2026/06/15															
#  Date Last Updated: 												            	
#  Description:	This Master program establishes                                     
#                       Python Modules                                                                      
#                       Today macro variable                                            
#                       Local Filepath where Script will run                                  
#  Notes:  	Requires these MySQL tables tables exported from MySQL (DBeaver) with
#           "_yyyymmdd" filename suffix to be saved in "Input" subfolder
#           awards
#           pi_emails
#           progress_tracker
#           reporter
# 																					
#  -------------------------------------------------------------------------------- 


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
import logging

import argparse
from typing import Optional, Tuple


# ==============================================================================
# 0. SET GLOBAL MACROS
# ==============================================================================
today = "20260827"
heal_dir = Path(
    # "SET PATH HERE"
)

# Inject into environment variables for child processes (convert Path to string)
os.environ["today"] = today
os.environ["dir"] = str(heal_dir)

# ==============================================================================
# 1. Input/Output by Program
# ==============================================================================

#HEAL_01_ResNetDocTables
    #Needs
    #   Input\HEAL_research_networks_ref_table_for_MySQL.xlsx Copied from Google Drive
    #Creates
    #   Output\research_networks_{today}.csv

#HEAL_02_ImportMerge
    #Needs
    #   [HEAL_01] Output\research_networks_{today}.csv
    #   Input\awards_{today}.csv
    #   Input\pi_emails_{today}.csv
    #   Input\progress_tracker_{today}.csv
    #   Input\reporter_{today}.csv
    #   Input\correct_foanoa_values.csv
    #Creates
    #   mysql_{today}.csv
    #   nihtables_{today}.csv

#HEAL_03_DQ_Audit
    #Needs
    #   [HEAL_02] Output\research_networks_{today}.csv
    #   [HEAL_02] Output\nihtables_{today}.csv
    #   Input\heal_awards_reporter_sn_04242026_out.csv
    #   Input\reporter_dqaudit_04242026.csv
    #Creates
    #   Output\reporter_dqaudit.csv


#HEAL_09_StudyMetrics
#   #Needs
    #   [HEAL_02] Output\mysql_{today}.csv
    #   [HEAL_04] Output\study_lookup_table.csv
    #   Input\.csv
    #Creates
    #   Output\StudyMetrics_{today}.xlsx


# ==============================================================================
# 2. SETUP LOGGING
# ==============================================================================
# Ensure the HEAL folder exists before writing logs to it
heal_dir.mkdir(parents=True, exist_ok=True)

# Create a log file name based on today's macro
log_file = heal_dir / f"HEAL_pipeline_run_{today}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),  # Also prints to your terminal screen
    ],
)

logging.info("==================================================")
logging.info(f"STARTING HEAL MASTER PIPELINE FOR RUN: {today}")
logging.info(f"Target Directory: {heal_dir}")
logging.info("==================================================")

# ==============================================================================
# 3. RUN SEQUENTIAL SCRIPTS WITH ERROR HANDLING
# ==============================================================================
# List your scripts in the exact order they need to execute
# HEAL_04 requires CLI arguments; build its invocation separately.
# Set manual_matches_path to your study_manual_matches.xlsx file in Input/.
manual_matches_path = str(heal_dir / "Input" / "study_manual_matches.xlsx")
heal04_cmd = [
    sys.executable, "HEAL_04_StudyTable.py",
    "--mysql-data",        str(heal_dir / "Output" / f"mysql_{today}.csv"),
    "--reporter-dqaudit",  str(heal_dir / "Output" / "reporter_dqaudit.csv"),
    "--manual-matches",    manual_matches_path,
    "--output-dir",        str(heal_dir / "Output"),
    "--debug-dir",         str(heal_dir / "Output" / "debug"),
]

# Simple scripts (use env vars for today/dir; no extra CLI args needed)
scripts_to_run = [
    "HEAL_01_ResNetDocTables.py"
    , "HEAL_02_ImportMerge.py"
    , "HEAL_03_DQAudit.py"
    , "HEAL_05_EngagementTable.py"
    , "HEAL_06_CompilebyStudy.py"
    , "HEAL_07_QC.py"
    , "HEAL_08_GTDTargets.py"
    , "HEAL_09_StudyMetrics.py"
    ]

def run_step(cmd, label):
    logging.info(f"Executing step: {label}...")
    try:
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(f"SUCCESS: {label} completed cleanly.")
    except subprocess.CalledProcessError as e:
        logging.error(f"CRITICAL CRASH: {label} failed with exit code {e.returncode}!")
        logging.error("----- PYTHON ERROR LOG BELOW -----")
        logging.error(e.stderr if e.stderr else "No error text captured (check terminal).")
        logging.error("----------------------------------")
        logging.error("Pipeline execution halted to prevent data corruption.")
        sys.exit(1)
    except FileNotFoundError:
        logging.error(f"CRITICAL CRASH: Could not find '{label}'!")
        sys.exit(1)

# Steps 01-03: produce mysql_{today}.csv and reporter_dqaudit.csv
for script in scripts_to_run[:3]:
    run_step([sys.executable, script], script)

# Step 04: StudyTable (needs explicit CLI args)
run_step(heal04_cmd, "HEAL_04_StudyTable.py")

# Steps 05-09: downstream consumers
for script in scripts_to_run[3:]:
    run_step([sys.executable, script], script)

logging.info("==================================================")
logging.info("SUCCESS: All pipeline steps completed successfully!")
logging.info("==================================================")







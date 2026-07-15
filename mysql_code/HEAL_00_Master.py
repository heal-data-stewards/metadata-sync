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
today = "20260615"
heal_dir = Path(
    r"C:\Users\berman\OneDrive - Research Triangle Institute\Python Environment\HEAL"
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
scripts_to_run = [
    "HEAL_01_ResNetDocTables.py"
    , "HEAL_02_ImportMerge.py"
    , "HEAL_03_DQAudit.py"
    , "HEAL_09_StudyMetrics.py"
    ]

for script in scripts_to_run:
    logging.info(f"Executing step: {script}...")

    try:
        # Run the script using the current Python environment
        result = subprocess.run(
            [sys.executable, script],
            check=True,  # Raises CalledProcessError if the script crashes
            capture_output=True,  # Captures internal print statements and errors
            text=True,  # Returns output as clean text instead of raw bytes
        )

        logging.info(f"SUCCESS: {script} completed cleanly.")

    except subprocess.CalledProcessError as e:
        logging.error(f"CRITICAL CRASH: {script} failed with exit code {e.returncode}!")
        logging.error("----- PYTHON ERROR LOG BELOW -----")
        logging.error(
            e.stderr if e.stderr else "No error text captured (check terminal)."
        )
        logging.error("----------------------------------")
        logging.error("Pipeline execution halted to prevent data corruption.")
        sys.exit(1)  # Stop the master script entirely
    except FileNotFoundError:
        logging.error(
            f"CRITICAL CRASH: Could not find the file '{script}' in this folder!"
        )
        sys.exit(1)

logging.info("==================================================")
logging.info("SUCCESS: All pipeline steps completed successfully!")
logging.info("==================================================")







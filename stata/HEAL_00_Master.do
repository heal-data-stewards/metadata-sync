/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford, Becky Boyles													*/
/* Program: HEAL_00_Master															*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/02/29															*/
/* Date Last Updated: 2026/01/16													*/
/* Description:	This is the master Stata program for MySQL data processing. It sets	*/
/* global macros before calling the following programs:								*/
/*		1. Generate Research Networks Documentation Tables							*/
/*		2. Import & merge data														*/
/*		3. Generate Study Table														*/
/*		4. Generate CTN crosswalk and outputs										*/
/*		5. Generate engagement_flags table											*/
/*		6. Generate Get the Data target lists										*/
/*		97. Perform data quality audit on Reporter									*/
/*		98. Generate study metrics report											*/
/*		99. Generate QC report														*/
/*																					*/
/*		X. [Archived] Generate Research Networks Table								*/
/*		X. Scratch																	*/
/*		X. Manage archiving of MySQL tables											*/
/*																					*/
/* Notes:  																			*/
/*	2024/07/18 - The programs that generate the Research Networks table and Study	*/
/*		Table were swapped in order due to a dependency in the latter.				*/
/*	2024/05/28 - This program originally native to the HEAL_Study program tree.	It	*/
/*		has	now been split out because it is a necessary first step to all 			*/
/*		processing. This program should be run before any other HEAL programs.		*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear all 


/* ----- SET MACROS -----*/

/* ----- 1. Dates ----- */
* Today's date *;
local xt: display %td_CCYY_NN_DD date(c(current_date), "DMY")
local today = subinstr(trim("`xt'"), " " , "-", .)
global today "`today'"
/*global today "2026-02-12"*/

/* ----- 2. Filepaths ----- */

global dir "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\MySQL"
global raw $dir\Extracts
global cal $dir\Raw\Calendly
global der $dir\Derived
global prog $dir\Programs
global doc $dir\Documentation
global temp $dir\temp
global qc $dir\Output\QC
global out $dir\Output
global backups $dir\Backups
global nih $dir\NIH_Data


/* ----- 3. Variables ----- */
* Variables used to identify studies *;
global stewards_id_vars proj_ser_num subproj_id proj_num_spl_sfx_code
global key_vars study_id xstudy_id_stewards appl_id hdp_id archived num_appl_by_xstudyidstewards num_hdp_by_appl num_hdp_by_xstudyidstewards



/* ----- PROGRAMS -----*/

* Define value labels *;
do "$prog/HEAL_valuelabels"



/* ----- DATA MANAGEMENT PROGRAMS -----*/

/* ----- 1. Generate Research Network Documentation Tables ----- */
do "$prog/HEAL_01_ResNetDocTables.do"

/* ----- 2. Import latest MySQL data ----- */
do "$prog/HEAL_02_ImportMerge.do"

/* ----- 3. Perform data quality audit on Reporter ----- */
do "$prog/HEAL_03_DQAudit.do"

/* ----- 4. Generate Study Table ----- */
do "$prog/HEAL_04_StudyTable.do"

/* ----- 5. Generate Engagement table ----- */
do "$prog/HEAL_05_EngagementTable.do"

/* ----- 6. Compile data tables by study ----- */
do "$prog/HEAL_06_CompilebyStudy.do"



/* ----- REPORTING PROGRAMS -----*/

/* ----- 7. Generate QC report ----- */
do "$prog/HEAL_07_QC.do"

/* ----- 8. Generate Get the Data target lists ----- */
do "$prog/HEAL_08_GTDTargets.do"

/* ----- 9. Generate study metrics report ----- */
do "$prog/HEAL_09_StudyMetrics.do"



/* ----- OTHER PROGRAMS -----*/

/* ----- 96. Generate CTN crosswalk and outputs ----- */
do "$prog/HEAL_96_CTN.do"









/* ----- ARCHIVED PROGRAMS -----*/
/* ----- Generate Research Networks Table ----- */
do "$prog/xHEAL_02_ResNetTable.do"



/*
/* ----- ONE-OFF PROGRAMS -----*/

/* ----- Scratch ----- */
do "HEAL_scratch.do"

/* ----- Manage archiving of MySQL tables ----- */
do "HEAL_TableArchiving.do"
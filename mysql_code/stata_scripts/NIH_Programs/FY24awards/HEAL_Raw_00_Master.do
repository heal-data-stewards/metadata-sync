/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_Raw_00_Master														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/11/23															*/
/* Date Last Updated: 2024/11/25													*/
/* Description:	This program is the master program for preparing raw administrative */
/*  data about FY24 HEAL awards for ingest into MySQL. It sets global macros before	*/
/* 	calling other programs. 														*/
/*		1. Import NIH FY24 data														*/
/*		2. Import Heal Funded Projects FY24 data									*/
/*		3. Merge NIH + HFP data														*/
/*		4. Output tables					 										*/
/*																					*/
/* Notes:  																			*/
/*	The data was emailed by Jessica Mazerick as attachments on several emails from 	*/
/*		11/7-11/8/24.																*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear all 


/* ----- 1. SET MACROS -----*/

/* ----- 1. Dates ----- */
* Today's date *;
local xt: display %td_CCYY_NN_DD date(c(current_date), "DMY")
local today = subinstr(trim("`xt'"), " " , "-", .)
/*global today "`today'"*/
global today "2024-11-23"

/* ----- 2. Filepaths ----- */
global dir "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\MySQL\Raw"
global nih $dir\NIH_FY24
global hfp $dir\HEALFundedProjects
global prog $dir\Raw_Programs
global temp $dir\temp
global der $dir\Derived
global extracts "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\MySQL\Extracts"


/* ----- 3. Variables ----- */
* Var lists *;
global order_core appl_id proj_num proj_title rfa res_prg adm_ic_code pi all_pi_emails prg_ofc
global order_more awd_not_date nofo_number nofo_title org_nm org_cy org_st 

global awards_tbl appl_id rfa res_prg /*data_src heal_funded goal*/
global vars_notin_reporter all_pi_emails



/* ----- PROGRAMS -----*/

/* ----- 1. Import NIH FY24 data ----- */
do "$prog/HEAL_Raw_01_NIHFY24.do"

/* ----- 2. Import Heal Funded Projects FY24 data ----- */
do "$prog/HEAL_Raw_02_HFP.do"

/* ----- 3. Merge NIH + HFP data ----- */
do "$prog/HEAL_Raw_03_Merge.do"

/* ----- 4. Output tables ----- */
do "$prog/HEAL_Raw_04_Output.do"


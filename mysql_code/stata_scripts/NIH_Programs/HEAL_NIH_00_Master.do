/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_NIH_00_Master														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/11/23															*/
/* Date Last Updated: 2025/11/10													*/
/* Description:	This program is the master program for preparing raw administrative */
/*  data from NIH for ingest into MySQL. It sets global macros before calling the	*/
/* 	other programs in the tree.														*/
/*		1. Import NIH FY24 data														*/
/*		2. Import NIH FY25 data														*/
/*																					*/
/* Notes:  																			*/
/*	The 2025 data was emailed by Rachel Kelley in 2 batches. One email came on		*/
/*		9/10, the other on 12/12.													*/
/*	The 2024 data was emailed by Jessica Mazerick as attachments on several emails	*/
/*		from 11/7-11/8/24.															*/
/*	The 2025 data was emailed by Rachel Kelley as attachment on one email sent		*/
/*		2025/08/11.																	*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear all 


/* ----- 1. SET MACROS -----*/

/* ----- 1. Dates ----- */
* Today's date *;
local xt: display %td_CCYY_NN_DD date(c(current_date), "DMY")
local today = subinstr(trim("`xt'"), " " , "-", .)
global today "`today'"
/*global today "2024-12-19"*/

/* ----- 2. Filepaths ----- */
global dir "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\MySQL\NIH_Data"
global nih24 $dir\NIH_FY24
global nih25 $dir\NIH_FY25
global hfp $dir\HEALFundedProjects
global prog $dir\NIH_Programs
global temp $dir\temp
global der $dir\Derived
global doc $dir\Docs
global extracts "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\MySQL\Extracts"


/* ----- 3. Variables ----- */
* Var lists *;
global order_core appl_id proj_num proj_title rfa res_prg adm_ic_code pi all_pi_emails prg_ofc
global order_more awd_not_date nofo_number nofo_title org_nm org_cy org_st 

global awards_tbl appl_id rfa res_prg /*data_src heal_funded goal*/
global vars_notin_reporter all_pi_emails



/* ----- PROGRAMS -----*/

/* ----- 1. Import NIH FY24 data ----- */
do "$prog/HEAL_NIH_01_FY24.do"

/* ----- 2. Import NIH FY25 data - August email ----- */
do "$prog/HEAL_NIH_02_FY25_Aug.do"

/* ----- 3. Import NIH FY25 data - December email ----- */
do "$prog/HEAL_NIH_03_FY25_Dec.do"


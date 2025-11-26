/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_05_EngagementTable													*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/12/03															*/
/* Date Last Updated: 2025/11/26													*/
/* Description:	This program creates the engagement_flags table, which contains 	*/
/*	 several indicators that are new for PM use as of the FY24 new awards batch.	*/
/*		1. Create flags																*/
/*		2. Generate Engagement Table 												*/
/*																					*/
/* Notes:  																			*/
/*		- This table was added during the FY24 awards cycle.						*/
/*																					*/
/* -------------------------------------------------------------------------------- */



/* ----- 1. Create flags ----- */

use "$temp/nihtables_$today.dta", clear
sort appl_id

* Do not engage *;
gen do_not_engage=0
replace do_not_engage=1 if act_code=="T90" | act_code=="R90"
replace do_not_engage=1 if nih_foa_heal_lang=="0"
replace do_not_engage=1 if nih_aian=="1"
label var do_not_engage "Do not engage"

* Checklist exempt *;
gen checklist_exempt_all=0
replace checklist_exempt_all=1 if do_not_engage==1
replace checklist_exempt_all=1 if fund_mech=="SBIR/STTR"
label var checklist_exempt_all "All HEAL checklist steps are optional"

* Output *;
keep appl_id do_not_engage checklist_exempt_all
save "$temp/pm_flags.dta", replace 



/* ----- 2. Generate Engagement Table ----- */

use "$der/study_lookup_table.dta", clear
sort appl_id
merge m:1 appl_id using "$temp/pm_flags.dta"
keep if _merge==3
drop _merge

* Apply flags to all appl_ids for the study *;
sort xstudy_id appl_id
foreach var in do_not_engage checklist_exempt_all {
	by xstudy_id: egen z`var'=max(`var')
	copydesc `var' z`var'
	drop `var' 
	rename z`var' `var'
	}

* Output *;
keep appl_id do_not_engage checklist_exempt_all
duplicates drop
sort appl_id
save "$der/engagement_flags.dta", replace
export delimited using "$der/engagement_flags.csv", nolab quote replace /*n=1959 ; note this n is smaller than reporter or awards tables because not every appl_id belongs to a study entity */
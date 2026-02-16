/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_05_EngagementTable													*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/12/03															*/
/* Date Last Updated: 2026/02/03													*/
/* Description:	This program creates the engagement_flags table, which contains 	*/
/*	 several indicators that are new for PM use as of the FY24 new awards batch.	*/
/*		1. Create flags																*/
/*		2. Generate Engagement Table 												*/
/*																					*/
/* Notes:  																			*/
/*	- 2026/02/03 reporter_dqaudit appl_ids are now included in the output table		*/
/*	- 2026/02/02 Carolyn Conlin in a 2026/01/07 email gave the instruction: 		*/
/*	  "Stewards should engage with the MedTech Optimizer studies and should not 	*/
/*	  engage with the MedTech Seedlings."											*/
/*	- 2025/12/22 NIH gave us information about NOAs instead of FOAs in 2025. The 	*/
/*	  logic for flagging do not engage was updated to include the new variable for	*/
/*	  nih_noa_heal_lang.															*/
/*	- Awards table fields changed in 2025 as NIH provided some data points in a form*/
/*	  not backwards compatible with prior data structures. Flag creation logic was	*/
/*	  updated to include relevant new awards table fields.							*/
/*	- This table was added during the FY24 awards cycle.							*/
/*																					*/
/* -------------------------------------------------------------------------------- */



/* ----- 1. Create flags ----- */

use "$temp/nihtables_$today.dta", clear
sort appl_id
merge 1:1 appl_id using "$raw/research_networks_$today.dta" 
replace res_net=upper(res_net)
drop if _merge==2
drop _merge res_net_override_flag


* Do not engage *;
gen do_not_engage=0
replace do_not_engage=1 if act_code=="T90" | act_code=="R90"
replace do_not_engage=1 if nih_foa_heal_lang=="0" | nih_noa_heal_lang=="0"
replace do_not_engage=1 if nih_aian=="1"
replace do_not_engage=1 if res_net=="MEDTECH" & strpos(lower(proj_title), "seedling")>0 
label var do_not_engage "Do not engage"

* Checklist exempt *;
gen checklist_exempt_all=0
replace checklist_exempt_all=1 if do_not_engage==1
replace checklist_exempt_all=1 if fund_mech=="SBIR/STTR"
replace checklist_exempt_all=1 if nih_noa_notes=="but encouraged to comply" /* Note: these rows are from Dec 2025 email, and NIH confirmed over email that awards designated this way are encouraged but not required to participate in the HDE */
label var checklist_exempt_all "All HEAL checklist steps are optional"

* Output *;
keep appl_id do_not_engage checklist_exempt_all
save "$temp/pm_flags.dta", replace 



/* ----- 2. Generate Engagement Table ----- */

use "$der/study_lookup_table.dta", clear
sort appl_id
merge m:1 appl_id using "$temp/pm_flags.dta"
drop if _merge==2
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
export delimited using "$der/engagement_flags.csv", nolab quote replace /*n=2627 note the final table includes reporter_dqaudit appl_ids*/
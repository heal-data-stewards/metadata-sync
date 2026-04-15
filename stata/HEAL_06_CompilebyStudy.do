/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* RTI PI: Kira Bradford															*/
/* Program: HEAL_06_CompilebyStudy													*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2026/02/13															*/
/* Date Last Updated: 2026/02/13													*/
/* Description:	This program merges together data from multiple MySQL DB tables with*/
/*	the study_lookup_table, producing a large compiled dataset that provides key	*/
/*	values by study. 																*/
/*		1. Merge source data 														*/
/*		2. Derive study-level values for data that exists at appl_id level  		*/
/*		3. Merge derived study-level values 										*/
/*																					*/
/* Notes:  																			*/
/*	-The output dataset is analogous, but not identical, to the data extract that is*/
/*		prepared by Hina Shah for upload to monday.com HEAL Studies Board.			*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear 



/* ----- 1. Merge source data ----- */

* -- Stack reporter_dqaudit rows with the merged reporter, awards, progress_tracker & research_networks dataset -- *;
use "$der/mysql_$today.dta", clear /*n=2521*/
append using "$der/reporter_dqaudit.dta", force /*n=3010*/
sort appl_id
order appl_id
drop if appl_id=="" /*Note: consider which records are dropped by this, and if we want them to be dropped*/
drop compound_key
sort appl_id hdp_id
egen compound_key=concat(appl_id hdp_id), punct(_)
save "$temp/xalldata_$today.dta", replace /*n=2957*/


* -- Merge data from other tables to study_lookup_table -- *;
use "$der/study_lookup_table.dta", clear /*n=2683*/
sort compound_key
merge m:1 compound_key using "$temp/xalldata_$today.dta"
/* tab entity_type if _merge==2 */
drop if _merge==2 /* n=305 dropped; these are all non-study entities (CTN protocols or Others)*/
drop _merge 

* Engagement flags *;
merge m:1 appl_id using "$der/engagement_flags.dta", keepusing(do_not_engage checklist_exempt_all)
drop if _merge==2
drop _merge

* PI Emails *;
merge m:1 appl_id using "$raw/pi_emails_$today.dta", keepusing(pi_email)
drop if _merge==2
drop _merge

sort xstudy_id
save "$temp/alldata_$today.dta", replace /*n=2683*/



/* ----- 2. Derive study-level values for data that exists at appl_id level in the raw ----- */

* -- PI emails -- *;
*Apply the latest non-missing pi_email for the study to rows where pi_email is missing *;
use "$temp/alldata_$today.dta", clear
keep if pi_email!=""
gen study_pi_email=""
	
replace study_pi_email=pi_email if appl_id==study_most_recent_appl
	
by xstudy_id: gen n=_n
by xstudy_id: egen num_rows=max(n)

replace study_pi_email=pi_email if study_pi_email=="" & num_rows==1
	
gen xstudy_has_email=0
replace xstudy_has_email=1 if study_pi_email!=""
sort xstudy_id xstudy_has_email
by xstudy_id: egen indic=max(xstudy_has_email)
sort indic xstudy_id proj_end_date_date

drop if indic==1 & xstudy_has_email==0

	/*note: temporary time-saving rule. manually checked and all rest are dupes*/
	replace study_pi_email=pi_email if study_pi_email=="" 
	
keep xstudy_id study_pi_email
duplicates drop
duplicates list xstudy_id
sort xstudy_id
save "$temp/pi_emails_key.dta", replace


* -- Research Network -- *;
* Apply the non-missing res_net for the study to rows where res_net is missing *;
use "$temp/alldata_$today.dta", clear
keep if res_net!=""
keep xstudy_id res_net
sort xstudy_id res_net
duplicates drop
by xstudy_id: gen n=_n
by xstudy_id: egen num_rows=max(n)

tab num_rows
	/* note: only tabbed # is 1, meaning there are no conflicting values of res_net within any xstudy_id */

rename res_net study_res_net
keep xstudy_id study_res_net
save "$temp/res_net_key.dta", replace


* -- Live/archived status of the study HDP ID -- *;
use "$raw/progress_tracker_$today.dta", clear
keep hdp_id archived
sort hdp_id
rename hdp_id study_hdp_id
rename archived study_hdp_status
save "$temp/livearchkey.dta", replace



/* ----- 3. Merge derived study-level values ----- */
use "$temp/alldata_$today.dta", clear
merge m:1 xstudy_id using "$temp/pi_emails_key.dta", keepusing(study_pi_email)
drop _merge

merge m:1 xstudy_id using "$temp/res_net_key.dta"
drop _merge

sort study_hdp_id
merge m:1 study_hdp_id using "$temp/livearchkey.dta"
drop if _merge==2
drop _merge

* -- Format for output -- *;
rename proj_title project_title_reporter
rename project_title project_title_platform

order study_hdp_status, after(study_hdp_id)
sort xstudy_id fisc_yr
save "$der/alldata_$today.dta", replace

export delimited using "$der/alldata_$today.csv", nolab quote replace
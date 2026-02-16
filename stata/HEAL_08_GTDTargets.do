/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* RTI PI: Kira Bradford															*/
/* Program: HEAL_08_GTDTargets														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2026/02/03															*/
/* Date Last Updated: 2026/02/12													*/
/* Description:	This program performs ad-hoc queries.								*/
/*																					*/
/* Notes:  																			*/
/*	-2026/02/02 pulled historic queries out of scratch file 						*/
/*																					*/
/* -------------------------------------------------------------------------------- */


/* --------------------------- */
/* ------- PARAMETERS -------- */
/* --------------------------- */
/*

Include the following columns:
	appl_id
	Project Number
	Contact PI Name (First and Last)
	Contact PI Email
	NIH Reporter Link
	Project Start Date
	Project End Date
	SBIR/STTR
	heal_funded
	HDP ID
	xstudy_id
	study_first_appl
	study_most_recent_appl
	study_hdp_id
	study_hdp_id_appl
	checklist exempt
	Research Network 
	Project titles: proj_title (reporter) & project_title (platform)
	
* - FILTERS - *;
For Get the Data target list:
- Keep if the study's latest known proj_end_date is in 2026 
- Keep the study_most_recent_appl to represent the study
- Exclude "do not engage"
- Drop studies with archived HDP IDs

For Early Award target list:
- Keep if the study's earliest known project start date is in 2025
   * Note: confirm this results in 1 appl_id per xstudy_id, and add filtering if it doesn't
- Exclude records flagged "do not engage"
- Exclude SBIR/STTR
- Drop studies with archived HDP IDs
- Drop targets that were on the Get the Data target list

Other information to provide the PMs:
- MySQL Data Dictionary link
- Exported full study_lookup_table (maybe)


*/






/* ----- Query: 2026/02/03 ----- */
/* Note: Updated 2026/02/12 to add project title columns from both reporter & platform data sources, and to apply res_net designation at study level not appl_id level */


clear all 



/* ----- 1. Prepare data ----- */

* Import pi _emails *;
foreach dtaset in pi_emails_$today {
import delimited using "$raw/`dtaset'.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		}
		
sort appl_id
save "$raw/`dtaset'.dta", replace
}


* Get all Reporter data rows stacked up *;
use "$der/mysql_$today.dta", clear /*n=2516*/
append using "$der/reporter_dqaudit.dta" /*n=3005*/
sort appl_id
order appl_id
drop if appl_id==""
drop compound_key
sort appl_id hdp_id
egen compound_key=concat(appl_id hdp_id), punct(_)
save "$temp/xalldata_$today.dta", replace /*n=2953*/


* Merge to study_lookup_table *;
use "$der/study_lookup_table.dta", clear /*n=2661*/
sort compound_key
merge m:1 compound_key using "$temp/xalldata_$today.dta"
drop if _merge==2 /* n=304 dropped; these are non-study entities (CTN protocols or Others)*/
drop _merge 

replace res_net=upper(res_net)

merge m:1 appl_id using "$der/engagement_flags.dta", keepusing(do_not_engage checklist_exempt_all)
drop if _merge==2
drop _merge

merge m:1 appl_id using "$raw/pi_emails_$today.dta", keepusing(pi_email)
drop if _merge==2
drop _merge

sort xstudy_id
save "$temp/gtd_targets_$today.dta", replace /*n=2660*/


* Apply the latest non-missing pi_email for the study to rows where pi_email is missing *;
use "$temp/gtd_targets_$today.dta", clear
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


* Apply the non-missing res_net for the study to rows where res_net is missing *;
use "$temp/gtd_targets_$today.dta", clear
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



* Find live/archived status of study_hdp_id *;
use "$raw/progress_tracker_$today.dta", clear
keep hdp_id archived
sort hdp_id
rename hdp_id study_hdp_id
rename archived study_hdp_status
save "$temp/livearchkey.dta", replace



* Merge in found maxes of key variables *;
use "$temp/gtd_targets_$today.dta", clear
merge m:1 xstudy_id using "$temp/pi_emails_key.dta", keepusing(study_pi_email)
drop _merge

merge m:1 xstudy_id using "$temp/res_net_key.dta"
drop _merge

sort study_hdp_id
merge m:1 study_hdp_id using "$temp/livearchkey.dta"
drop if _merge==2
drop _merge

rename proj_title project_title_reporter
rename project_title project_title_platform

order study_hdp_status, after(study_hdp_id)
sort xstudy_id fisc_yr
save "$out/GTD_Targets/gtd_targets_$today.dta", replace








/* ----- 2. Get the Data Target List ----- */
use "$out/GTD_Targets/gtd_targets_$today.dta", clear

	* Limit to studies where the latest known project end date is in 2026 *;
	sort xstudy_id proj_end_date_date
	by xstudy_id: egen latest_end_date=max(proj_end_date_date) /*n=9 missing values generated */
	gen latest_end_yr=year(latest_end_date)
	tab latest_end_yr
	keep if latest_end_yr==2026 /*n=773*/
	
	/*	* Check: # unique study IDs *;
		keep xstudy_id
		sort xstudy_id
		duplicates drop 
		save "$temp/gtd_studies_filter1.dta", replace /*n=367*/
	*/
	
	* Keep the most recent appl_id left to represent the study *;
	keep if appl_id==study_most_recent_appl /*n=367*/
	duplicates list xstudy_id
	
	* Exclude do not engage *;
	drop if do_not_engage==1 /*n=98 deleted */
	
	* Exclude if the study_hdp_id is archived *;
	drop if study_hdp_status=="archived" /*n=206*/

	keep appl_id proj_num ctc_pi_nm study_pi_email proj_url proj_strt_date proj_end_date fund_mech heal_funded nih_core_cde hdp_id xstudy_id study_hdp_id study_hdp_id_appl study_first_appl study_most_recent_appl do_not_engage checklist_exempt_all study_res_net project_title_reporter project_title_platform
	order appl_id proj_num ctc_pi_nm study_pi_email proj_url proj_strt_date proj_end_date fund_mech heal_funded nih_core_cde hdp_id xstudy_id study_hdp_id study_hdp_id_appl study_first_appl study_most_recent_appl do_not_engage checklist_exempt_all study_res_net project_title_reporter project_title_platform
sort study_res_net checklist_exempt proj_end_date
	save "$out/GTD_Targets/gtd_general.dta", replace	
	
export excel using "$out/GTD_Targets/gtd_targets_2026_$today.xlsx", sheet("gtd") firstrow(var) nolabel keepcellfmt replace


			* Create key of STUDIES on the GTD target list *;
			use "$out/GTD_Targets/gtd_general.dta", clear
			keep xstudy_id
			sort xstudy_id
			duplicates drop
			gen on_gtd_list=1
			save "$temp/ongtds.dta", replace

	

* Early Awards Target List *;
use "$out/GTD_Targets/gtd_targets_$today.dta", clear

	* Limit to studies where the earliest known project start date is in 2025 (the project start date of the study_first_appl)*;
	sort xstudy_id proj_strt_date_date
	by xstudy_id: egen first_strt_date=min(proj_strt_date_date) /*n=43 missing values generated */
	gen first_strt_yr=year(first_strt_date)
	tab first_strt_yr
	keep if first_strt_yr==2025 /*n=119*/
	
	* Exclude do not engage *;
	drop if do_not_engage==1 
	
	* Exclude SBIR/STTR *;
	drop if fund_mech=="SBIR/STTR" /*n=103 */
	
	/*	* Check: # unique study IDs *;
		keep xstudy_id
		sort xstudy_id
		duplicates drop 
		save "$temp/gtd_studies_filter3.dta", replace /*n=103*/
	*/
	
	* Exclude if the study_hdp_id is archived *;
	drop if study_hdp_status=="archived" /*n=0 dropped*/
	
	* Drop targets already on the GtD tab list *;
	merge 1:1 xstudy_id using "$temp/ongtds.dta"
	keep if _merge==1
	drop on_gtd_list _merge
	
	keep appl_id proj_num ctc_pi_nm study_pi_email proj_url proj_strt_date proj_end_date fund_mech heal_funded nih_core_cde hdp_id xstudy_id study_hdp_id study_hdp_id_appl study_first_appl study_most_recent_appl do_not_engage checklist_exempt_all study_res_net project_title_reporter project_title_platform
	order appl_id proj_num ctc_pi_nm study_pi_email proj_url proj_strt_date proj_end_date fund_mech heal_funded nih_core_cde hdp_id xstudy_id study_hdp_id study_hdp_id_appl study_first_appl study_most_recent_appl do_not_engage checklist_exempt_all study_res_net project_title_reporter project_title_platform
sort study_res_net checklist_exempt proj_end_date
	save "$out/GTD_Targets/gtd_earlyaward.dta", replace
	
	
export excel using "$out/GTD_Targets/gtd_targets_2026_$today.xlsx", sheet("earlyawards") firstrow(var) nolabel keepcellfmt 

	
	

	
	
	
* Pull visuals for slidedeck *;
use "$out/GTD_Targets/gtd_targets_$today.dta", clear

order xstudy_id $stewards_id_vars hdp_id archived appl_id proj_num fisc_yr proj_strt_date proj_end_date 
destring xstudy_id, replace
sort proj_ser_num xstudy_id
keep if inlist(xstudy_id,11,12,26,27,28,29,30,1056,1057,612,613)
keep xstudy_id-study_first_appl proj_title study_name res_net



	


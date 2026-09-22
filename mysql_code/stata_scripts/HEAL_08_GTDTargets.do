/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* RTI PI: Kira Bradford															*/
/* Program: HEAL_08_GTDTargets														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2026/02/03															*/
/* Date Last Updated: 2026/02/12													*/
/* Description:	This program prepares Get the Data target lists for PMs.			*/
/*																					*/
/* Notes:  																			*/
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

Other information to provide the PMs alongside target list:
- MySQL Data Dictionary link
- Exported full study_lookup_table (if requested)


*/






/* ----- Query: 2026/02/12 ----- */

/* ----- 1. Get the Data Target List ----- */
use "$der/alldata_$today.dta", clear

	* Limit to studies where the latest known project end date is in 2026 *;
	sort xstudy_id proj_end_date_date
	by xstudy_id: egen latest_end_date=max(proj_end_date_date)
	gen latest_end_yr=year(latest_end_date)
	tab latest_end_yr
	keep if latest_end_yr==2026
	
	/*	* Check: # unique study IDs *;
		keep xstudy_id
		sort xstudy_id
		duplicates drop 
		save "$temp/gtd_studies_filter1.dta", replace
	*/
	
	* Keep the most recent appl_id left to represent the study *;
	keep if appl_id==study_most_recent_appl
	duplicates list xstudy_id
	
	* Exclude do not engage *;
	drop if do_not_engage==1
	
	* Exclude if the study_hdp_id is archived *;
	drop if study_hdp_status=="archived"

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
use "$der/alldata_$today.dta", clear

	* Limit to studies where the earliest known project start date is in 2025 (the project start date of the study_first_appl)*;
	sort xstudy_id proj_strt_date_date
	by xstudy_id: egen first_strt_date=min(proj_strt_date_date)
	gen first_strt_yr=year(first_strt_date)
	tab first_strt_yr
	keep if first_strt_yr==2025 
	
	* Exclude do not engage *;
	drop if do_not_engage==1 
	
	* Exclude SBIR/STTR *;
	drop if fund_mech=="SBIR/STTR"
	
	/*	* Check: # unique study IDs *;
		keep xstudy_id
		sort xstudy_id
		duplicates drop 
		save "$temp/gtd_studies_filter3.dta", replace 
	*/
	
	* Exclude if the study_hdp_id is archived *;
	drop if study_hdp_status=="archived" 
	
	* Drop targets already on the GtD tab list *;
	merge 1:1 xstudy_id using "$temp/ongtds.dta"
	keep if _merge==1
	drop on_gtd_list _merge
	
	keep appl_id proj_num ctc_pi_nm study_pi_email proj_url proj_strt_date proj_end_date fund_mech heal_funded nih_core_cde hdp_id xstudy_id study_hdp_id study_hdp_id_appl study_first_appl study_most_recent_appl do_not_engage checklist_exempt_all study_res_net project_title_reporter project_title_platform
	order appl_id proj_num ctc_pi_nm study_pi_email proj_url proj_strt_date proj_end_date fund_mech heal_funded nih_core_cde hdp_id xstudy_id study_hdp_id study_hdp_id_appl study_first_appl study_most_recent_appl do_not_engage checklist_exempt_all study_res_net project_title_reporter project_title_platform
sort study_res_net checklist_exempt proj_end_date
	save "$out/GTD_Targets/gtd_earlyaward.dta", replace
	
	
export excel using "$out/GTD_Targets/gtd_targets_2026_$today.xlsx", sheet("earlyawards") firstrow(var) nolabel keepcellfmt 

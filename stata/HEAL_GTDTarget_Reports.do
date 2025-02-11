/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_scratch															*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/05/13															*/
/* Date Last Updated: 2024/12/04													*/
/* Description:	This program performs ad-hoc queries.								*/
/*																					*/
/* Notes:  																			*/
/*		- 2024/06/11 reversed order of queries so new queries are added to top of	*/
/*					 program.														*/
/*																					*/
/* -------------------------------------------------------------------------------- */


clear all 


/* ---------------------- */
/* ------- QUERY -------- */
/* ---------------------- */


/* ----- Query: 2024/12/04	----- */
/* Note: Gabi over email requested "a list of research network studies expiring in 2025". */

* Get 1 value of res_net for each xstudy_id *;
use "$der/study_lookup_table.dta", clear
sort appl_id
merge m:1 appl_id using "$der/research_networks.dta"
drop if _merge==2
drop _merge res_net_override_flag
keep xstudy_id res_net
sort xstudy_id res_net
duplicates drop /*n=1419*/
	* check uniqueness *;
	egen xgroups=group(xstudy_id res_net), miss
	by xstudy_id: egen max_xgroups=max(xgroups)
	gen xnon_unique_study=1 if max_xgroups!=xgroups
	by xstudy_id: egen non_unique_study=max(xnon_unique_study)
	browse if non_unique_study==1 /* Note; 12/4/24, there are never 2 diff values of res_net for the same study_id, only one missing and one non-missing value of res_net */
	drop if non_unique_study==1 & res_net==""
keep xstudy_id res_net
rename res_net study_res_net
duplicates list xstudy_id
save "$temp/study_res_net_key.dta", replace
	

* Prep study info *;
use "$der/study_lookup_table.dta", clear
destring xstudy_id, generate(study_id_final)
sort xstudy_id
merge m:1 xstudy_id using "$temp/study_res_net_key.dta"
drop _merge
sort appl_id
merge m:1 appl_id using "$der/engagement_flags.dta"
drop _merge xstudy_id study_hdp_id_appl
order study_id_final
sort study_id_final study_most_recent_appl study_hdp_id 
drop appl_id
duplicates drop 
save "$temp/study_info.dta", replace /*n=1415*/

* Prep full dataset for appl_ids belonging to a study */
use "$der/mysql_$today.dta", clear /*n=1988*/
drop if mds_ctn_flag==1  /*n=40 dropped */
drop if proj_ser_num=="" /*n=6 dropped*/
egen compound_key=concat(appl_id hdp_id), punct(_)

* Merge study info *;
merge 1:1 compound_key using "$doc/studyidkey.dta", keepusing(study_id_final)
drop _merge
merge m:1 study_id_final using "$temp/study_info.dta"
drop _merge /*n=1942*/

* Exclusion criteria *;
drop if entity_type!="Study" /*n=0 dropped*/
drop if study_res_net=="" /*n=1199 dropped*/
drop if merge_awards_mds==2 /*n=0 dropped*/
drop if do_not_engage==1 /*n=0*/
	* N=557 remaining *;
	
* Expiring in 2025 *;
* Note: interpreted as latest appl_id for project ends in 2025 *;
gen year_end=year(proj_end_date_date) if study_most_recent_appl==appl_id /*n=130 missing values */
keep if year_end==2025 /*n=42 left*/

* Output results *;
keep study_id_final year_end study_most_recent_appl study_hdp_id study_res_net

sort study_most_recent_appl study_hdp_id
export delimited using "$out/resnets_ending_2025.csv", quote replace




/* ----- Query: 2024/10/01	----- */
/* Note: RJ over email requested "a list of independent studies and SBIR's expiring in 2025". */

* Prep study info *;
use "$der/study_lookup_table.dta", clear
drop appl_id
destring xstudy_id, generate(study_id_final)
drop xstudy_id
order study_id_final
sort study_id_final study_most_recent_appl study_hdp_id 
duplicates drop 
save "$temp/study_info.dta", replace /*n=1214*/


* Merge res_net *;
use "$der/mysql_$today.dta", clear /*n=1719*/
egen compound_key=concat(appl_id hdp_id), punct(_)
sort appl_id
merge m:1 appl_id using "$der/research_networks.dta"
drop _merge
merge 1:1 compound_key using "$doc/studyidkey.dta", keepusing(study_id_final)
drop _merge
merge m:1 study_id_final using "$temp/study_info.dta"
drop _merge

* Exclusion criteria *;
drop if entity_type!="Study" /*n=46 dropped*/
drop if merge_awards_mds==2 /*n=2 dropped*/
keep if res_net=="" /*n=739 dropped*/

* Expiring in 2025 *;
gen year_end=year(proj_end_date_date)
keep if year_end==2025 /*n=133 left*/

* Output results *;
keep appl_id hdp_id fisc_yr fund_mech year_end study_id_final study_id_final study_hdp_id study_hdp_id_appl merge_awards_mds

	browse if hdp_id=="" /* Note: these do make it into study_lookup_table */

sort fund_mech appl_id hdp_id
export delimited using "$out/appls_ending_2025.csv", quote replace
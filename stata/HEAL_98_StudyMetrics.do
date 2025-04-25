/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_98_StudyMetrics													*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/06/23															*/
/* Date Last Updated: 2025/04/25													*/
/* Description:	This program produces a report of HDE study metrics.				*/
/*		1. Number of NIH awards in MySQL											*/
/*		2. Number of studies with VLMD on Platform									*/
/*		3. Number of studies with VLMD available in HSS								*/
/*		4. Number of studies who've submitted CDE usage								*/
/*		5. HEAL studies by data sharing intention									*/
/*		6. Number of studies registered 											*/
/*		7. Number of studies submitted SLMD											*/
/*		8. Number of studies selected repo											*/
/*		9. Number of studies with data linked on Platform							*/
/*		10. Other: MySQL entities and awards										*/
/*												*/
/*																					*/
/* Notes:  																			*/
/*		- Metrics should be calculated using only Platform data.					*/
/*		- In June 2024, a "6-Month Report" was generated to measure Get the Data	*/
/*		  (GtD) progress. This contained metrics which resemble but predate the		*/
/*		  formally agreed-upon study metrics. Thus, this program supersedes the 	*/
/*		  previous program HEAL_6Mo_Report.do; the latter has been archived.		*/
/*																					*/
/* Version changes																	*/
/*		- 2025/04/25 - Met w/ Kathy and Hina and removed conditional exclusion of	*/
/*		  drop if gen3_data_availability=="not_available" from registration and		*/
/*		  SLMD completion metrics.													*/
/*		- 2024/12/13 - Code updated after changing MySQL's progress_tracker table	*/
/*		  to include more fields and getting specification from the Platform on how */
/*		  they calculate metrics. See "Study Tracking in Platform and MySQL_Running */
/*		  Agenda", section "Revised HDE Metrics, Definitions and Data Sources". 	*/
/*		- v1 - HEAL_6Mo_Report.do, now archived. June 2024. 						*/	
/*																					*/
/* -------------------------------------------------------------------------------- */


clear



/* ----- 0. Prepare standard dataset for metrics report ----- */
use "$der/mysql_$today.dta", clear
drop if merge_awards_mds==1 /* keep only records that appear in Platform */
keep if inlist(guid_type,"discovery_metadata","unregistered_discovery_metadata") /* keep only active records on the Platform */
save "$temp/metrics_$today.dta", replace




/* ----- 1. Number of HEAL Studies ----- */
asdoc, text(--------------1. Number of HEAL Studies--------------) fs(14), save($qc/StudyMetrics_$today.doc) replace
asdoc, text(HDE Metric= HEAL study, producing data. Defined as yes when _guid_type=discovery_metadata OR _guid_type=unregistered_discovery_metadata.) save($qc/StudyMetrics_$today.doc) append label

use "$temp/metrics_$today.dta", clear
keep hdp_id entity_type
gen HEAL_studies=_n
asdoc sum HEAL_studies, statistics(max) save($qc/StudyMetrics_$today.doc) append label
asdoc, text( ) fs(14), save($qc/StudyMetrics_$today.doc) append

asdoc, text(Note that the definition of 'HEAL study' here is really just 'HDP ID'. It thus includes all 3 Stewards-defined entity types: a study, a CTN protocol, or an 'other' entity. For example, an HDP ID that represents a CTN Protocol would be counted in all following metrics as a 'HEAL study' even though it is not an entity_type='Study'.) save($qc/StudyMetrics_$today.doc) append label
asdoc tab entity_type, title(Breakdown: HEAL Studies by entity_type) save($qc/StudyMetrics_$today.doc) append label




/* ----- 2. Number of studies with VLMD on Platform ----- */
asdoc, text(--------------2. Number of studies with VLMD on Platform--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(HDE Metric= Studies producing data for which Data Dictionary VLMD is available on the Platform. Studies are counted when num_data_dictionaries>0. num_data_dictionaries is a field created in the progress_tracker table by the code that moves MDS data into MySQL.) save($qc/StudyMetrics_$today.doc) append label

use "$temp/metrics_$today.dta", clear
destring num_data_dictionaries, replace
keep if num_data_dictionaries>0
keep hdp_id num_data_dictionaries
gen vlmd_available_platform=_n
label var vlmd_available_platform "VLMD available on Platform?"
asdoc sum vlmd_available_platform, statistics(max) save($qc/StudyMetrics_$today.doc) append label




/* ----- 3. Number of studies with VLMD available in HSS ----- */
asdoc, text(--------------3. Number of studies with VLMD available in HSS--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(HDE Metric= Studies producing data for which VLMD is available in HSS. Estimated by the number of items in the Stewards 'Data Dictionary Tracker' board for which 'in HSS' = Yes and is from group = 'DD file in hand'.) save($qc/StudyMetrics_$today.doc) append label

*  Read in monday.com spreadsheet export tabs *;
* Note: Some manual reformatting of the Data Dictionary Tracker board export file was done to make it more machine readable. The separate table sections of the original export were all on one tab; these were manually moved to distinct tabs, and the tabs were name as defined in the following global macro. *;
global tabs dd_file_in_hand /*engagement_in_progress no_vlmd_expected ctn_dds*/
foreach tab in $tabs {
	import excel using "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\monday_boards\Data_Dictionary_Tracker_1734110341.xlsx", sheet("`tab'") firstrow /*case(lower)*/ allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		/*label var `x' `"`=`x'[1]'"'*/
		}
	missings dropvars * , force /* drop columns with no data */
	missings dropobs * , force /* drop rows with no data */
	gen source_tab="`tab'"
	save "$temp/`tab'.dta", replace
	}
	
/*clear
foreach tab in $tabs {
	append using "$temp/`tab'.dta"
	}
*/
gen vlmd_in_hss=0
replace vlmd_in_hss=1 if InHSS=="Yes"
label var vlmd_in_hss "VLMD in HSS"
do "$prog/HEAL_valuelabels"
/*label define yesno 0 "No" 1 "Yes"*/
label values vlmd_in_hss yesno
save "$temp/monday_data.dta", replace


* Add section to report *;
use "$temp/monday_data.dta", clear
keep if vlmd_in_hss==1
asdoc sum vlmd_in_hss, statistics(N) save($qc/StudyMetrics_$today.doc) append label
asdoc, text( ) fs(12), save($qc/StudyMetrics_$today.doc) append 




/* ----- 4. Number of studies who've submitted CDE usage----- */
asdoc, text(--------------4. Number of studies who've submitted CDE usage--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(HDE Metric= Studies producing data which have submitted CDE usage to the Platform. Estimated by the number of records where num_common_data_elements > 0. num_common_data_elements is a field created in the progress_tracker table by the code that moves MDS data into MySQL) save($qc/StudyMetrics_$today.doc) append label
asdoc, text( ) fs(12), save($qc/StudyMetrics_$today.doc) append 

use "$temp/metrics_$today.dta", clear
destring num_common_data_elements, replace
keep if num_common_data_elements>0
keep hdp_id num_common_data_elements
gen cdes=_n
label var cdes "Studies reporting CDE usage"
asdoc sum cdes, statistics(max) save($qc/StudyMetrics_$today.doc) append label




/* ----- 5. HEAL studies by data sharing intention ----- */
asdoc, text(--------------5. HEAL studies by data sharing intention--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(Corresponds to HDE Metric= Studies confirmed as producing but not sharing data. That metric is estimated by the number of studies on the Platform with Data Availability status - gen3_discovery.data_availability - flagged as Not Available. The below table shows gen3_discovery.data_availability for every 'HEAL Study' as defined in metric 1.) save($qc/StudyMetrics_$today.doc) append label

use "$temp/metrics_$today.dta", clear
label var gen3_data_availability "Data availability"
asdoc tab gen3_data_availability, miss save($qc/StudyMetrics_$today.doc) append 
asdoc, text(not_available means the HDP ID is producing but not sharing data. unaccessible means the HDP ID is sharing data, but the Platform cannot currently access it. a missing value . means neither of the other two conditions apply.) fs(12), save($qc/StudyMetrics_$today.doc) append 
asdoc, text( ) fs(12), save($qc/StudyMetrics_$today.doc) append 



/* ----- 6. Number of studies registered ----- */
asdoc, text(--------------6. Number of studies registered--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(HDE Metric=% of studies producing data which have completed Platform registration.) save($qc/StudyMetrics_$today.doc) append label

use "$temp/metrics_$today.dta", clear
label var is_registered "Registered on Platform"
asdoc tab is_registered, miss save($qc/StudyMetrics_$today.doc) append label




/* ----- 7. Number of studies submitted SLMD ----- */
asdoc, text(--------------7. Number of studies submitted SLMD--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(HDE Metric=% of studies producing data which have completed SLMD submission. Estimated using overall_percent_complete, which is a field created in the progress_tracker table by the code that moves MDS data into MySQL. SLMD is considered submitted if the CEDAR form completion rate is >=50%.) save($qc/StudyMetrics_$today.doc) append label


use "$temp/metrics_$today.dta", clear
destring overall_percent_complete, replace
gen slmd=0
replace slmd=1 if overall_percent_complete>=50 & overall_percent_complete!=.
label var slmd "SLMD Submitted"
label values slmd yesno
asdoc tab slmd, miss save($qc/StudyMetrics_$today.doc) append label




/* ----- 8. Number of studies selected repo ----- */
asdoc, text(--------------8. Number of studies selected repo--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(HDE Metric= % of studies producing and sharing data which have selected a repo. Estimated using repository_name.) save($qc/StudyMetrics_$today.doc) append label

use "$temp/metrics_$today.dta", clear
drop if gen3_data_availability=="not_available" /* drop studies producing but not sharing data */
gen has_repo=.
replace has_repo=0 if strtrim(repository_name)==""
replace has_repo=1 if strtrim(repository_name)!=""
label var has_repo "Repository selected"
label values has_repo yesno
asdoc tab has_repo, miss save($qc/StudyMetrics_$today.doc) append label




/* ----- 9. Number of studies with data linked on Platform ----- */
asdoc, text(--------------9. Number of studies with data linked on Platform--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label
asdoc, text(HDE Metric= % of studies producing and sharing data which have data linked on the Platform. Estimated using data_linked_on_platform, which is a field created in the progress_tracker table by the code that moves MDS data into MySQL.) save($qc/StudyMetrics_$today.doc) append label

use "$temp/metrics_$today.dta", clear
drop if gen3_data_availability=="not_available" /* drop studies producing but not sharing data */
label var data_linked_on_platform "Study has data linked on Platform"
asdoc tab data_linked_on_platform, miss save($qc/StudyMetrics_$today.doc) append label




/* ----- 10. Other: MySQL entities and awards ----- */
asdoc, text(--------------10. Other: MySQL entities and awards--------------) fs(14), save($qc/StudyMetrics_$today.doc) append label

* Number of CTN protocols *;
use "$der/mysql_$today.dta", clear
keep if mds_ctn_flag==1
gen ctns=_n
label var ctns "Number of CTN Protocols"
asdoc sum ctns, statistics(max) save($qc/StudyMetrics_$today.doc) append label
asdoc, text(The number of CTN Protocols is determined by how many HDP IDs on Platform are records for a CTN Protocol.) save($qc/StudyMetrics_$today.doc) append label
asdoc, text( ) save($qc/StudyMetrics_$today.doc) append label

* Number of MySQL studies *;
use "$der/study_lookup_table.dta", clear
destring xstudy_id, replace
label var xstudy_id "Number of MySQL studies"
asdoc sum xstudy_id, statistics(max) save($qc/StudyMetrics_$today.doc) append label
asdoc, text(The number of distinct studies tracked in the MySQL DB, where 'study' is defined by the HEAL Stewards. The number reported above is a count of unique values of xstudy_id.) save($qc/StudyMetrics_$today.doc) append label
asdoc, text( ) save($qc/StudyMetrics_$today.doc) append label

* Number of awards in MySQL *;
use "$der/mysql_$today.dta", clear
drop if merge_awards_mds==2
keep appl_id
sort appl_id
drop if appl_id==""
duplicates drop
gen appls=_n
label var appls "Number of MySQL awards"
asdoc sum appls, statistics(max) save($qc/StudyMetrics_$today.doc) append label
asdoc, text(The number of awards tracked in the MySQL DB, where an award is defined as a unique NIH appl_id.) save($qc/StudyMetrics_$today.doc) append label
asdoc, text( ) save($qc/StudyMetrics_$today.doc) append label


* Awards in MySQL by entity type *;
use "$der/mysql_$today.dta", clear
drop if merge_awards_mds==2
keep appl_id entity_type
sort appl_id
duplicates drop
merge 1:1 appl_id using "$der/research_networks.dta"
replace entity_type="CTN Protocol" if res_net=="CTN"
asdoc tab entity_type, title(appl_ids by entity type) save($qc/StudyMetrics_$today.doc) append label
asdoc, text(This tabulation shows the number of awards belonging to each type of entity. Here, the number for CTN Protocol indicates the total number of appl_ids associated with the 40 CTN Protocol numbers. The 'Other' entity type indicates 6 awards that do not have project serial numbers and appear to be contracts or other agreements - these 6 are listed out in the regular QC Report. Every other appl_id in the MySQL DB belongs to a Study, where 'study' is defined by the Stewards.) save($qc/StudyMetrics_$today.doc) append label
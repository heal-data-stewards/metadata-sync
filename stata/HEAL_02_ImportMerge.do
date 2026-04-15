/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_01_ImportMerge														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/02/29															*/
/* Date Last Updated: 2026/02/03													*/
/* Description:	This program imports the latest data from MySQL, merges it, and 	*/
/*  cleans it.																		*/
/*		1. Import data																*/
/*		2. Clean progress_tracker data												*/
/*		3. Clean awards table data													*/
/*		4. Merge data 																*/
/*		5. Clean merged data														*/
/*																					*/
/* Notes:  																			*/
/*		- This program is a necessary first step to all Stata processing. It must 	*/
/*		  be run before any other Stata HEAL programs.								*/	
/*		- Both project_num and appl_id fields in MDS are populated with the CTN 	*/
/*		  protocol number if the HDP_ID is for a CTN protocol						*/
/*		- progress_tracker only includes records hosted on Platform's MDS. Records	*/
/*		  hosted somewhere else, such as PDAPS or the AggMDA, are not included.		*/
/*																					*/
/* Version changes																	*/
/*		- 2025/09/02 Platform now contains some records that do not match any NIH	*/
/*		  appl_id or NIH study.	These were originally in the AggMDS system, but have*/
/*		  moved to Platform MDS. They are often links to repository data deposits. 	*/
/*		  They have appl_id="0".													*/
/*		- 2024/04/29 The reporter table may contain records for appl_ids not present*/
/*		  in the awards table. This occurs when Platform adds a record for a study	*/
/*		  that isn't HEAL-funded, but is related to HEAL-funded work ("HEAL-adjacent*/
/*		  studies"). Such records appear in NIH Reporter but they don't appear in	*/
/*		  the HEAL-funded specific data sources used to populate the awards table.	*/ 
/*		- 2024/05/15 Platform has performed QC on appl_id to fix format errors; the */
/*		  code block that fixed these errors has been archived at end of program, 	*/
/*		  in case it's ever needed again.											*/
/*																					*/
/* -------------------------------------------------------------------------------- */



/* ----- 1. Import data ----- */
foreach dtaset in reporter_$today awards_$today progress_tracker_$today research_networks_$today pi_emails_$today {
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



/* ----- 2. Clean progress_tracker data ----- */

use "$raw/progress_tracker_$today.dta", clear /*n=1691*/ 
order appl_id 
drop if appl_id==""

* Identify and flag bad values of project_num*;
gen mds_flag_bad_projnum=0

* -- CTN Protocols -- *;
gen mds_ctn_flag=regexm(project_num,"^CTN") 
gen mds_ctn_number=project_num if mds_ctn_flag==1
replace project_num="" if mds_ctn_flag==1 /*n=46*/ /* Remove CTN values from appl_id and project_num fields */
replace appl_id="" if mds_ctn_flag==1 /* Note: these appl_ids all equal 0 in MDS data */

* -- Other entities -- *;
* Identified by a value in project_num that doesn't follow the format of NIH project numbers *;

	* Too many dashes in project_num *;
	foreach var in project_num {
		egen sieved`var'=sieve(`var') , char(-)
		gen num_dashes=length(strtrim(sieved`var'))
		}
	replace mds_flag_bad_projnum=1 if num_dashes>1 /*n=6 changes made*/
	gen mds_bad_projnum=project_num if num_dashes>1 /*n=6 changes made*/
	drop sievedproject_num num_dashes

	* ICPSR data deposits *;
	replace mds_flag_bad_projnum=1 if substr(project_num,1,5)=="ICPSR" /*n=5 changes made*/
	
* Count number of hdp_ids for a given appl_id *;
sort appl_id hdp_id
by appl_id: egen num_hdp_by_appl=count(hdp_id)
replace num_hdp_by_appl=0 if num_hdp_by_appl==. /*n=0 changes made */
replace num_hdp_by_appl=0 if appl_id=="0" 
replace num_hdp_by_appl=. if appl_id==""
	/*Note: 2026/03/23: there are 37 appl_ids that have >1 HDP_ID associated, and the max number of HDP_IDs associated is 6. */

* Entity type *;
gen entity_type="Study"
replace entity_type="CTN" if mds_ctn_flag==1
replace entity_type="Other" if mds_flag_bad_projnum==1
replace entity_type="Other" if appl_id=="0" & mds_ctn_flag!=1 /* Only records belonging to a Study entity should go in the study_lookup_table. Records without an appl_id must be CTN or Other entities. */

* -- Non-NIH studies on Platform -- *;
replace appl_id="" if appl_id=="0" /*n=7*/

save "$temp/progress_tracker_$today.dta", replace




/* ----- 3. Clean awards table data ----- */
* Note: This step may not be needed if the awards table is not altered during export from MySQL. Sabrina had issues of NULL/missing values being set to 0 during export. As a quick check, note that the value of nih_foa_heal_lang and of nih_noa_heal_lang should be NULL in a majority of records, since NIH has only indicated values of these variables in the 2024 and 2025 new awards lists. *;
use "$raw/awards_$today.dta", clear
drop if appl_id==""
foreach acr in foa noa {
	replace nih_`acr'_heal_lang="NULL" if nih_`acr'_heal_lang==""
	rename nih_`acr'_heal_lang xnih_`acr'_heal_lang
	} 
sort appl_id
merge 1:1 appl_id using "$doc/correct_foanoa_values.dta", keepusing(nih_foa_heal_lang nih_noa_heal_lang)
drop if _merge==2
drop _merge x*
foreach acr in foa noa {
	replace nih_`acr'_heal_lang="NULL" if nih_`acr'_heal_lang==""
	}
order nih_noa_notes, last
save "$temp/awards_$today.dta", replace /*n=2442*/

	


/* ----- 4. Merge data ----- */
* Merge awards reporter *;
use "$raw/reporter_$today.dta", clear /*n=2442*/
drop if appl_id==""
merge 1:1 appl_id using "$temp/awards_$today.dta" /*n=2442*/
drop if appl_id==""
rename _merge merge_reporter_awards
label define awrep 1 "In reporter only" 2 "In awards only" 3 "In both tables"
label values merge_reporter_awards awrep
save "$temp/nihtables_$today.dta", replace /*n=2442*/

* Merge progress_tracker table *;
use "$temp/nihtables_$today.dta", clear
merge 1:m appl_id using "$temp/progress_tracker_$today.dta" 
rename _merge merge_awards_mds
label var merge_awards_mds "Merge of MySQL and MDS"
label define sqlmds 1 "In MySQL only" 2 "In MDS only" 3 "In both databases"
label values merge_awards_mds sqlmds
save "$temp/dataset_$today.dta", replace /*n=2521*/

* Merge research_networks table *;
use "$temp/dataset_$today.dta", clear
merge m:1 appl_id using "$raw/research_networks_$today.dta"
drop if _merge==2
drop _merge res_net_override_flag
replace res_net=upper(res_net)
replace entity_type="CTN" if res_net=="CTN" /*n=219 changes made*/
replace entity_type="Study" if entity_type==""
save "$temp/dataset2_$today.dta", replace




/* ----- 5. Clean merged data ----- */
use "$temp/dataset2_$today.dta", clear 

* Flag supplement awards *;
gen xsupp_flag=substr(proj_num,-2,1)
gen supplement_flag=1 if xsupp_flag=="S" /*n=622 supplements*/
drop xsupp_flag	

* Dates *;
destring fisc_yr, replace
foreach var in awd_not_date bgt_end bgt_strt proj_end_date proj_strt_date {
	gen x`var'=substr(`var',1,10)
	gen `var'_date=date(x`var',"YMD")
	format `var'_date %td
	drop x`var'
	order `var'_date,after(`var')
	label var `var'_date "Stata date format"
	}

* Compound appl_id & hdp_id key for study id merging later *;
sort appl_id hdp_id
egen compound_key=concat(appl_id hdp_id), punct(_)

save "$der/mysql_$today.dta", replace /*n=2521*/	
	

	





/* 
/* ----- X. Archived code ----- */
* Fix appl_id format - code temporarily needed until Platform performs QC on this field *; 
	foreach var in appl_id {
		gen x`var'=`var'
		egen sieved`var'=sieve(`var') , keep(alphabetic space other)
		}
	tab sievedappl_id /*note: only non-numeric characters are dashes*/

	foreach var of varlist appl_id {
	   replace `var'=regexr(`var', "\-.*", "") /* dash and everything that follows it */
	   }

	replace appl_id="" if appl_id=="0"
	/* Note: there are n=5 records where no appl_id is recorded in either the appl_id or nih_reporter_link variables. For these 5, there is a non-missing project number, but it has been modified from source by Platform */
	drop if appl_id==""
	
* Extract project number components *;
		* Remove lowercase letters (these were inserted by Platform) *;
	foreach var of varlist project_num {
	   replace `var'=regexr(`var', "[a-z]", "") 
	   } /*n=0 real changes*/
	   
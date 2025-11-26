/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford, Becky Boyles													*/
/* Program: HEAL_01_ImportMerge														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/02/29															*/
/* Date Last Updated: 2025/11/26													*/
/* Description:	This program imports the latest data from MySQL, merges it, and 	*/
/*  cleans it.																		*/
/*		1. Import data																*/
/*		2. Clean progress_tracker data												*/
/*		3. Clean awards data														*/
/*		4. Merge data 																*/
/*		5. Clean merged data														*/
/*																					*/
/* Notes:  																			*/
/*		- This program is a necessary first step to all Stata processing. It must 	*/
/*		  be run before any other Stata HEAL programs.								*/	
/*		- Both project_num and appl_id fields in MDS are populated with the CTN 	*/
/*		  protocol number if the HDP_ID is for a CTN protocol						*/
/*																					*/
/* Version changes																	*/
/*		- 2025/09/02 Platform now contains some records that do not match any NIH	*/
/*		  appl_id or NIH study.	These were originally in the AggMDS system, but have*/
/*		  moved to Platform MDS. They are often links to repository data deposits. 	*/
/*		  They have appl_id="0" and get dropped because they have nothing to do with*/
/*		  Stewards engagement.														*/
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
foreach dtaset in reporter_$today awards_$today progress_tracker_$today {
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

use "$raw/progress_tracker_$today.dta", clear /*n=1592*/ 
order appl_id 
drop if appl_id==""

* -- CTN Protocols -- *;
gen mds_ctn_flag=regexm(project_num,"^CTN") 
gen mds_ctn_number=project_num if mds_ctn_flag==1
replace project_num="" if mds_ctn_flag==1 /*n=44*/ /* Remove CTN values from appl_id and project_num fields */
replace appl_id="" if mds_ctn_flag==1

* -- Non-NIH studies on Platform -- *;
drop if appl_id=="0" /*n=7*/


* -- Project numbers -- *;
* Split project_num into components needed for xstudy_id *;
foreach var in project_num {
	gen x`var'=`var'
	egen sieved`var'=sieve(`var') , char(-)
	gen num_dashes=length(strtrim(sieved`var'))
	}
replace xproject_num="" if num_dashes>1 /*n=6 changes made*/ 
	
	* Identify and flag bad values of project_num*;
	gen mds_flag_bad_projnum=1 if num_dashes>1 /*n=6 changes made*/
	gen mds_bad_projnum=project_num if num_dashes>1 /*n=6 changes made*/
	
	/* ARCHIVED - Platform fixed source data errors 
	* If an underscore was inserted, remove it and everything that follows it *;
	foreach var of varlist xproject_num {
	   replace `var'=regexr(`var', "\_.*", "") 
	   }
	*/
gen proj_num_spl_ty_code=substr(xproject_num,1,1)
gen proj_num_spl_act_code=substr(xproject_num,2,3)
gen proj_ser_num=substr(xproject_num,5,8)
	split xproject_num, p(-)
	drop xproject_num1
	rename xproject_num2 proj_nm_spl_supp_yr
gen proj_num_spl_sfx_code=substr(proj_nm_spl_supp_yr,3,.)
foreach var in proj_num_spl_ty_code proj_num_spl_act_code proj_ser_num proj_nm_spl_supp_yr proj_num_spl_sfx_code {
	rename `var' mds_`var'
	}
	
* Count number of hdp_ids for a given appl_id *;
sort appl_id hdp_id
by appl_id: egen num_hdp_by_appl=count(hdp_id)
replace num_hdp_by_appl=0 if num_hdp_by_appl==. /*n=0 changes made */
replace num_hdp_by_appl=. if appl_id==""
	/*Note: 2025-1-24: there are 9 appl_ids that have >1 HDP_ID associated, and the max number of HDP_IDs associated is 5. This excludes CTN records where appl_id==.*/
	/*Note: 2025/06/10: there are 31 appl_ids that have >1 HDP_ID associated, and the max number of HDP_IDs associated is 5. */
	/*Note: 2025/11/10: there are 33 appl_ids that have >1 HDP_ID associated, and the max number of HDP_IDs associated is 5. */
	
* Save prepped data *;
drop sievedproject_num num_dashes xproject_num
save "$temp/progress_tracker_$today.dta", replace




/* ----- 3. Clean awards table data ----- */
* Correct nih_foa_heal_lang to null *;
use "$raw/awards_$today.dta", clear
rename nih_foa_heal_lang xnih_foa_heal_lang
sort appl_id
merge 1:1 appl_id using "$doc/correct_foa_values.dta", keepusing(nih_foa_heal_lang)
replace nih_foa_heal_lang="NULL" if nih_foa_heal_lang==""
drop _merge xnih_foa_heal_lang
save "$temp/awards_$today.dta", replace

	


/* ----- 4. Merge data ----- */
* Merge awards reporter *;
use "$raw/reporter_$today.dta", clear /*n=2145*/
drop if appl_id==""
merge 1:1 appl_id using "$temp/awards_$today.dta" /*n=2146*/
drop if appl_id==""
rename _merge merge_reporter_awards
label define awrep 1 "In reporter only" 2 "In awards only" 3 "In both tables"
label values merge_reporter_awards awrep
save "$temp/nihtables_$today.dta", replace /*n=2146*/

* Merge MDS data (via progress_tracker) *;
use "$temp/nihtables_$today.dta", clear
merge 1:m appl_id using "$temp/progress_tracker_$today.dta" 
rename _merge merge_awards_mds
label var merge_awards_mds "Merge of MySQL and MDS"
label define sqlmds 1 "In MySQL only" 2 "In MDS only" 3 "In both databases"
label values merge_awards_mds sqlmds
save "$temp/dataset_$today.dta", replace /*n=2236*/




/* ----- 5. Clean merged data ----- */
use "$temp/dataset_$today.dta", clear 

* Update values of variables used for identifiers *;
foreach var in proj_ser_num proj_num_spl_sfx_code {
	replace `var'=mds_`var' if strtrim(`var')==""
	}
	/* Note: subproj_id is not available in the MDS data */ /*n=24 and n=1 real changes made*/
	
	replace proj_ser_num=mds_proj_ser_num if strtrim(proj_ser_num)==""

* Flag supplement awards *;
gen xsupp_flag=substr(proj_num,-2,1)
gen supplement_flag=1 if xsupp_flag=="S"
tab supplement_flag /*n=578*/
drop xsupp_flag	

* Dates *;
destring fisc_yr, replace
foreach var in bgt_end bgt_strt proj_end_date {
gen x`var'=substr(`var',1,10)
gen `var'_date=date(x`var',"YMD")
format `var'_date %td
drop x`var'
order `var'_date,after(`var')
label var `var'_date "Stata date format"
}

* Entity type *;
gen entity_type="Study"
replace entity_type="CTN" if mds_ctn_flag==1
replace entity_type="Other" if mds_flag_bad_projnum==1

save "$der/mysql_$today.dta", replace /*n=2236*/	
	

	





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
	   
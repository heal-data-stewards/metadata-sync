/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford													*/
/* Program: HEAL_99_QC																*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/05/07															*/
/* Date Last Updated: 2025/01/27													*/
/* Description:	This program creates a QC report for data contained in MySQL. 		*/				
/*		1. progress_tracker table 													*/
/*		2. awards table																*/
/*		3. Compare appl_ids in MySQL tables 										*/
/*		4. Metrics by Study  														*/
/*																					*/
/* Notes:  																			*/
/*		- */
/*																					*/
/* -------------------------------------------------------------------------------- */


clear



/* ----- 1. progress_tracker table ----- */
asdoc, text(--------------1. progress_tracker table--------------) fs(14), save($qc/QCReport_$today.doc) replace

asdoc, text(The MYSQL progress_tracker table is automatically updated daily with a pull of fresh data from the Platform MDS.) save($qc/QCReport_$today.doc) append label

* -- appl_id -- *;
use "$raw/progress_tracker_$today.dta", clear 
gen missing_appl_id=0
label var missing_appl_id "Missing appl_id"
replace missing_appl_id=1 if appl_id==""
asdoc tab missing_appl_id , title(1A. Number of records missing appl_id) save($qc/QCReport_$today.doc) append label


* -- project number -- *;
use "$raw/progress_tracker_$today.dta", clear
gen missing_proj_num=0
label var missing_proj_num "Missing project_num"
replace missing_proj_num=1 if strtrim(project_num)==""
foreach var in project_num {
	gen x`var'=`var'
	egen sieved`var'=sieve(`var') , char(-)
	gen num_dashes=length(strtrim(sieved`var'))
	}
gen project_num_badformat=0
label var project_num_badformat "project_num bad format"
gen ctn_flag=regexm(project_num,"^CTN") /*n=40*/
label var ctn_flag "CTN Protocol"

replace project_num_badformat=1 if num_dashes>1 | ctn_flag==1

asdoc tab missing_proj_num, title(1B. Number of records missing project_num) save($qc/QCReport_$today.doc) append label

asdoc tab project_num_badformat , title(1C. Number of records with bad format for project_num) save($qc/QCReport_$today.doc) append label

	* Cause of bad project number format *;
	asdoc tab ctn_flag if project_num_badformat==1, title(1D. Number of bad project_number formats due to CTN protocol in project_num field) save($qc/QCReport_$today.doc) append label

	* List of bad format project numbers, excluding CTN protocols *;
	keep if project_num_badformat==1
	drop if ctn_flag==1
	keep project_num hdp_id appl_id study_name
	order project_num hdp_id appl_id study_name
	duplicates drop
	asdoc list *, title(1E. List of records where project_num has bad format, excluding CTN protocols) save($qc/QCReport_$today.doc) append label

	/* Note: Can repeat reporting on appl_id and proj_num in awards and reporter tables, too */


/*
/* ----- 2. awards table  ----- */
asdoc, text(--------------2. awards table--------------) fs(14), save($qc/QCReport_$today.doc) append

use "$raw/awards_$today.dta", clear
keep appl_id heal_funded
/* keep if heal_funded=="" */ /* 12/12/2024 Note: this command isn't working because of issues exporting the awards table out of MySQL. During export, null values for TINYINT type columns are overwritten to 0. The null values are preserved inside MySQL as they should be */
asdoc list *, title(2A. appl_ids with unknown HEAL funding) save($qc/QCReport_$today.doc) append label 
*/





/* ----- 3. Compare appl_ids in MySQL tables ----- */
asdoc, text( ) fs(14) save($qc/QCReport_$today.doc) append label
asdoc, text(--------------3. Compare appl_ids in MySQL tables--------------) fs(14), save($qc/QCReport_$today.doc) append
* -- Awards and reporter -- *;
use "$temp/nihtables_$today.dta", clear 
asdoc tab merge_reporter_awards, title(3A. Compare: reporter and awards) save($qc/QCReport_$today.doc) append label
/*drop if merge_reporter_awards==3*/
keep appl_id merge_reporter_awards
sort appl_id
if merge_reporter_awards!=3 {
	asdoc list *, title(3B. List of appl_ids in reporter or awards only, but not both) save($qc/QCReport_$today.doc) append label 
}


* -- MySQL (Awards+reporter) and MDS (progress_tracker) -- *;
use "$der/mysql_$today.dta", clear 
asdoc tab merge_awards_mds, title(3C. Compare: MySQL [reporter & awards] and MDS) save($qc/QCReport_$today.doc) append label
keep if merge_awards_mds==2
drop if mds_ctn_number!=""
keep appl_id hdp_id mds_ctn_number merge_awards_mds
sort appl_id
asdoc list *, title(3D. List of appl_ids in MDS only, but not in NIH-source MySQL tables, excluding CTN protocol HDP IDs) save($qc/QCReport_$today.doc) append label




/* ----- 4. Metrics by Study ----- */
asdoc, text(--------------4. Metrics by Study--------------) fs(14), save($qc/QCReport_$today.doc) append
* -- # of studies --*;
asdoc, text(--------------4A. Number of studies, by Stewards xstudy_id--------------) fs(12), save($qc/QCReport_$today.doc) append 
use "$der/study_lookup_table.dta", clear
keep xstudy_id
destring xstudy_id, replace
sort xstudy_id
duplicates drop
asdoc sum xstudy_id, statistics(N) save($qc/QCReport_$today.doc) append label
asdoc, text( ) fs(14), save($qc/QCReport_$today.doc) append

* -- # of studies with 0 associated HDP IDs --*;
use "$der/study_lookup_table.dta", clear
keep xstudy_id study_hdp_id
sort xstudy_id study_hdp_id
duplicates drop
gen study_has_hdp=0
replace study_has_hdp=1 if study_hdp_id!=""
label var study_has_hdp "Study has an associated HDP ID"
label values study_has_hdp yn
label define yn 0 "No" 1 "Yes"
asdoc, text(--------------HDP_ID missingness, by study--------------) fs(12), save($qc/QCReport_$today.doc) append
asdoc tab study_has_hdp, title(4B. Number of studies with/out a HDP ID) save($qc/QCReport_$today.doc) append label


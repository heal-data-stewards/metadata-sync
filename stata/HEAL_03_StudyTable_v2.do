/* THIS IS A BRANCH OF THE STUDY LOOKUP TABLE CODE TO DEVELOP PIPELINE FOR DQ AUDIT RESULTS */

/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_03_StudyTable_v2													*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/02/29															*/
/* Date Last Updated: 2025/04/16													*/
/* Description:	This program creates the xstudy_id field and the study_lookup_table.*/
/*		1. Prep data																*/	
/*		QC: Check alignment between MySQL and MDS primary key fields				*/
/*		2. Split up records to prepare for handling appl_ids missing a study_id   	*/
/*		3. Update hdpid0.dta records												*/
/*		4. Update hdpid1.dta records												*/
/*		5. Update studyidbad.dta records   											*/
/*		6. Update missing values of study_id in the full dataset					*/
/*		7. Most recent appl_id for each study 										*/
/*		8. hdp id and associated appl_ids for each study 							*/
/*		9. Create study table														*/
/*		10. Create data dictionary for study table									*/
/*																					*/	
/* Notes:  																			*/
/*		- The study lookup table only contains the entity "study." Other entities 	*/
/*		  that Stewards track, CTN protocols and "other" entities, are excluded		*/
/*		  from the study_lookup_table.												*/
/*		- Type 3 awards are always supplements. Supplements are always type 3 awards*/
/*		- Supplement awards related to each other are identified by the suffix in	*/
/*		  the project number (proj_num) field. Project numbers ending in S1 are		*/
/*		  related; ending in S2 are related; and so forth.							*/
/*		- The field subproj_id is often missing. When non-missing, it identifies	*/
/*		  separate studies under a consortia/DCC/other large award. 				*/
/*																					*/
/* Version changes																	*/
/*		- 2024/10/10 compound_key introduced to merge study id key back into full	*/
/*		   dataset. Prior merge settings were not correctly handling a small number */
/*		   of appl_ids.																*/
/*		- 2024/04/05 export subset of records for QC review							*/
/*		- 2024/04/09 create unique_studies table for import into MySQL				*/
/*		- 2024/04/17 create new study_id field based on study_id_stewards and hdp_id*/
/*			, capture appl_id of the most recent record/study plus appl_id that 	*/
/* 			performed the merge between MySQL and MDS records, rename program to 	*/
/*			HEAL_MYSQL_02_StudyKey from HEAL_MYSQL_02_SelectStewardsRecord to show	*/
/*			changed contents and purpose											*/
/*		- 2024/04/30 multistep code for updating study_id to include hdp_id, some   */
/*		  CTN-related records temporarily excluded									*/
/*		- 2024/06/19 remove CTN-related records from pool before creating study ids	*/
/*		- 2024/09/12 changed merge condition to create final key from m:1 to m:m    */
/*			to accomodate new condition that arose, where there is a first-stage	*/
/*			study ID generated that involves 1 appl_id split over 2 hdp IDs, plus	*/
/*			1 different appl_id and hdp_id pair. 									*/ 
/*																					*/
/* -------------------------------------------------------------------------------- */


/* ----- 1. Prep data ----- */
use "$der/mysql_$today.dta", clear
order appl_id $stewards_id_vars hdp_id
sort $stewards_id_vars appl_id hdp_id	

* Exclude records of non-study entities (CTN & "other") *;
drop if mds_ctn_flag==1  /*n=40 dropped */
drop if proj_ser_num=="" /*n=6 dropped*/
sort appl_id
merge m:1 appl_id using "$raw/research_networks_$today.dta"
drop if _merge==2
drop _merge res_net_override_flag
drop if res_net=="CTN" /*n=199 deleted*/

	/*
	* Num of unique serial numbers: n=987 *;
	keep proj_ser_num
	sort proj_ser_num
	duplicates drop 
	*/

* Non-missing subproject number by serial number *;
gen xhas_subproj_num=1 if subproj_id!="" /*n=57*/
tab xhas_subproj_num
bysort proj_ser_num: egen has_subproj_num_by_sernum=max(xhas_subproj_num)
drop xhas_subproj_num

* Flag records in MySQL reporter/awards tables *;
gen in_mysql=1 /*n=1831*/


* Add DQ audit result appl_ids */
append using "$der/reporter_dqaudit.dta" /*n=2281*/
replace in_mysql=0 if in_mysql==.


* Generate study identifier *;
/* Note: this must be done in 2 stages because missing values among the $stewards_id_vars group should still be assigned a number (subproj_id is often missing), but missing values of hdp_id should *not* be assigned a number */
	* Stage 1 *;
	egen xstudy_id_stewards=group($stewards_id_vars), missing
	
	* Stage 2 *;
	egen study_id=group(xstudy_id_stewards hdp_id)
	label var study_id "Unique Study ID"
	order study_id xstudy_id_stewards

save "$temp/mysql_$today.dta", replace /*n=2281*/


		* Keep to check completeness later on *;
		use "$temp/mysql_$today.dta", clear
		keep if study_id==. & xstudy_id!=.
		save "$temp/check_studyid_assigns.dta", replace /*n=946*/


* Count # of appl_ids for each xstudy_id_stewards *;
use "$temp/mysql_$today.dta", clear
keep xstudy_id_stewards appl_id
sort xstudy_id_stewards appl_id
duplicates drop
by xstudy_id_stewards: egen num_appl_by_xstudyidstewards=count(appl_id)
keep xstudy_id_stewards num_appl_by_xstudyidstewards
duplicates drop
save "$temp/sis_count.dta", replace

* Count # of hdp_ids for each xstudy_id_stewards *;
use "$temp/mysql_$today.dta", clear
keep xstudy_id_stewards hdp_id
sort xstudy_id_stewards hdp_id
duplicates drop /*n=1336*/
by xstudy_id_stewards: egen num_hdp_by_xstudyidstewards=count(hdp_id)
keep xstudy_id_stewards num_hdp_by_xstudyidstewards
duplicates drop /*n=1336*/
save "$temp/hdpid_count.dta", replace

* Merge in count vars
use "$temp/mysql_$today.dta", clear
sort xstudy_id_stewards
merge m:1 xstudy_id_stewards using "$temp/sis_count.dta"
drop _merge
merge m:1 xstudy_id_stewards using "$temp/hdpid_count.dta"
drop _merge

* Create compound key for merging later *;
sort appl_id hdp_id
egen compound_key=concat(appl_id hdp_id), punct(_)

order $key_vars compound_key
save "$temp/mysql_noctn_$today.dta", replace /*n=2281*/



/* ----- QC: Check alignment between MySQL and MDS primary key fields ----- */
use "$temp/mysql_noctn_$today.dta", clear 
	
* -- Check n's for QC -- *;
gen valid_flag=0

* 0 or 1 HDP_IDs matched to xstudy_id_stewards *;
replace valid_flag=1 if num_hdp_by_xstudyidstewards==0 | num_hdp_by_xstudyidstewards==1 /*n=2010*/

* Every appl_id under xstudy_id_stewards matched to exactly 1 HDP_ID *;
replace valid_flag=1 if num_appl_by_xstudyidstewards==num_hdp_by_xstudyidstewards & num_hdp_by_appl==1 /*n=54*/

* The xstudy_id_stewards only has 1 appl_id associated, and this 1 appl_id matched to >1 HDP_ID *;
replace valid_flag=1 if num_appl_by_xstudyidstewards==1 /*n=8*/

* Everything else: valid_flag==0 *;
/*browse if valid_flag==0  */
keep if valid_flag==0 /*n=209*/
 /*
  Note: appl_ids may appear only in MDS or MySQL data instead of both
	tab merge_awards_mds if valid_flag==0
	n=34 rows are appl_ids only in MySQL, not in MDS
	n=0 rows are appl_ids only in MDS, not in MySQL
  */
export delimited using "$qc/sis_hdpid_comparison_issues.csv", replace





/* ----- 2. Split up records to prepare for handling appl_ids missing a study_id ----- */

* -- 0 HDP_IDs matched to xstudy_id_stewards --*; 
	*Note: the unique value of xstudy_id_stewards indicates records in a group together. Variable study_id is always missing in this subset because of the egen option specified. Populate study_id with a new value that hasn't yet been used in the study_id variable. *;
use "$temp/mysql_noctn_$today.dta", clear	
keep if num_hdp_by_xstudyidstewards==0 
save "$temp/hdpid0.dta", replace /*n=194*/

* -- 1 HDP_ID matched to xstudy_id_stewards --*; 
	* Note: the unique value of study_id for the one record with an HDP_ID should be applied to ALL records sharing the same xstudy_id_stewards *;
use "$temp/mysql_noctn_$today.dta", clear
keep if num_hdp_by_xstudyidstewards==1 
save "$temp/hdpid1.dta", replace /*n=1816*/

* -- Every appl_id under xstudy_id_stewards matched to exactly 1 HDP_ID --*; 
	* Note: No further action needed for these records; they have all been assigned a study_id. *;
use "$temp/mysql_noctn_$today.dta", clear	
drop if num_hdp_by_xstudyidstewards==0 | num_hdp_by_xstudyidstewards==1
keep if num_appl_by_xstudyidstewards==num_hdp_by_xstudyidstewards & num_hdp_by_appl==1 
keep study_id appl_id hdp_id compound_key
save "$temp/studyidgood1.dta", replace /*n=54*/

* -- The xstudy_id_stewards only has 1 appl_id associated, and this 1 appl_id matched to >1 HDP_ID --*; 
	* Note: No further action needed for these records; they have all been assigned a study_id. *;
	* Note: This set of records is excluded from building the studyidkey.dta file below because it breaks the 1:1 merge if included *;
use "$temp/mysql_noctn_$today.dta", clear
drop if num_hdp_by_xstudyidstewards==0 | num_hdp_by_xstudyidstewards==1
keep if num_appl_by_xstudyidstewards==1 
keep study_id appl_id hdp_id compound_key
save "$temp/studyidgood2.dta", replace /*n=8*/


* -- What remains: xstudy_id_stewards where some appl_ids merged to an hdp_id and some didn't, and >1 hdp_id is involved -- *;
use "$temp/mysql_noctn_$today.dta", clear	
drop if num_hdp_by_xstudyidstewards==0 | num_hdp_by_xstudyidstewards==1
drop if num_appl_by_xstudyidstewards==num_hdp_by_xstudyidstewards & num_hdp_by_appl==1
drop if num_appl_by_xstudyidstewards==1 
save "$temp/studyidbad.dta", replace /*n=209*/





/* ----- 3. Update hdpid0.dta records ----- */

* Max value of study_id *;
use "$temp/mysql_noctn_$today.dta", clear
summarize study_id, meanonly
scalar maxid=r(max)

* Create new values of study_id, beginning after previous max value *;
use "$temp/hdpid0.dta", clear
sort xstudy_id_stewards
egen tempn=group(xstudy_id_stewards)
gen xstudy_id=tempn+scalar(maxid)
replace study_id=xstudy_id
	* tab in_mysql *;
keep study_id appl_id hdp_id compound_key in_mysql
save "$temp/studyidgood3.dta", replace /*n=197*/





/* ----- 4. Update hdpid1.dta records ----- */

* Create key of the study_id for each xstudy_id_stewards value *;
use "$temp/hdpid1.dta", clear
keep study_id xstudy_id_stewards
drop if study_id==.
sort xstudy_id_stewards
rename study_id xstudy_id
save "$temp/studyid_sis_key.dta", replace

* Update missing values of study_id *;
use "$temp/hdpid1.dta", clear
sort xstudy_id_stewards
merge m:1 xstudy_id_stewards using "$temp/studyid_sis_key.dta", keepusing(xstudy_id)
replace study_id=xstudy_id if study_id==.
keep study_id appl_id hdp_id compound_key in_mysql
save "$temp/studyidgood4.dta", replace /*n=1816*/





/* ----- 5. Update studyidbad.dta records ----- */

* -- Split up records with non-missing and missing hdp_id -- *;
* Non-missing HDP ID *;
use "$temp/studyidbad.dta", clear
  /* browse if inlist(appl_id,"10885121","10900634","10900700","10900807") */
keep if hdp_id!=""
keep study_id xstudy_id_stewards appl_id proj_ser_num act_code hdp_id compound_key proj_num_spl_ty_code in_mysql
foreach var of varlist study_id appl_id	proj_ser_num act_code proj_num_spl_ty_code in_mysql {
	rename `var' z`var'
	}
sort xstudy_id_stewards
save "$temp/studyidbad_nonmiss.dta", replace /*n=121*/

* Missing HDP ID *;
use "$temp/studyidbad.dta", clear
keep if hdp_id==""
sort xstudy_id_stewards
save "$temp/studyidbad_miss.dta", replace /*n=88*/

* -- Fix Missing: Find which study_id value should be assigned to records missing study_id, using activity code (act_code) -- *;
use "$temp/studyidbad_miss.dta", clear
joinby xstudy_id_stewards using "$temp/studyidbad_nonmiss.dta" /* creates all possible combinations */
gen act_code_match=1 if act_code==zact_code
gen typ_match=1 if proj_num_spl_ty_code==zproj_num_spl_ty_code

* Check this results in only 1 record for each appl_id. *;
/* Note: As of 12/11/2024, there are some appl_ids with >1 potential match. One is an HBDC study and the other 3 appl's are IMPOWR. The issue is the same proj_ser_num matches to multiple appl_ids, then there's another appl_id per proj_ser_num and we don't know which of the HDP IDs it goes with.*/
tab appl_id act_code_match 
bysort appl_id: egen count_actcode_matches=sum(act_code_match)
bysort appl_id: egen count_typ_matches=sum(typ_match)
save "$temp/xstudyidgood5.dta", replace /*n=165*/

	* 5a. Pull out unambiguous matches - act_code *;
	use "$temp/xstudyidgood5.dta", clear
	keep if count_actcode_matches==1 & act_code_match==1
	replace study_id=zstudy_id
	keep study_id appl_id hdp_id compound_key
	save "$temp/xstudyidgood5a.dta", replace /*n=49*/
	
	* 5b. Pull out unambiguous matches - type code *;
		/*  This new step to match based on type code resolved issues with proj_ser_num=DA055325. There was a former type 5 (archived now) so we can match it uniquely based on type 5. */
	use "$temp/xstudyidgood5.dta", clear
	drop if count_actcode_matches==1 
	keep if count_typ_matches==1 & typ_match==1 
	replace study_id=zstudy_id
	keep study_id appl_id hdp_id compound_key
	save "$temp/xstudyidgood5b.dta", replace /*n=5*/
	
	* 5c. Investigate ambiguous matches *;
	use "$temp/xstudyidgood5.dta", clear
	keep if count_actcode_matches>1
	drop if count_typ_matches==1 /*n=79*/
	/* pull all these appl_ids and zappl_ids into the inlist command below to see all the full reporter records stacked on top of each other */
	
		use "$temp/studyidbad.dta", clear
		/*browse if inlist(appl_id,"10900634","10900700","10900807") | inlist(appl_id,"10378422","10378910","10391075")*/
		sort proj_ser_num appl_id hdp_id 
		/* These can't be uniquely matched b/c they are type 5 continuing awards for type 1's that got split up into >1 HDP ID. For ex, proj_ser_num=DA055437 is a type 5 award continuing a type 1 where each of 3 aims of the type 1 was a separate study and got its own HDP ID. We ignore these new type 5 appls, ultimately omitting them from the study_lookup_table because there is no way to associate 1 appl_id with >1 study ID. Trying to do so would result in all of the affected study IDs getting associated together and lead to definition-breaking associations that show 2 distinct HDP IDs for the same study.*/


	* Combine good matches  *;
	use "$temp/xstudyidgood5a.dta", clear
	append using "$temp/xstudyidgood5b.dta" 
	save "$temp/studyidgood5.dta", replace /*n=54*/




* -- Capture Non-missing -- *;
use "$temp/studyidbad_nonmiss.dta", clear
rename zstudy_id study_id
rename zappl_id appl_id
keep study_id appl_id hdp_id compound_key
save "$temp/studyidgood6.dta", replace /*n=121*/





/* ----- 6. Update missing values of study_id in the full dataset ----- */

* Create key of appl_id and study_id *
use "$temp/studyidgood1.dta", clear
forv i=2/6 {
	append using "$temp/studyidgood`i'.dta" 	
	} /*n=2250*/
sort compound_key
duplicates list compound_key /*n=0 dupes*/
rename study_id study_id_final
save "$doc/studyidkey.dta", replace

* Update study_id in full dataset *;
use "$temp/mysql_noctn_$today.dta", clear /*n=2284*/ /* Note: the n=34 difference between studyidkey and this data set is the n=34 appl_ids that coudn't be uniquely matched in the preceding step 5c. */
sort compound_key
merge 1:1 compound_key using "$doc/studyidkey.dta", keepusing(study_id_final) 
order study_id_final in_mysql
sort study_id_final appl_id hdp_id
drop study_id xstudy_id_stewards _merge
save "$temp/mysql_studyid_$today.dta", replace





/* ----- 7. Most recent appl_id for each study ----- */
use "$temp/mysql_studyid_$today.dta", clear 
sort study_id_final fisc_yr
drop if study_id_final==. /*n=34 dropped*/ /* Note: these are appl_ids that coudn't be uniquely matched to a study ID in the preceding step 5c. */

* Flag: latest project end date for the record *;
by study_id_final: egen latest_proj_end_dt_forstudy=max(proj_end_date_date)
  format latest_proj_end_dt_forstudy %td 

* Latest fiscal year *;
by study_id_final: egen latest_fy=max(fisc_yr)
keep if latest_fy==fisc_yr /*n=1490*/

* Latest budget end *;
sort study_id_final bgt_end_date
by study_id_final: egen latest_bgt_end=max(bgt_end_date)
  format latest_bgt_end %td 
keep if latest_bgt_end==bgt_end_date 

* Latest award notice date *;
* Note: had to add this level to control after adding DQ audit appls *;
drop award_notice_date award_notice_date_date
foreach var in awd_not_date {
	gen x`var'=substr(`var',1,10)
	gen `var'_date=date(x`var',"YMD")
	format `var'_date %td
	drop x`var'
	order `var'_date,after(`var')
	label var `var'_date "Stata date format"
	}
sort study_id_final awd_not_date_date
by study_id_final: egen latest_awd_not_date=max(awd_not_date_date)
	format latest_awd_not_date %td
keep if latest_awd_not_date==awd_not_date_date /*n=1470*/

	* Check # of duplicates by study_id *;
	duplicates list study_id_final
	/* n=0 duplicates */
	
	* Latest project end date *;
	gen proj_end_date_stata=date(proj_end_date,"YMD")
	

* Create key with most recent appl_id for study_id *;
keep study_id_final appl_id
rename appl_id study_most_recent_appl	
sort study_id_final
save "$temp/mostrecentapplid.dta", replace /*n=1469*/





/* ----- 8. hdp id and associated appl_ids for each study ----- */
use "$temp/mysql_studyid_$today.dta", clear 
drop if study_id_final==. /*n=34 dropped*/
drop if hdp_id=="" /*n=912 dropped, including all the in_mysql==0 rows*/
keep study_id_final hdp_id appl_id
sort study_id_final hdp_id appl_id
rename appl_id study_hdp_id_appl
rename hdp_id study_hdp_id
save "$temp/hdpapplid.dta", replace /*n=1335*/
	* Check # of duplicates by study_id *;
	duplicates list study_id_final
	/* n=0 duplicates */





/* ----- 9. Create study table ----- */
use "$temp/mysql_studyid_$today.dta", clear 
drop if study_id_final==. /*n=34*/
keep study_id_final appl_id in_mysql
 rename in_mysql appl_id_in_repawd
 label var appl_id_in_repawd "appl_id appears in reporter or awards tables"
sort study_id_final appl_id 
merge m:1 study_id_final using "$temp/hdpapplid.dta"
drop _merge
merge m:1 study_id_final using "$temp/mostrecentapplid.dta"
drop _merge
order appl_id appl_id_in_repawd study_id_final study_most_recent_appl study_hdp_id study_hdp_id_appl

* Drop appls / studies where the whole "study" is composed of appl_ids found during DQ audit. These correspond to non-study entities, mainly CTN protocols, which are always excluded from the study lookup table. See note in 'Section 1. Prep data' *;
bysort study_id_final: egen max_flag=max(appl_id_in_repawd)
drop if max_flag==0
drop max_flag

* Drop rows of appl_ids that are NOT in reporter/awards tables *;
* Note: These appl's should only appear (if applicable under the rules) in the study_most_recent_appl column *;
drop if appl_id_in_repawd==0 /*n=384 dropped*/
drop appl_id_in_repawd


* Format and output table *;
rename study_id_final xstudy_id /* Note: prefixed with x to indicate variable is volatile and may change on a new run of program tree */
tostring xstudy_id, replace
label var appl_id "Application ID"
label var study_most_recent_appl "Most recent appl_id for the study"
label var study_hdp_id "The study's hdp_id"
label var study_hdp_id_appl "The appl_id of the study's hdp_id"
save "$der/study_lookup_table.dta", replace	
export delimited using "$der/study_lookup_table.csv", nolab quote replace /*n=1828*/



/* ----- 10. Create data dictionary for study table ----- */
use "$der/study_lookup_table.dta", clear
redcapture *, file("$temp/study_table_dd") form(study_lookup_table) text(appl_id xstudy_id study_most_recent_appl study_hdp_id_appl study_hdp_id) /*validate(appl_id xstudy_id study_most_recent_appl study_hdp_id_appl) validtype(integer integer integer integer) validmin(9000000 1 9000000 9000000) validmax(11000000 2000 11000000 11000000)*/

import delimited using "$temp/study_table_dd.csv", varnames(1) stringcols(_all) clear

* Cols *; 
drop sectionheader branchinglogicshowfieldonlyif requiredfield customalignment questionnumbersurveysonly matrixgroupname textvalidationtypeshowslidernum
rename variablefieldname var_name
rename fieldlabel var_label
rename fieldtype var_fmt
rename formname table_name
rename choicescalculationssliderlabels choicelist
foreach word in min max {
	rename textvalidation`word' var_`word'
	}
gen var_length=""
rename fieldnote var_note

order table_name var_name var_label var_fmt choicelist var_min var_max var_length identifier var_note
 
 
* Cells *; 
foreach var in appl_id study_most_recent_appl study_hdp_id_appl {
	replace var_fmt="VARCHAR(8)" if var_name=="`var'"
	replace var_length="8" if var_name=="`var'"
	}
replace var_fmt="VARCHAR(4)" if var_name=="xstudy_id"
	replace var_length="4" if var_name=="xstudy_id"
replace var_fmt="CHAR(8)" if var_name=="study_hdp_id"
	replace var_length="8" if var_name=="study_hdp_id"

foreach x in min max {
	replace var_`x'="" if var_name=="study_hdp_id"
	}

/*replace identifier="PK" if var_name=="appl_id" */ 
	/*Note: A PK can't be assigned for this table because no column has only unique values. This is a consequence of the way the table is structured, to allow lookup of both the latest appl_id and the associated hdp_id (if any) for any appl_id. */
replace identifier="FK" if var_name=="study_hdp_id"

replace var_note="The study ID is generated by HEAL Stewards" if var_name=="xstudy_id"
replace var_note="There cannot be more than 1 HDP ID for a given Study ID, by definition" if var_name=="study_hdp_id"
export delimited using "$doc/study_table_dd.csv", replace

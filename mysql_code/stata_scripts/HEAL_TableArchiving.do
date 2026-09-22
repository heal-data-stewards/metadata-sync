/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford, Becky Boyles													*/
/* Program: HEAL_TableArchiving														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/08/23															*/
/* Date Last Updated: 2024/08/23													*/
/* Description:	This program manages MySQL tables prior to archiving. Several tables*/
/*		can benefit from basic data quality improvement actions to improve their 	*/
/*		future usability. Archival actions were taken August-September 2024.		*/					
/*		1. focused_dai_responses 													*/
/*		2. dai_responses															*/
/*		3. repo_mapping																*/
/*		4. RepoPredictionExercise													*/
/*																					*/
/* Notes:  																			*/
/*		- */
/*																					*/
/* Version changes																	*/
/*		- */	
/*																					*/
/* -------------------------------------------------------------------------------- */



/* ----- 1. focused_dai_responses ----- */

* Import data *;
foreach dtaset in focused_dai_responses /*dai_responses repo_mapping RepoPredictionExercise */{
import delimited using "$backups/`dtaset'.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' `"`=`x'[106]'"'
		}

* Drop if all variables (except nmiss) are missing *;
egen nmiss=rowmiss(*)
describe, short
drop if nmiss==`=`r(k)'-1'
drop nmiss

* Sort and drop duplicates *;
sort timestamp pi_name
duplicates drop

* Drop rows that contain varnames and varlabels *;
drop if inlist(timestamp,"timestamp","Timestamp")

* Clean out extraneous info at beginning of cells*;
foreach var of varlist pi_name resp_name resp_email proj_num da_name da_title dm_name dm_title study_conc {
   replace `var'=regexr(`var', "^a\.", "") /* a. */
   replace `var'=regexr(`var', "^b\.", "") /* b. */
   replace `var'=regexr(`var', "^13\.", "") /* 13. */
   replace `var'=regexr(`var', "^32\.", "") /* 32. */
   replace `var'=regexr(`var', "^33\.", "") /* 33. */
   replace `var'=regexr(`var', "^34\.", "") /* 34. */
   replace `var'=regexr(`var', "^36\.", "") /* 36. */
	}

foreach x in proj_num {
	replace `x'=subinstr(`x', "`=char(32)'", "", .) /* replace linebreaks inside cells with a space */
	}

save "$backups/dai1_focused.dta", replace
export delimited using "$backups/ForArchiving/dai1_focused.csv", delimiter(tab) datafmt quote replace
}



/* ----- 2. dai_responses ----- */

* Import data *;
foreach dtaset in dai_responses {
import delimited using "$backups/`dtaset'.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		}

* Drop if all variables (except nmiss) are missing *;
egen nmiss=rowmiss(*)
describe, short
drop if nmiss==`=`r(k)'-1'
/*drop nmiss*/
	* Drop if nmiss=99, aka all but timestamp and bioteam cols are empty? *;

* Clean out extraneous info at beginning of cells*;
foreach var of varlist pi_name resp_name resp_email proj_num {
   replace `var'=regexr(`var', "^[1-9]\.", "") /* any number followed by period at beginning of string */
   replace `var'=regexr(`var', "^a\.", "") /* a. */
   replace `var'=regexr(`var', "^b\.", "") /* b. */
	}
	
sort correct_proj_num timestamp
save "$backups/dai1_full.dta", replace
export delimited using "$backups/ForArchiving/dai1_full.csv", delimiter(tab) datafmt quote replace
}



/* ----- 3. repo_mapping ----- */

* Import data *;
foreach dtaset in repo_mapping {
import delimited using "$backups/`dtaset'.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		}

* Drop if all variables (except nmiss) are missing *;
egen nmiss=rowmiss(*)
describe, short
drop if nmiss==`=`r(k)'-1'
drop nmiss

sort appl_id
save "$backups/repo_mapping.dta", replace
}



/* ----- 4. RepoPredictionExercise ----- */

* Import data *;
foreach dtaset in RepoPredictionExercise {
import delimited using "$backups/`dtaset'.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		}

* Drop if all variables (except nmiss) are missing *;
egen nmiss=rowmiss(*)
describe, short
drop if nmiss==`=`r(k)'-1'
drop nmiss

sort project_num subproject_id
save "$backups/RepoPredictionExercise.dta", replace

}



/* ----- 5. dai2_full ----- */
/* Note: this one didn't previously exist in MySQL. The previous table, dai2_report, was a subset of a small number of cols from the full DAI-2 dataset. */

use "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\DAI2\Derived\dai2_clean.dta", clear
drop xheal_dai2_assessment_timestamp serial_no_check mysql_proj_ser_num
export delimited using "$backups/ForArchiving/dai2_full.csv", delimiter(tab) datafmt quote replace

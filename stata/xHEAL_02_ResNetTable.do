/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford, Becky Boyles													*/
/* Program: HEAL_02_ResNetTable														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/05/13															*/
/* Date Last Updated: 2024/11/26													*/
/* Description:	This program generates and populates the res_net field in the 		*/
/*	research_networks table in MySQL. 												*/
/*		1. Import keys 																*/
/*		2. Create research_networks table											*/
/*		3. Create data dictionary for research_networks table						*/
/*		4. Check key contains all values of res_prg									*/
/*		5. Test MySQL script for generating research_networks table					*/
/*																					*/
/* Notes:  																			*/
/*		- 2024/09/24 this procedure is being migrated to a MySQL Script 			*/
/*		- 2024/05/21 first run of code to generate research_networks table 			*/
/*																					*/
/* -------------------------------------------------------------------------------- */



/* ----- 1. Import keys ----- */

foreach tab in ref_table value_overrides {
	import excel using "$doc/HEAL_research_networks_ref_table_for_MySQL.xlsx", sheet("`tab'") firstrow /*case(upper)*/ allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		}
	missings dropvars * , force /* drop columns with no data */
	missings dropobs * , force /* drop rows with no data */
	save "$temp/`tab'.dta", replace
	export delimited using "$doc/res_net_`tab'.csv", quote replace
	}

use "$temp/ref_table.dta", clear
keep if res_net!=""
replace res_net=upper(res_net)
sort res_net
keep res_prg res_net
egen res_prg_tomerge=sieve(res_prg), keep(alpha)
replace res_prg_tomerge=lower(res_prg_tomerge)
sort res_prg_tomerge
drop if inlist(res_prg,"Pain Management Effectiveness Research Network","Justice Community Opioid Innovation Network")
save "$doc/ref_table.dta", replace

use "$temp/value_overrides.dta", clear
sort appl_id
replace res_net=upper(res_net)
rename res_net res_net_override
gen res_net_override_flag=1
save "$doc/value_overrides.dta", replace



/* ----- 2. Create research_networks table ----- */
use "$temp/nihtables_$today.dta", clear
egen res_prg_tomerge=sieve(res_prg), keep(alpha)
replace res_prg_tomerge=lower(res_prg_tomerge)
sort res_prg_tomerge
merge m:1 res_prg_tomerge using "$doc/ref_table.dta" /*n=1934*/
rename _merge merge_ref_table
sort appl_id
merge 1:1 appl_id using "$doc/value_overrides.dta"
replace res_net=res_net_override if res_net_override_flag==1 /*n=103 changes made*/
replace res_net_override_flag=0 if res_net_override_flag==.
drop res_net_override _merge
save "$temp/res_net_key.dta", replace /*n=1934*/

use "$temp/res_net_key.dta", clear
keep appl_id res_net res_net_override_flag
sort appl_id
/*encode res_net, generate(zres_net)
drop res_net
rename zres_net res_net
order res_net, after(appl_id)*/
label var appl_id "Application ID"
label var res_net "Research Network"
label var res_net_override_flag "Value of res_net overridden"
save "$der/research_networks.dta", replace
export delimited using "$der/research_networks.csv", replace /*n=1934*/



/* ----- 3. Create data dictionary for research_networks table ----- */
* Use redcapture command to generate preliminary *;
use "$der/research_networks.dta", clear
redcapture *, file("$temp/research_networks_dd") form(research_networks) text(appl_id res_net_override_flag res_net)

* -- Customize to fit Stewards DD template -- *;
import delimited using "$temp/research_networks_dd.csv", varnames(1) stringcols(_all) clear

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
replace var_fmt="VARCHAR(8)" if var_name=="appl_id"
replace var_fmt="SET" if var_name=="res_net"
replace var_fmt="BOOLEAN" if var_name=="res_net_override_flag"

replace var_min="0" if var_name=="res_net_override_flag"
replace var_max="1" if var_name=="res_net_override_flag"
replace var_length="8" if var_name=="appl_id"

replace identifier="PK" if var_name=="appl_id"
replace var_note="This variable has a value of 1 if the appl_id appears in the value_overrides tab of the research networks sheet, and a missing value otherwise" if var_name=="res_net_override_flag"

export delimited using "$doc/research_networks_dd.csv", replace


/*
/* ----- 4. Check key contains all values of res_prg ----- */

use "$temp/ref_table.dta", clear
keep res_prg
sort res_prg
duplicates drop
gen in_key=1
drop if res_prg==""
save "$temp/res_prg_key.dta", replace

use "$temp/dataset_$today.dta", clear
keep res_prg
sort res_prg
duplicates drop
drop if res_prg=="" | res_prg=="0"
merge 1:1 res_prg using "$temp/res_prg_key.dta"
keep if _merge==1
keep res_prg
export delimited using "$temp/missing res prg.csv", replace




/* ----- 5. Test MySQL script for generating research_networks table ----- */
/* Note: This tests a MySQL script to update the research_networks table. It compares the results of the MySQL script and the Stata code. This code block was formerly stroed in the Stata program HEAL_scratch. Initial testing and approval completed 2024/09/23. */

* Read in MySQL script results *;
import delimited using "$dir/Backups/research_networks_mysql.csv", varnames(1) stringcols(_all) bindquote(strict) favorstrfixed clear /*n=1665*/
	foreach x of varlist res_net res_net_override_flag {
		destring `x', replace
		}
replace res_net=upper(res_net)
tab res_net
sort appl_id
save "$temp/mysql_resnet.dta", replace


* Report Comparison *;
asdoc, text(--------------Compare MySQL and Stata results for research_networks--------------) fs(14), save($qc/MySQL_resnet_script_$today.doc) replace

use "$der/research_networks.dta", clear /*n=1665*/
asdoc tab res_net, miss title(Stata res_net) save($qc/MySQL_resnet_script_$today.doc) append label
asdoc tab res_net_override_flag, miss title(Stata res_net_override_flag) save($qc/MySQL_resnet_script_$today.doc) append label

use "$temp/mysql_resnet.dta", clear
asdoc tab res_net, miss title(MySQL res_net) save($qc/MySQL_resnet_script_$today.doc) append label
asdoc tab res_net_override_flag, miss title(MySQL res_net_override_flag) save($qc/MySQL_resnet_script_$today.doc) append label


* Merge Datasets to Compare *;
use "$temp/mysql_resnet.dta", clear
rename res_net mysql_res_net
rename res_net_override_flag mysql_res_net_override
merge 1:1 appl_id using "$der/research_networks.dta"
rename res_net stata_res_net
rename res_net_override_flag stata_res_net_override
drop _merge
save "$temp/compare_res_net.dta", replace


*Override flag discrepancies *;
asdoc, text(--------------Override flag discrepancies--------------) fs(14), save($qc/MySQL_resnet_script_$today.doc) append
asdoc, text(This appears to be an issue with the MySQL script. Spot checking the below appl_ids where stata flagged it as an override shows they are in the value_overrides tab) save($qc/MySQL_resnet_script_$today.doc) append 
use "$temp/compare_res_net.dta", clear /*n=0 discrepancies*/
keep if stata_res_net_override!=mysql_res_net_override
asdoc list *, title(override flag discrepancies) save($qc/MySQL_resnet_script_$today.doc) append 


*Research network discrepancies *;
asdoc, text(--------------Research network discrepancies--------------) fs(14), save($qc/MySQL_resnet_script_$today.doc) append
asdoc, text(There are no discrepancies in research_networks after updating the reference table in MySQL.) save($qc/MySQL_resnet_script_$today.doc) append
use "$temp/compare_res_net.dta", clear /*n=0 discrepancies*/
keep if stata_res_net!=mysql_res_net
asdoc list *, title(res_net discrepancies) save($qc/MySQL_resnet_script_$today.doc) append


/*  
use "$temp/compare_res_net.dta", clear 
browse if stata_res_net!=mysql_res_net /*n=141 discrepancies*/
   * none of these have any override flag-1*;

keep if stata_res_net!=mysql_res_net
asdoc tab mysql_res_net, title(MySQL values of res_net that don't match Stata) save($qc/MySQL_resnet_script_$today.doc) append
asdoc tab stata_res_net, title(Stata values of res_net that don't match MySQL) save($qc/MySQL_resnet_script_$today.doc) append
*/




/* ----- X. Archived code ----- */

	* Output changed values for QC *;
	use "$temp/res_net_key.dta", clear
	keep if res_net!=res_net_old
	order res_net res_net_old
	save "$qc/compare_old_new_res_net.dta", replace
	export delimited using "$qc/compare_old_new_res_net.csv", replace
*/

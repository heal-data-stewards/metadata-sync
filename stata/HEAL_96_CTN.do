/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford, Becky Boyles													*/
/* Program: HEAL_04_CTN																*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/06/17															*/
/* Date Last Updated: 2024/08/19													*/
/* Description:	This Stata program generates a crosswalk for Clinical Trials Network*/
/*	protocols and associated project numbers. It identifies application IDs that 	*/
/*	belong to project numbers identified by responsible SMEs as part of the Clinical*/
/*  Trials Network (CTN).															*/
/*		1. Read in CTN spreadsheet													*/
/*		2. Format for Reporter														*/
/* 		3. Query NIH Reporter API													*/
/*		4. Read in results of NIH Report API export									*/
/*		5. Create CTN crosswalk														*/
/*		6. Output CTN sheet tab populated with appl_ids								*/
/*		7. Check CTN appl_ids are in reporter + awards tables						*/
/*																					*/
/* Notes:  																			*/
/*		- Project numbers identified in CTN HEAL Studies - 11.13.23 sheet.			*/
/*		- As a precursor to running this program, feed all project numbers from the */
/*		  sheet through the NIH Reporter API.										*/
/*		- 2024/06/17 code moved out of scratch program.								*/
/*																					*/
/* -------------------------------------------------------------------------------- */


clear all 



/* ----- 1. Read in CTN spreadsheet ----- */

* tab: CTN Appl_IDs *;
import excel using "$doc/CTN HEAL Studies - 11.13.23.xlsx", sheet("CTN Appl_IDs") firstrow allstring clear
rename ProjectNumbers project_num
keep project_num
	foreach x of varlist project_num {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		}
drop if project_num==""
sort project_num
duplicates drop /*n=148*/
replace project_num="DA040316-04S3" if project_num=="DA040316-043"
  /* Note: The original project number cannot be found in Reporter and is likely a typo. Manually checked that DA040316-04 is a CTN project number. Querying this will return all supplements sharing the value as well. */
duplicates list project_num /* 0 dupes*/
/*save "$temp/tab_ctn_projnums.dta", replace*/
save "$temp/ctn_tab_applids.dta", replace


* tab: Protocol List *;
import excel using "$doc/CTN HEAL Studies - 11.13.23.xlsx", sheet("Protocol List") firstrow cellrange(A2:M42) allstring clear
	foreach x of varlist Grant2018 Grant2019 Grant2020 Grant2021 Grant2022 Grant2023 {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		}
keep Study Grant*
foreach n in 2018 2019 2020 2021 2022 2023 {
	split Grant`n', p(" ")
	drop Grant`n'
	}
reshape long Grant, i(Study) j(year)
drop year 
drop if strtrim(Grant)==""
rename Study ctn_protocol_num
label var ctn_protocol_num "CTN Protocol Number"
rename Grant project_num
sort ctn_protocol_num project_num
replace project_num="DA040316-04S3" if project_num=="DA040316-043"
  /* Note: The original project number cannot be found in Reporter and is likely a typo. Manually checked that DA040316-04 is a CTN project number. Querying this will return all supplements sharing the value as well. */
replace project_num="DA013035-18S4" if project_num=="UG1DA013035-18S4" /*Note: remove UG1 so format is consistent with other rows*/
save "$temp/ctn_tab_protocollist.dta", replace


* Combine data from both tabs *;
use "$temp/ctn_tab_protocollist.dta", clear
sort project_num ctn_protocol_num
merge m:1 project_num using "$temp/ctn_tab_applids.dta"
label define ctn_nums 1 "Project num only in tab Protocol List" 2 "Project num only in tab Complete list CTN Appl_IDs" 3 "Project num in both tabs"
label values _merge ctn_nums
save "$temp/CTN_Heal_Studies_tabs.dta", replace


* Create report *;
asdoc, text(---Project_nums in spreadsheet--) fs(14), save($qc/CTN_Reporter_Query.doc) replace
asdoc, text(This table compares project numbers appearing in 2 tabs of the spreadsheet 'CTN HEAL Studies - 11.13.23'. The tabs are 'Protocol list' and 'CTN Appl_IDs'.)
use "$temp/CTN_Heal_Studies_tabs.dta", clear
asdoc tab _merge, title(Project_nums in 2 tabs) save($qc/CTN_Reporter_Query.doc) append label
keep if _merge!=3
asdoc list *, title(List of project nums in only 1 tab) save($qc/CTN_Reporter_Query.doc) append label
asdoc, text(A value of 1 means the project_num is only in tab 'Protocol List'. A value of 2 means the project_num is only in tab 'CTN Appl_IDs.') append label
asdoc, text( ) append label



/* ----- 2. Format for Reporter ----- */
use "$temp/CTN_Heal_Studies_tabs.dta", clear
keep project_num
sort project_num
duplicates drop
gen paren=`"""'
gen comma=","
egen input=concat(paren project_num paren comma)
keep input
save "$dir/Scripts/nih_reporter_api_ctn_projnums.dta", replace
export excel using "$dir/Scripts/nih_reporter_api_ctn_projnums.xlsx", replace /*n=149 project numbers*/



/* ----- 3. Query NIH Reporter API ----- */

/* Note: Format the list of project numbers output in "$dir/Scripts/nih_reporter_api_ctn_projnums.xlsx" for inclusion in the criteria -> project_nums field of "$dir/Scripts/nih_reporter_api_ctn.txt". Next, paste the full txt file into NIH Reporter API online. Follow the link included in the output txt file to the page of NIH Reporter results. Click export at the top of the results page. Save the exported file in $raw. */



/* ----- 4. Read in results of NIH Report API export ----- */

import delimited using "$raw/SearchResult_Export_19Aug2024_074427.csv", varnames(6) rowrange(7) colrange(1:14) stringcols(_all) clear
	foreach x of varlist projectnumber {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		} 
gen xcheck=substr(projectnumber,1,4)
tab xcheck
gen project_num=substr(projectnumber,5,.)
rename applicationid appl_id
sort project_num appl_id
keep project_num appl_id 
duplicates drop
save "$temp/reporter_results.dta", replace /*n=170 */



/* ----- 5. Create CTN crosswalk ----- */

* Merge CTN list to Reporter API results *;
use "$temp/CTN_Heal_Studies_tabs.dta", clear
drop _merge
sort project_num
merge m:m project_num using "$temp/reporter_results.dta"
drop if _merge==2 /* Note: Reporter returned all records whose project number exactly matched the submitted value, plus records where project_number included the submitted value as a substring. For example, the project num=DA049435-02 was on the SME-provided list. Querying Reporter for this project_num returned an exact match for project num=DA049435-02, plus partial matches like project num=DA049435-02S1. These extra appl_ids are excluded because not every CTN study is HEAL-funded and supplements don't necessarily have the same HEAL affiliation as the parent award.*/
drop _merge
sort ctn_protocol_num project_num appl_id
save "$temp/ctn_crosswalk.dta", replace


* Add HDP ID to crosswalk *;
use "$der/mysql_$today.dta", clear
keep if mds_ctn_flag==1
keep hdp_id mds_ctn_number
rename mds_ctn_number ctn_protocol_num
sort ctn_protocol_num
	* Fix one format discrepancy that prevents merging *;
	replace ctn_protocol_num="CTN-0095-A-2" if ctn_protocol_num=="CTN-0095A2"
save "$temp/ctn_hdp_ids.dta", replace


use "$temp/ctn_crosswalk.dta", clear
merge m:1 ctn_protocol_num using "$temp/ctn_hdp_ids.dta"
drop _merge
rename hdp_id hdp_id_ctn_protocol
order ctn_protocol_num hdp_id_ctn_protocol
label var hdp_id_ctn_protocol "HDP ID for CTN Protocol Number"
label var project_num "Partial NIH Project Number"
save "$doc/ctn_crosswalk.dta", replace
export delimited "$doc/ctn_crosswalk.csv", quote replace




/* ----- 6. Output CTN sheet tab populated with appl_ids ----- */

* For CTN HEAL Studies - 11.13.23 *;
use "$doc/ctn_crosswalk.dta", clear
keep project_num appl_id
sort project_num appl_id
duplicates drop
export delimited "$out/ctn_appl_ids_CTNHEALStudies.csv", quote replace 

* For res_net table, value_overrides tab *;
use "$doc/ctn_crosswalk.dta", clear
keep appl_id
drop if appl_id==""
sort appl_id
gen res_net="CTN"
export delimited "$out/ctn_appl_ids_res_net.csv", quote replace





/* ----- 7. Check CTN appl_ids are in reporter + awards tables ----- */
* Key of CTN appl_ids *; 
use "$doc/ctn_crosswalk.dta", clear
keep appl_id
sort appl_id
duplicates drop /*n=162*/
gen ctn=1
save "$temp/ctn_appl_ids.dta", replace

* Reporter table comparison *;
use "$der/mysql_$today.dta", clear
keep appl_id merge_reporter_awards
sort appl_id
duplicates drop /*n=1622*/
drop if appl_id==""
merge 1:1 appl_id using "$temp/ctn_appl_ids.dta"
drop if _merge==1
/*tab merge_reporter_awards if _merge==3*/
keep if _merge==2
keep appl_id
/*asdoc list *, title(List of CTN appl_ids not in MySQL) save($qc/CTN_Reporter_Query.doc) append label*/ /* commented out 9/24 because command now errors out when no appl_ids are left */

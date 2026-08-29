/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_Raw_04_Output														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/11/23															*/
/* Date Last Updated: 2024/12/02													*/
/* Description:	This program outputs data in various formats for updating MySQL		*/
/*   tables.																		*/
/*		1. reporter: Output appl_ids for Reporter API query							*/
/*		2. awards 																	*/
/*		3. pi_emails 															*/
/*		4. 						 										*/
/*		5. 									*/
/*		6. 											*/
/*																					*/
/*																					*/
/* Notes:  																			*/
/*	Some manual data formatting was done in spreadsheets before read-in to speed up */
/*  processing. These manual changes are noted by source spreadsheet below.			*/
/*																					*/
/* -------------------------------------------------------------------------------- */


/* ----- 1. reporter: Output appl_ids for Reporter API query -----*/
use "$der/merged_clean.dta", clear
keep appl_id proj_num rfa res_prg
duplicates drop
drop if rfa==""
save "$der/fy24_new_awards_appls.dta", replace
export delimited using "$der/fy24_new_awards_appls.csv", nolabel quote replace

	
	
	
	

	
/* ----- 2. awards -----*/
use "$der/merged_clean.dta", clear
keep appl_id rfa res_prg nih*

* -- Generate variables -- *; 
gen goal=""
label var goal "Goal Category"
replace goal="Cross-Cutting Research" if rfa=="Cross-Cutting Research" | rfa=="Training the Next Generation of Researchers in HEAL"
replace goal="OUD" if goal=="" & inlist(rfa,"Enhanced Outcomes for Infants and Children Exposed to Opioids","New Strategies to Prevent and Treat Opioid Addiction","Novel Therapeutic Options for Opioid Use Disorder and Overdose","Translation of Research to Practice for the Treatment of Opioid Addiction")
replace goal="Pain mgt" if goal=="" & inlist(rfa,"Clinical Research in Pain Management","Preclinical and Translational Research in Pain Management")

gen heal_funded="Y"
	label var heal_funded "Funded by HEAL?"
gen data_src="9"
	label var data_src "Data source used to populate row"

order rfa res_prg appl_id goal data_src heal_funded nih*

save "$temp/awards_fy24.dta", replace


* -- Combine with existing awards table -- *;
import delimited using "$extracts/awards_2024-12-02.csv", varnames(1) case(lower) bindquotes(strict) stringcols(_all) clear /*n=1667*/
gen in_mysql=1
append using "$temp/awards_fy24.dta"
sort appl_id
duplicates tag appl_id, generate(dupes)
drop if dupes==1 & in_mysql==1
drop dupes in_mysql

* -- Format -- *;

* Consistent choicelists for rfa, res_prg, goal *;
foreach var in rfa res_prg goal {
	replace `var'="" if `var'=="0"
	}
replace rfa="" if rfa=="HEAL-related"

replace res_prg="Justice Community Opioid Innovation Network (JCOIN)" if res_prg=="Justice Community Opioid Innovation Network"
replace res_prg="Pain Management Effectiveness Research Network (ERN)" if res_prg=="Pain Management Effectiveness Research Network"

* goal *;
replace goal="OUD" if goal=="" & rfa=="New Strategies to Prevent and Treat Opioid Addiction"

* data_src *;
replace data_src="" if data_src==" often res"

* heal_funded *;
replace heal_funded="Y" if heal_funded=="" & rfa!="" 
replace heal_funded="Y" if appl_id=="10593312" /*note: not returned on HFP website, but is an IMPOWR center, and mentions HDE in their abstract*/
	/* Note: there are 2 appl_ids with a missing value of all other cols; they don't appear on the HFP website and are both supplements, so even if the parent serial # were HEAL funded, they might or might not be. These will get ingested into MySQL as 0 values for heal_funded because the tinyint(1) var type doesn't recognize NULL */
replace heal_funded="NULL" if heal_funded==""
replace heal_funded="1" if heal_funded=="Y"
replace heal_funded="0" if heal_funded=="N"
/*
gen xheal_funded=.
	replace xheal_funded=1 if heal_funded=="Y"
	replace xheal_funded=0 if heal_funded=="N"
drop heal_funded
rename xheal_funded heal_funded
*/

* nih_foa_heal_lang *;
tostring nih_foa_heal_lang, replace
replace nih_foa_heal_lang="NULL" if nih_foa_heal_lang=="."


* -- Export -- *;
order appl_id goal rfa res_prg data_src heal_funded nih_aian nih_core_cde nih_foa_heal_lang
save "$der/awards_fy24.dta", replace
export delimited using "$der/awards_fy24.csv", replace

		/* * If heal_funded != "Y" but there's a non-missing rfa and/or res_prg, is this a data quality issue ? *;
		keep if heal_funded!="Y"
		sort appl_id
		merge 1:1 appl_id using "$extracts/reporter_2024-12-02.dta", keepusing(proj_num)
		drop if _merge==2
		save "$temp/review_hf_status.dta", replace /*n=49*/
		     * If heal_funded="N" and res_prg is non-missing, they DON'T appear when searching the HFP webiste *;
			 * If heal_funded="" and res_prg is non-missing, they DO appear on the HFP website *;
		*/




/* ----- 3. pi emails -- */
use "$der/merged_clean.dta", clear
keep appl_id contact_pi_email
rename contact_pi_email pi_email
duplicates drop
save "$temp/pi_emails_fy24.dta", replace

* Combine with existing pi_emails table, reformat *;
import delimited using "$extracts/pi_emails_2024-11-26.csv", varnames(1) case(lower) bindquotes(strict) stringcols(_all) clear
gen in_mysql=1
append using "$temp/pi_emails_fy24.dta"
sort appl_id
duplicates tag appl_id, generate(dupes)
drop if dupes==1 & in_mysql==1
keep pi_email appl_id
order appl_id
drop if appl_id==""
save "$der/pi_emails_fy24.dta", replace
export delimited using "$der/pi_emails_fy24.csv", replace
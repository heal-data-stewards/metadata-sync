/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_NIH_02_FY25_Aug													*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2025/08/22															*/
/* Date Last Updated: 2025/12/19													*/
/* Description:	This program reads in data about FY25 HEAL awards emailed by Rachel */
/*   Kelly as attachments an email from 2025/08/11.									*/
/*		1. Read in data																*/
/*		2. Clean data																*/
/*		3. Export awards table rows													*/
/*		4. Export pi_email table rows												*/
/*																					*/
/*																					*/
/* Notes:  																			*/
/*	- This program was re-run on date received August 2025 to address backwards-	*/
/*	  compatibility issues with new December 2025 data files, to present awards		*/
/*	  variables in a consistent and usable manner.									*/
/*  - In December 2025, NIH sent a new spreadsheet that contained *almost* all of 	*/
/*	  the same appl_ids contained in this file. 									*/
/*	- Some manual data formatting was done in spreadsheets before read-in to speed  */
/*	  processing. Namely, removed periods and spaces from file/sheet names.			*/
/*	- Because the existing awards table is downloaded and appended to new data, the	*/
/*    file "$der/awards_fy25.dta" includes prior "$der/awards_fy24.dta". Newly 		*/
/*	  derived files will always be cumulative in this way.							*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear 


/* ----- 1. Read in data -----*/
import excel using "$nih25\09-11-25 HEALFY25 PI emails.xlsx", sheet("FY25 in RePORTER") firstrow case(lower) allstring clear
missings dropvars, force
missings dropobs, force
save "$temp\pi_emails_fy25_aug.dta", replace


import excel using "$nih25\NIDA NINDS HEAL FY25 Awards Update_Data Stewards_09_10_25.xlsx", sheet("FY25 in RePORTER") firstrow case(lower) allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}

missings dropvars, force
missings dropobs, force
drop reporterprojinfo
		
		
	* Rename & label vars *;
	rename applid appl_id
		label var appl_id "Application ID"
	rename grant proj_num
		label var proj_num "Project number"
		replace proj_num=subinstr(proj_num," ","",.)
	rename title proj_title
	rename healresearchprogram res_prg
		label var res_prg "Research program"
	rename healfocusarea rfa
		label var rfa "Research Focus Area"
	rename piname  pi
		label var pi "PI Full Name(s)"
	rename foatitle nofo_title
	rename nofofoa nofo_number
	rename adminic adm_ic_code
		label var adm_ic_code "Administering Institute or Center code"
	rename institution org_nm
	rename healdatasharinglanguageinno xnih_noa_heal_lang /*in FY24 the info we got was nih_foa_heal_lang, so it was FOA and not NOA related */
	rename aiantribalawardsthatsteward nih_aian
	rename requiredtousehealcorecdes xnih_core_cde

	

	
/* ----- 2. Clean data -----*/

* Drop empty rows *;
drop if appl_id==""
drop if proj_num==""

* Research focus area *;
replace rfa="Novel Therapeutic Options for Opioid Use Disorder and Overdose" if rfa=="Novel Therapeutics for Opioid Use Disorder and Overdose"

* Research program *;
replace res_prg="" if res_prg=="BLANK"
replace res_prg="Preventing Opioid Use Disorder" if res_prg=="Preventing OUD"
foreach var in res_prg {
	replace `var'="Justice Community Opioid Innovation Network (JCOIN)" if `var'=="Justice Community Opioid Innovation Network"
	replace `var'="Focusing Medication Development to Prevent and Treat Opioid Use Disorder and Overdose" if `var'=="Focusing Medication Development to Prevent and Treat Opioid Use Disorders and Overdose"
	}

* PI name *; 
gen contact_pi=lower(pi)
split contact_pi, p(,)
rename contact_pi1 contact_pi_last
split contact_pi2, p(" ")
rename contact_pi21 contact_pi_first
rename contact_pi22 contact_pi_middle
drop contact_pi2 

* HEAL language in NOA *;
replace xnih_noa_heal_lang=lower(xnih_noa_heal_lang)
split xnih_noa_heal_lang, p(".")

gen nih_noa_heal_lang=""
label var nih_noa_heal_lang "NIH: HEAL data sharing language in NOA?"
replace nih_noa_heal_lang="0" if xnih_noa_heal_lang1=="no" | xnih_noa_heal_lang1=="n/a"
replace nih_noa_heal_lang="1" if xnih_noa_heal_lang1=="yes"

gen nih_noa_notes=strtrim(xnih_noa_heal_lang2)
label var nih_noa_notes "NIH: NOA language descriptive notes"

drop xnih_noa_heal_lang*


* HEAL Core CDEs *;
gen nih_core_cde="0" if lower(xnih_core_cde)=="no"
replace nih_core_cde="1" if lower(xnih_core_cde)=="yes"


* Save *;
order appl_id proj_num
keep appl_id contact_pi nih_noa_heal_lang nih_noa_notes rfa res_prg nih_aian nih_core_cde
sort appl_id
save "$temp\nida_ninds_healfy25_aug.dta", replace /*n=125*/
		
		
		
		
/* ----- 3. awards -----*/
use "$temp\nida_ninds_healfy25_aug.dta", clear
keep appl_id rfa res_prg nih*

* -- Generate variables -- *; 
gen goal=""
label var goal "Goal Category"
replace goal="Cross-Cutting Research" if rfa=="Cross-Cutting Research" | rfa=="Training the Next Generation of Researchers in HEAL"
replace goal="OUD" if goal=="" & inlist(rfa,"Enhanced Outcomes for Infants and Children Exposed to Opioids","New Strategies to Prevent and Treat Opioid Addiction","Novel Therapeutic Options for Opioid Use Disorder and Overdose","Translation of Research to Practice for the Treatment of Opioid Addiction")
replace goal="Pain mgt" if goal=="" & inlist(rfa,"Clinical Research in Pain Management","Preclinical and Translational Research in Pain Management")

gen heal_funded="1"
	label var heal_funded "Funded by HEAL?"
gen data_src="10"
	label var data_src "Data source used to populate row"

order rfa res_prg appl_id goal data_src heal_funded nih*

save "$temp/awards_fy25_aug.dta", replace

* -- Create key of appls sent in Aug 2025 -- *;
use "$temp/awards_fy25_aug.dta", clear
keep appl_id
sort appl_id
gen in_fy25aug=1
save "$doc/appls_in_fy25aug.dta", replace


* -- Combine with existing awards table -- *;
* Prep old awards data *;
import delimited using "$extracts/xold/awards_2025-09-14.csv", varnames(1) case(lower) bindquotes(strict) stringcols(_all) clear /*n=2020*/
gen in_mysql=1

* Correct nih_foa_heal_lang to null *;
rename nih_foa_heal_lang xnih_foa_heal_lang
sort appl_id
merge 1:1 appl_id using "$doc/correct_foa_values24.dta", keepusing(nih_foa_heal_lang)
replace nih_foa_heal_lang="NULL" if nih_foa_heal_lang==""
drop _merge xnih_foa_heal_lang

* Add fy25 awards data *;
append using "$temp/awards_fy25_aug.dta"
sort appl_id
duplicates tag appl_id, generate(dupes)
drop if dupes==1 & in_mysql==1 /* drop old record added via Change Log; no data loss */
drop dupes in_mysql


* -- Format -- *;
* Consistent choicelists for rfa, res_prg, goal *;
foreach var in rfa res_prg goal {
	replace `var'="" if `var'=="0"
	}
replace rfa="" if rfa=="HEAL-related"

foreach var in nih_noa_heal_lang nih_foa_heal_lang{
	replace `var'="NULL" if `var'==""
	}


* -- Export -- *;
order appl_id goal rfa res_prg data_src heal_funded nih_aian nih_core_cde nih_foa_heal_lang
save "$der/awards_fy25_aug.dta", replace /*n=2144*/
export delimited using "$der/awards_fy25_aug.csv", replace


* -- Save new key of correct FOA values *;
use "$der/awards_fy25_aug.dta", clear
keep appl_id nih_foa_heal_lang nih_noa_heal_lang
save "$doc/correct_foanoa_values.dta", replace
save "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\MySQL\Documentation\correct_foanoa_values.dta", replace

		
		
		
/* ----- 4. pi_emails -----*/
* Combine with existing pi_emails table, reformat *;
import delimited using "$extracts/xold/pi_emails_2025-09-02.csv", varnames(1) case(lower) bindquotes(strict) stringcols(_all) clear
gen in_mysql=1
append using "$temp/pi_emails_fy25_aug.dta"
sort appl_id
duplicates tag appl_id, generate(dupes)
drop if dupes==1 & in_mysql==1
keep pi_email appl_id
order appl_id
drop if appl_id==""
save "$der/pi_emails_fy25_aug.dta", replace
export delimited using "$der/pi_emails_fy25_aug.csv", replace

	
	


	
	
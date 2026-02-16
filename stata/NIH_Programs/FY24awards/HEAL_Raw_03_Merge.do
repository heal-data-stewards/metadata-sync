/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_Raw_03_Merge														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/11/23															*/
/* Date Last Updated: 2024/11/23													*/
/* Description:	This program merges NIH and HEAL funded projects website data. 		*/
/*		1. Merge data																*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear 


/* ----- 1. Merge data -----*/
use "$der\nih_clean.dta", clear
sort proj_num
merge m:1 proj_num using "$der\hfp_clean.dta"
	drop if _merge==2 /* note: there's only 1 _merge==2, and it's the Platform's new award */
drop _merge
save "$temp\merged.dta", replace



/* ----- 2. Clean data -----*/
use "$temp\merged.dta", clear
sort appl_id

* Research focus area *;
replace rfa="Novel Therapeutic Options for Opioid Use Disorder and Overdose" if rfa=="Novel Therapeutics for Opioid Use Disorder and Overdose"
gen final_rfa=hfp_rfa
	replace final_rfa=rfa if hfp_rfa==""
	label var final_rfa "Research Focus Area"

* Research program *;
foreach var in res_prg hfp_res_prg {
	replace `var'="Justice Community Opioid Innovation Network (JCOIN)" if `var'=="Justice Community Opioid Innovation Network"
	replace `var'="Focusing Medication Development to Prevent and Treat Opioid Use Disorder and Overdose" if `var'=="Focusing Medication Development to Prevent and Treat Opioid Use Disorders and Overdose"
	}
gen final_res_prg=hfp_res_prg
	replace final_res_prg=res_prg if final_res_prg==""
	label var final_res_prg "Research Program"

	* Collapse *;
	foreach var in rfa res_prg {
		drop `var' hfp_`var'
		rename final_`var' `var'
		}

* Contact PI *;
/* browse pi hfp_pis if pi!=hfp_pis */
/* Note: manually checked mismatches, and they are all only mismatch due to 1) hfp_pis being blank when pi is filled in, or 2) minor formatting, such as spaces around separators or periods after initials. Thus, we ignore hfp_pis entirely. */

drop hfp_pis
replace pi=lower(pi)
split pi, p(;)

gen contact_pi=""
forv i=1/9 {
	replace contact=pi`i' if regexm(pi`i', "contact") == 1
	}
replace contact_pi=pi1 if contact_pi==""
replace contact_pi=regexr(contact_pi, "\(contact\)", "") /* removes contact tag */
replace contact_pi=subinstr(contact_pi,"'","",.)
drop pi1-pi9

split contact_pi, p(,)
rename contact_pi1 contact_pi_last
split contact_pi2, p(" ")
rename contact_pi21 contact_pi_first
egen contact_pi_middle=concat(contact_pi22 contact_pi23), p(" ")
drop contact_pi2 contact_pi22 contact_pi23


* PI emails *;
replace all_pi_emails=lower(all_pi_emails)
split all_pi_emails, g(email) p(;)

gen contact_pi_email=""
replace contact_pi_email=email1 if email2=="" /* if there's only 1 email address, use it */

forv i=1/9 {
	gen email_match`i'= regexm(email`i',contact_pi_last)
	replace contact_pi_email=email`i' if email_match`i'==1 & contact_pi_email==""
	}
	
/*	* Check that only 1 email address matched author last name *;
	egen xcount=anycount(email_match1 email_match2 email_match3 email_match4 email_match5 email_match6 email_match7 email_match8 email_match9), v(1)
	tab xcount */
	
save "$temp/xmerged_clean.dta", replace
	
	* Manually mark which email to use for those that didn't match
	use "$temp/xmerged_clean.dta", clear
	keep if contact_pi_email=="" & all_pi_emails!=""
	keep appl_id contact_pi email* email_match* contact_pi_email
	order appl_id contact_pi email1 email_match1 email2 email_match2 email3 email_match3 email4 email_match4 email5 email_match5 email6 email_match6 email7 email_match7 email8 email_match8 email9 email_match9
	foreach var in email* {
		rename `var' z`var'
		}
	save "$temp/zemail_match.dta", replace
	export delimited using "$temp/zemail_match.csv", datafmt quote replace
	
	import delimited using "$temp/email_match.csv", varn(1) stringcols(1) clear
	keep appl_id z*
	drop zemail7-zemail_match9
	save "$temp/email_match.dta", replace
	
	* Manually look up PI emails in NIH Reporter for appl_ids where no PI email info was given *;
	use "$temp/xmerged_clean.dta", clear
	browse appl_id contact_pi if contact_pi_email==""
	import delimited using "$temp/email_lookup.csv", varn(1) stringcols(1) clear
	save "$temp/email_lookup.dta", replace

* Merge in manually-marked info *;
use "$temp/xmerged_clean.dta", clear
merge m:1 appl_id using "$temp/email_match.dta"
forv i=1/6 {
	replace contact_pi_email=zemail`i' if zemail_match`i'==1 & contact_pi_email==""
	}
	drop _merge z* email*
merge m:1 appl_id using "$temp/email_lookup.dta"
replace contact_pi_email=nih_pi_email if contact_pi_email==""
replace contact_pi_email=subinstr(contact_pi_email," ","",.)
drop nih_pi_email _merge
label var contact_pi_email "Email for contact PI"
save "$temp/merged_clean.dta", replace







/* ----- 3. Create new rows NIH wants -----*/
use "$temp/merged_clean.dta", clear
keep appl_id proj_num_spl_act_code contact_pi contact_pi_email nih_foa_heal_lang nih_aian nih_core_cde rfa res_prg /*tab_src */
sort appl_id
duplicates drop

* Flags in NIH spreadsheets *;
foreach var in nih_aian nih_core_cde nih_foa_heal_lang {
	by appl_id: egen z`var'=max(`var')
	copydesc `var' z`var'
	drop `var'
	rename z`var' `var' 
	}
	* Note: checked both max and min for foa_heal_lang, and results of each egen were identical *;

duplicates drop
duplicates list appl_id /*n=0*/

save "$der/merged_clean.dta", replace






/*
/* SBIRs */
/* all R43 and R44 */

/* Training grants */
/* all T90 and R90 */
	

There will need to be a new table for do not engage, checklist exempt, etc rules so that we don't have both raw data and computed columns in the awards table. Also, we'll need to use study IDs to take max's of these conditions so they show up for any award the PM might pull, not only the 2024 awards 


gen nih_dne=1
	label var nih_dne "NIH: Do not engage"

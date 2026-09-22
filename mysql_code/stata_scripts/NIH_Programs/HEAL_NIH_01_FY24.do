/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_NIH_01_FY24														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/11/11															*/
/* Date Last Updated: 2025/11/10													*/
/* Description:	This program reads in data about FY24 HEAL awards emailed by Jessica*/
/*   Mazerick as attachments on several emails from 11/7-11/8/24.					*/
/*		1. Read in data																*/
/*		2. Create a key of variables across tabs 									*/
/*		3. Compile data 															*/
/*		4. Read in HEAL Funded Projects data										*/
/*		5. Merge NIH-emailed and HFP website data									*/
/*		6. Clean merged data														*/
/*		7. Derive new flag variables												*/
/*		8. reporter: Output appl_ids for Reporter API query							*/
/*		9. awards: Add new rows and update table									*/
/*		10. pi emails: Add new rows and update table								*/
/*																					*/
/* Notes:  																			*/
/*	Some manual data formatting was done in spreadsheets before read-in to speed up */
/*  processing. These manual changes are noted by source spreadsheet below.			*/
/*	2025/11/10. Code from multiple programs was condensed into this program, and 	*/
/*		some filenames and filepaths were updated in the interests of simplification*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear 


/* ----- 1. Read in data -----*/

* -- a. CDEs // FY 24 HEAL CDE Studies_Updated_11.8 -- *;
/* Manual changes: space in tab names removed. created reporter_url to display full URL hyperlinked in the RePORTERLink column. Fixed a duplicated link in one row by manually looking up the project number in NIH Reporter and copying in the appl_id's url. */

global cde_tabs HEALCDEStudies Otherstudiestonote

import excel using "$nih24\FY 24 HEAL CDE Studies_Updated_11.8.xlsx", sheet("HEALCDEStudies") firstrow case(lower) allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}
		
	* Rename & label vars *;
	rename project proj_num
		label var proj_num "Project number"
	rename projecttitle proj_title
	rename healprogram res_prg
		label var res_prg "Research program"
	rename pinamesidentifycontactpii pi
		label var pi "PI Full Name(s)"
	rename nofotitle nofo_title
	rename institution org_nm
	rename pooptional prg_ofc
		label var prg_ofc "Program Officer Full Name"
	
	* Clean *;
	drop if proj_num==""
	split reporter_url, gen(rptr_) p(/)
	rename rptr_7 appl_id
		label var appl_id "Application ID"
	drop rptr* reporter_url
	order appl_id
		
	* Derive *;
	gen tab_src="HEALCDEStudies"
	label var tab_src "Name of tab source"
	
	* Save *;
	order appl_id proj_num
	sort appl_id
	drop status reporterlink notes
	save "$temp\HEALCDEStudies.dta", replace /*n=18*/
	descsave using "$temp\HEALCDEStudies.dta", list(,) idstr(HEALCDEStudies) saving("$temp\varlist_HEALCDEStudies.dta", replace)

	
	
* -- b. HEAL & CDEs // FY24 HEAL AWARDS_updated_30Oct2024 -- *;
/* Manual changes: space in tab names removed. created reporter_url to display full URL hyperlinked in the RePORTERLink column. Fixed a project number cell where the same project num was typed in twice. Added flags and removed single-cell headers for T90/R90 and SBIR. */

global healcde_tabs HEALstudies CDEstudies

foreach tab in $healcde_tabs {
	import excel using "$nih24\FY24 HEAL AWARDS_updated_30Oct2024.xlsx", sheet("`tab'") firstrow case(lower)allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}
		
	* Rename & label vars *;
	rename noadate awd_not_date
		label var awd_not_date "Award notice date"
	rename applid appl_id 
		label var appl_id "Application ID"
	rename project proj_num
		label var proj_num "Project number"
	rename projecttitle proj_title
	rename healfocusarea rfa
		label var rfa "Research Focus Area"
	rename healprogram res_prg
		label var res_prg "Research program"
	rename pinamesidentifycontactpii pi
		label var pi "PI Full Name(s)"
	rename nofotitle nofo_title
	rename nofonumber nofo_number
	rename administeringic adm_ic_code
		label var adm_ic_code "Administering Institute or Center code"
	rename institution org_nm
	rename locationcitystateexbethe org_st
		label var org_st "Organization State abbreviation"
	rename n org_cy
		label var org_cy "Organization city"
	rename pooptional prg_ofc
		label var prg_ofc "Program Officer Full Name"

	* Clean *;
	drop if appl_id==""
		
	* Derive *;
	gen tab_src="`tab'"
	label var tab_src "Name of tab source"
		
	* Save *;
	drop reporterlink councilroundoptional
	order appl_id proj_num
	sort appl_id
	save "$temp\\`tab'.dta", replace
	descsave using "$temp\\`tab'.dta", list(,) idstr(`tab') saving("$temp\varlist_`tab'.dta", replace)
	}
	

	
* -- c. NIDA // FY24 New NIDA HEAL Programs for Ecosystem_Revised_30Oct2024 -- *;
/* Manual changes: space in tab names removed. NCREW and tribal tab wouldn't read in at all, so copied contents to a fresh tab named ai_an. Removed empty rows. */

global nida_tabs HEALFOAsnontribal ai_an nonHEALFOAs

foreach tab in $nida_tabs {
	import excel using "$nih24\FY24 New NIDA HEAL Programs for Ecosystem_Revised_30Oct2024.xlsx", sheet("`tab'") firstrow case(lower) allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}
			
	* Rename & label vars *;
	rename noadate awd_not_date
		label var awd_not_date "Award notice date"
	rename projecttitle proj_title
	rename healfocusarea rfa
		label var rfa "Research Focus Area"
	rename healprogram res_prg
		label var res_prg "Research program"
	rename pinames pi
		label var pi "PI Full Name(s)"
	rename nofotitle nofo_title
	rename nofonumber nofo_number
	rename administeringic adm_ic_code
		label var adm_ic_code "Administering Institute or Center code"
	rename institution org_nm
	rename busofcst org_st
		label var org_st "Organization State abbreviation"
	rename busofccity org_cy
		label var org_cy "Organization city"
	rename poname prg_ofc
		label var prg_ofc "Program Officer Full Name"	
	rename applid appl_id 
		label var appl_id "Application ID"
		
	* Clean *;
	drop if appl_id=="" & proj_title==""
	egen proj_num=sieve(project), omit(" ")
		label var proj_num "Project number"
	replace res_prg="" if res_prg=="BLANK" | res_prg=="BLANK - Needs new tag in future"
		
	* Derive *;
	gen tab_src="`tab'"
	label var tab_src "Name of tab source"
	
	* Save *;
	drop project reporterprojinfo council rknotes
	order appl_id proj_num
	sort appl_id
	save "$temp\\`tab'.dta", replace	
	descsave using "$temp\\`tab'.dta", list(,) idstr(`tab') saving("$temp\varlist_`tab'.dta", replace)
	}

	* Add flags to tabs *;
	use "$temp\HEALFOAsnontribal.dta", clear
	gen nih_foa_heal_lang=1
	label var nih_foa_heal_lang "NIH: HEAL data sharing language in FOA?"
		* fix missing appl_ids *;
		replace appl_id="10893370" if proj_num=="5R33DA057747-04"
		replace appl_id="10818411" if proj_num=="5R33AT010619-04"
	sort appl_id
	save "$temp\HEALFOAsnontribal.dta", replace /*n=107*/
	
	use "$temp\nonHEALFOAs.dta", clear
	gen nih_foa_heal_lang=0
	label var nih_foa_heal_lang "NIH: HEAL data sharing language in FOA?"
	save "$temp\nonHEALFOAs.dta", replace /*n=33*/
	
	use "$temp\ai_an.dta", clear
	gen nih_aian=1
	label var nih_aian "NIH: AI/AN tribal award?"
	save "$temp\ai_an.dta", replace /*n=19*/
	
	
	
* -- d. AllFY24 // NIDA HEAL FY24 Projects Categorized_Type 5 Ecosystem Check -- *;
/* Manual changes: space in tab names removed. Formatting removed. Hidden rows unhidden. Manually created nih_foa_heal_lang=0 column for the rows which Jess indicated in email text did not include HEAL language in the FOA. */
/* Note: Jess clarified by email 11/18 that the hidden rows should be disregarded. However, we will check if some of the hidden rows may in fact be Type 5 awards that are continuations of existing HEAL awards. We handle this below by reading in the full file and 1) tagging the rows Jess indicated for our attention and 2) dropping the rows we can do nothing with b/c they lack an appl_id value. We can then investigate whether any hidden rows correspond to existing HEAL awards by matching project serial numbers to existing records in the MySQL database. */

import excel using "$nih24\NIDA HEAL FY24 Projects Categorized_Type 5 Ecosystem Check.xlsx", sheet("AllFY24") firstrow case(lower) allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}	
				
	* Rename & label vars *;
	rename reldt awd_not_date
		label var awd_not_date "Award notice date"
	rename grantnospace proj_num
		label var proj_num "Project number"
	rename title proj_title
		label var proj_title "Project title"
	rename healfocusarea rfa
		label var rfa "Research Focus Area"
	rename healresearchprogram res_prg
		label var res_prg "Research program"
	rename piname pi
		label var pi "PI Full Name(s)"
	rename nofofoa nofo_number
	rename foatitle nofo_title
	rename adminic adm_ic_code
		label var adm_ic_code "Administering Institute or Center code"
	rename institution org_nm
	rename busofcst org_st
		label var org_st "Organization State abbreviation"
	rename busofccity org_cy
		label var org_cy "Organization city"
	rename poname prg_ofc
		label var prg_ofc "Program Officer Full Name"	
	rename applid appl_id 
		label var appl_id "Application ID"
	rename amount tot_fund
		label var tot_fund "Total funding (by FY)"
	label var nih_foa_heal_lang "NIH: HEAL data sharing language in FOA?"
	destring nih_foa_heal_lang, replace
	
	
	* Clean *;
	drop if appl_id==""
	gen jess_disregard=1
	replace jess_disregard=0 if _n>=1 & _n<=23
	
	* Derive *;
	gen tab_src="allfy24"
	label var tab_src "Name of tab source"
	
	* Save *;
	drop type activity reporterprojinfo council notes
	order appl_id proj_num
	sort appl_id
	save "$temp\allfy24.dta", replace /*n=265*/
	descsave using "$temp\allfy24.dta", list(,) idstr(allfy24) saving("$temp\varlist_allfy24.dta", replace)




/* ----- 2. Create a key of variables across tabs ----- */
/* n=7 tabs of data */	
clear
foreach tab in allfy24 ai_an CDEstudies HEALCDEStudies HEALFOAsnontribal HEALstudies nonHEALFOAs {
	append using "$temp\varlist_`tab'.dta"
	}
drop vallab
rename name varname
sort varname varlab idstr 
by varname: gen xcount=_n
by varname: egen countnm=max(_n)
drop xcount
save "$doc\varlist_key.dta", replace




	
/* ----- 3. Compile data -----*/

* -- a. Combine CDE files -- *;
use "$temp\HEALCDEstudies.dta", clear
rename res_prg res_prg_abbv
merge 1:1 appl_id using "$temp\CDEstudies.dta", keepusing(res_prg awd_not_date all_pi_emails rfa nofo_number adm_ic_code org_st org_cy)
drop res_prg_abbv _merge
order $order_core $order_more
gen nih_core_cde=1
	label var nih_core_cde "NIH: Core CDEs required?"
 /* note: _merge=1 for one record only, =3 for all others. This checks out because Jess sent a second email saying she "missed one study" in her first emailed list of CDE */
save "$temp\nih_cdes.dta", replace


* -- b. Compile all data -- *;
clear
foreach tab in allfy24 ai_an HEALFOAsnontribal HEALstudies nonHEALFOAs {
	append using "$temp\\`tab'.dta"
	}
append using "$temp\nih_cdes.dta" /*n=514 */
sort appl_id proj_num

* Clean *;
drop if appl_id==""
drop if jess_disregard==1

replace res_prg="" if res_prg=="BLANK" | res_prg=="BLANK - Needs new tag in future" | res_prg=="https://heal.nih.gov/research/clinical-research/back-pain" | res_prg=="TBD"

gen proj_num_spl_act_code=substr(proj_num,2,3)
order proj_num_spl_act_code, after(proj_num)

	
foreach x of varlist nih* {
	destring `x', replace
	}
	
	* Cross-check NIH conditions *;
	gen xt90r90=1 if proj_num_spl_act_code=="T90" | proj_num_spl_act_code=="R90"
	by appl_id: egen any_xtr90=max(xt90r90)
	by appl_id: egen any_nihtr90=max(nih_t90r90) 
		/*browse if any_xtr90!=any_nihtr90 /*n=0*/ */
		
	gen xsbir=1 if proj_num_spl_act_code=="R43" | proj_num_spl_act_code=="R44"
	by appl_id: egen any_xsbir=max(xsbir)
	by appl_id: egen any_nihsbir=max(nih_sbir)
		/*browse if any_xsbir!=any_nihsbir /*n=24, some of these are tagged jess_disregard=1*/ */

	drop x* any* awd_not_date nofo* adm_ic_code org* prg_ofc tot_fund jess_disregard
		
save "$der/nih24_clean.dta", replace


	
/* ----- 4. Read in HEAL Funded Projects data -----*/
import delimited using "$hfp\awarded_2024-11-23.csv", varnames(1) stringc(_all) favorstrfixed bindquotes(strict) clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}
	
	* Rename & label vars *;
	rename project proj_num
		label var proj_num "Project number"
	rename researchfocusarea rfa
	rename researchprogram res_prg
		
	* Clean *;
	keep if yearawarded=="2024"
	drop administeringics institutions locations summary yearawarded
	
	foreach x of varlist projecttitle-investigators {
		rename `x' hfp_`x'
		}
	rename hfp_investigators hfp_pis
	
	sort proj_num
	duplicates drop
	save "$der\hfp24_clean.dta", replace




/* ----- 5. Merge NIH-emailed and HFP website data -----*/
use "$der\nih24_clean.dta", clear
sort proj_num
merge m:1 proj_num using "$der\hfp24_clean.dta"
	drop if _merge==2 /* note: there's only 1 _merge==2, and it's the Platform's new award */
drop _merge
save "$temp\merged.dta", replace



/* ----- 6. Clean merged data -----*/
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
replace contact_pi_email=email1 if email2==""

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
	
	* Manually update $temp/zemail_match.csv and save results as $doc/email_match.csv"
	
	/*import delimited using "$temp/email_match.csv", varn(1) stringcols(1) clear*/
	import delimited using "$doc/email_match.csv", varn(1) stringcols(1) clear
	keep appl_id z*
	drop zemail7-zemail_match9
	save "$temp/email_match.dta", replace
	
	* Manually look up PI emails in NIH Reporter for appl_ids where no PI email info was given *;
	/*use "$temp/xmerged_clean.dta", clear
	browse appl_id contact_pi if contact_pi_email==""
	import delimited using "$temp/email_lookup.csv", varn(1) stringcols(1) clear*/
	import delimited using "$doc/email_lookup.csv", varn(1) stringcols(1) clear
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




/* ----- 7. Derive new flag variables -----*/
use "$temp/merged_clean.dta", clear
keep appl_id proj_num_spl_act_code contact_pi contact_pi_email nih_foa_heal_lang nih_aian nih_core_cde rfa res_prg
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

save "$der/merged24_clean.dta", replace





/* ----- 8. reporter: Output appl_ids for Reporter API query -----*/
use "$der/merged24_clean.dta", clear
keep appl_id proj_num rfa res_prg
duplicates drop
drop if rfa==""
save "$der/fy24_new_awards_appls.dta", replace
export delimited using "$der/fy24_new_awards_appls.csv", nolabel quote replace



		
/* ----- 9. awards: Add new rows and update table -----*/
use "$der/merged24_clean.dta", clear
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
import delimited using "$extracts/xold/awards_2024-12-02.csv", varnames(1) case(lower) bindquotes(strict) stringcols(_all) clear /*n=1667*/
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
		
* -- Create key of correct FOA values -- *;
/* Note: MySQL sometimes replaces NULL with 0 values when exporting the awards table as a .csv, which results in incorrect values for the column nih_foa_heal_lang. Saving the below key file enables a workaround for this issue */
use "$der/awards_fy24.dta", clear
keep appl_id nih_foa_heal_lang
save "$doc/correct_foa_values24.dta", replace




/* ----- 10. pi emails: Add new rows and update table ----- */
use "$der/merged24_clean.dta", clear
keep appl_id contact_pi_email
rename contact_pi_email pi_email
duplicates drop
save "$temp/pi_emails_fy24.dta", replace

* Combine with existing pi_emails table, reformat *;
import delimited using "$extracts/xold/pi_emails_2024-11-26.csv", varnames(1) case(lower) bindquotes(strict) stringcols(_all) clear
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
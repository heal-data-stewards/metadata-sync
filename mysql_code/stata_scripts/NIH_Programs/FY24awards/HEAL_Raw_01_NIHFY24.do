/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_Raw_01_NIHFY24														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/11/11															*/
/* Date Last Updated: 2024/11/23													*/
/* Description:	This program reads in data about FY24 HEAL awards emailed by Jessica*/
/*   Mazerick as attachments on several emails from 11/7-11/8/24.					*/
/*		1. Read in data																*/
/*		2. Create a key of variables across tabs 									*/
/*		3. Compile data 															*/
/*																					*/
/*																					*/
/* Notes:  																			*/
/*	Some manual data formatting was done in spreadsheets before read-in to speed up */
/*  processing. These manual changes are noted by source spreadsheet below.			*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear 


/* ----- 1. Read in data -----*/

* -- a. CDEs // FY 24 HEAL CDE Studies_Updated_11.8 -- *;
/* Manual changes: space in tab names removed. created reporter_url to display full URL hyperlinked in the RePORTERLink column. Fixed a duplicated link in one row by manually looking up the project number in NIH Reporter and copying in the appl_id's url. */

global cde_tabs HEALCDEStudies Otherstudiestonote

import excel using "$nih\FY 24 HEAL CDE Studies_Updated_11.8.xlsx", sheet("HEALCDEStudies") firstrow case(lower) allstring clear
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
	* Note: the res_prg field may need cleaning. It appears to contain shorthand instead of the full Research Program name *;
	
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
	import excel using "$nih\FY24 HEAL AWARDS_updated_30Oct2024.xlsx", sheet("`tab'") firstrow case(lower)allstring clear
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
	import excel using "$nih\FY24 New NIDA HEAL Programs for Ecosystem_Revised_30Oct2024.xlsx", sheet("`tab'") firstrow case(lower) allstring clear
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

import excel using "$nih\NIDA HEAL FY24 Projects Categorized_Type 5 Ecosystem Check.xlsx", sheet("AllFY24") firstrow case(lower) allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}	
				
	* Rename & label vars *;
	rename reldt awd_not_date /* is this correct mapping? */
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
save "$der\varlist_key.dta", replace
	
	
	
	
	

	
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
		browse if any_xtr90!=any_nihtr90 /*n=0*/
		
	gen xsbir=1 if proj_num_spl_act_code=="R43" | proj_num_spl_act_code=="R44"
	by appl_id: egen any_xsbir=max(xsbir)
	by appl_id: egen any_nihsbir=max(nih_sbir)
		browse if any_xsbir!=any_nihsbir /*n=24, some of these are tagged jess_disregard=1*/

	drop x* any* awd_not_date nofo* adm_ic_code org* prg_ofc tot_fund jess_disregard
		
save "$der/nih_clean.dta", replace

	/*	
	* Check on number of unique appl_ids *;

		* Including all FY24 rows *;
		keep appl_id
		duplicates drop /*n=352*/

		* Excluding hidden FY24 rows *;
		drop if jess_disregard==1 /*n=242 dropped*/
		keep appl_id
		duplicates drop /*n=269*/
		*/	
	

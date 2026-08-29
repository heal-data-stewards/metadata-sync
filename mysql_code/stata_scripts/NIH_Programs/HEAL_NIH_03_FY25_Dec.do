/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_NIH_02_FY25														*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2025/12/17															*/
/* Date Last Updated: 2026/02/16													*/
/* Description:	This program reads in data about FY25 HEAL awards emailed by Rachel */
/*   Kelly as attachments an email from 2025/12/12.									*/
/*		1. Read in data																*/
/*		2. Clean data																*/
/*		3. Export awards table rows													*/
/*		4. Export pi_email table rows												*/
/*		5. Update CDE information													*/
/*																					*/
/*																					*/
/* Notes:  																			*/
/*	- 2026/02/16 - added section to incorporate updated CDE information from NIH	*/
/*	- 2026/01/16 - added lines 173 & 174 to drop appl_ids NIH said were included in */
/*	  error. Created key of appls on the Dec 2025 list.								*/
/*  - There were n=3 appl_ids contained in the August email data that are not in	*/
/*	  the December email data. All other appl_ids from August were repeated in the	*/
/*	  December email.																*/
/*  - In the filename, periods in the date were replaced with underscores. 			*/
/*  - Because the existing awards table is downloaded and appended to new data, the	*/
/*    file "$der/awards_fy25.dta" includes prior "$der/awards_fy24.dta". Newly 		*/
/*	  derived files will always be cumulative in this way.							*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear 


/* ----- 1. Read in data -----*/
import excel using "$nih25\NIDA NINDS HEAL FY25 Awards Update_Data Stewards_12_12_25.xlsx", sheet("FY25 in RePORTER") firstrow case(lower) allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}

missings dropvars, force
missings dropobs, force
drop reporterprojinfo adminic institution
				
	* Rename & label vars *;
	rename applid appl_id
		label var appl_id "Application ID"
	rename grant proj_num
		label var proj_num "Project number"
		replace proj_num=subinstr(proj_num," ","",.)
	rename title proj_title
	rename piname  pi
		label var pi "PI Full Name(s)"
	rename contactpiemail pi_email 
	rename foatitle nofo_title
	rename nofofoa nofo_number
	rename healfocusarea rfa
		label var rfa "Research Focus Area"
	rename healresearchprogram res_prg
		label var res_prg "Research program"		
	rename healdatasharinglanguageinno xnih_noa_heal_lang /*in FY24 the info we got was nih_foa_heal_lang, so it was FOA and not NOA related */
	rename aiantribalawardsthatsteward nih_aian
	rename requiredtousehealcorecdes xnih_core_cde
	rename poname prg_ofc
		label var prg_ofc "Program Officer Full Name"
	rename poemail prg_ofc_email
		label var prg_ofc_email "Program Officer Email"

	
/* ----- 2. Clean data -----*/
* Drop empty rows *;
drop if appl_id==""
drop if proj_num==""
/*drop if xnih_noa_heal_lang=="N/A" note: the n=2 awards with N/A are the Stewards and Platform OT awards*/

* Research focus area *;
replace rfa="Novel Therapeutic Options for Opioid Use Disorder and Overdose" if rfa=="Novel Therapeutics for Opioid Use Disorder and Overdose"
replace rfa="Cross-Cutting Research" if inlist(rfa,"Cross-Cutting Research","Cross Cutting Research")

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
egen contact_pi_middle=concat(contact_pi22 contact_pi23), punct(" ")
drop contact_pi2 contact_pi22 contact_pi23

* HEAL language in NOA *;
replace xnih_noa_heal_lang=lower(xnih_noa_heal_lang)
split xnih_noa_heal_lang, p(".")

gen nih_noa_heal_lang=""
replace nih_noa_heal_lang="0" if inlist(xnih_noa_heal_lang1,"no","n/a")
	replace nih_noa_heal_lang="0" if xnih_noa_heal_lang1=="n crew specific language"
replace nih_noa_heal_lang="1" if xnih_noa_heal_lang1=="yes"
replace nih_noa_heal_lang="NULL" if nih_noa_heal_lang==""

gen nih_noa_notes=xnih_noa_heal_lang2 
	replace nih_noa_notes=xnih_noa_heal_lang1 if substr(xnih_noa_heal_lang,1,6)=="n crew"
	replace nih_noa_notes=xnih_noa_heal_lang if xnih_noa_heal_lang=="contact jeremiah bertz"
	foreach var in nih_noa_notes {
		replace `var'=strtrim(`var')
		egen sieved`var'=sieve(`var') , keep(alphabetic space)
		drop `var'
		rename sieved`var' `var'
		}

drop xnih_noa_heal_lang*

* AI/AN *;
replace nih_aian="1" if nih_aian=="1 Check Any Previous Outreach"

* HEAL Core CDEs *;
replace xnih_core_cde=lower(xnih_core_cde)
tab xnih_core_cde
gen nih_core_cde="0" if xnih_core_cde=="no"
replace nih_core_cde="1" if xnih_core_cde=="yes"

* Save *;
order appl_id proj_num
sort appl_id
save "$temp\nida_ninds_healfy25_dec.dta", replace /*n=420*/


	* Create key of appls in 2025 awards list *;
	use "$temp\nida_ninds_healfy25_dec.dta", clear
	keep appl_id
	sort appl_id
	duplicates drop
	gen in_fy25dec=1
	save "$doc/appls_in_fy25dec.dta", replace
	
	
		
		
		
		
/* ----- 3. awards -----*/
use "$temp\nida_ninds_healfy25_dec.dta", clear
keep appl_id rfa res_prg nih*

* -- Generate variables -- *; 
gen goal=""
label var goal "Goal Category"
replace goal="Cross-Cutting Research" if rfa=="Cross-Cutting Research" | rfa=="Training the Next Generation of Researchers in HEAL"
replace goal="OUD" if goal=="" & inlist(rfa,"Enhanced Outcomes for Infants and Children Exposed to Opioids","New Strategies to Prevent and Treat Opioid Addiction","Novel Therapeutic Options for Opioid Use Disorder and Overdose","Translation of Research to Practice for the Treatment of Opioid Addiction")
replace goal="Pain mgt" if goal=="" & inlist(rfa,"Clinical Research in Pain Management","Preclinical and Translational Research in Pain Management")

gen heal_funded="1"
	label var heal_funded "Funded by HEAL?"
gen data_src="11"
	label var data_src "Data source used to populate row"

order rfa res_prg appl_id goal data_src heal_funded nih*

save "$temp/awards_fy25_dec.dta", replace /*n=420*/


* -- Combine with existing awards table -- *;
* Prep old awards data *;
use "$der/awards_fy25_aug.dta", clear

* Tag appls sent in august 2025 *;
sort appl_id
merge 1:1 appl_id using "$doc/appls_in_fy25aug.dta"
drop _merge

* Append Dec fy25 awards data *;
append using "$temp/awards_fy25_dec.dta"
sort appl_id
duplicates tag appl_id, generate(dupes)
drop if dupes==1 & in_fy25aug==1 /* use Dec 2025 instead of Aug 2025 data for appls that appeared in both emails */
drop dupes in_fy25aug

* Drop appl_ids NIH said were mistakenly included on August list *;
drop if inlist(appl_id,"11138769","11138589","11141218")


* -- Format -- *;
foreach var in nih_noa_heal_lang nih_foa_heal_lang nih_core_cde {
	replace `var'="NULL" if `var'==""
	}


* -- Export -- *;
order appl_id goal rfa res_prg data_src heal_funded nih_aian nih_core_cde nih_foa_heal_lang
save "$der/awards_fy25.dta", replace
export delimited using "$der/awards_fy25.csv", replace /*n=2439*/


* -- Save new key of correct FOA values *;
use "$der/awards_fy25.dta", clear
keep appl_id nih_foa_heal_lang nih_noa_heal_lang
save "$doc/correct_foanoa_values.dta", replace
save "C:\Users\smccutchan\OneDrive - Research Triangle Institute\Documents\HEAL\MySQL\Documentation\correct_foanoa_values.dta", replace

		
		
		
/* ----- 4. pi_emails -----*/
* Combine with existing pi_emails table, reformat *;
use "$temp\nida_ninds_healfy25_dec.dta", clear
keep appl_id pi_email /*n=418*/
gen in_fy25dec=1
append using "$der/pi_emails_fy25_aug.dta"
duplicates drop appl_id pi_email, force

sort appl_id
duplicates tag appl_id, generate(dupes)
drop if dupes==1 & in_fy25dec!=1
keep pi_email appl_id
order appl_id
drop if appl_id==""
save "$der/pi_emails_fy25.dta", replace
export delimited using "$der/pi_emails_fy25.csv", replace /*n=1749*/

		
		
		
/* ----- 5. Update CDE information -----*/

* Make key of CDE req field by appl_id with data sent in Feb *;
import excel using "$nih25\FY25 studies - HEAL CDE changes plus email info.xlsx", sheet("Sheet1") firstrow case(lower) allstring clear
	foreach x of varlist * {
		replace `x'=subinstr(`x', "`=char(10)'", "`=char(32)'", .) /* replace linebreaks inside cells with a space */
		replace `x'=strtrim(`x')
		replace `x'=stritrim(`x')
		replace `x'=ustrtrim(`x')
		label var `x' "`x'"
		}

missings dropvars, force
missings dropobs, force

keep applid requiredtousehealcorecdes
rename applid appl_id
replace appl_id="" if appl_id=="NA"
gen xnih_core_cde=lower(requiredtousehealcorecdes)

gen znih_core_cde="0" if xnih_core_cde=="no"
replace znih_core_cde="1" if substr(xnih_core_cde,1,3)=="yes"

keep appl_id znih_core_cde
sort appl_id
save "$temp/feb26_cdes.dta", replace

* Merge by appl_id *;
use "$temp/feb26_cdes.dta", clear
drop if appl_id==""
save "$temp/feb26_cdes_byappl.dta", replace

* Merge by I HEAL Z number *;



* Update awards table with latest CDE info *;
use "$raw/awards_$today.dta", clear 
sort appl_id
merge 1:1 appl_id using "$temp/feb26_cdes_byappl.dta", keepusing(znih_core_cde)
drop if _merge==2
drop _merge
replace nih_core_cde=znih_core_cde if znih_core_cde!=""
drop znih_core_cde
save "$der/awards_fy25.dta", replace
export delimited using "$der/awards_fy25.csv", replace /*n=2439*/














/* ARCHIVED CODE BELOW THIS POINT
		
		
		
/* ----- 5. New awards analysis -----*/
asdoc, text(--------------Aug2025 vs Dec2025 new award lists--------------) fs(14), save($dir/FY25NewAwards.doc) replace

asdoc, text(NIH emailed two new awards lists, one in August and one in December. The appl_ids contained on each overlap as shown below.) save($dir/FY25NewAwards.doc) append label

* Total new awards *;
* How many were on preliminary list
* How many new since then
use "$temp/awards_fy25_dec.dta", clear
gen in_fy25dec=1 if data_src=="11" /*n=420*/
sort appl_id
merge 1:1 appl_id using "$doc/appls_in_fy25aug.dta"
sort _merge
keep appl_id in*
label var in_fy25aug "On Aug list"
label var in_fy25dec "On Dec list"
asdoc tab in_fy25dec in_fy25aug, miss title(Cross-tab of Aug and Dec lists) save($dir/FY25NewAwards.doc) append label

keep if in_fy25aug==1 & in_fy25dec!=1
asdoc list appl_id, title(Appl IDs on the August but not December list) save($dir/FY25NewAwards.doc) append label

* Which are res_net; add or not add *;
* Pending Gabi/Sarah *;

List of awards we believe represent new studies and should be added to the Platform


	
	



	



	
	
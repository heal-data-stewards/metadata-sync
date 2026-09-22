/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_Raw_02_HFP															*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/11/23															*/
/* Date Last Updated: 2024/11/23													*/
/* Description:	This program reads in data about FY24 HEAL awards downloaded from 	*/
/*   the HEAL Funded Projects website on 11/23/2024.								*/
/*		1. Read in data																*/
/*																					*/
/* -------------------------------------------------------------------------------- */

clear 


/* ----- 1. Read in data -----*/

import delimited using "$hfp\awarded_$today.csv", varnames(1) stringc(_all) favorstrfixed bindquotes(strict) clear
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
	save "$der\hfp_clean.dta", replace
	
	
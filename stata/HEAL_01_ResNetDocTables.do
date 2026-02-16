/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_01_ResNetDocTables													*/
/* Programmer: Sabrina McCutchan (CDMS)												*/
/* Date Created: 2024/05/13															*/
/* Date Last Updated: 2026/01/23													*/
/* Description:	This program prepares the key of research networks by appl_id from	*/
/*  Google Drive for read into MySQL. A MySQL script creates the research_networks  */
/*	table in MySQL. This program also imports the research_networks table from MySQL*/
/*  for use in later code.															*/
/*		1. Import documentation tables												*/
/*																					*/
/* Notes:  																			*/
/*		- 2026/01/13 made this first program in the tree							*/
/*		- 2025/01/24 this program contains a subset of code from the retired 		*/
/*	 	  HEAL_02_ResNetTable.do that creates .csv versions of the documentation	*/
/*		  tables to read into MySQL.												*/
/*		- 2024/09/24 this procedure migrated to a MySQL Script 						*/
/*		- 2024/05/21 first generation of research_networks table 					*/
/*																					*/
/* -------------------------------------------------------------------------------- */



/* ----- 1. Import documentation tables ----- */
foreach tab in ref_table value_overrides_byappl {
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


	
/* Manual step: read output of step 1 into MySQL, then export research_networks table for read-in in program 01 */


use "$temp/ref_table.dta", clear
gen lng_rp=length(res_prg)
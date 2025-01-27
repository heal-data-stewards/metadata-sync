/* -------------------------------------------------------------------------------- */
/* Project: HEAL 																	*/
/* PI: Kira Bradford																*/
/* Program: HEAL_02_ResNetDocTables													*/
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
/*		- 2025/01/24 this program contains a subset of code from the retired 		*/
/*	 	  HEAL_02_ResNetTable.do that creates .csv versions of the documentation	*/
/*		  tables to read into MySQL.												*/
/*		- 2024/09/24 this procedure is being migrated to a MySQL Script 			*/
/*		- 2024/05/21 first run of code to generate research_networks table 			*/
/*																					*/
/* -------------------------------------------------------------------------------- */




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

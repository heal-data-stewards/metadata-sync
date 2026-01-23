# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 13:17:53 2026

@author: mariad
"""

# Run this script to preparer the reporter file for ingestion into MySQL
# Before running: 1) update input file to read and 2) update ouptup file to save


import pandas as pd

# Read CSV
df = pd.read_csv(
    r"C:\Users\mariad\Downloads\heal_awards_reporter_all_01212026.csv"
)

# 1️DROP columns you do NOT want
cols_to_drop = [
    "agency_ic_fundings.direct_cost_ic",
    "agency_ic_fundings.indirect_cost_ic",
    "date_added",
    "geo_lat_lon.lat",
    "geo_lat_lon.lon",
    "organization.org_ueis",
    "organization.primary_duns",
    "organization.primary_uei"
]

df.drop(columns=cols_to_drop, inplace=True, errors="ignore")

# 2   Rename ALL columns
rename_map = {
    "activity_code":"act_code",
    "agency_ic_admin.abbreviation":"adm_ic",
    "agency_ic_admin.code":"adm_ic_code",
    "agency_ic_admin.name":"adm_ic_nm",
    "direct_cost_amt":"amt_dir",
    "appl_id":"appl_id",
    "arra_funded":"arra_fund",
    "award_notice_date":"awd_not_date",
    "award_type":"awd_ty",
    "budget_end":"bgt_end",
    "budget_start":"bgt_strt",
    "cfda_code":"cfda_code",
    "cong_dist":"cong_dist",
    "covid_response":"covid_res",
    "core_project_num":"cr_pro_num",
    "contact_pi_name":"ctc_pi_nm",
    "fiscal_year":"fisc_yr",
    "opportunity_number":"ful_foa",
    "agency_ic_fundings.abbreviation":"fund_ic",
    "agency_ic_fundings.name":"fund_ic_nm",
    "agency_ic_fundings.total_cost":"fund_ic_tot_cst",
    "funding_mechanism":"fund_mech",
    "agency_code":"ic_code",
    "agency_ic_fundings.code":"ic_fund_code",
    "agency_ic_fundings.fy":"ic_fund_yr",
    "indirect_cost_amt":"indct_cst_amt",
    "is_active":"is_act",
    "is_new":"is_new",
    "mechanism_code_dc":"mech_code_dc",
    "organization.org_country":"org_ctry",
    "organization.org_city":"org_cy",
    "organization.dept_type":"org_dept_type",
    "organization.org_duns":"org_duns",
    "organization.external_org_id":"org_ext_id",
    "organization.org_fips":"org_fips",
    "organization.org_ipf_code":"org_ipf_code",
    "organization.org_name":"org_nm",
    "organization.org_state":"org_st",
    "organization_type.code":"org_ty_code",
    "organization_type.name":"org_ty_nm",
    "organization_type.is_other":"org_ty_oth",
    "organization.org_zipcode":"org_zip_code",
    "phr_text":"phr_text",
    "principal_investigators.full_name":"pi",
    "principal_investigators.first_name":"pi_fst_nm",
    "principal_investigators.is_contact_pi":"pi_is_ctc",
    "principal_investigators.last_name":"pi_lst_nm",
    "principal_investigators.middle_name":"pi_mid_nm",
    "principal_investigators.profile_id":"pi_prof_id",
    "principal_investigators.title":"pi_title",
    "pref_terms":"pref_terms",
    "program_officers.full_name":"prg_ofc",
    "program_officers.first_name":"prg_ofc_fst_nm",
    "program_officers.last_name":"prg_ofc_lst_nm",
    "program_officers.middle_name":"prg_ofc_mid_nm",
    "abstract_text":"proj_abs",
    "project_end_date":"proj_end_date",
    "project_num_split.full_support_year":"proj_nm_spl_supp_yr",
    "project_num_split.support_year":"proj_nm_spl_yr",
    "project_num":"proj_num",
    "project_num_split.activity_code":"proj_num_spl_act_code",
    "project_num_split.ic_code":"proj_num_spl_ic_code",
    "project_num_split.suffix_code":"proj_num_spl_sfx_code",
    "project_num_split.appl_type_code":"proj_num_spl_ty_code",
    "project_num_split.serial_num":"proj_ser_nm_spl",
    "project_serial_num":"proj_ser_num",
    "project_start_date":"proj_strt_date",
    "project_title":"proj_title",
    "project_detail_url":"proj_url",
    "spending_categories":"spd_cat",
    "spending_categories_desc":"spd_cat_[0]",
    "full_study_section.sra_designator_code":"sty_sec_ful_des_code",
    "full_study_section.sra_flex_code":"sty_sec_ful_flex_code",
    "full_study_section.group_code":"sty_sec_ful_grp_code",
    "full_study_section.name":"sty_sec_ful_nm",
    "full_study_section.srg_code":"sty_sec_ful_srg_code",
    "full_study_section.srg_flex":"sty_sec_ful_srg_flex",
    "subproject_id":"subproj_id",
    "award_amount":"tot_fund",
    "terms":"trms"
}

df.rename(columns=rename_map, inplace=True)


# Save result
df.to_csv(
    r"C:\Users\mariad\Downloads\reporter_01212025.csv",
    index=False
)

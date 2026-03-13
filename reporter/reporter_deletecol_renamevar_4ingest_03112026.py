# -*- coding: utf-8 -*-
"""
Created on Tue Jan 20 13:17:53 2026
updated on Wed March 11 14:31:45 2026: deleted colums based on MySQL team decission

@author: mariad
"""

# Run this script to preparer the reporter file for ingestion into MySQL
# Before running: 1) update input file to read and 2) update ouptup file to save


import pandas as pd

# Read CSV
df = pd.read_csv(
    r"C:\Users\mariad\OneDrive - Research Triangle Institute\Documents\HEAL Relational Database\MySQL updates\all_mysql_update_12222025\reporter_all_01212026\heal_awards_reporter_all_01212026.csv"
)

# 1️DROP columns you do NOT want
cols_to_drop = [
    "agency_code",
    "agency_ic_fundings.direct_cost_ic",
    "agency_ic_fundings.indirect_cost_ic",
    "agency_ic_fundings.total_cost",
    "cfda_code",
    "cong_dist",
    "full_study_section.group_code",
    "full_study_section.name",
    "full_study_section.sra_designator_code",
    "full_study_section.sra_flex_code",
    "full_study_section.srg_code",
    "geo_lat_lon.lat",
    "geo_lat_lon.lon",
    "is_active",
    "is_new",
    "mechanism_code_dc",
    "organization_type.code",
    "organization_type.is_other",
    "organization_type.name",
    "organization.dept_type",
    "organization.external_org_id",
    "organization.org_city",
    "organization.org_country",
    "organization.org_duns",
    "organization.org_fips",
    "organization.org_ipf_code",
    "organization.org_state",
    "organization.org_ueis",
    "organization.org_zipcode",
    "organization.primary_duns",
    "organization.primary_uei",
    "project_detail_url",
"spending_categories"
]

df.drop(columns=cols_to_drop, inplace=True, errors="ignore")

# 2   Rename ALL columns
rename_map = {
    "abstract_text":"proj_abs",
    "activity_code":"act_code",
    "agency_ic_admin.abbreviation":"adm_ic",
    "agency_ic_admin.code":"adm_ic_code",
    "agency_ic_admin.name":"adm_ic_nm",
    "agency_ic_fundings.abbreviation":"fund_ic",
    "agency_ic_fundings.code":"ic_fund_code",
    "agency_ic_fundings.fy":"ic_fund_yr",
    "agency_ic_fundings.name":"fund_ic_nm",
    "appl_id":"appl_id",
    "arra_funded":"arra_fund",
    "award_amount":"tot_fund",
    "award_notice_date":"awd_not_date",
    "award_type":"awd_ty",
    "budget_end":"bgt_end",
    "budget_start":"bgt_strt",
    "contact_pi_name":"ctc_pi_nm",
    "core_project_num":"cr_pro_num",
    "direct_cost_amt":"amt_dir",
    "fiscal_year":"fisc_yr",
    "funding_mechanism":"fund_mech",
    "indirect_cost_amt":"indct_cst_amt",
    "opportunity_number":"ful_foa",
    "organization.org_name":"org_nm",
    "phr_text":"phr_text",
    "pref_terms":"pref_terms",
    "principal_investigators.first_name":"pi_fst_nm",
    "principal_investigators.full_name":"pi",
    "principal_investigators.is_contact_pi":"pi_is_ctc",
    "principal_investigators.last_name":"pi_lst_nm",
    "principal_investigators.middle_name":"pi_mid_nm",
    "principal_investigators.profile_id":"pi_prof_id",
    "principal_investigators.title":"pi_title",
    "program_officers.first_name":"prg_ofc_fst_nm",
    "program_officers.full_name":"prg_ofc",
    "program_officers.last_name":"prg_ofc_lst_nm",
    "program_officers.middle_name":"prg_ofc_mid_nm",
    "project_end_date":"proj_end_date",
    "project_num":"proj_num",
    "project_num_split.activity_code":"proj_num_spl_act_code",
    "project_num_split.appl_type_code":"proj_num_spl_ty_code",
    "project_num_split.full_support_year":"proj_nm_spl_supp_yr",
    "project_num_split.ic_code":"proj_num_spl_ic_code",
    "project_num_split.serial_num":"proj_ser_nm_spl",
    "project_num_split.suffix_code":"proj_num_spl_sfx_code",
    "project_num_split.support_year":"proj_nm_spl_yr",
    "project_serial_num":"proj_ser_num",
    "project_start_date":"proj_strt_date",
    "project_title":"proj_title",
    "spending_categories_desc":"spd_cat_[0]",
    "subproject_id":"subproj_id",
    "terms":"trms",
    "date_added":"date_added"
}

df.rename(columns=rename_map, inplace=True)


# Save result
df.to_csv(
    r"C:\Users\mariad\OneDrive - Research Triangle Institute\Documents\HEAL Relational Database\MySQL updates\all_mysql_update_12222025\reporter_all_01212026\reporter_03112026.csv",
    index=False
)

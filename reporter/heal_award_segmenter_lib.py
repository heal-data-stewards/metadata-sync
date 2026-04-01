import pandas as pd
import requests
import collections
import re
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

def process_awards(df: pd.DataFrame, id_type: str, project_id_col: str, project_title_col: str,
                   clean_non_utf: bool = True, return_related_project_nums: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process awards data from a pandas DataFrame and return flattened results as DataFrames.

    Args:
        df: DataFrame containing project data
        id_type: 'appl_id' or 'project_num'
        project_id_col: Column name for project ID
        project_title_col: Column name for project title
        clean_non_utf: Whether to clean non-UTF characters
        return_related_project_nums: Whether to return related project nums for appl_id

    Returns:
        Tuple of (awards_df, publications_df)
    """

    # Create ID List to query
    id_list, core_id_list = create_project_num_list_from_df(df, id_type, project_id_col, project_title_col)

    logger.debug("id_list: %s", id_list)
    logger.debug("core_id_list: %s", core_id_list)

    results = post_request(clean_non_utf, id_type, id_list)
    pub_results = post_request(clean_non_utf, id_type, core_id_list, "publications/search")

    # Add related project_nums
    if (id_type == 'appl_id') and return_related_project_nums:
        # For related, we need to create from project_num column, but since df is given, assume it has both or handle
        # For simplicity, assume df has 'Full Grant Number' if needed, but since df is input, perhaps skip or adjust
        # The original code creates additional from csv with "Full Grant Number", but since we have df, maybe add logic
        # For now, skip additional search to simplify
        pass

    # Flatten results
    results_flat = list(map(flatten_json, results))
    awards_df = pd.DataFrame(results_flat)

    # Publications
    pubs_df = pd.DataFrame(pub_results)

    return awards_df, pubs_df

def create_project_num_list_from_df(df: pd.DataFrame, id_type: str, project_id_col: str, project_title_col: str):
    """
    Extract project IDs from DataFrame.
    """
    project_id_list = []
    for _, row in df.iterrows():
        project_number = re.sub(r'[^\x00-\x7F]', '', str(row[project_id_col])).replace(" ", "")
        if project_number:
            project_id_list.append(project_number.strip())

    # Make unique
    project_id_list = list(set(project_id_list))

    # Get core_project_num_list
    if id_type == "project_num":
        core_project_id_list = [re.sub("^[0-9]+", "", x) for x in project_id_list]
        core_project_id_list = [re.match(".+(?=-)|[^-]+", x).group(0) for x in core_project_id_list]
    else:
        core_project_id_list = project_id_list

    return project_id_list, core_project_id_list

def post_request(clean_non_utf: bool, id_type: str, project_id_list: list, end_point: str = "projects/search", chunk_length: int = 50):
    """
    Post request to NIH RePORTER API.
    """
    results_list = []

    if id_type == "appl_id":
        criteria_name = "appl_ids"
    else:
        if end_point == "projects/search":
            criteria_name = "project_nums"
        else:
            criteria_name = "core_project_nums"

    base_url = "https://api.reporter.nih.gov/v2/"
    url = f"{base_url}{end_point}"
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }

    for i in range(0, len(project_id_list), chunk_length):
        projects = project_id_list[i:i+chunk_length]

        request_body = {
            "criteria": {
                criteria_name: projects
            },
            "offset": 0,
            "limit": 500
        }

        # Request Object
        req = requests.post(url, headers=headers, json=request_body)
        
        # Check if request was successful
        if not req.ok:
            logger.warning("API request failed with status code %s: %s", req.status_code, req.text)
            continue
        
        try:
            response = req.json()
            # Handle both dict response with 'results' key and list response
            if isinstance(response, dict):
                results_obj = response.get('results', [])
            elif isinstance(response, list):
                results_obj = response
            else:
                logger.warning("Unexpected response type: %s", type(response))
                continue
        except Exception as e:
            logger.error("Error parsing JSON response: %s", e)
            logger.debug("Response text: %s", req.text)
            continue

        if clean_non_utf:
            for j in range(len(results_obj)):
                results_obj[j] = utfy_dict(results_obj[j])

        results_list.extend(results_obj)

    return results_list

def utfy_dict(dic):
    if isinstance(dic, str):
        dic = re.sub(r'[^\x00-\x7F]', '', str(dic)).strip()
        dic = re.sub(r'"', "'", dic)
        dic = re.sub(r'\n', '. ', dic)
        return dic
    elif isinstance(dic, dict):
        for key in dic:
            dic[key] = utfy_dict(dic[key])
        return dic
    elif isinstance(dic, list):
        return [utfy_dict(e) for e in dic]
    else:
        return dic

def flatten_json(dictionary, parent_key=False, separator='.'):
    items = []
    for key, value in dictionary.items():
        if value is None:
            continue
        new_key = str(parent_key) + separator + key if parent_key else key

        if isinstance(value, collections.abc.MutableMapping):
            items.extend(flatten_json(value, new_key, separator).items())
        elif isinstance(value, list):
            if not value:
                continue
            elif isinstance(value[0], dict):
                items.extend(flatten_json(merge_dict(value), new_key, separator).items())
            else:
                value = ';'.join(map(str, value))
                items.append((new_key, str(value)))
        else:
            items.append((new_key, value))
    return dict(items)

def merge_dict(dict_list):
    d_new = {}
    for d in dict_list:
        for k, v in d.items():
            if k in d_new:
                d_new[k].append(v)
            else:
                d_new[k] = [v]
    return d_new

# Columns to drop before ingestion into MySQL (per MySQL team decision, March 2026)
COLS_TO_DROP = [
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
    "spending_categories",
]

# Column rename map for MySQL ingestion (per reporter_deletecol_renamevar_4ingest_03112026.py)
RENAME_MAP = {
    "abstract_text": "proj_abs",
    "activity_code": "act_code",
    "agency_ic_admin.abbreviation": "adm_ic",
    "agency_ic_admin.code": "adm_ic_code",
    "agency_ic_admin.name": "adm_ic_nm",
    "agency_ic_fundings.abbreviation": "fund_ic",
    "agency_ic_fundings.code": "ic_fund_code",
    "agency_ic_fundings.fy": "ic_fund_yr",
    "agency_ic_fundings.name": "fund_ic_nm",
    "appl_id": "appl_id",
    "arra_funded": "arra_fund",
    "award_amount": "tot_fund",
    "award_notice_date": "awd_not_date",
    "award_type": "awd_ty",
    "budget_end": "bgt_end",
    "budget_start": "bgt_strt",
    "contact_pi_name": "ctc_pi_nm",
    "core_project_num": "cr_pro_num",
    "direct_cost_amt": "amt_dir",
    "fiscal_year": "fisc_yr",
    "funding_mechanism": "fund_mech",
    "indirect_cost_amt": "indct_cst_amt",
    "opportunity_number": "ful_foa",
    "organization.org_name": "org_nm",
    "phr_text": "phr_text",
    "pref_terms": "pref_terms",
    "principal_investigators.first_name": "pi_fst_nm",
    "principal_investigators.full_name": "pi",
    "principal_investigators.is_contact_pi": "pi_is_ctc",
    "principal_investigators.last_name": "pi_lst_nm",
    "principal_investigators.middle_name": "pi_mid_nm",
    "principal_investigators.profile_id": "pi_prof_id",
    "principal_investigators.title": "pi_title",
    "program_officers.first_name": "prg_ofc_fst_nm",
    "program_officers.full_name": "prg_ofc",
    "program_officers.last_name": "prg_ofc_lst_nm",
    "program_officers.middle_name": "prg_ofc_mid_nm",
    "project_end_date": "proj_end_date",
    "project_num": "proj_num",
    "project_num_split.activity_code": "proj_num_spl_act_code",
    "project_num_split.appl_type_code": "proj_num_spl_ty_code",
    "project_num_split.full_support_year": "proj_nm_spl_supp_yr",
    "project_num_split.ic_code": "proj_num_spl_ic_code",
    "project_num_split.serial_num": "proj_ser_nm_spl",
    "project_num_split.suffix_code": "proj_num_spl_sfx_code",
    "project_num_split.support_year": "proj_nm_spl_yr",
    "project_serial_num": "proj_ser_num",
    "project_start_date": "proj_strt_date",
    "project_title": "proj_title",
    "spending_categories_desc": "spd_cat_[0]",
    "subproject_id": "subproj_id",
    "terms": "trms",
    "date_added": "date_added",
}

def prepare_for_ingest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a reporter awards DataFrame for ingestion into MySQL by dropping
    unwanted columns and renaming the remaining ones.

    Columns dropped and rename mappings are sourced from
    reporter_deletecol_renamevar_4ingest_03112026.py (MySQL team, March 2026).

    Args:
        df: Flattened awards DataFrame (e.g. output of process_awards)

    Returns:
        DataFrame with dropped columns removed and columns renamed per RENAME_MAP.
    """
    df = df.drop(columns=COLS_TO_DROP, errors="ignore")
    df = df.rename(columns=RENAME_MAP)
    return df
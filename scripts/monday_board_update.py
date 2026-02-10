####
# This script takes in Excel export from HEAL Studies Monday Board, and Mysql tables exported into csv format from the HEAL MySQL HEAL data store
# and generates an updated version for the HEAL Studies Monday Board.
####


## If running on Google Colab, uncomment the following lines
# from google.colab import drive
# drive.mount('/content/drive', force_remount=True)
import pandas as pd
from pathlib import Path
import numpy as np
import re
from datetime import datetime
import click
import logging
import sys

## Dictionary that maps MySQL fields to Monday Board column names
RENAME_DICT = {'proj_num':'Project #',
                'proj_title':'Title',
                    'rfa':'Research Focus',
                    'res_prg':'Research Program',
                    'ctc_pi_nm':'Contact PI',
                    'pi_email':'Contact Email',
                    'adm_ic':'Administering IC',
                    'prg_ofc':'NIH PO',
                    'org_nm': 'Institution(s)',
                    'pi':'PI(s)',
                    'org_cy':'City',
                    'org_st':'State',
                    'act_code':'Activity Code',
                    'awd_ty':'Award Type',
                    'fisc_yr':'Award Year',
                    'tot_fund':'Total Funded',
                    'proj_abs':'Summary',
                    'fund_mech': 'SBIR/STTR',
                    'proj_strt_date':'Project Start',
                    'proj_end_date':'Project End',
                    'proj_url':'Reporter Link',
                    'res_net':'Research Network',
                    'time_of_registration':'Platform Reg Time',
                    'overall_percent_complete':'CEDAR Form %',
                    'repository_name' : 'Repo per Platform',
                    'archived':'Archived',
                    'heal_funded':'HEAL-Related',
                    'do_not_engage':'Do not Engage',
                    'data_type': 'Data Type',
                    'checklist_exempt_all':'Checklist Exempt',
                    'po_email': 'NIH PO Email'
                    }

RENAME_DICT_MDS = {'project_num':'Project #',
                    'project_title':'Title',
                    'investigators_name':'PI(s)',
                    'award_type':'Award Type',
                    'year_awarded':'Award Year',
                    'award_amount':'Total Funded',
                    'study_name':'Summary',
                    'project_end_date':'Project End',
                    'nih_reporter_link':'Reporter Link',
                    'time_of_registration':'Platform Reg Time',
                    'overall_percent_complete':'CEDAR Form %',
                    'repository_name' : 'Repo per Platform',
                    'archived':'Archived',
                    }

RENAME_DICT_CTN = {'project_num':'Project #',
                '   project_title':'Title',
                    'investigators_name':'PI(s)',
                    'award_type':'Award Type',
                    'year_awarded':'Award Year',
                    'award_amount':'Total Funded',
                    'study_name':'Summary',
                    'proj_end_date':'Project End',
                    'nih_reporter_link':'Reporter Link',
                    'time_of_registration':'Platform Reg Time',
                    'overall_percent_complete':'CEDAR Form %',
                    'repository_name' : 'Repo per Platform',
                    'archived':'Archived',
                    }


# input_dir = Path("/Users/hinashah/Documents/HEAL/MondayUpdate_Feb2025")


def get_unique_values(df:pd.DataFrame, col_name:str='appl_id'):
    if col_name in df.columns:
        return df[ ~pd.isna(df[col_name])][col_name].drop_duplicates()
    return None

def get_na_count(df:pd.DataFrame, col_name:str='appl_id'):
    if col_name in df.columns:
        return len(df[pd.isna(df[col_name])])
    return -1

def create_mysql_subset(in_df:pd.DataFrame, extra_fields = ['appl_id'], rename_dict=RENAME_DICT):
        subset = in_df[[k for k in rename_dict.keys() if k in in_df.columns] + extra_fields].copy(deep=True)
        subset.rename(columns={k:v for k,v in rename_dict.items() if k in in_df.columns}, inplace=True)
        return subset

'''
Import study lookup table from MySQL database for HEAL.
This lookup table is the result of the logic to identifying HDPIDs (Unique HEAL Identifiers),
and their most current appl_ids (NIH Award numbers), and appl_ids that they are associated 
with on HEAL Data Platform.
'''
def import_study_lookup_table(input_dir:Path):
    gt_file = pd.read_csv(input_dir/"study_lookup_table.csv", dtype=str)
    gt_file.replace("0", np.nan, inplace=True)

    logging.info(f"Number of entries in study lookup table: {len(gt_file)}")

    ### QC the lookup table:
    for k in gt_file.columns:
        logging.info(f"Number of distinct values in --{k}--: {len(get_unique_values(gt_file, k))}")
        logging.info(f"---- NA count: {get_na_count(gt_file, k)}")
        ## Look for patterns?
        if 'appl' in k:
            d = gt_file[[ (not pd.isna(l)) and (not l.isdigit()) for l in gt_file[k] ]]
            if len(d) > 0:
                logging.warning(f"Number of funky looking appl_ids: {len(d)}")
        elif k == 'study_hdp_id':
            d = gt_file[ [ (not pd.isna(l)) and (re.match(r'HDP[\d]+', l) is None) for l in gt_file[k]]]
            if len(d) > 0:
                logging.warning(f" Number of funky looking HDPIDs: {len(d)}")
    return gt_file

'''
Import Monday HEAL Studies Board data from an excel file export from Monday 
'''
def import_monday_board(input_dir:Path, file_name="HEAL_Studies_*.xlsx", rows_to_skip = 4):
    board_files_list = list(input_dir.glob(file_name))
    print(board_files_list)
    df = pd.read_excel(board_files_list[0], skiprows=rows_to_skip)
    print(df.columns)
    monday_board = df[~(df.Name.isin(['Studies under investigation', 'Name', 'Studies Not Added to Platform', 'CTN Protocols', 'Pending assessment results']))]
    monday_board = monday_board[~pd.isna(monday_board['Name'])]
    logging.info(f"Number of records on Monday Board: {len(monday_board)}")
    return monday_board

'''
Compare Lookup Table and Monday board export to find what's missing.
'''
def compare_study_loookup_monday(gt_file:pd.DataFrame, monday_board:pd.DataFrame):
    ### - Start with the lookup table from MySQL Database
    ## From Study lookup table, get unique set of most_recent_appl, study_hdp_id, and study_hdp_id_appl
    lookup_fields = gt_file[['study_hdp_id', 'study_most_recent_appl', 'study_hdp_id_appl']].copy(deep=True).drop_duplicates()
    ## Create a column "Key" that will either have study_hdp_id OR most_recent_appl when study_hdp_id is empty
    lookup_fields['key'] = [m if pd.isna(h) else h for (h, m) in lookup_fields[['study_hdp_id', 'study_most_recent_appl']].values ]

    ### A few checks on study lookup table:
    ## How many of the "keys" from Monday board are in lookup fields?
    logging.info(f"Number records from Monday already in lookup table: {len(monday_board[monday_board.Name.isin(lookup_fields.key)])}")
    ## How many of the keys from MOnday board are not there in looup fields
    mondayboard_missingin_lookup = monday_board[~monday_board.Name.isin(lookup_fields.key)]
    logging.info(f"Number records from Monday that are not in lookup table (Consider these as discrepancies **Investigate**): {len(mondayboard_missingin_lookup)}")
    ## How many of the keys from lookup fields are not there in Monday??
    lookup_missingin_mondayboard = lookup_fields[~lookup_fields.key.isin(monday_board.Name)]
    logging.info(f"Number records from lookup table that are not on Monday (Potentially new entries): {len(lookup_missingin_mondayboard)}")

    logging.info("Entries in Monday that are not in lookup table")
    ## TODO: Should this rather be an export?
    logging.debug(mondayboard_missingin_lookup)
    return mondayboard_missingin_lookup, lookup_fields

'''
Import MySQL tables and combine all information
params:
input_dir: Path to directory with Monday board exports
gt_file: Data frame generated by import_study_lookup_table above
monday_board: Data frame generated by import_monday_board above
lookup_fields: Starting points for all the data combining, initiated by compare_study_loookup_monday above
'''
def import_mysql_data(input_dir:Path, gt_file:pd.DataFrame, monday_board:pd.DataFrame, lookup_fields:pd.DataFrame):
# Get rest of the tables from MySQL
    convert_dict = {'appl_id':str}
    awards_df = pd.read_csv(input_dir/"awards.csv", low_memory=False, dtype=convert_dict)
    awards_df = awards_df.dropna(how='all')
    logging.info(f"Awards table has: {len(awards_df)} entries, with {len(get_unique_values(awards_df))} appl_ids")
    if (input_dir/"reporter_dqaudit.csv").exists():
        logging.info("*** Combining the two reporter tables")
        reporter_df_1 = pd.read_csv(input_dir/"reporter.csv", low_memory=False, dtype=convert_dict)
        reporter_df_1 = reporter_df_1.dropna(how='all')
        logging.info(len(reporter_df_1))
        reporter_df_2 = pd.read_csv(input_dir/"reporter_dqaudit.csv", low_memory=False, dtype=convert_dict)
        reporter_df_2 = reporter_df_2.dropna(how='all')
        logging.info(len(reporter_df_2))
        reporter_df = pd.concat([reporter_df_1, reporter_df_2])
        logging.info(len(reporter_df))
    else:
        reporter_df = pd.read_csv(input_dir/"reporter.csv", low_memory=False, dtype=convert_dict)
        reporter_df = reporter_df.dropna(how='all')

    logging.info(f"Reporter table has: {len(reporter_df)} entries, with {len(get_unique_values(reporter_df))} appl_ids")
    progress_tracker_df = pd.read_csv(input_dir/"progress_tracker.csv", low_memory=False, dtype=convert_dict)
    logging.info(f"Platform generated table has: {len(progress_tracker_df)} entries, with {len(get_unique_values(progress_tracker_df))} appl_ids")
    logging.info(f"Platform table has {len(get_unique_values(progress_tracker_df))} unique HDP IDs")
    pi_emails_df = pd.read_csv(input_dir/"pi_emails.csv", low_memory=False, dtype=convert_dict)
    logging.info(f"Repo mapping table has: {len(pi_emails_df)} entries, with {len(get_unique_values(pi_emails_df))} appl_ids")
    resnet_df = pd.read_csv(input_dir/"research_networks.csv", low_memory=False, dtype=convert_dict)
    logging.info(f"Research Network table has: {len(resnet_df)} entries, with {len(get_unique_values(resnet_df))} appl_ids")
    engagement_flags_df = pd.read_csv(input_dir/"engagement_flags.csv", low_memory=False, dtype=convert_dict)
    logging.info(f"Engagment Flags table has: {len(engagement_flags_df)} entries, with {len(get_unique_values(engagement_flags_df))} appl_ids")
    po_emails_df = pd.read_csv(input_dir/"po_emails.csv", low_memory=False, dtype=convert_dict)
    logging.info(f"PO Emails table has: {len(po_emails_df)} entries, with {len(get_unique_values(po_emails_df   ))} appl_ids")


    logging.info("--- Wrangling PI Emails")
    ## Manipulate emails to carry forward emails from a previous appl_id to the most recent one according to the lookup table and email table
    appl_ids = gt_file[['appl_id', 'study_most_recent_appl']].drop_duplicates()
    appl_ids_emails = pd.merge(appl_ids, pi_emails_df, how='left', on='appl_id')

    most_recent_emails = appl_ids_emails[ ~pd.isna(appl_ids_emails.pi_email)][['study_most_recent_appl', 'pi_email']].drop_duplicates()
    most_recent_emails.rename(columns={'pi_email':'pi_email_latest'}, inplace=True)
    logging.debug(f"ALL PI emails associated with a project (identified by most_recent_appl)\n {most_recent_emails}")
    email_counts = most_recent_emails.groupby('study_most_recent_appl').size()
    appl_ids_counts = appl_ids_emails.groupby('study_most_recent_appl').size()
    logging.info(f"Statistics on number of emails per project:: \n {email_counts.describe()} ")

    appl_ids_emails['email_count'] = [email_counts[k] if k in email_counts else 0 for k in appl_ids_emails['study_most_recent_appl']]
    appl_ids_emails['applid_count'] = [appl_ids_counts[k] if k in appl_ids_counts else 0 for k in appl_ids_emails['study_most_recent_appl']]
    appl_ids_emails['pi_email'] = appl_ids_emails['pi_email'].fillna('')
    appl_ids_emails['keep'] = [1 if (c==0 or (c==1 and len(e)>0) or (c>1 and a==m)) else 0 for (c,a,m,e) in appl_ids_emails[['email_count', 'appl_id', 'study_most_recent_appl', 'pi_email' ]].values]

    pi_emails_df_updated = appl_ids_emails[appl_ids_emails['keep']==1][['study_most_recent_appl', 'pi_email']].drop_duplicates()
    pi_emails_df_updated['pi_email'] = [k.strip() for k in pi_emails_df_updated['pi_email']]
    logging.debug("Emails that were kept for each project when there's only one email available, or when email is available for the most recent award (study_most_recent_appl)")
    logging.debug(pi_emails_df_updated)

    ## Get Monday board emails, and fill in any that are different from mysql..
    pi_emails_df_updated_monday = pd.merge(pi_emails_df_updated, monday_board[['Most Recent Appl_ID', 'Contact Email']].drop_duplicates(), how='left', left_on='study_most_recent_appl', right_on='Most Recent Appl_ID').drop(columns='Most Recent Appl_ID')
    pi_emails_df_updated_monday['Contact Email'] = pi_emails_df_updated_monday['Contact Email'].replace('-', '')
    pi_emails_df_updated_monday['Contact Email'] = pi_emails_df_updated_monday['Contact Email'].fillna('-')
    pi_emails_df_updated_monday['pi_email_updated'] = [me if (len(e)==0 and len(me) > 1) else e for (e,me) in pi_emails_df_updated_monday[['pi_email', 'Contact Email']].values]
    logging.debug("Adding any Monday Board emails when emails missing from MySql. Data looks like: ")
    logging.debug(pi_emails_df_updated_monday)
    pi_emails_df_updated_monday.to_csv(input_dir/"email_updates.csv", index=False)
    appl_ids_emails.to_csv(input_dir/"email_counts.csv", index=False)

    pi_emails_df_updated = pi_emails_df_updated_monday[['study_most_recent_appl', 'pi_email_updated']].rename(columns={'pi_email_updated':'pi_email'})

    ## Update the research network table so that Most Recent Appl ID from lookup table is assigned research network from any of the appl ids. 
    resnet_added = pd.merge(appl_ids, resnet_df[['appl_id', 'res_net']], how = 'left', left_on='appl_id', right_on='appl_id' )
    resnet_most_recent_appl_id = resnet_added[~pd.isna(resnet_added.res_net)][['study_most_recent_appl', 'res_net']]
    resnet_added_updated = pd.merge(appl_ids, resnet_most_recent_appl_id, how='left', left_on='study_most_recent_appl', right_on='study_most_recent_appl')
    resnet_df = resnet_added_updated[['study_most_recent_appl', 'res_net']].drop_duplicates()
    resnet_added_updated.to_csv("/tmp/tmp_resnet_udpated.csv", index=False)
    resnet_df.to_csv("/tmp/tmp_resnet_df.csv", index=False)

    ## Collect fields from report/awards tables that are required by Monday Board
    mysql_fields_reporter = create_mysql_subset(awards_df)
    mysql_fields_awards = create_mysql_subset(reporter_df)
    mysql_fields_platform = create_mysql_subset(progress_tracker_df, extra_fields=['hdp_id'])
    mysql_fields_piemails = create_mysql_subset(pi_emails_df_updated, extra_fields=['study_most_recent_appl'])
    mysql_fields_resnet = create_mysql_subset(resnet_df, extra_fields=['study_most_recent_appl'])
    mysql_fields_resnet['Research Network'] = [k.upper() if not pd.isna(k) else '' for k in mysql_fields_resnet['Research Network']]
    mysql_fields_enagementflags = create_mysql_subset(engagement_flags_df)
    mysql_po_emails = create_mysql_subset(po_emails_df)

    logging.info("---- STEP 5: Gathering relevant data fields from MySQL tables")
    ## Combine all the fields into one table using "Most Recent Appl_ID" as the key. Monday Board will display information from the most recent appl_id for a project, which is available in mysql's study lookup table.
    logging.info(f"Number of fields from lookup table: {len(lookup_fields)}")
    data_merge_1 = pd.merge(lookup_fields, mysql_fields_reporter, how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id')
    logging.info(f"Number of fields after adding reporter table fields: {len(data_merge_1)}")
    data_merge_2 = pd.merge(data_merge_1, mysql_fields_awards, how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id')
    logging.info(f"Number of fields after adding awards table fields: {len(data_merge_2)}")
    data_merge_1 = pd.merge(data_merge_2, mysql_fields_platform, how='left', left_on='study_hdp_id', right_on='hdp_id')
    logging.info(f"Number of fields after adding Platform MDS table fields: {len(data_merge_1)}")
    data_merge_2 = pd.merge(data_merge_1, mysql_fields_resnet, how='left', left_on='study_most_recent_appl', right_on='study_most_recent_appl')
    logging.info(f"Number of fields after adding research network table fields: {len(data_merge_2)}")
    data_merge_1 = pd.merge(data_merge_2, mysql_fields_enagementflags, how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id')
    logging.info(f"Number of fields after adding engagegment flag table fields: {len(data_merge_1)}")
    data_merge_2 = pd.merge(data_merge_1, mysql_po_emails, how='left', left_on='study_most_recent_appl', right_on='appl_id').drop(columns='appl_id')
    logging.info(f"Number of fields after adding PO Emai fields: {len(data_merge_2)}")
    combined_data_ph1 = pd.merge(data_merge_2, mysql_fields_piemails, how='left', on='study_most_recent_appl')
    logging.info(f"Number of fields after adding PI Emails: {len(combined_data_ph1)}")
    logging.info(f"Total entries in this combined dataset: {len(combined_data_ph1.drop_duplicates())}")
    return combined_data_ph1

'''
Function to fill in any holes in the NIH reporter data from MDS data
All of this information should be available in the MySQL datasets.

input parameters:
input_dir: Path to all the mysql exports
mysql_data: Dataframe created by import_mysql_data function above in which to fill in holes with

returns:
DataFrame with filled holes.
'''
def fill_in_holes_from_mds(input_dir:Path, mysql_data:pd.DataFrame):
    convert_dict = {'appl_id':str}
    progress_tracker_df = pd.read_csv(input_dir/"progress_tracker.csv", low_memory=False, dtype=convert_dict)
    # Fill in holes in the mysql data using the progress tracker data a.k.a platform MDS data.
    progress_tracker_data = progress_tracker_df.copy(deep=True)
    progress_tracker_data['project_title'] = progress_tracker_data['project_title'].replace('0', '')
    progress_tracker_data = create_mysql_subset(progress_tracker_data, extra_fields=['hdp_id'], rename_dict=RENAME_DICT_MDS)

    progress_tracker_data['PI(s)'] = progress_tracker_data['PI(s)'].fillna('')
    progress_tracker_data['PI(s)'] = [ k.translate(str.maketrans(',', ';', "[]\'")) for k in  progress_tracker_data['PI(s)']]

    progress_tracker_data['key'] = progress_tracker_data['hdp_id']
    progress_tracker_data['study_hdp_id'] = progress_tracker_data['hdp_id']
    progress_tracker_data['Research Network'] = [ 'CTN' if k.startswith('CTN') else '' for k in progress_tracker_data['Project #']]

    fill_in_data = pd.merge(mysql_data, progress_tracker_data, how='left', on='study_hdp_id')
    fill_in_data.to_csv(input_dir/"tmp.csv", index=False)

    columns_to_compare = list(RENAME_DICT_MDS.values())
    columns_to_compare.extend(['key', 'Research Network'])

    for k in columns_to_compare:
        k_x = k+'_x'
        k_y = k+'_y'
        fill_in_data[k] = [v_y if pd.isna(v_x) else v_x for (v_x, v_y) in fill_in_data[[k_x, k_y]].values]
        fill_in_data.drop(columns=[k_x, k_y], inplace=True)

    columns = fill_in_data.columns.sort_values()
    fill_in_data = fill_in_data[columns]
    fill_in_data.to_csv(input_dir/"tmp.csv", index=False)
    combined_data_ph1 = fill_in_data.copy(deep=True)
    return combined_data_ph1

'''
Function to grab CTN data from MDS export in progress tracker table on MySQL.

'''
def get_ctndata_from_mds(input_dir:Path):
    convert_dict = {'appl_id':str}
    progress_tracker_df = pd.read_csv(input_dir/"progress_tracker.csv", low_memory=False, dtype=convert_dict)
    
    ctn_data = progress_tracker_df[[k.startswith('CTN') for k in progress_tracker_df['project_num']]]
    ctn_data['project_title'] = ctn_data['project_title'].replace('0', '')
    logging.info(f"Number of CTN entries found in Platform MDS {len(ctn_data)}")
    ctn_fields_platform = create_mysql_subset(ctn_data, extra_fields=['hdp_id'], rename_dict=RENAME_DICT_CTN)
    ## Edit pi name
    ctn_fields_platform['PI(s)'] = ctn_fields_platform['PI(s)'].fillna('')
    ctn_fields_platform['PI(s)'] = [ k.translate(str.maketrans(',', ';', "[]\'")) for k in  ctn_fields_platform['PI(s)']]
    ctn_fields_platform['key'] = ctn_fields_platform['hdp_id']
    ctn_fields_platform['study_hdp_id'] = ctn_fields_platform['hdp_id']
    ctn_fields_platform['Research Network'] = ['CTN']*len(ctn_fields_platform)
    return ctn_fields_platform


'''
Combine MySQL and CTN data, and report out on various fields. Maybe do validataion here.

input parameters:
mysql_data: MySQL export data - either the one exported by import_mysql_data or fill_in_holes_from_mds

return: combined datasets
'''
def combine_mysql_ctn(mysql_data:pd.DataFrame, ctn_data:pd.DataFrame):
## Combine the data to the other data set
    all_data = pd.concat([mysql_data, ctn_data])
    
    # TODO: Add validation checks to the all_data dataset.
    logging.debug("------------ Preview of the final combined dataset ---------------")
    logging.debug(all_data)

    logging.info("==== Frequencies of several research networks in the combined dataset ========")
    logging.info(all_data['Research Network'].value_counts())

    ## Find out which columns have NA values, and investigate for incompletemess?
    logging.info("Fields and frequencies of any empty values in the final dataset")
    na_dict = {k: get_na_count(all_data, k) for k in list(all_data.columns)}
    na_dict_sub = {k: v for k, v in na_dict.items() if v>0}
    import pprint
    logging.info(pprint.pformat(na_dict_sub))

    return all_data

'''
Prepare the data set for import to Monday HEAL Studies board

input: data DataFrame.
return: DataFrame which is ready for Monday HEAL Studies import.
'''
def prepare_for_monday(all_data:pd.DataFrame):

    ## Making a copy of the dataframe.
    combined_data = all_data.copy(deep=True)

    ## Adding a study_type column to indicate types of entries. This will be used to put rows into their respective categories on Monday Board
    combined_data['study_type'] = [ 'CTN' if m.startswith('CTN') else ('APPLIDONLY' if pd.isna(k) else 'HDP') for (m,k) in combined_data[['Project #', 'study_hdp_id_appl']].values]
    logging.info("Counts for study types in the final dataset")
    logging.info(combined_data.study_type.value_counts())

    ## Create a column named "Location"
    combined_data['City'] = combined_data[['City']].fillna('-')
    combined_data['State'] = combined_data[['State']].fillna('-')
    combined_data['Location'] = [c+","+s for (c,s) in combined_data[['City', "State"]].values]

    ## Convert dates to ISO format
    combined_data['Project Start'] = pd.to_datetime(combined_data['Project Start'], format='%Y-%m-%d', errors='coerce').dt.date
    combined_data['Project End'] = pd.to_datetime(combined_data['Project End'], format='%Y-%m-%d', errors='coerce').dt.date
    combined_data['Platform Reg Time'] = pd.to_datetime(combined_data['Platform Reg Time'], utc=True).dt.date

    ## Change archived column to have "archived/n" values and Y/N type values in HEAL-related and SBIR/STTR columns
    combined_data['Archived'] = [a if a=='archived' else 'n' for a in combined_data['Archived']]
    combined_data['HEAL-Related'] = ['Y' if ((p != 'CTN' ) and (pd.isna(a))) else 'N' for (p,a) in combined_data[['study_type', 'HEAL-Related']].values]
    combined_data['SBIR/STTR'] = ['Y' if 'SBIR/STTR'==t else 'N' for t in combined_data['SBIR/STTR']]
    combined_data['Checklist Exempt'] = ['Y' if 1==t else 'N' for t in combined_data['Checklist Exempt']]
    combined_data['Do not Engage'] = ['Y' if 1==t else 'N' for t in combined_data['Do not Engage']]

    ## Rename a few of the other columns:
    combined_data.rename(columns={'study_most_recent_appl':'Most Recent Appl_ID', 'study_hdp_id_appl':'HDP appl_ID'}, inplace=True)
    combined_data.drop(columns=['study_hdp_id', 'hdp_id', 'hdp_id_x', 'hdp_id_y'], inplace=True)

    handled_columns = ['study_type', 'City', 'State', 'Location', 'Project Start', 'Project End', 'Platform Reg Time', 'Archived', 'HEAL-Related', 'SBIR/STTR', 'Checklist Exempt', 'Do not Engage']
    rest_obj_columns = [k for k in combined_data.columns if k not in handled_columns and combined_data[k].dtype in ['object', 'str']]

    #combined_data[rest_obj_columns].fillna('-')
    logging.info("Setting empty cells to  '-' in the following colulmns:")
    logging.info(rest_obj_columns)

    for k in rest_obj_columns:
        combined_data[k] = ['-' if (t is np.nan) or (t=='') else t for t in combined_data[k]]
    
    return combined_data

''' 
Report out some statistics, make some checks, and export the final dataset
The export file will be called MondayBoard_Update.xlsx.

input parameters:
input_dir: Path to the directory where export document will live
final_dataset: THe dataframe with the dataset ready to be imported to Monday.
mondayboard_missing_in_data: Dataframe returned from compare_study_loookup_monday, which records descrepancy between study lookup table and Monday Board (before update)
monday_board: Prior monday board export -- only used for reporting
'''
def export_finaldata(input_dir:Path, final_dataset:pd.DataFrame, mondayboard_missing_in_data:pd.DataFrame, monday_board:pd.DataFrame):

    logging.info("******************* MONDAY COMPARISON  ******************************************")
    logging.info(f"Number records from Monday already in final dataset: {len(monday_board[monday_board.Name.isin(final_dataset.key)])}")
    ## How many of the keys from MOnday board are not there in looup fields
    mondayboard_missing_in_data = monday_board[~monday_board.Name.isin(final_dataset.key)]
    logging.info(f"Number records from Monday that are not in final dataset (Consider these as discrepancies **Investigate**): {len(mondayboard_missing_in_data)}")
    ## How many of the keys from lookup fields are not there in Monday??
    data_missing_in_mondayboard = final_dataset[~final_dataset.key.isin(monday_board.Name)]
    logging.info(f"Number records from final dataset that are not on Monday (Potentially new entries): {len(data_missing_in_mondayboard)}")

    logging.warning("****** Investigate/Delete the following entries on Monday that are not in the final dataset")
    logging.warning(mondayboard_missing_in_data[['Name', 'Most Recent Appl_ID', 'study_type']].values)

    #having an index column is created as a temporary column and is integral to the QA process. See SOP for more specific notes.
    final_dataset.reset_index(drop=True, inplace=True)
    final_dataset.index.name = 'index'

    key_counts = final_dataset.groupby('key').size()
    t = key_counts.describe()
    logging.info("******************* FINAL DATASET NUMBERS ******************************************")
    logging.info(f"Number records in the final dataset: {len(final_dataset)}")
    logging.info(f"Making sure uniqueness of key values. Do we have one row per key(HDPID/APPLID)? ::::  {bool(t['min'] == 1 and t['max'] == 1)}")

    outfile = input_dir/"MondayBoard_Update.xlsx"
    logging.info("******************* EXPORTING ******************************************")
    logging.info(f"Exporting data to excel file at {outfile}")
    final_dataset.to_excel(outfile, engine='xlsxwriter', index=True)
    batch_size = 1500
    num_batches = (len(final_dataset) - 1) // batch_size + 1
    for batch_num in range(num_batches):
        start_idx = batch_num * batch_size
        end_idx = min((batch_num + 1) * batch_size, len(final_dataset))
        
        batch_df = final_dataset.iloc[start_idx:end_idx].copy()
        
        outfile = input_dir / f"MondayBoard_Update_batch_{batch_num + 1}_records_{start_idx + 1}_to_{end_idx}.xlsx"
        logging.info(f"Exporting batch {batch_num + 1} ({end_idx - start_idx} records) to {outfile}")
        
        batch_df.to_excel(outfile, engine='xlsxwriter', index=True)
    
    logging.info("******************* DONE! ******************************************")

# Run list_mds_data_dictionaries() if not used as a library.
# Set up command line arguments.
@click.command()
@click.option(
    "--input_dir",
    default="/tmp/input",
    help="Path to a folder with HEAL Studies Monday board export, and HEAL MySql table exports. Updated Excel file will be exported here.", 
)
@click.option(
    "--debug", default=False, is_flag=True, help="Use to run the script in debug mode"
)
def create_monday_update_file(input_dir:str, debug:bool):

    ## Set the input_dir to the directory where:
    ## 1- Monday studies board has been exported
    ## 2- All relevant tables from MySql database for HEAL have been exported as a csv. Refer to SOP for which tables.

    input_dir = Path(input_dir)

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        filename= input_dir / "report-log.txt",
    )
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    ### Steps for updating Monday board:
    ### - Start with the lookup table from MySQL Database
    ##% - for each HDPID in lookup table, look for most recent appl id, and get MySQL data fields needed for the board
    ### - Add CTN Data from Platform MDS Data
    ### - Combine everything together

    logging.info("---- STEP 1: Looking at Study Lookup Table")
    gt_file = import_study_lookup_table(input_dir)
    
    logging.info("---- STEP 2: Importing Monday Studies Board")
    monday_board = import_monday_board(input_dir)

    logging.info("---- STEP 3: Compare lookup table and Monday Board")
    mondayboard_missingin_lookup, lookup_fields = compare_study_loookup_monday(gt_file, monday_board)
    
    logging.info("---- STEP 4: Importing tables from MySQL and combining relevant information")
    combined_data_ph1 = import_mysql_data(input_dir, gt_file, monday_board, lookup_fields)

    logging.info("---- STEP 5: Filling holes with MDS data")
    combined_data_ph1 = fill_in_holes_from_mds(input_dir, combined_data_ph1)
    
    ## Add CTN  data
    logging.info("---- STEP 6: Adding any CTN data from MDS")
    ctn_fields_platform = get_ctndata_from_mds(input_dir)

    logging.info("---- STEP 7: Combining everything together")
    all_data = combine_mysql_ctn(combined_data_ph1, ctn_fields_platform)

    ### Final manipulation of the combined data to make it Monday board ready
    logging.info("---- STEP 8: Final Manipulation of all the data to make it Monday Board ready")
    combined_data = prepare_for_monday(all_data)

    #TODO: Find what's in Monday.com board, but not in mysql extract
    # Mark these entries for deletion, and these would have to be deleted manually on Monday.com
    ### A few checks:
    ## How many of the "keys" from Monday board are in lookup fields?
    logging.info("---- STEP 9: Final numbers and Export")
    export_finaldata(input_dir, combined_data, mondayboard_missingin_lookup, monday_board)
    
if __name__ == "__main__":
    create_monday_update_file()
import os,sys
import webbrowser
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime
import re
from sqlalchemy import create_engine
import mysql.connector
from dotenv import load_dotenv
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# https://docs.aws.amazon.com/lambda/latest/dg/python-package.html

def lambda_handler(event, context):


    ####################################################################################
    ### Gather metadata into useful form
    ####################################################################################
    print(f">>> Gather metadata into useful form")
    cedar_fields = ["data",
                    "study_type",
                    "minimal_info",
                    "data_availability",
                    "metadata_location",
                    "study_translational_focus",
                    "human_subject_applicability",
                    "human_condition_applicability",
                    "human_treatment_applicability",
                    "time_of_registration",
                    "time_of_last_cedar_updated"]

    # Create a function to clean the metadata so that all unfilled dictionaries or lists are seen as NaN
    # leave empty strings as `''`
    def clean_data(df):
        for col in df.columns:
            for i in range(len(df)):
                if type(df[col].iloc[i]) == bool or type(df[col].iloc[i]) == np.bool_:
                    continue
                elif type(df[col].iloc[i]) == dict:
                    if not any(list(df[col].iloc[i].values())):
                        df.at[i, col]= np.nan
                elif type(df[col].iloc[i]) == int or type(df[col].iloc[i]) == np.float64 or type(df[col].iloc[i]) == float:
                    continue
                elif type(df[col].iloc[i]) == list:
                    if len(df[col].iloc[i]) == 0:
                        df.at[i, col]= np.nan
        return df


    # Create a function to transform the metadata to a dataframe format
    # Use the clean_data function during transformation
    def transform_data(meta_dict):
        df = pd.DataFrame.from_dict(meta_dict)
        df = df.T
        df.index.name = 'guids'
        df.reset_index(inplace=True)
        df = clean_data(df)
        return df

    ####################################################################################
    ### Find MDS record for the study searching by project number, appl_id, or hdpid
    ####################################################################################
    print(">>> Find MDS record for the study searching by project number, appl_id, or hdpid")
    query = 'https://healdata.org/mds/metadata?data=True&limit=1000000'
    print(f'Query: {query}')

    response = requests.get(f"{query}")
    response_json = response.json()
    guids = response_json.keys()

    metadata = {'nih_metadata': {}, 'ctgov_metadata': {}, 'gen3_metadata': {}}

    no_gen3_metadata = []
    missed_keys = []

    for guid in guids:

        if 'gen3_discovery' in response_json[guid].keys():
            metadata['gen3_metadata'][guid] = response_json[guid]['gen3_discovery'] # get majority metadata

            if '_guid_type' in response_json[guid].keys():
                metadata['gen3_metadata'][guid]['registration_status'] = response_json[guid]['_guid_type'] # get registration status

            if 'study_metadata' in response_json[guid]['gen3_discovery'].keys():
                for key1 in response_json[guid]['gen3_discovery']['study_metadata'].keys():
                    for key2 in response_json[guid]['gen3_discovery']['study_metadata'][key1].keys():
                        if key1 in cedar_fields:
                            metadata['gen3_metadata'][guid][f'cedar_study_metadata.{key1}.{key2}'] = response_json[guid]['gen3_discovery']['study_metadata'][key1][key2]
                        else:
                            metadata['gen3_metadata'][guid][f'study_metadata.{key1}.{key2}'] = response_json[guid]['gen3_discovery']['study_metadata'][key1][key2]
                del metadata['gen3_metadata'][guid]['study_metadata']


        if 'nih_reporter' in response_json[guid].keys():
            metadata['nih_metadata'][guid] = response_json[guid]['nih_reporter']

        if 'clinicaltrials_gov' in response_json[guid].keys():
            metadata['ctgov_metadata'][guid] = response_json[guid]['clinicaltrials_gov']


    df1 = transform_data(metadata['gen3_metadata'])
    df2 = transform_data(metadata['ctgov_metadata'])
    df3 = transform_data(metadata['nih_metadata'])
    df4 = pd.merge(df1, df3, how='outer', on='guids')

    df_apid = df3['appl_id']
    df1.drop(['appl_id'], axis=1, errors='ignore')
    df1 = pd.concat([df1, df_apid], axis=1)



    ####################################################################################
    ### Pull out relevant metadata
    ####################################################################################
    print(">>> Pull out relevant metadata")
    def replace_single_quote(input_list):
        modified_list = [name.replace("'", "''") for name in input_list]
        return str("[{}]".format(", ".join("'{}'".format(name) for name in modified_list)))


    def mydf1function(rowdf):
        projname = rowdf.iloc[0]['project_title']
        projnumber = rowdf.iloc[0]['project_number']
        projPI = rowdf.iloc[0]['investigators_name']

        url = rowdf.iloc[0]['cedar_study_metadata.metadata_location.nih_reporter_link']
        ctid = rowdf.iloc[0]['cedar_study_metadata.metadata_location.clinical_trials_study_ID']
        ctlink = rowdf.iloc[0]['cedar_study_metadata.metadata_location.clinical_trials_study_link']
        data_repositories = rowdf.iloc[0]['cedar_study_metadata.metadata_location.data_repositories']
        repository_name = ''
        repository_study_id = ''
        if data_repositories != '':
            repository_name = data_repositories[0]['repository_name'] #rowdf.iloc[0]['cedar_study_metadata.metadata_location.data_repositories.repository_name']
            repository_study_id = data_repositories[0]['repository_study_ID'] #rowdf.iloc[0]['cedar_study_metadata.metadata_location.data_repositories.repository_study_ID']
        if rowdf.iloc[0]['registration_status'] == 'discovery_metadata_archive':
            archivestatus = 'archived'
            archivedate = rowdf.iloc[0]['archive_date']
        else:
            archivestatus = 'live'
            # archivedate = 'na'
            archivedate = ''
        if rowdf.iloc[0]['is_registered'] == True:
            regstatus = 'is registered'
            regdate = rowdf.iloc[0]['time_of_registration']
            reguser = rowdf.iloc[0]['registrant_username']
        else:
            regstatus = 'not registered'
            # regdate = 'na'
            # reguser = 'na'
            regdate = ''
            reguser = ''


        return {
            ### TODO contact pi name
            'study_name': str(projname).replace("'", "''"),
            'project_num': projnumber,
            'investigators_name': replace_single_quote(projPI),
            'is_registered': regstatus,
            'time_of_registration': regdate,
            'Registering user': reguser,
            'archived': archivestatus,
            'archive_date': archivedate,
            'nih_reporter_link': url,
            'clinical_trials_study_ID': ctid,
            'ov': ctlink,
            'repository_name': repository_name,
            'repository_study_id': repository_study_id,
            'year_awarded': rowdf.iloc[0]['year_awarded'],
            'dmp_plan': [],
            'heal_cde_used':[],
            'vlmd_metadata':[]
        }

    # Grab necessary metadata from NIH Metadata
    def mydf3function(rowdf):
        appl_id = rowdf.iloc[0]['appl_id']
        award_type = rowdf.iloc[0]['award_type']
        award_amount = rowdf.iloc[0]['award_amount']
        award_notice_date = rowdf.iloc[0]['award_notice_date']
        project_end_date = rowdf.iloc[0]['project_end_date']
        project_title = rowdf.iloc[0]['project_title']
        # project_num = rowdf.iloc[0]['project_num']

        return {
            'appl_id':appl_id,
            'award_type':award_type,
            'award_amount':award_amount,
            'award_notice_date':award_notice_date,
            'project_end_date':project_end_date,
            'project_title':project_title
        }

    df1_null = df1.replace(np.nan, '')
    res_series1 = df1_null.groupby('guids').apply(mydf1function)
    res_df1 = pd.DataFrame(res_series1.tolist(), index=res_series1.index)
    res_series3 = df3.groupby('guids').apply(mydf3function)
    res_df3 = pd.DataFrame(res_series3.tolist(), index=res_series3.index)


    ####################################################################################
    ### CEDAR Completion
    ####################################################################################
    print(">>> CEDAR Completion")
    # create a list to store all the gathered data
    cedar_comp_info = []

    # these are fields in the Metadata Location section in the MDS that are not on the CEDAR form.
    # We are excluding them to keep from confusing the PIs if we need to share the list
    noncedar = [
        'cedar_study_metadata.metadata_location.data_repositories',
        'cedar_study_metadata.metadata_location.nih_reporter_link',
        'cedar_study_metadata.metadata_location.nih_application_id',
        'cedar_study_metadata.metadata_location.clinical_trials_study_ID',
        'cedar_study_metadata.metadata_location.cedar_study_level_metadata_template_instance_ID'
    ]

    #for each row
    #if the column name begins with cedar_study_metadata.XXX.
    #loop through the columns with that prefix
    #count if not empty string/NaN/0
    now = datetime.now()
    time_now = now.strftime('%Y-%m-%d %H:%M:%S')
    print(f"* * * time_now: {time_now}")
    for index, row in df1.iterrows():
        if 'time_of_last_cedar_updated' in df1:
            cedar_update = df1.loc[0, 'time_of_last_cedar_updated']
        else:
            cedar_update = ''

        hid = row['guids']
        # if hid == searchhid: print(row)
        #if the column name begins with cedar_study_metadata.minimal_info.
        sel_min_info = row.index.str.startswith("cedar_study_metadata.minimal_info.")
        # how many fields are not completed in Minimal Info section

        not_empty_string = (row.loc[sel_min_info] != "")
        not_0 = (row.loc[sel_min_info] != "0")
        not_nan = ~row.loc[sel_min_info].isna()
        completed_min_info = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # how many total fields in Minimal Info section ***FLAG***
        total_min_info=len(row.loc[sel_min_info])

        # find the fields that are not completed
        is_empty_string = (row.loc[sel_min_info] == "")
        is_0 = (row.loc[sel_min_info] == "0")
        is_nan = row.loc[sel_min_info].isna()
        is_noncedar = row.loc[sel_min_info].index.isin(noncedar)
        missing_min_info = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_min_info = row.loc[sel_min_info].loc[missing_min_info].index.tolist()

        #if the column name is cedar_study_metadata.metadata_location.other_study_websites
        # if is not nan
        completed_websites = 2 if row["cedar_study_metadata.metadata_location.other_study_websites"] != np.nan else 1

        # only 2 fields, including autopop field
        total_websites = 2
        #if the column name begins with cedar_study_metadata.metadata_location.
        sel_met_loc = row.index.str.startswith("cedar_study_metadata.metadata_location.")
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_met_loc] == "")
        is_0 = (row.loc[sel_met_loc] == "0")
        is_nan = row.loc[sel_met_loc].isna()
        is_noncedar = row.loc[sel_met_loc].index.isin(noncedar)
        missing_met_loc = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_met_loc = row.loc[sel_met_loc].loc[missing_met_loc].index.tolist()

        #if the column name begins with cedar_study_metadata.data_availability.
        sel_data_avail = row.index.str.startswith("cedar_study_metadata.data_availability.")
        # how many total fields in Data Availability section section
        total_data_avail=len(row.loc[sel_data_avail])
        # how many fields are not completed in Data Availability section
        not_empty_string = (row.loc[sel_data_avail] != "")
        not_0 = (row.loc[sel_data_avail] != "0")
        not_nan = ~row.loc[sel_data_avail].isna()
        completed_data_avail = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_data_avail] == "")
        is_0 = (row.loc[sel_data_avail] == "0")
        is_nan = row.loc[sel_data_avail].isna()
        is_noncedar = row.loc[sel_data_avail].index.isin(noncedar)
        missing_data_avail = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_data_avail = row.loc[sel_data_avail].loc[missing_data_avail].index.tolist()

        #if the column name begins with cedar_study_metadata.study_translational_focus.
        sel_trans_focus = row.index.str.startswith("cedar_study_metadata.study_translational_focus.")
        # how many total fields in Study Translational Focus section
        total_trans_focus=len(row.loc[sel_trans_focus])
        # how many fields are not completed in Study Translational Focus section
        not_empty_string = (row.loc[sel_trans_focus] != "")
        not_0 = (row.loc[sel_trans_focus] != "0")
        not_nan = ~row.loc[sel_trans_focus].isna()
        completed_trans_focus = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_trans_focus] == "")
        is_0 = (row.loc[sel_trans_focus] == "0")
        is_nan = row.loc[sel_trans_focus].isna()
        is_noncedar = row.loc[sel_trans_focus].index.isin(noncedar)
        missing_trans_focus = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_trans_focus = row.loc[sel_trans_focus].loc[missing_trans_focus].index.tolist()

        #if the column name begins with cedar_study_metadata.study_type.
        sel_study_type = row.index.str.startswith("cedar_study_metadata.study_type.")
        # how many total fields in Study Type section
        total_study_type=len(row.loc[sel_study_type])
        # how many fields are not completed in Study Type section
        not_empty_string = (row.loc[sel_study_type] != "")
        not_0 = (row.loc[sel_study_type] != "0")
        not_nan = ~row.loc[sel_study_type].isna()
        completed_study_type = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_study_type] == "")
        is_0 = (row.loc[sel_study_type] == "0")
        is_nan = row.loc[sel_study_type].isna()
        is_noncedar = row.loc[sel_study_type].index.isin(noncedar)
        missing_study_type = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_study_type = row.loc[sel_study_type].loc[missing_study_type].index.tolist()

        #if the column name begins with cedar_study_metadata.human_treatment_applicability.
        sel_hum_treat = row.index.str.startswith("cedar_study_metadata.human_treatment_applicability.")
        # how many total fields in Human Treatment Applicability section
        total_hum_treat=len(row.loc[sel_hum_treat])
        # how many fields are not completed in Human Treatment Applicability section
        not_empty_string = (row.loc[sel_hum_treat] != "")
        not_0 = (row.loc[sel_hum_treat] != "0")
        not_nan = ~row.loc[sel_hum_treat].isna()
        completed_hum_treat = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_hum_treat] == "")
        is_0 = (row.loc[sel_hum_treat] == "0")
        is_nan = row.loc[sel_hum_treat].isna()
        is_noncedar = row.loc[sel_hum_treat].index.isin(noncedar)
        missing_hum_treat = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_hum_treat = row.loc[sel_hum_treat].loc[missing_hum_treat].index.tolist()

        #if the column name begins with cedar_study_metadata.human_condition_applicability.
        sel_hum_cond = row.index.str.startswith("cedar_study_metadata.human_condition_applicability.")
        # how many total fields in Human Condition Applicability section
        total_hum_cond=len(row.loc[sel_hum_cond])
        # how many fields are not completed in Human Condition Applicability section
        not_empty_string = (row.loc[sel_hum_cond] != "")
        not_0 = (row.loc[sel_hum_cond] != "0")
        not_nan = ~row.loc[sel_hum_cond].isna()
        completed_hum_cond = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_hum_cond] == "")
        is_0 = (row.loc[sel_hum_cond] == "0")
        is_nan = row.loc[sel_hum_cond].isna()
        is_noncedar = row.loc[sel_hum_cond].index.isin(noncedar)
        missing_hum_cond = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_hum_cond = row.loc[sel_hum_cond].loc[missing_hum_cond].index.tolist()

        #if the column name begins with cedar_study_metadata.human_subject_applicability.
        sel_hum_subj = row.index.str.startswith("cedar_study_metadata.human_subject_applicability.")
        # how many total fields in Human Subject Applicability section
        total_hum_subj=len(row.loc[sel_hum_subj])
        # how many fields are not completed in Human Subject Applicability section
        not_empty_string = (row.loc[sel_hum_subj] != "")
        not_0 = (row.loc[sel_hum_subj] != "0")
        not_nan = ~row.loc[sel_hum_subj].isna()
        completed_hum_subj = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_hum_subj] == "")
        is_0 = (row.loc[sel_hum_subj] == "0")
        is_nan = row.loc[sel_hum_subj].isna()
        is_noncedar = row.loc[sel_hum_subj].index.isin(noncedar)
        missing_hum_subj = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_hum_subj = row.loc[sel_hum_subj].loc[missing_hum_subj].index.tolist()

        #if the column name begins with cedar_study_metadata.data.
        sel_data = row.index.str.startswith("cedar_study_metadata.data.")
        # how many total fields in Data section
        total_data=len(row.loc[sel_data])
        # how many fields are not completed in Data section
        not_empty_string = (row.loc[sel_data] != "")
        not_0 = (row.loc[sel_data] != "0")
        not_nan = ~row.loc[sel_data].isna()
        completed_data = (not_empty_string & not_0 & not_nan).value_counts().get(True, default=0)
        # Find the fields that are not completed
        is_empty_string = (row.loc[sel_data] == "")
        is_0 = (row.loc[sel_data] == "0")
        is_nan = row.loc[sel_data].isna()
        is_noncedar = row.loc[sel_data].index.isin(noncedar)
        missing_data = (is_empty_string | is_0 | is_nan) & ~is_noncedar
        is_missing_data = row.loc[sel_data].loc[missing_data].index.tolist()
        # Collapse original 2 cells into single
        overall_total = total_min_info + total_websites + total_data_avail + total_trans_focus + total_study_type + total_hum_treat + total_hum_cond + total_hum_subj + total_data
        overall_complete = completed_min_info + completed_websites + completed_data_avail + completed_trans_focus + completed_study_type + completed_hum_treat + completed_hum_cond + completed_hum_subj + completed_data
        overall_pct = round((100 * overall_complete / overall_total), 1)

        cedar_comp_info.append(
            [
                hid,
                cedar_update,
                overall_pct,
                overall_complete,
                time_now
            ])

    col_names = [
        "guids",
        "last_cedar_update",
        'overall_percent_complete',
        'overall_num_complete',
        'date_last_mds_update'
    ]
    complxn_stats = pd.DataFrame(cedar_comp_info, columns=col_names)

    ####################################################################################
    ### Combining all dataframes
    ####################################################################################
    print(">>> Combining all dataframes")
    merged_df = pd.merge(res_df1, res_df3, how='outer', on='guids')
    merged_df = pd.merge(merged_df, complxn_stats, how='outer', on='guids')
    merged_df = merged_df.rename(columns={'guids': 'hdp_id'})
    final_df = merged_df.T.transpose()

    ####################################################################################
    ### Insert DataFrame into MySQL
    ####################################################################################
    print(">>> Insert DataFrame into MySQL")
    tmp_df = final_df
    tmp_df.fillna(0, inplace=True)

    print(str(tmp_df['appl_id']))
    # tmp_df['appl_id'] = tmp_df['appl_id'].astype(float).astype(int).astype(str)
    tmp_df['appl_id'] = tmp_df['appl_id'].astype(str)

    insert_df = tmp_df.astype(str)
    # MySQL connection parameters
    # Load environment variables from .env file
    load_dotenv()

    # Accessing variables
    db_username = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_database = os.getenv('DB_NAME')
    table_name = os.getenv('TABLE_NAME')

    # db_password = 'password!'
    # db_host = 'v07-myrdsinstance-npmuiprkdmcp.cqhcdiezce0d.us-east-1.rds.amazonaws.com'
    # db_database = 'V07daiV5'
    # table_name = 'checklistv6'

    # Drop existing records
    connection = mysql.connector.connect(
        host=db_host,
        database=db_database,
        user=db_username,
        password=db_password
    )
    cursor = connection.cursor()
    # SQLAlchemy engine for MySQL
    engine_url = f'mysql+pymysql://{db_username}:{db_password}@{db_host}/{db_database}'
    engine = create_engine(engine_url)

    # Convert DataFrame to string type
    insert_df = tmp_df.astype(str)

    # Insert DataFrame into MySQL table
    try:
        insert_df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Success!")
    except sqlalchemy.exc.SQLAlchemyError as e:
        print(f'Unsuccessful insert. Error: {e}')

    # Update non-registered studies to have 0% completion
    try:
        cursor.execute(f"update {table_name} set overall_percent_complete='0' where is_registered ='not registered';")
        connection.commit()  # Commit the transaction to apply the changes
        print("unregistered studies zeroed out")
    except mysql.connector.Error as err:
        print("unsuccessful update, error:", err)


    # Update non-registered studies to have 0% completion
    try:
        cursor.execute(f"select appl_id, is_registered, overall_percent_complete  from progress_tracker where appl_id in ('10056337', '9608089', '9867358', '9900258', '10320676', '9839124', '10304570');")
        # Fetch the results
        rows = cursor.fetchall()

        # Get column names
        column_names = [column[0] for column in cursor.description]

        # Convert results to a list of dictionaries
        results = [dict(zip(column_names, row)) for row in rows]

        # Convert to JSON and pretty print
        print(json.dumps(results, indent=4))

    except mysql.connector.Error as err:
        print("unsuccessful update, error:", err)

    finally:
        cursor.close()

    response = {
        'statusCode': 200,
        'result': json.dumps(results, indent=4)
        }
    return response
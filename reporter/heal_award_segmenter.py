import csv
import requests
import collections
import re
import argparse

def main(args):
    filepath = args.input_filepath
    output_path = args.output_path
    output_suffix = args.output_suffix
    clean_non_utf = args.replace_non_utf
    return_related_project_nums = args.return_related_project_nums
    id_type = args.id_type
    project_id = args.project_id_column
    project_title = args.project_title_column

    # Create ID List to query  
    id_list,core_id_list = create_project_num_list_from_csv(filepath, output_path, output_suffix, id_type, project_id, project_title) # add core project num list
    results = post_request(clean_non_utf, id_type, id_list)
    pub_results = post_request(clean_non_utf, id_type, core_id_list, "publications/search")
    
    # Add related project_nums
    if (id_type == 'appl_id') & return_related_project_nums:
        additional_id_list, additional_core_id_list = create_project_num_list_from_csv(filepath, output_path, output_suffix, "project_num", "Full Grant Number", project_title)
        additional_results = post_request(clean_non_utf, "project_num", additional_id_list)
        for addl_result in additional_results:
            if str(addl_result['appl_id']) not in id_list:
                addl_result['_second_search_flag'] = 1 # To understand which we returned from our secondary search
                results.append(addl_result)

    # Projects not in reporter
    projects_not_in_reporter = []
    results_project_ids = [str(x[id_type]) for x in results]
    for project in id_list:
        if str(project) not in results_project_ids:
            projects_not_in_reporter.append(project)

    proj_not_in_reporter_file = f"{output_path}/projects_not_in_reporter_{output_suffix}.txt"

    with open(proj_not_in_reporter_file, "w") as f:
            for project in projects_not_in_reporter:
                f.write("%s\n" % project)
   
    # Map flatten function to resultss
    results_flat = list(map(flatten_json, results))
    fieldnames = []
    for result in results_flat:
        fieldnames.extend(list(result.keys()))
    fieldnames = list(set(fieldnames))
    fieldnames.sort() # sort alphabetically
    
    # Write flattened results dicts to CSV
    heal_awards_file = f"{output_path}/heal_awards_{output_suffix}.csv"
    with open(heal_awards_file, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results_flat:
            writer.writerow(result)

    # Write publications results dicts to CSV
    heal_pubs_file = f"{output_path}/heal_publications_{output_suffix}.csv"

    with open(heal_pubs_file, 'w', newline='') as csvfile:
        fieldnames = []
        for result in pub_results:
            fieldnames.extend(list(result.keys()))
        fieldnames = list(set(fieldnames))
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in pub_results:
            writer.writerow(result)

def create_project_num_list_from_txt(txt_filepath,header=True):
    '''
    **PROBABLY CAN BE DEPRECATED 03 FEB 2022**
    
    create_project_num_list_from_txt takes a newline-separated .txt file with 
    project numbers that may optionally have a header and returns a 
    list object of project numbers.
    '''
    project_num_list = []

    with open(txt_filepath, "r") as f:
        lines = f.readlines()
        lines = lines[1:] if header else lines
        for line in lines:
            if line=="\n":
                continue # skip if blank line
            line = line.strip()
            project_num_list.append(line)
    
    return(project_num_list)

def create_project_num_list_from_csv(csv_filepath, output_path, output_suffix, id_type, project_id_col, project_title_col):
    '''
    create_project_num_list_from_csv takes the 'awarded.csv' file from https://heal.nih.gov/funding/awarded
    and returns 2 separate list objects of project numbers and core project numbers.  For debugging purposes, it also
    returns a list of project titles that do not have a project number associated.
    '''
    project_id_list = []
    missing_ids_list = []
    missing_ids_file = f"{output_path}/project_with_missing_ids_{output_suffix}.txt"

    with open(csv_filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            project_number = re.sub(r'[^\x00-\x7F]', '',row[project_id_col]).replace(" ","") # eliminate non UTF-8 characters
            project_title = re.sub(r'[^\x00-\x7F]', '', row[project_title_col]) # eliminate non UTF-8 characters
            if row[project_id_col] == "":
                missing_ids_list.append(project_title.strip())
            else:
                project_id_list.append(project_number.strip())
        
    # Make id list unique
    project_id_list = list(set(project_id_list))
    
    # Get core_project_num_list using re (if project nums)
    if id_type == "project_num":
        core_project_id_list = list(map(lambda x: re.sub("^[0-9]+","",x), project_id_list)) # remove beginning
        core_project_id_list = list(map(lambda x: re.match(".+(?=-)|[^-]+",x).group(0), core_project_id_list)) # find last instance of '-' and remove.
    else:
        core_project_id_list = project_id_list

    # Write to projects
    with open(missing_ids_file, "w") as f:
        for title in missing_ids_list:
            f.write("%s\n" % title)


    return(project_id_list,core_project_id_list)

def post_request(clean_non_utf, id_type, project_id_list, end_point = "projects/search", chunk_length=50):
    '''
    post_request hits the NIH reporter API end point and returns a list of results.
    Currently, the request body is hardcoded.  We could abstract out if needed.
    We split up API requests in increments equal to 'chunk_length' to avoid errors.
    '''
    # Add Wildcard Search
    #project_search_list = list(map(lambda x: re.match(".+(?=-)|[^-]+",x).group(0) + "*", project_num_list))

    # Initialize Results List
    results_list = []
    
    # Choosing which IDs to use
    if id_type == "project_serial_num":
        criteria_name = "project_serial_nums"
    else:
        # assumes project_num if not appl_id
        if end_point == "projects/search":
            criteria_name = "project_nums"
        else:
            criteria_name = "core_project_nums"

    # Request Details
    base_url = "https://api.reporter.nih.gov/v2/"
    url = f"{base_url}{end_point}"
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json'
        }

    for i in range(0,len(project_id_list),chunk_length):
        projects = project_id_list[i:i+chunk_length]

        # Request Body
        request_body = {
            
            "criteria":
            {
                #"include_active_projects": "true",
                criteria_name: projects
            },
            "offset":0,
            "limit":500
        }

        # Request Object
        req = requests.request(method = 'POST',
                            url = url,
                            headers = headers, 
                            json = request_body)

        # Create results variable
        results_obj = req.json()['results']

        # Clean non-utf-8 chars if flag is set
        if clean_non_utf:
            for j in range(0,len(results_obj)):
                # Convert to utf-8
                results_obj[j] = utfy_dict(results_obj[j])
                '''
                for k,v in results_obj[j].items():
                    if k == "abstract_text" or k == "project_title":
                        new_v = re.sub(r'"',"'",re.sub(r'[^\x00-\x7F]',"",str(v))).strip()
                        results_obj[j][k] = new_v
                '''        
        # Extend list
        results_list.extend(results_obj)

    return(results_list)

def utfy_dict(dic):
    if isinstance(dic,str):
        dic = re.sub(r'[^\x00-\x7F]','',str(dic)).strip()
        dic = re.sub(r'"',"'",dic)
        dic = re.sub(r'\n','. ',dic)
        return(dic)
        #return(dic.encode("utf-8").decode())
    elif isinstance(dic,dict):
        for key in dic:
            dic[key] = utfy_dict(dic[key])
        return(dic)
    elif isinstance(dic,list):
        new_l = []
        for e in dic:
            new_l.append(utfy_dict(e))
        return(new_l)
    else:
        return(dic)

def flatten_json(dictionary, parent_key=False, separator='.'):
    # https://github.com/ScriptSmith/socialreaper/blob/master/socialreaper/tools.py
    '''
    Turn a nested dictionary into a flattened dictionary
    :param dictionary: The dictionary to flatten
    :param parent_key: The string to prepend to dictionary's keys
    :param separator: The string used to separate flattened keys
    :return: A flattened dictionary
    '''

    items = []
    for key, value in dictionary.items():
        if value==None: # skip if None
            continue
        new_key = str(parent_key) + separator + key if parent_key else key

        if isinstance(value, collections.MutableMapping):
            items.extend(flatten_json(value, new_key, separator).items()) #recurse

        elif isinstance(value, list):
            if not value: #skip list if empty
                continue
            elif isinstance(value[0], dict):
                items.extend(flatten_json(merge_dict(value), new_key, separator).items()) #recurse
            else:
                value = ';'.join(map(str,value))
                items.append((new_key, str(value))) # append as tuple
            
        else:
            items.append((new_key, value)) # append as tuple
    return dict(items)

def merge_dict(dict_list):
    '''
    Merges dictionaries in a list by turning values of same keys into a 
    list of values in the target dictionary.
    '''
    d_new = {}
    for d in dict_list:
        for k,v in d.items():
            if k in d_new:
                d_new[k].append(v)
            else:
                d_new[k] = [v]
    return(d_new)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run script to query NIH RePORTER given Award IDs")
    parser.add_argument('id_type', action="store", help = "Specify ID type to use for the query - either 'appl_id' or 'project_serial_num'")
    parser.add_argument('input_filepath', action="store", help ="Specify path to file (.csv) containing list of IDs")
    parser.add_argument('output_path', action="store", help ="Specify absolute path for outputs")
    parser.add_argument('output_suffix', action="store", help ="Specify suffix string for file outputs")
    parser.add_argument('--project-id-column', dest="project_id_column", action="store", help = "Specify the column name in the file which contains the ID (application ID or project number)")
    parser.add_argument('--project-title-column', dest="project_title_column", action="store", help = "Specify the column name in the file which contains the project title")
    parser.add_argument('--replace-non-utf', dest="replace_non_utf", action="store_true", help = "Replace non-utf-8 characters in Title and Abstracts (optional)" )
    parser.add_argument('--return-related-project-nums', dest="return_related_project_nums_utf", action="store_true", help = "When searching by appl_id, do a second search for project_num" )
    parser.add_argument('--keep_non-utf', dest='replace_non_utf', action='store_false', help = "DO NOT replace non-utf-8 characters in Title and Abstracts (optional)")
    parser.set_defaults(replace_non_utf=True, return_related_project_nums=True)
    args = parser.parse_args()
    main(args)
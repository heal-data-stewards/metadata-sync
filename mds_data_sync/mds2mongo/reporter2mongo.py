import requests
import json
from pymongo import MongoClient
import re

def save_to_mongodb(collection, data):
    """Save data to MongoDB collection."""
    if isinstance(data, list):
        collection.insert_many(data)
    else:
        collection.insert_one(data)

def post_request(clean_non_utf, id_type, project_id_list, nih_reporter_collection, awards_collection, end_point="projects/search", chunk_length=50):
    """Send POST requests to the NIH RePORTER API and save responses to MongoDB."""
    results_list = []

    # Choosing which IDs to use
    if id_type == "appl_id":
        criteria_name = "appl_ids"
    else:
        criteria_name = "project_nums" if end_point == "projects/search" else "core_project_nums"

    # Request Details
    base_url = "https://api.reporter.nih.gov/v2/"
    url = f"{base_url}{end_point}"
    headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json'
    }
    for i in range(0, len(project_id_list), chunk_length):
        print("*" * 50)
        projects = project_id_list[i:i + chunk_length]

        request_body = {
            "criteria": {
                criteria_name: projects
            },
            "offset": 0,
            "limit": 500
        }

        response = requests.post(url, headers=headers, json=request_body)

        if response.status_code == 200:
            print(f"Successfully fetched data for {len(projects)} projects")
            results_obj = response.json().get('results', [])
            print(json.dumps(results_obj, indent=2))
            if clean_non_utf:
                results_obj = [utfy_dict(result) for result in results_obj]

            results_list.extend(results_obj)

            # # Separate results into reporter and awards collections
            # reporter_data = [result for result in results_obj if 'nih_reporter_field' in result]  # Example filter
            # award_data = [result for result in results_obj if 'award_field' in result]  # Example filter

            # # Save to respective collections
            # if reporter_data:

            #     save_to_mongodb(nih_reporter_collection, reporter_data)
            # if award_data:
            #     save_to_mongodb(awards_collection, award_data)
            
            save_to_mongodb(nih_reporter_collection, results_list)
            request_body["offset"] += 500
        else:
            print(f"Failed to fetch data: {response.status_code} {response.text}")

    return results_list

# def utfy_dict(dic):
#     """Clean non-UTF characters in a dictionary."""
#     if isinstance(dic, str):
#         dic = re.sub(r'[^\x00-\x7F]','',str(dic)).strip()
#         dic = re.sub(r'"',"'",dic)
#         dic = re.sub(r'\n','. ',dic)
#         return dic
#     elif isinstance(dic, dict):
#         return {k: utfy_dict(v) for k, v in dic.items()}
#     elif isinstance(dic, list):
#         return [utfy_dict(item) for item in dic]
#     else:
#         return dic

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

def get_unique_appl_ids(mongo_uri, database, collection):
    """Retrieve unique application IDs from MongoDB."""
    client = MongoClient(mongo_uri)
    result = client[database][collection].aggregate([
        {
            '$project': {
                'combined_appl_ids': {
                    '$setUnion': [
                        {
                            '$cond': [
                                {
                                    '$isArray': '$gen3_discovery.appl_id'
                                }, '$gen3_discovery.appl_id', [
                                    {
                                        '$convert': {
                                            'input': '$gen3_discovery.appl_id', 
                                            'to': 'int', 
                                            'onError': None, 
                                            'onNull': None
                                        }
                                    }
                                ]
                            ]
                        }, [
                            {
                                '$convert': {
                                    'input': '$nih_reporter.appl_id', 
                                    'to': 'int', 
                                    'onError': None, 
                                    'onNull': None
                                }
                            }
                        ]
                    ]
                }
            }
        }, {
            '$unwind': '$combined_appl_ids'
        }, {
            '$match': {
                'combined_appl_ids': {
                    '$ne': None
                }
            }
        }, {
            '$group': {
                '_id': None, 
                'unique_appl_ids': {
                    '$addToSet': '$combined_appl_ids'
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'unique_appl_ids': 1
            }
        }
    ])

    unique_appl_ids = []
    for doc in result:
        unique_appl_ids.extend(doc.get('unique_appl_ids', []))

    return unique_appl_ids

def main():
    import os
    # MongoDB setup
    mongo_uri = os.getenv("MONGODB_ATLAS_SRV")
    source_database = os.getenv("MONGODB_DB_NAME")
    source_collection = os.getenv("MONGODB_SNAPSHOT_COLLECTION")
    target_database = source_database

    client = MongoClient(mongo_uri)
    db = client[target_database]
    nih_reporter_collection = db[os.getenv("MONGODB_REPORTER_COLLECTION")]
    awards_collection = db[os.getenv("MONGODB_AWARD_COLLECTION")]

    # reset reporter, awards collections
    nih_reporter_collection.delete_many({})
    awards_collection.delete_many({})

    # Get unique application IDs
    unique_appl_ids = get_unique_appl_ids(mongo_uri, source_database, source_collection)

    print(len(unique_appl_ids))
    # Query API and save to MongoDB
    post_request(clean_non_utf=True, id_type="appl_id", project_id_list=unique_appl_ids, 
                 nih_reporter_collection=nih_reporter_collection, awards_collection=awards_collection)

if __name__ == "__main__":
    main()

import requests
from pymongo import MongoClient
import json
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv(override=True)

# Define CEDAR fields to be included
CEDAR_FIELDS = [
    "data",
    "study_type",
    "minimal_info",
    "data_availability",
    "metadata_location",
    "study_translational_focus",
    "human_subject_applicability",
    "human_condition_applicability",
    "human_treatment_applicability"
]

# Define fields to explicitly exclude
EXCLUDED_FIELDS = [
    "metadata_location.data_repositories",
    "metadata_location.nih_reporter_link",
    "metadata_location.nih_application_id",
    "metadata_location.clinical_trials_study_ID",
    "metadata_location.cedar_study_level_metadata_template_instance_ID"
]

def fetch_metadata(url):
    """Fetch data from the endpoint."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return list(data.values()) if isinstance(data, dict) else data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {url}: {e}")
        return []

def calculate_cedar_completion(doc):
    """Calculate the number of completed CEDAR form fields, but set completion percentage to 0% if unregistered."""
    gen3_discovery = doc.get("gen3_discovery", {})
    
    # Check registration status
    is_registered = gen3_discovery.get("is_registered", False)

    # Extract study metadata
    cedar_metadata = gen3_discovery.get("study_metadata", {})
    total_fields = 0
    completed_fields = 0
    missing_fields = []

    for section, fields in cedar_metadata.items():
        if section in CEDAR_FIELDS:
            if isinstance(fields, dict):  # If section is a dictionary, iterate through its fields
                for field_name, value in fields.items():
                    full_field_name = f"{section}.{field_name}"
                    
                    # Skip excluded fields
                    if full_field_name in EXCLUDED_FIELDS:
                        continue
                    
                    total_fields += 1
                    
                    # Field is considered completed if it's not empty, not "0", and not NaN
                    if value not in ["", None, "0"]:
                        completed_fields += 1
                    else:
                        missing_fields.append(full_field_name)  # Track missing fields for debugging

    # Calculate the completion percentage but set it to 0% if unregistered
    completion_percentage = 0 if not is_registered else round((completed_fields / total_fields) * 100, 2) if total_fields > 0 else 0

    return completed_fields, total_fields, completion_percentage, missing_fields

def process_data(data):
    """Process data to extract and compute CEDAR form completion stats."""
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for doc in data:
        completed, total, percent, missing = calculate_cedar_completion(doc)
        doc["cedar_completed_fields"] = completed
        doc["cedar_total_fields"] = total
        doc["cedar_completion_percent"] = percent  # Always 0% if unregistered
        doc["cedar_missing_fields"] = missing  # (Optional) Store missing fields for validation/debugging
        doc["cedar_last_updated"] = now  # Timestamp for last update
    return data

def save_to_mongodb(data, mongo_uri, db_name, collection_name):
    """Save data to MongoDB collection."""
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]

        # Drop the collection before inserting new data
        collection.delete_many({})
        print(f"Collection '{collection_name}' dropped.")

        # Insert processed data into MongoDB
        if isinstance(data, list):
            result = collection.insert_many(data)
            print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")
        else:
            print("Data is not a list. Skipping MongoDB insertion.")
    except Exception as e:
        print(f"Error saving data to MongoDB: {e}")
    finally:
        client.close()

def main():
    # Define parameters
    endpoint = "https://healdata.org/mds/metadata?data=True&limit=1000000"
    mongo_uri = os.getenv("MONGODB_ATLAS_SRV")
    db_name = os.getenv("MONGODB_DB_NAME")
    collection_name = os.getenv("MONGODB_SNAPSHOT_COLLECTION")

    # Fetch data from endpoint
    data = fetch_metadata(endpoint)
    if data:
        print("Data fetched successfully.")

        # Process data to include CEDAR completion stats
        processed_data = process_data(data)

        # Save processed data to MongoDB
        save_to_mongodb(processed_data, mongo_uri, db_name, collection_name)
    else:
        print("No data to save.")

if __name__ == "__main__":
    main()



# import requests
# from pymongo import MongoClient
# import json
# import os
# from dotenv import load_dotenv
# from datetime import datetime

# load_dotenv(override = True)


# # Define CEDAR fields to be included
# CEDAR_FIELDS = [
#     "data",
#     "study_type",
#     "minimal_info",
#     "data_availability",
#     "metadata_location",
#     "study_translational_focus",
#     "human_subject_applicability",
#     "human_condition_applicability",
#     "human_treatment_applicability"
# ]

# # Define fields to explicitly exclude
# EXCLUDED_FIELDS = [
#     "metadata_location.data_repositories",
#     "metadata_location.nih_reporter_link",
#     "metadata_location.nih_application_id",
#     "metadata_location.clinical_trials_study_ID",
#     "metadata_location.cedar_study_level_metadata_template_instance_ID"
# ]

# def fetch_metadata(url):
#     """Fetch data from the endpoint."""
#     try:
#         response = requests.get(url)
#         response.raise_for_status()
#         data = response.json()
#         return list(data.values()) if isinstance(data, dict) else data
#     except requests.exceptions.RequestException as e:
#         print(f"Error fetching data from {url}: {e}")
#         return []

# def calculate_cedar_completion(doc):
#     """Calculate the number and percentage of completed CEDAR form fields."""
#     gen3_discovery = doc.get("gen3_discovery", {})
    
#     # If 'is_registered' is False, set completion to 0
#     is_registered = gen3_discovery.get("is_registered", False)
#     if not is_registered:
#         return 0, 0, 0, []

#     cedar_metadata = gen3_discovery.get("study_metadata", {})
#     total_fields = 0
#     completed_fields = 0
#     missing_fields = []

#     for section, fields in cedar_metadata.items():
#         if section in CEDAR_FIELDS:
#             if isinstance(fields, dict):  # If section is a dictionary, iterate through its fields
#                 for field_name, value in fields.items():
#                     full_field_name = f"{section}.{field_name}"
                    
#                     # Skip excluded fields
#                     if full_field_name in EXCLUDED_FIELDS:
#                         continue
                    
#                     total_fields += 1
                    
#                     # Field is considered completed if it's not empty, not "0", and not NaN
#                     if value not in ["", None, "0"]:
#                         completed_fields += 1
#                     else:
#                         missing_fields.append(full_field_name)  # Track missing fields for debugging

#     completion_percentage = round((completed_fields / total_fields) * 100, 2) if total_fields > 0 else 0
#     return completed_fields, total_fields, completion_percentage, missing_fields

# def process_data(data):
#     """Process data to extract and compute CEDAR form completion stats."""
#     now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#     for doc in data:
#         completed, total, percent, missing = calculate_cedar_completion(doc)
#         doc["cedar_completed_fields"] = completed
#         doc["cedar_total_fields"] = total
#         doc["cedar_completion_percent"] = percent
#         doc["cedar_missing_fields"] = missing  # (Optional) Store missing fields for validation/debugging
#         doc["cedar_last_updated"] = now  # Timestamp for last update
#     return data

# def save_to_mongodb(data, mongo_uri, db_name, collection_name):
#     """Save data to MongoDB collection."""
#     try:
#         client = MongoClient(mongo_uri)
#         db = client[db_name]
#         collection = db[collection_name]

#         # Drop the collection before inserting new data
#         collection.delete_many({})
#         print(f"Collection '{collection_name}' dropped.")

#         # Insert processed data into MongoDB
#         if isinstance(data, list):
#             result = collection.insert_many(data)
#             print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")
#         else:
#             print("Data is not a list. Skipping MongoDB insertion.")
#     except Exception as e:
#         print(f"Error saving data to MongoDB: {e}")
#     finally:
#         client.close()

# def main():
#     # Define parameters
#     endpoint = "https://healdata.org/mds/metadata?data=True&limit=1000000"
#     mongo_uri = os.getenv("MONGODB_ATLAS_SRV")
#     db_name = os.getenv("MONGODB_DB_NAME")
#     collection_name = os.getenv("MONGODB_SNAPSHOT_COLLECTION")

#     # Fetch data from endpoint
#     data = fetch_metadata(endpoint)
#     if data:
#         print("Data fetched successfully.")

#         # Process data to include CEDAR completion stats
#         processed_data = process_data(data)

#         # Save processed data to MongoDB
#         save_to_mongodb(processed_data, mongo_uri, db_name, collection_name)
#     else:
#         print("No data to save.")

# if __name__ == "__main__":
#     main()
# # # Define CEDAR fields to be included
# # CEDAR_FIELDS = [
# #     "data",
# #     "study_type",
# #     "minimal_info",
# #     "data_availability",
# #     "metadata_location",
# #     "study_translational_focus",
# #     "human_subject_applicability",
# #     "human_condition_applicability",
# #     "human_treatment_applicability"
# # ]

# # # Define fields to explicitly exclude
# # EXCLUDED_FIELDS = [
# #     "metadata_location.data_repositories",
# #     "metadata_location.nih_reporter_link",
# #     "metadata_location.nih_application_id",
# #     "metadata_location.clinical_trials_study_ID",
# #     "metadata_location.cedar_study_level_metadata_template_instance_ID"
# # ]

# # def fetch_metadata(url):
# #     """Fetch data from the endpoint."""
# #     try:
# #         response = requests.get(url)
# #         response.raise_for_status()
# #         data = response.json()
# #         return list(data.values()) if isinstance(data, dict) else data
# #     except requests.exceptions.RequestException as e:
# #         print(f"Error fetching data from {url}: {e}")
# #         return []

# # def calculate_cedar_completion(doc):
# #     """Calculate the number and percentage of completed CEDAR form fields."""
# #     cedar_metadata = doc.get("gen3_discovery", {}).get("study_metadata", {})

# #     total_fields = 0
# #     completed_fields = 0
# #     missing_fields = []

# #     for section, fields in cedar_metadata.items():
# #         if section in CEDAR_FIELDS:
# #             if isinstance(fields, dict):  # If section is a dictionary, iterate through its fields
# #                 for field_name, value in fields.items():
# #                     full_field_name = f"{section}.{field_name}"
                    
# #                     # Skip excluded fields
# #                     if full_field_name in EXCLUDED_FIELDS:
# #                         continue
                    
# #                     total_fields += 1
                    
# #                     # Field is considered completed if it's not empty, not "0", and not NaN
# #                     if value not in ["", None, "0"]:
# #                         completed_fields += 1
# #                     else:
# #                         missing_fields.append(full_field_name)  # Track missing fields for debugging

# #     completion_percentage = round((completed_fields / total_fields) * 100, 2) if total_fields > 0 else 0
# #     return completed_fields, total_fields, completion_percentage, missing_fields

# # def process_data(data):
# #     """Process data to extract and compute CEDAR form completion stats."""
# #     now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

# #     for doc in data:
# #         completed, total, percent, missing = calculate_cedar_completion(doc)
# #         doc["cedar_completed_fields"] = completed
# #         doc["cedar_total_fields"] = total
# #         doc["cedar_completion_percent"] = percent
# #         doc["cedar_missing_fields"] = missing  # (Optional) Store missing fields for validation/debugging
# #         doc["cedar_last_updated"] = now  # Timestamp for last update
# #     return data

# # def save_to_mongodb(data, mongo_uri, db_name, collection_name):
# #     """Save data to MongoDB collection."""
# #     try:
# #         client = MongoClient(mongo_uri)
# #         db = client[db_name]
# #         collection = db[collection_name]

# #         # Drop the collection before inserting new data
# #         collection.delete_many({})
# #         print(f"Collection '{collection_name}' dropped.")

# #         # Insert processed data into MongoDB
# #         if isinstance(data, list):
# #             result = collection.insert_many(data)
# #             print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")
# #         else:
# #             print("Data is not a list. Skipping MongoDB insertion.")
# #     except Exception as e:
# #         print(f"Error saving data to MongoDB: {e}")
# #     finally:
# #         client.close()

# # def main():
# #     # Define parameters
# #     endpoint = "https://healdata.org/mds/metadata?data=True&limit=1000000"
# #     mongo_uri = os.getenv("MONGODB_ATLAS_SRV")
# #     db_name = os.getenv("MONGODB_DB_NAME")
# #     # collection_name = os.getenv("MONGODB_DB_COLLECTION")
# #     collection_name = os.getenv("MONGODB_SNAPSHOT_COLLECTION")


# #     # Fetch data from endpoint
# #     data = fetch_metadata(endpoint)
# #     if data:
# #         print("Data fetched successfully.")

# #         # Process data to include CEDAR completion stats
# #         processed_data = process_data(data)

# #         # Save processed data to MongoDB
# #         save_to_mongodb(processed_data, mongo_uri, db_name, collection_name)
# #     else:
# #         print("No data to save.")

# # if __name__ == "__main__":
# #     main()

# # # # Define fields that belong to the CEDAR form
# # # CEDAR_FIELDS = [
# # #     "data",
# # #     "study_type",
# # #     "minimal_info",
# # #     "data_availability",
# # #     "metadata_location",
# # #     "study_translational_focus",
# # #     "human_subject_applicability",
# # #     "human_condition_applicability",
# # #     "human_treatment_applicability",
# # #     "time_of_registration",
# # #     "time_of_last_cedar_updated"
# # # ]

# # # EXCLUDED_FIELDS = [
# # #     "metadata_location.nih_reporter_link",
# # #     "metadata_location.nih_application_id",
# # #     "metadata_location.clinical_trials_study_ID",
# # #     "metadata_location.cedar_study_level_metadata_template_instance_ID"
# # # ]

# # # def fetch_metadata(url):
# # #     """Fetch data from the endpoint."""
# # #     try:
# # #         response = requests.get(url)
# # #         response.raise_for_status()
# # #         data = response.json()
# # #         if isinstance(data, dict):
# # #             return list(data.values())  # Convert dictionary to a list of objects
# # #         elif isinstance(data, list):
# # #             return data  # Return as is if already a list
# # #         else:
# # #             print("Unexpected data format from API.")
# # #             return []
# # #     except requests.exceptions.RequestException as e:
# # #         print(f"Error fetching data from {url}: {e}")
# # #         return []

# # # def extract_cedar_metadata(doc):
# # #     """Extract CEDAR metadata fields from `study_metadata` dynamically."""
# # #     cedar_metadata = {}

# # #     # Ensure the document has `gen3_discovery` and `study_metadata`
# # #     study_metadata = doc.get("gen3_discovery", {}).get("study_metadata", {})

# # #     for section, fields in study_metadata.items():
# # #         if section in CEDAR_FIELDS:
# # #             cedar_metadata[section] = fields

# # #     return cedar_metadata

# # # def calculate_cedar_completion(doc):
# # #     """Calculate the number and percentage of completed CEDAR form fields."""
# # #     cedar_metadata = extract_cedar_metadata(doc)
    
# # #     total_fields = 0
# # #     completed_fields = 0

# # #     for section, fields in cedar_metadata.items():
# # #         if isinstance(fields, dict):  # If section is a dictionary, iterate through its fields
# # #             for field_name, value in fields.items():
# # #                 full_field_name = f"{section}.{field_name}"
                
# # #                 # Exclude explicitly ignored fields
# # #                 if full_field_name in EXCLUDED_FIELDS:
# # #                     continue
                
# # #                 total_fields += 1
# # #                 if value not in ["", None]:  # Consider non-empty fields as completed
# # #                     completed_fields += 1

# # #     completion_percentage = round((completed_fields / total_fields) * 100, 2) if total_fields > 0 else 0
# # #     return completed_fields, total_fields, completion_percentage

# # # def process_data(data):
# # #     """Process data to extract and compute CEDAR form completion stats."""
# # #     for doc in data:
# # #         completed, total, percent = calculate_cedar_completion(doc)
# # #         doc["cedar_completed_fields"] = completed
# # #         doc["cedar_total_fields"] = total
# # #         doc["cedar_completion_percent"] = percent
# # #     return data

# # # def save_to_mongodb(data, mongo_uri, db_name, collection_name):
# # #     """Save data to MongoDB collection."""
# # #     try:
# # #         client = MongoClient(mongo_uri)
# # #         db = client[db_name]
# # #         collection = db[collection_name]

# # #         # Drop the collection before inserting new data
# # #         collection.delete_many({})
# # #         print(f"Collection '{collection_name}' dropped.")

# # #         # Insert processed data into MongoDB
# # #         if isinstance(data, list):
# # #             result = collection.insert_many(data)
# # #             print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")
# # #         else:
# # #             print("Data is not a list. Skipping MongoDB insertion.")
# # #     except Exception as e:
# # #         print(f"Error saving data to MongoDB: {e}")
# # #     finally:
# # #         client.close()

# # # def main():
# # #     # Define parameters
# # #     endpoint = "https://healdata.org/mds/metadata?data=True&limit=1000000"
# # #     mongo_uri = os.getenv("MONGODB_ATLAS_SRV")
# # #     db_name = os.getenv("MONGODB_DB_NAME")
# # #     # collection_name = os.getenv("MONGODB_DB_COLLECTION")
# # #     collection_name = os.getenv("MONGODB_SNAPSHOT_COLLECTION")

# # #     # Fetch data from endpoint
# # #     data = fetch_metadata(endpoint)
# # #     if data:
# # #         print("Data fetched successfully.")

# # #         # Process data to include CEDAR completion stats
# # #         processed_data = process_data(data)

# # #         # Save processed data to MongoDB
# # #         save_to_mongodb(processed_data, mongo_uri, db_name, collection_name)
# # #     else:
# # #         print("No data to save.")

# # # if __name__ == "__main__":
# # #     main()

# # # # def fetch_metadata(url):
# # # #     """Fetch data from the endpoint."""
# # # #     try:
# # # #         response = requests.get(url)
# # # #         response.raise_for_status()
# # # #         data = response.json()
# # # #         if isinstance(data, dict):
# # # #             # Convert the dict of objects to a list of objects
# # # #             return list(data.values())
# # # #         elif isinstance(data, list):
# # # #             return data  # Return as is if already a list
# # # #         else:
# # # #             print("Unexpected data format from API.")
# # # #             return []
# # # #     except requests.exceptions.RequestException as e:
# # # #         print(f"Error fetching data from {url}: {e}")
# # # #         return []

# # # # def save_to_mongodb(data, mongo_uri, db_name, collection_name):
# # # #     """Save data to MongoDB collection."""
# # # #     try:
# # # #         client = MongoClient(mongo_uri)
# # # #         db = client[db_name]
# # # #         collection = db[collection_name]

# # # #         # Drop the collection before inserting new data
# # # #         collection.delete_many({})
# # # #         print(f"Collection '{collection_name}' dropped.")

# # # #         # Insert data into the collection
# # # #         if isinstance(data, list):
# # # #             result = collection.insert_many(data)
# # # #             print(f"Inserted {len(result.inserted_ids)} documents into MongoDB.")
# # # #         else:
# # # #             print("Data is not a list. Skipping MongoDB insertion.")
# # # #     except Exception as e:
# # # #         print(f"Error saving data to MongoDB: {e}")
# # # #     finally:
# # # #         client.close()

# # # # def main():
# # # #     # Define parameters
# # # #     endpoint = "https://healdata.org/mds/metadata?data=True&limit=1000000"
# # # #     mongo_uri = os.getenv("MONGODB_ATLAS_SRV")
# # # #     db_name = os.getenv("MONGODB_DB_NAME")
# # # #     collection_name = os.getenv("MONGODB_DB_COLLECTION")

# # # #     # Fetch data from endpoint
# # # #     data = fetch_metadata(endpoint)
# # # #     if data:
# # # #         print("Data fetched successfully.")

# # # #         # Save data to MongoDB
# # # #         save_to_mongodb(data, mongo_uri, db_name, collection_name)
# # # #     else:
# # # #         print("No data to save.")

# # # # if __name__ == "__main__":
# # # #     main()

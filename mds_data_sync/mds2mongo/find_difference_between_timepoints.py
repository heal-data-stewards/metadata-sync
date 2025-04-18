from deepdiff import DeepDiff
from dotenv import load_dotenv
from pymongo import MongoClient
import json
import os
import csv

load_dotenv(override=True)

# MongoDB Connection
client = MongoClient(os.getenv("MONGODB_ATLAS_SRV"))
db = client[os.getenv("MONGODB_DB_NAME")]
collection_before = db["mds_jan2025"]
collection_after = db["mds_feb2025"]

output_file = "differences.csv"
csv_data = []  # Collect data in memory before writing to the file

print(f">>> Comparing collections '{collection_before.name}' and '{collection_after.name}' <<<")

# Track all document IDs from both collections
before_ids = set(doc["_id"] for doc in collection_before.find({}, {"_id": 1}))
after_ids = set(doc["_id"] for doc in collection_after.find({}, {"_id": 1}))

# Find documents that were deleted
deleted_ids = before_ids - after_ids
# Find documents that were created
created_ids = after_ids - before_ids

# Handle deleted documents
for doc_id in deleted_ids:
    print(f"Document with _id {doc_id} was deleted.")
    csv_data.append([collection_before.name, collection_after.name, doc_id, "N/A", "Document deleted", "N/A"])

# Handle created documents
for doc_id in created_ids:
    print(f"Document with _id {doc_id} was created.")
    csv_data.append([collection_before.name, collection_after.name, doc_id, "N/A", "N/A", "Document created"])

# Compare existing documents
for doc_before in collection_before.find():
    doc_id = doc_before["_id"]
    doc_after = collection_after.find_one({"_id": doc_id})

    if not doc_after:
        continue  # Skip since it's already handled in the deleted section

    # Compare using DeepDiff
    diff = DeepDiff(doc_before, doc_after, verbose_level=2)

    if diff:
        print("#" * 80)
        print(f"### Changes for document with _id {doc_id}")
        print("#" * 80)
        
        try:
            appl_id = doc_before["nih_reporter"]["appl_id"]
        except KeyError:
            appl_id = "N/A"
            print("No appl_id found.")

        for change_type, changes in diff.items():
            for path, details in changes.items():
                old_value = details.get("old_value", "N/A")
                new_value = details.get("new_value", "N/A")

                # Collect change data in memory
                csv_data.append([collection_before.name, collection_after.name, doc_id, appl_id, old_value, new_value])

                # Print change details
                print(f"{change_type}: ** {path}:\n{json.dumps(details, indent=2)}")

client.close()

# Write collected data to CSV at the end
with open(output_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["collection_before", "collection_after", "doc_id", "appl_id", "old_value", "new_value"])
    writer.writerows(csv_data)

print(f"\nChanges saved to {output_file}")

# Monday API PROD HEAL Studies Board Extract all Data
# Getting all the data from a board and saving to csv

import csv
import requests
import time

from datetime import date

# --- CONFIGURATION ---
MONDAY_API_KEY = "COPY YOUR API KEY HERE"
BOARD_ID = "1953661502"  
OUTPUT_FILE = (
    r"C:\Users\berman\OneDrive - Research Triangle Institute\Python Environment\HEAL\Output"
    + f"\\monday_heal_studies_{date.today().isoformat()}.csv"
)

API_URL = "https://api.monday.com/v2"


# API call tracking
api_call_count = 0
api_call_log = []


def monday_api_request(query, headers):
    """Send a Monday.com API request and track usage."""

    global api_call_count

    api_call_count += 1

    start_time = time.time()

    response = requests.post(
        API_URL,
        json={"query": query},
        headers=headers
    )

    elapsed_time = time.time() - start_time

    api_call_log.append({
        "api_call": api_call_count,
        "status_code": response.status_code,
        "elapsed_seconds": round(elapsed_time, 2)
    })

    response.raise_for_status()

    result = response.json()

    if "errors" in result:
        raise RuntimeError(f"Monday.com API error: {result['errors']}")

    return result



# ------------------------------------------------
# API USAGE SUMMARY
# ------------------------------------------------

print("\nMONDAY.COM API USAGE SUMMARY")
print("-" * 40)
print(f"Total API requests: {api_call_count}")
print(f"Successful requests: {sum(x['status_code'] == 200 for x in api_call_log)}")
print(f"Failed HTTP requests: {sum(x['status_code'] != 200 for x in api_call_log)}")









headers = {
    "Authorization": MONDAY_API_KEY,
    "Content-Type": "application/json",
    "API-Version": "2024-01",  # Pinning stable API version
}



def fetch_board_page(cursor=None):
    """Queries a single page of items from the Monday.com board."""
    # If we have a cursor, we pass it to items_page. Otherwise, we fetch the first page.
    if cursor:
        query = """
        query ($board_id: [ID!], $cursor: String!) {
          boards(ids: $board_id) {
            items_page(limit: 100, cursor: $cursor) {
              cursor
              items {
                id
                name
                column_values {
                  column {
                    title
                  }
                  text
                }
              }
            }
          }
        }
        """
        variables = {"board_id": [BOARD_ID], "cursor": cursor}
    else:
        query = """
        query ($board_id: [ID!]) {
          boards(ids: $board_id) {
            items_page(limit: 100) {
              cursor
              items {
                id
                name
                column_values {
                  column {
                    title
                  }
                  text
                }
              }
            }
          }
        }
        """
        variables = {"board_id": [BOARD_ID]}

    response = requests.post(
        API_URL, json={"query": query, "variables": variables}, headers=headers
    )

    if response.status_code != 200:
        raise Exception(f"Query failed with status code {response.status_code}")

    res_json = response.json()
    if "errors" in res_json:
        raise Exception(f"API Error: {res_json['errors']}")

    return res_json["data"]["boards"][0]["items_page"]


def main():
    all_rows = []
    headers_set = set()
    cursor = None
    has_more = True

    print("Fetching data from Monday.com...")

    # 1. Paginate through all items on the board
    while has_more:
        page_data = fetch_board_page(cursor)
        items = page_data["items"]
        cursor = page_data["cursor"]

        for item in items:
            # Base item data
            row_dict = {"Item ID": item["id"], "Item Name": item["name"]}

            # Extract column values dynamically
            for cv in item["column_values"]:
                col_title = cv["column"]["title"]
                col_text = cv["text"]
                row_dict[col_title] = col_text
                headers_set.add(col_title)

            all_rows.append(row_dict)

        # If cursor is None or empty, we reached the end of the board
        if not cursor:
            has_more = False

    if not all_rows:
        print("No data found on this board.")
        return

    # 2. Structure CSV headers
    # csv_headers = ["Item ID", "Item Name"] + sorted(list(headers_set))
    csv_headers = ["Item ID", "Item Name"] + list(headers_set)
    # 3. Write data to CSV file
    with open(OUTPUT_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=csv_headers)
        writer.writeheader()
        for row in all_rows:
            # Ensure missing columns default to blank strings
            final_row = {col: row.get(col, "") for col in csv_headers}
            writer.writerow(final_row)

    print(f"Successfully extracted {len(all_rows)} items to '{OUTPUT_FILE}'!")


if __name__ == "__main__":
    main()

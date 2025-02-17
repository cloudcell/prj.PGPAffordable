# read the table molecules from bio_data.duck.db
# get the id from there and use it in the following query

# "query {
#   drug(chemblId: "CHEMBL1637") {
#     name
#     tradeNames
#     mechanismsOfAction {
#       rows {
#         actionType
#         mechanismOfAction
#         references {
#           source
#           urls
#         }
#         targets {
#           id
#           approvedName
#         }
#       }
#     }
#   }
# }
# "

import json
from time import sleep
import duckdb
import requests

# Connect to DuckDB
con = duckdb.connect("bio_data.duck.db")

# Query the molecules table to get IDs
molecule_ids = con.execute("SELECT id FROM molecules").fetchall()

# Convert to a list of IDs
molecule_ids = [row[0] for row in molecule_ids]

# Print first few IDs to check
print(molecule_ids[:5])

# GraphQL endpoint (modify if needed)
GRAPHQL_ENDPOINT = "https://api.platform.opentargets.org/api/v4/graphql"

# Function to send request
def fetch_drug_data(chembl_id):
    query = f"""
    query {{
      drug(chemblId: "{chembl_id}") {{
        name
        tradeNames
        mechanismsOfAction {{
          rows {{
            actionType
            mechanismOfAction
            references {{
              source
              urls
            }}
            targets {{
              id
              approvedName
            }}
          }}
        }}
      }}
    }}
    """
    
    response = requests.post(GRAPHQL_ENDPOINT, json={"query": query})
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {chembl_id}: {response.status_code}")
        return None

# Fetch data for first 5 IDs and save to files
for i, chembl_id in enumerate(sorted(molecule_ids), 1):
    print(f'{i}/{len(molecule_ids)} {chembl_id}                  ', end='\r')
    drug_data = fetch_drug_data(chembl_id)
    sleep(0.5)  # Respect API rate limits

    if drug_data:
        output_file = f"data_tmp/mol_{chembl_id}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(drug_data, f, indent=4)
    else:
        print(f"⚠️ No data found for {chembl_id}")

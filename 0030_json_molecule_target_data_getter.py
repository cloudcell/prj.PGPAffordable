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

import os
import json
import shutil
from time import sleep

import duckdb
import requests
from tqdm import tqdm


# Connect to DuckDB
con = duckdb.connect("bio_data.duck.db")

# Query the molecules table to get IDs
molecule_ids = con.execute("SELECT id FROM molecules").fetchall()
con.close()

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
for chembl_id in tqdm(sorted(molecule_ids), smoothing=1):
    output_file = f"data_tmp/mol_target_{chembl_id}.json"
    output_file_tmp = 'data_tmp/mol_target_tmp'
    if os.path.exists(output_file):
        continue
    drug_data = fetch_drug_data(chembl_id)
    sleep(0.5)  # Respect API rate limits

    if drug_data:
        with open(output_file_tmp, "w", encoding="utf-8") as f:
            json.dump(drug_data, f, indent=4)
        shutil.move(output_file_tmp, output_file)
    else:
        print(f"⚠️ No data found for {chembl_id}")

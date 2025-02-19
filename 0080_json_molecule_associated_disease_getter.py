
import os
import json
import shutil
from time import sleep

import duckdb
import requests
from tqdm import tqdm


data_dir = "data_tmp"

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
def fetch_linked_diseases_data(chembl_id):
    query = f"""
    query {{
        drug(chemblId: "{chembl_id}") {{
            linkedDiseases {{
                rows {{
                    id
                    name
                    description
                }}
            }}
        }}
    }}
    """

    response = requests.post(GRAPHQL_ENDPOINT, json={"query": query})

    if response.status_code == 200:
        linked_diseases = response.json().get('data', {}).get('drug', {}).get('linkedDiseases', {})
        return linked_diseases.get('rows', []) if linked_diseases is not None else []
    else:
        print(f"Error fetching data for {chembl_id}: {response.status_code}")
        return None

output_file_tmp = os.path.join(data_dir, 'mol_disease_tmp')

# Fetch data and save to files
for chembl_id in tqdm(sorted(molecule_ids), smoothing=1):
    output_file = os.path.join(data_dir, f'mol_disease_{chembl_id}.json')
    if os.path.exists(output_file):
        continue
    linked_diseases_list = fetch_linked_diseases_data(chembl_id)
    sleep(0.5)  # Respect API rate limits

    if linked_diseases_list is not None:
        with open(output_file_tmp, "w", encoding="utf-8") as f:
            json.dump(linked_diseases_list, f, indent=4)
        shutil.move(output_file_tmp, output_file)
    else:
        print(f"⚠️ No data found for {chembl_id}")

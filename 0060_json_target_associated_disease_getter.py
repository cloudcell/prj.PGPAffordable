
import os
import json
import shutil
from time import sleep

import duckdb
import requests
from tqdm import tqdm


data_dir = "data_tmp"

con = duckdb.connect("bio_data.duck.db")
targets = con.execute("SELECT target_id FROM targets").fetchall()
con.close()

# Convert to a list of IDs
targets = [row[0] for row in targets]

# Print first few IDs to check
print(targets[:5])

# GraphQL endpoint (modify if needed)
GRAPHQL_ENDPOINT = "https://api.platform.opentargets.org/api/v4/graphql"

# Function to send request
def fetch_associated_diseases_data(target_id):
    query = f"""
    query {{
        target(ensemblId: "{target_id}") {{
            associatedDiseases {{
                rows {{
                    disease {{
                        id
                        name
                        description
                    }}
                }}
            }}
        }}
    }}
    """

    response = requests.post(GRAPHQL_ENDPOINT, json={"query": query})

    if response.status_code == 200:
        return response.json().get('data', {}).get('target', {}).get('associatedDiseases', {}).get('rows', [])
    else:
        print(f"Error fetching data for {target_id}: {response.status_code}")
        return None

output_file_tmp = os.path.join(data_dir, 'target_disease_tmp')

for target_id in tqdm(sorted(targets), smoothing=1):
    output_file = os.path.join(data_dir, f'target_disease_{target_id}.json')
    if os.path.exists(output_file):
        continue
    associated_diseases_list = fetch_associated_diseases_data(target_id)
    sleep(0.5)  # Respect API rate limits

    if associated_diseases_list:
        with open(output_file_tmp, "w", encoding="utf-8") as f:
            json.dump(associated_diseases_list, f, indent=4)
        shutil.move(output_file_tmp, output_file)
    else:
        print(f"⚠️ No data found for {target_id}")

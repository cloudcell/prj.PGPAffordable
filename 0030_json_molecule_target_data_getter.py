import os
import json
import shutil
from time import sleep

import duckdb
import requests
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

data_dir = "data_tmp"

# Connect to DuckDB
con = duckdb.connect("bio_data.duck.db")

# Query the tbl_molecules table to get IDs
molecule_ids = con.execute("SELECT id FROM tbl_molecules").fetchall()
con.close()

# Convert to a list of IDs
molecule_ids = [row[0] for row in molecule_ids]

# Print first few IDs to check
print(molecule_ids[:5])

# GraphQL endpoint (modify if needed)
GRAPHQL_ENDPOINT = "https://api.platform.opentargets.org/api/v4/graphql"

# Set up session with retries
session = requests.Session()
retries = Retry(
    total=5,                 # Maximum retry attempts
    backoff_factor=2,        # Exponential backoff (2, 4, 8, 16, ...)
    status_forcelist=[500, 502, 503, 504],  # Retry on server errors
    allowed_methods=["POST"] # Ensure retries for POST requests
)
session.mount("https://", HTTPAdapter(max_retries=retries))

# Function to send request with exponential backoff
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
            }}
          }}
        }}
      }}
    }}
    """
    
    for attempt in range(5):  # Manual retry loop with backoff
        try:
            response = session.post(GRAPHQL_ENDPOINT, json={"query": query})
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Error fetching data for {chembl_id}: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error on attempt {attempt + 1} for {chembl_id}: {e}")
        
        sleep(2 ** attempt)  # Exponential backoff (1s, 2s, 4s, 8s, ...)
    
    print(f"❌ Failed to fetch data for {chembl_id} after retries.")
    return None

output_file_tmp = os.path.join(data_dir, 'mol_target_tmp')

# Fetch data and save to files
for chembl_id in tqdm(sorted(molecule_ids), smoothing=1):
    output_file = os.path.join(data_dir, f'mol_target_{chembl_id}.json')
    if os.path.exists(output_file):
        continue
    drug_data = fetch_drug_data(chembl_id)
    sleep(0.1)  # Respect API rate limits

    if drug_data:
        with open(output_file_tmp, "w", encoding="utf-8") as f:
            json.dump(drug_data, f, indent=4)
        shutil.move(output_file_tmp, output_file)
    else:
        print(f"⚠️ No data found for {chembl_id}")

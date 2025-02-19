
from time import sleep

import duckdb
import requests
from tqdm import tqdm


con = duckdb.connect("bio_data.duck.db")
targets = con.execute("SELECT target_id FROM targets").fetchall()

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


for target_id in tqdm(sorted(targets), smoothing=1):
    associated_diseases_list = fetch_associated_diseases_data(target_id)
    sleep(0.5)  # Respect API rate limits

    if associated_diseases_list:
        for row in associated_diseases_list:
            disease_id = row['disease']['id']
            name = row['disease']['name']
            description = row['disease'].get('description')

            q = 'INSERT OR IGNORE INTO diseases VALUES ($disease_id, $name, $description)'
            params = {'disease_id': disease_id, 'name': name, 'description': description}
            con.execute(q, params)

            q = 'INSERT OR IGNORE INTO disease_target VALUES ($disease_id, $target_id)'
            params = {'disease_id': disease_id, 'target_id': target_id}
            con.execute(q, params)
    else:
        print(f"⚠️ No data found for {target_id}")


con.sql("SELECT * FROM diseases LIMIT 10").show()
print(f'diseases: {con.execute("SELECT count(*) FROM diseases").fetchone()[0]} rows')
print(f'disease_target: {con.execute("SELECT count(*) FROM disease_target").fetchone()[0]} rows')

con.close()

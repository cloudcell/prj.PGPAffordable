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

for chembl_id in molecule_ids:
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
    print(query)  # Print query for verification



# GraphQL endpoint (modify if needed)
GRAPHQL_ENDPOINT = "https://api.opentargets.io/v3/platform/graphql"

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
for chembl_id in molecule_ids[:5]:
    drug_data = fetch_drug_data(chembl_id)
    sleep(0.5)  # Respect API rate limits

    if drug_data:
        output_file = f"data_tmp/mol_{chembl_id}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(drug_data, f, indent=4)
        print(f"✅ Saved: {output_file}")
    else:
        print(f"⚠️ No data found for {chembl_id}")

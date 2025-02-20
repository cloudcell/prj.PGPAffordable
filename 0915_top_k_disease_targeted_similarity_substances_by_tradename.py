import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Ask the user to enter either a disease name or ID
user_input = input("Enter the disease name or ID: ").strip()

# Check if the input is a disease ID (assume disease IDs follow a specific pattern like 'EFO_0005766' or 'DOID:1234')
if user_input.startswith(("EFO_", "DOID:")) or user_input.isdigit():
    query = f"""
        SELECT disease_id, description FROM diseases
        WHERE disease_id = '{user_input}'
    """
    disease_matches = con.execute(query).fetchdf()
else:
    # Search by disease description (allowing partial match)
    query = f"""
        SELECT disease_id, description FROM diseases
        WHERE description ILIKE '%{user_input}%'
    """
    disease_matches = con.execute(query).fetchdf()

# Handle results
if disease_matches.empty:
    print("No matching disease found.")
    con.close()
    exit()

elif len(disease_matches) > 1:
    print("Multiple diseases found. Please refine your selection:")
    print(disease_matches.to_string(index=False))
    selected_disease_id = input("Enter the exact disease ID from the list: ").strip()

    if selected_disease_id not in disease_matches["disease_id"].values:
        print("Invalid selection.")
        con.close()
        exit()
    
    disease_id = selected_disease_id
else:
    disease_id = disease_matches.iloc[0]["disease_id"]
    print(f"Using disease ID: {disease_id}")

# Retrieve target IDs associated with the disease
query = f"""
    SELECT DISTINCT target_id FROM disease_target
    WHERE disease_id = '{disease_id}'
"""
target_ids = con.execute(query).fetchdf()

if target_ids.empty:
    print("No targets found for this disease.")
    con.close()
    exit()

print(f"Found {len(target_ids)} target(s). Proceeding with full compound set.")

# Fetch all compounds for similarity calculation
query = "SELECT * FROM vector_array"
df = con.execute(query).fetchdf()

if df.empty:
    print("No vector data found in vector_array.")
    con.close()
    exit()

# Extract ChEMBL IDs and vectors
vector_data = df.set_index("chembl_id").to_dict(orient="index")

# Ask the user for reference ChEMBL ID
ref_chembl_id = input("Enter the reference ChEMBL ID: ")

if ref_chembl_id not in vector_data:
    print("Reference ChEMBL ID not found in dataset.")
    con.close()
    exit()

vec_ref = np.array(list(vector_data[ref_chembl_id].values()), dtype=np.float32)

# Compute cosine similarity with all compounds
similarities = {}
for chembl_id, vector in vector_data.items():
    vec = np.array(list(vector.values()), dtype=np.float32)
    similarity = np.dot(vec_ref, vec) / (np.linalg.norm(vec_ref) * np.linalg.norm(vec))
    similarities[chembl_id] = similarity

# Rank and display results
ranked_results = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
df_results = pd.DataFrame(ranked_results, columns=["ChEMBL ID", "Cosine Similarity"])

# Display top-k results
top_k = 10
df_top_k = df_results.head(top_k)
print(df_top_k)

# Close connection
con.close()

import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Ask the user for disease name
disease_name = input("Enter the disease name: ")

# Retrieve the disease_id if the name is unique
query = f"""
    SELECT disease_id FROM diseases
    WHERE description ILIKE '%' || '{disease_name}'
"""
disease_ids = con.execute(query).fetchall()

if len(disease_ids) == 0:
    print("No matching disease found.")
    con.close()
    exit()
elif len(disease_ids) > 1:
    print("Multiple diseases found. Please refine your query.")
    con.close()
    exit()

disease_id = disease_ids[0][0]
print(f"Using disease ID: {disease_id}")

# Retrieve target IDs associated with the disease
query = f"""
    SELECT DISTINCT target_id FROM disease_target_map
    WHERE disease_id = '{disease_id}'
"""
target_ids = con.execute(query).fetchdf()

if target_ids.empty:
    print("No targets found for this disease.")
    con.close()
    exit()

target_list = target_ids["target_id"].tolist()
print(f"Found {len(target_list)} target(s). Filtering compounds...")

# Retrieve compounds associated with these targets
formatted_targets = ','.join([f'"{t}"' for t in target_list])
query = f"""
    SELECT DISTINCT chembl_id FROM compound_target_map
    WHERE target_id IN ({formatted_targets})
"""
chembl_ids = con.execute(query).fetchdf()

if chembl_ids.empty:
    print("No compounds found for these targets.")
    con.close()
    exit()

chembl_list = chembl_ids["chembl_id"].tolist()
print(f"Found {len(chembl_list)} compounds.")

# Fetch vector data
formatted_chembls = ','.join([f'"{c}"' for c in chembl_list])
query = f"""
    SELECT * FROM vector_array
    WHERE chembl_id IN ({formatted_chembls})
"""
df = con.execute(query).fetchdf()

if df.empty:
    print("No vector data found for selected compounds.")
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

# Compute cosine similarity
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

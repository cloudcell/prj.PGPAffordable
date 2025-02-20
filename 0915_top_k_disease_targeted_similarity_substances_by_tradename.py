import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# ---------------------- DISEASE SELECTION ----------------------
user_input = input("Enter the disease name or ID: ").strip()

# Check if input is a disease ID
if user_input.startswith(("EFO_", "DOID:")) or user_input.isdigit():
    query = f"SELECT disease_id, description FROM diseases WHERE disease_id = '{user_input}'"
else:
    query = f"SELECT disease_id, description FROM diseases WHERE description ILIKE '%{user_input}%'"

disease_matches = con.execute(query).fetchdf()

if disease_matches.empty:
    print("No matching disease found.")
    con.close()
    exit()
elif len(disease_matches) > 1:
    print("Multiple diseases found. Please refine your selection:")
    print(disease_matches.to_string(index=False))
    disease_id = input("Enter the exact disease ID from the list: ").strip()
    if disease_id not in disease_matches["disease_id"].values:
        print("Invalid selection.")
        con.close()
        exit()
else:
    disease_id = disease_matches.iloc[0]["disease_id"]
    print(f"Using disease ID: {disease_id}")

# ---------------------- TARGET SELECTION ----------------------
query = f"SELECT DISTINCT target_id FROM disease_target WHERE disease_id = '{disease_id}'"
target_ids = con.execute(query).fetchdf()

if target_ids.empty:
    print("No targets found for this disease.")
    con.close()
    exit()

print(f"Found {len(target_ids)} target(s). Proceeding with full compound set.")

# ---------------------- COMPOUND SELECTION ----------------------
compound_input = input("Enter the compound ChEMBL ID, name, or trade name: ").strip()

# Try to find a matching compound by ChEMBL ID, name, or trade name
if compound_input.startswith("CHEMBL"):
    query = f"SELECT chembl_id, name, tradeNames FROM substances WHERE chembl_id = '{compound_input}'"
else:
    query = f"""
        SELECT chembl_id, name, tradeNames FROM substances
        WHERE name ILIKE '%{compound_input}%'
        OR tradeNames::STRING ILIKE '%{compound_input}%'
    """

compound_matches = con.execute(query).fetchdf()

# Debugging checks
if compound_matches.empty:
    print("No matching compound found.")
    print("Query executed:", query)  # Debugging output
    con.close()
    exit()

print("Columns returned by the query:", compound_matches.columns.tolist())  # Debugging output

# Normalize column names to avoid case sensitivity issues
compound_matches.columns = compound_matches.columns.str.lower()

if "chembl_id" not in compound_matches.columns:
    print("Error: 'chembl_id' column not found in query result.")
    print("Available columns:", compound_matches.columns.tolist())
    con.close()
    exit()

if len(compound_matches) > 1:
    print("Multiple compounds found. Please refine your selection:")
    print(compound_matches.to_string(index=False))
    ref_chembl_id = input("Enter the exact ChEMBL ID from the list: ").strip()
    if ref_chembl_id not in compound_matches["chembl_id"].values:
        print("Invalid selection.")
        con.close()
        exit()
else:
    ref_chembl_id = compound_matches.iloc[0]["chembl_id"]
    print(f"Using ChEMBL ID: {ref_chembl_id}")


# ---------------------- VECTOR RETRIEVAL ----------------------
query = "SELECT * FROM vector_array"
df = con.execute(query).fetchdf()

# Debug: Check column names
print("Available columns in vector_array:", df.columns.tolist())

if "chembl_id" not in df.columns:
    print("Error: 'chembl_id' column not found. Please check your database schema.")
    con.close()
    exit()

# Extract ChEMBL IDs and vectors
vector_data = df.set_index("chembl_id").to_dict(orient="index")

if ref_chembl_id not in vector_data:
    print("Reference ChEMBL ID not found in dataset.")
    con.close()
    exit()

vec_ref = np.array(list(vector_data[ref_chembl_id].values()), dtype=np.float32)

# ---------------------- SIMILARITY CALCULATION ----------------------
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
# ---------------------- END OF SCRIPT ----------------------
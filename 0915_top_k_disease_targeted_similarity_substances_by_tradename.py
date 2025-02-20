import duckdb
import numpy as np
import pandas as pd

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# ---------------------- DISEASE SELECTION ----------------------
user_input = input("Enter the disease name or ID: ").strip()

# Check if input is a disease ID
if user_input.startswith(("EFO_", "DOID:")) or user_input.isdigit():
    query = "SELECT disease_id, description FROM diseases WHERE disease_id = ?"
    disease_matches = con.execute(query, [user_input]).fetchdf()
else:
    query = "SELECT disease_id, name, description FROM diseases WHERE name ILIKE ? OR description ILIKE ?"
    disease_matches = con.execute(query, [f"%{user_input}%", f"%{user_input}%"]).fetchdf()

if disease_matches.empty:
    print("No matching disease found.")
    con.close()
    exit()
elif len(disease_matches) > 1:
    print("\nMultiple diseases found:\n")
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
query = "SELECT DISTINCT target_id FROM disease_target WHERE disease_id = ?"
target_ids = con.execute(query, [disease_id]).fetchdf()

if target_ids.empty:
    print("No targets found for this disease.")
    con.close()
    exit()

print(f"Found {len(target_ids)} target(s). Proceeding with full compound set.")

# ---------------------- COMPOUND SELECTION ----------------------
compound_input = input("Enter the compound ChEMBL ID, name, or trade name: ").strip()

query = """
    SELECT DISTINCT chembl_id AS ChEMBL_id, 
           COALESCE(name, 'N/A') AS molecule_name, 
           COALESCE(tradeNames::STRING, 'N/A') AS trade_name
    FROM substances
    WHERE chembl_id = ?
       OR name ILIKE ?
       OR tradeNames::STRING ILIKE ?
"""

compound_matches = con.execute(query, [compound_input, f"%{compound_input}%", f"%{compound_input}%"]).fetchdf()

if compound_matches.empty:
    print(f"No matches found for '{compound_input}'.")
    con.close()
    exit()

if len(compound_matches) > 1:
    print("\nMultiple compounds found. Please refine your selection:\n")
    print(compound_matches.to_string(index=False))
    ref_chembl_id = input("Enter the exact ChEMBL ID from the list: ").strip()
    if ref_chembl_id not in compound_matches["ChEMBL_id"].values:
        print("Invalid selection.")
        con.close()
        exit()
else:
    ref_chembl_id = compound_matches.iloc[0]['ChEMBL_id']

trade_name = compound_matches.loc[compound_matches["ChEMBL_id"] == ref_chembl_id, "trade_name"].values[0]
molecule_name = compound_matches.loc[compound_matches["ChEMBL_id"] == ref_chembl_id, "molecule_name"].values[0]

print(f"Using ChEMBL ID: {ref_chembl_id} (Trade Name: {trade_name}, Name: {molecule_name})")

# ---------------------- VECTOR RETRIEVAL ----------------------
query = "SELECT * FROM vector_array"
df = con.execute(query).fetchdf()

# Normalize column names to avoid case sensitivity issues
df.columns = df.columns.str.lower()

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
similarities = []
for chembl_id, vector in vector_data.items():
    vec = np.array(list(vector.values()), dtype=np.float32)
    similarity = np.dot(vec_ref, vec) / (np.linalg.norm(vec_ref) * np.linalg.norm(vec))
    
    # Filter out similarities ≤ 0
    if similarity > 0:
        # Fetch molecule name from substances table
        name_query = "SELECT COALESCE(name, 'N/A') FROM substances WHERE chembl_id = ?"
        molecule_name_res = con.execute(name_query, [chembl_id]).fetchone()
        molecule_name = molecule_name_res[0] if molecule_name_res else "N/A"

        similarities.append((chembl_id, similarity, molecule_name))

# Rank results by similarity in descending order
ranked_results = sorted(similarities, key=lambda x: x[1], reverse=True)
df_results = pd.DataFrame(ranked_results, columns=["ChEMBL ID", "Cosine Similarity", "Molecule Name"])

# Display top-k results
top_k = 250
df_top_k = df_results.head(top_k)

# Print header
print(f"\nTop {top_k} Similarity Results for {ref_chembl_id} (Trade Name: {trade_name}, Name: {molecule_name}):\n")
print(f"{'ChEMBL ID':<15} {'Cosine Similarity':<20} {'Molecule Name'}")
print("-" * 60)

# Print each row explicitly to ensure all lines are visible
for index, row in df_top_k.iterrows():
    print(f"{row['ChEMBL ID']:<15} {row['Cosine Similarity']:<20.6f} {row['Molecule Name']}")

# Close connection
con.close()

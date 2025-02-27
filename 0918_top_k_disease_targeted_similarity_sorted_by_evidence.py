import json
import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

JSON_CHARS_TO_DISPLAY = 100

TOP_K = 100

STATUS_NUM = {
    'Active, not recruiting': 4,
    'Completed': 5,
    'Enrolling by invitation': 3,
    'Not yet recruiting': 1,
    'Recruiting': 2,
    'Suspended': 0,
    'Terminated': 0,
    'Unknown status': 0,
    'Withdrawn': 0,
    'N/A': 0,
}

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# ---------------------- DISEASE SELECTION ----------------------
user_input = input("Enter the disease ID, name, or description: ").strip()

if user_input.startswith(("MONDO_", "EFO_", "DOID:")) or user_input.isdigit():
    query = "SELECT id, description FROM tbl_diseases WHERE id = ?"
    disease_matches = con.execute(query, [user_input]).fetchdf()
else:
    query = "SELECT id, name, description FROM tbl_diseases WHERE name ILIKE ? OR description ILIKE ?"
    disease_matches = con.execute(query, [f"%{user_input}%", f"%{user_input}%"]).fetchdf()

if disease_matches.empty:
    print("No matching disease found.")
    con.close()
    exit()
elif len(disease_matches) > 1:
    print("\nMultiple diseases found:\n")
    print(disease_matches.to_string(index=False))
    disease_id = input("Enter the exact disease ID from the list: ").strip()
    if disease_id not in disease_matches["id"].values:
        print("Invalid selection.")
        con.close()
        exit()
else:
    disease_id = disease_matches.iloc[0]["id"]
    print(f"Using disease ID: {disease_id}")

# ---------------------- TARGET SELECTION ----------------------
query = "SELECT DISTINCT target_id FROM tbl_disease_target WHERE disease_id = ?"
target_ids_df = con.execute(query, [disease_id]).fetchdf()

if target_ids_df.empty:
    print("No targets found for this disease.")
    con.close()
    exit()

target_ids = set(target_ids_df["target_id"].tolist())
print(f"Found {len(target_ids)} target(s). Proceeding with full compound set.")

# set target_ids to lower case for case-insensitive matching
target_ids = {target_id.upper() for target_id in target_ids}

# print the entire list of target IDs without suppression using a loop
print("Target IDs:")
for target_id in target_ids:
    print(target_id)

# ---------------------- COMPOUND SELECTION ----------------------
compound_input = input("Enter the compound ChEMBL ID, name, or trade name: ").strip()

query = """
    SELECT DISTINCT chembl_id AS ChEMBL_id, 
           COALESCE(name, 'N/A') AS molecule_name, 
           COALESCE(tradeNames::STRING, 'N/A') AS trade_name
    FROM tbl_substances
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
query = "SELECT * FROM tbl_vector_array"
df = con.execute(query).fetchdf()

# Normalize column names to avoid case sensitivity issues
df.columns = df.columns.str.upper()

if "chembl_id".upper() not in df.columns:
    print("Error: 'chembl_id' column not found. Please check your database schema.")
    con.close()
    exit()

# Extract ChEMBL IDs and vectors
vector_data = df.set_index("chembl_id".upper()).to_dict(orient="index")

if ref_chembl_id not in vector_data:
    print("Reference ChEMBL ID not found in dataset.")
    con.close()
    exit()

# ---------------------- APPLY TARGET MASK ----------------------
# Identify vector feature names
vector_features = list(vector_data[ref_chembl_id].keys())

# Create a binary mask: 1 if the feature (column) matches a target_id, 0 otherwise
mask = np.array([1 if feature in target_ids else 0 for feature in vector_features], dtype=np.float32)

# print the mask in full without suppression
np.set_printoptions(threshold=np.inf)
print(mask)

print(f"Applying target mask. {np.sum(mask)} out of {len(mask)} features retained.")

# Apply mask to ALL vectors, including the reference vector
vec_ref = np.array(list(vector_data[ref_chembl_id].values()), dtype=np.float32) * mask

# ---------------------- SIMILARITY CALCULATION ----------------------
similarities = []
for chembl_id, vector in tqdm(vector_data.items()):
    vec = np.array(list(vector.values()), dtype=np.float32) * mask  # Apply mask to each vector
    norm_product = np.linalg.norm(vec_ref) * np.linalg.norm(vec)
    
    similarity = np.dot(vec_ref, vec) / (norm_product + 1e-9) if norm_product > 0 else 0  # Avoid division by zero
    
    if similarity > 0:
        similarities.append((chembl_id, similarity))

# Rank results by similarity in descending order
ranked_results = sorted(similarities, key=lambda x: x[1], reverse=True)
df_results = pd.DataFrame(ranked_results, columns=["ChEMBL ID", "Cosine Similarity"])

molecule_name_column = []
is_url_available_column = []
is_approved_column = []
phase_column = []
status_column = []
status_num_column = []
known_drugs_aggregated_column = []
for index, row in tqdm(df_results.iterrows()):
    chembl_id = row['ChEMBL ID']
    query = "SELECT COALESCE(name, 'N/A'), isApproved FROM tbl_substances WHERE chembl_id = ?"
    molecule_name, is_approved = con.execute(query, [chembl_id]).fetchone()
    molecule_name_column.append(molecule_name)
    is_approved_column.append(is_approved)

    query = "SELECT * FROM tbl_knownDrugsAggregated WHERE drugId = ? and diseaseId = ?"
    known_drugs_aggregated = [{column[0]: value for column, value in zip(con.description, row)} for row in con.execute(query, [chembl_id, disease_id]).fetchall()]
    if known_drugs_aggregated:
        is_url_available = any(row['urls'] for row in known_drugs_aggregated)
        max_phase = max(row['phase'] for row in known_drugs_aggregated)
        max_status_for_max_phase = max((row['status'] for row in known_drugs_aggregated if row['phase'] == max_phase), key=lambda x: STATUS_NUM.get(x, 0))
        if max_status_for_max_phase is None:
            max_status_for_max_phase = 'N/A'
        status_num = STATUS_NUM[max_status_for_max_phase]
    else:
        known_drugs_aggregated = []
        is_url_available = False
        max_phase = 0
        max_status_for_max_phase = 'N/A'
        status_num = 0

    known_drugs_aggregated_column.append(known_drugs_aggregated)
    is_url_available_column.append(is_url_available)
    phase_column.append(max_phase)
    status_column.append(max_status_for_max_phase)
    status_num_column.append(status_num)


df_results['Molecule Name'] = pd.Series(molecule_name_column)
df_results['isUrlAvailable'] = pd.Series(is_url_available_column)
df_results['isApproved'] = pd.Series(is_approved_column)
df_results['phase'] = pd.Series(phase_column)
df_results['status'] = pd.Series(status_column)
df_results['status_num'] = pd.Series(status_num_column)
df_results['fld_knownDrugsAggregated'] = pd.Series(known_drugs_aggregated_column)

reference_drug = df_results[df_results['ChEMBL ID'] == ref_chembl_id].to_dict('records')[0]

df_results = df_results[(df_results['ChEMBL ID'] != ref_chembl_id)]

# convert to list of dictionares
results = df_results.to_dict('records')

results.sort(key=lambda x: [x['Cosine Similarity'], x['isApproved'], x['isUrlAvailable'], x['phase'], x['status_num']], reverse=True)

# Display top-k results based on the top 10-th cosine similarity
if len(results) > TOP_K - 1:
    ref_similarity = results[TOP_K - 1]["Cosine Similarity"]
    print(f"\nTop {TOP_K} similarity threshold: {ref_similarity:.6f}")

    # filter the results based on the top-k threshold
    results_top_k = [row for row in results if row['Cosine Similarity'] >= ref_similarity]
else:
    results_top_k = results

results_top_k.insert(0, reference_drug)

# Print header
print(f"\nTop {TOP_K} Similarity Results for {ref_chembl_id} (Trade Name: {trade_name}, Name: {molecule_name}):\n")
print(f"{'ChEMBL ID':<15} {'Molecule Name':<30} {'Cosine Similarity':<20} {'isApproved':<12} {'isUrlAvailable':<15} {'phase':<7} {'status_num':<12} {'status':<24} {'fld_knownDrugsAggregated'}")
print("-" * 150)

# Print each row explicitly to ensure all lines are visible without sorting
for row in results_top_k:
    print(f"{row['ChEMBL ID']:<15} {row['Molecule Name']:<30} {row['Cosine Similarity']:<20.6f} {row['isApproved']:<12} {row['isUrlAvailable']:<15} {row['phase']:<7.1f} {row['status_num']:<12} {row['status']:<24} {str(row['fld_knownDrugsAggregated'])[:JSON_CHARS_TO_DISPLAY]}")

# Close connection
con.close()

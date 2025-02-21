import duckdb
import numpy as np
import json
from tqdm import tqdm

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all actions and their corresponding action types
actions_query = """
    SELECT a.ChEMBL_id, a.target_id, at.value
    FROM tbl_actions a
    JOIN tbl_action_types at ON a.actionType = at.actionType
"""
action_data = con.execute(actions_query).fetchall()

# Convert action data into a dictionary where keys are ChEMBL_ids and values are vectors
molecular_vectors = {}
for chembl_id, target_id, value in tqdm(action_data, desc="Processing actions"):
    if chembl_id not in molecular_vectors:
        molecular_vectors[chembl_id] = {}
    if target_id not in molecular_vectors[chembl_id]:
        molecular_vectors[chembl_id][target_id] = []
    molecular_vectors[chembl_id][target_id].append(value)

# Compute simple average for each target and normalize vectors
vectorized_data = []
for chembl_id, target_vector in tqdm(molecular_vectors.items(), desc="Computing averages and normalization"):
    averaged_vector = {target: np.mean(values) for target, values in target_vector.items()}
    target_ids = list(averaged_vector.keys())
    values = np.array(list(averaged_vector.values()), dtype=np.float32)
    norm = np.linalg.norm(values)
    if norm != 0:
        values /= norm
    vector_json = json.dumps(dict(zip(target_ids, values.tolist())))
    vectorized_data.append((chembl_id, vector_json))

# Create a new table for storing molecular vectors
con.execute("""
    CREATE TABLE IF NOT EXISTS tbl_molecular_vectors (
        ChEMBL_id STRING PRIMARY KEY,
        vector JSON
    )
""")

# Insert vectorized data into the table
for data in tqdm(vectorized_data, desc="Inserting vectorized data"):
    con.execute("""
        INSERT OR REPLACE INTO tbl_molecular_vectors (ChEMBL_id, vector) VALUES (?, ?)
    """, data)

# Verify insertion
con.sql("SELECT * FROM tbl_molecular_vectors LIMIT 10").show()

# Close connection
con.close()

print("✅ Molecular profile vectorization completed and stored in DuckDB.")

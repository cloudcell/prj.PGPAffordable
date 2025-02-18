"""
This script is used to vectorize molecular profiles from the generated actions table.
"""

import duckdb
import numpy as np
import json

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all actions and their corresponding action types
actions_query = """
    SELECT a.ChEMBL_id, a.target_id, at.value
    FROM actions a
    JOIN action_types at ON a.actionType = at.actionType
"""
action_data = con.execute(actions_query).fetchall()

# Convert action data into a dictionary where keys are ChEMBL_ids and values are vectors
molecular_vectors = {}
for chembl_id, target_id, value in action_data:
    if chembl_id not in molecular_vectors:
        molecular_vectors[chembl_id] = {}
    molecular_vectors[chembl_id][target_id] = value

# Normalize vectors and store them as JSON
vectorized_data = []
for chembl_id, target_vector in molecular_vectors.items():
    target_ids = list(target_vector.keys())
    values = np.array(list(target_vector.values()), dtype=np.float32)
    norm = np.linalg.norm(values)
    if norm != 0:
        values /= norm
    vector_json = json.dumps(dict(zip(target_ids, values.tolist())))
    vectorized_data.append((chembl_id, vector_json))

# Create a new table for storing molecular vectors
con.execute("""
    CREATE TABLE IF NOT EXISTS molecular_vectors (
        ChEMBL_id STRING PRIMARY KEY,
        vector JSON
    )
""")

# Insert vectorized data into the table
con.executemany("""
    INSERT OR REPLACE INTO molecular_vectors (ChEMBL_id, vector) VALUES (?, ?)
""", vectorized_data)

# Verify insertion
con.sql("SELECT * FROM molecular_vectors LIMIT 10").show()

# Close connection
con.close()

print("✅ Molecular profile vectorization completed and stored in DuckDB.")

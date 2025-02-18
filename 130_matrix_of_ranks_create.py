"""
Create a matrix of ranks from the sparse vectors representing molecular profiles stored in DuckDB.
The matrix is based on cosine similarity between the vectors, where higher values indicate greater similarity.
"""

import duckdb
import numpy as np
import json
import pandas as pd
from tqdm import tqdm
from scipy.spatial.distance import cosine

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all molecular vectors from vector_array
df = con.execute("SELECT * FROM vector_array").fetchdf()

# Extract ChEMBL IDs and convert dataframe to dictionary
chembl_ids = df["ChEMBL_id"].tolist()
vector_data = df.drop(columns=["ChEMBL_id"]).to_dict(orient="index")

# Initialize rank matrix DataFrame
rank_matrix = pd.DataFrame(index=chembl_ids, columns=chembl_ids, dtype=np.float32).fillna(0.0)

# Compute cosine similarity for ranking
for i, chembl_id_1 in enumerate(tqdm(chembl_ids, desc="Computing similarity")):
    vec_1 = np.array(list(vector_data[i].values()), dtype=np.float32)
    for j, chembl_id_2 in enumerate(chembl_ids[i+1:], start=i+1):
        vec_2 = np.array(list(vector_data[j].values()), dtype=np.float32)
        
        # Compute cosine similarity
        similarity = 1 - cosine(vec_1, vec_2)
        
        # Store in matrix
        rank_matrix.at[chembl_id_1, chembl_id_2] = similarity
        rank_matrix.at[chembl_id_2, chembl_id_1] = similarity

# Create a new table for storing the rank matrix
column_definitions = ", ".join([f'"{col}" FLOAT' for col in chembl_ids])
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS rank_matrix (
        ChEMBL_id STRING PRIMARY KEY, {column_definitions}
    )
"""
con.execute(create_table_query)

# Insert data into DuckDB with progress bar
data_tuples = [tuple([chembl_id] + row.tolist()) for chembl_id, row in rank_matrix.iterrows()]
placeholders = ", ".join(["?"] * (len(chembl_ids) + 1))
insert_query = f"INSERT OR REPLACE INTO rank_matrix VALUES ({placeholders})"

for data in tqdm(data_tuples, desc="Inserting rank matrix into DuckDB"):
    con.execute(insert_query, data)

# Verify insertion
con.sql("SELECT * FROM rank_matrix LIMIT 10").show()

# Close connection
con.close()

print("✅ Matrix of ranks creation completed and stored in DuckDB.")

"""
This script is used to create a matrix with rows-vectors from the generated vectorized molecular profiles in json format.
"""
import duckdb
import numpy as np
import json
import pandas as pd
from tqdm import tqdm

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all unique target IDs
targets_query = "SELECT DISTINCT target_id FROM tbl_actions"
target_ids = [row[0] for row in con.execute(targets_query).fetchall()]

# Fetch all vectorized molecular profiles
vectors_query = "SELECT ChEMBL_id, vector FROM tbl_molecular_vectors"
vector_data = con.execute(vectors_query).fetchall()

# Initialize a DataFrame with ChEMBL IDs as rows and target IDs as columns
vector_array = pd.DataFrame(index=[row[0] for row in vector_data], columns=target_ids, dtype=np.float32).fillna(0.0)

# Populate the DataFrame with vectorized values
for chembl_id, vector_json in tqdm(vector_data, desc="Processing molecular vectors"):
    vector_dict = json.loads(vector_json)
    for target_id, value in vector_dict.items():
        if target_id in vector_array.columns:
            vector_array.at[chembl_id, target_id] = value

# Create a new table for the vector array
column_definitions = ", ".join([f'"{col}" FLOAT' for col in target_ids])
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS tbl_vector_array (
        ChEMBL_id STRING PRIMARY KEY, {column_definitions}
    )
"""
con.execute(create_table_query)

# Insert data into DuckDB with progress bar
data_tuples = [tuple([chembl_id] + row.tolist()) for chembl_id, row in vector_array.iterrows()]
placeholders = ", ".join(["?"] * (len(target_ids) + 1))
insert_query = f"INSERT OR REPLACE INTO tbl_vector_array VALUES ({placeholders})"

for data in tqdm(data_tuples, desc="Inserting data into DuckDB"):
    con.execute(insert_query, data)

# Verify insertion
con.sql("SELECT * FROM tbl_vector_array LIMIT 10").show()

# Close connection
con.close()

print("✅ Vector array creation completed and stored in DuckDB.")

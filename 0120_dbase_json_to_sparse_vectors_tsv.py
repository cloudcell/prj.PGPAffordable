"""
This script is used to create a matrix with rows-vectors from the generated vectorized molecular profiles in json format.
"""
import duckdb
import numpy as np
import json
import pandas as pd
from tqdm import tqdm
import gc


TEMP_TSV_PATH = "data_tmp/temp_data.tsv"
BATCH_SIZE = 10 # rows
NULL = '<NULL>'

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all unique target IDs
targets_query = "SELECT DISTINCT target_id FROM tbl_actions"
target_ids = [row[0] for row in con.execute(targets_query).fetchall()]

# Fetch all vectorized molecular profiles
vectors_query = "SELECT ChEMBL_id, vector FROM tbl_molecular_vectors"
vector_data = con.execute(vectors_query).fetchall()  # [:50]

con.close()

# Initialize a DataFrame with ChEMBL IDs as rows and target IDs as columns
vector_array = pd.DataFrame(index=[row[0] for row in vector_data], columns=target_ids, dtype=np.float32).fillna(0.0)

# Populate the DataFrame with vectorized values
for chembl_id, vector_json in tqdm(vector_data, desc="Processing molecular vectors"):
    vector_dict = json.loads(vector_json)
    for target_id, value in vector_dict.items():
        if target_id in vector_array.columns:
            vector_array.at[chembl_id, target_id] = value

del vector_data
gc.collect()

# Insert data into DuckDB with progress bar
data_tuples = [tuple([chembl_id] + row.tolist()) for chembl_id, row in vector_array.iterrows()]

header = ['ChEMBL_id'] + list(vector_array.columns)

del vector_array
gc.collect()

with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
    f.write('\t'.join(map(str, header)) + '\n')
    for i1 in tqdm(range(int(len(data_tuples) / BATCH_SIZE)), desc="Writing to TSV"):
        i1 *= BATCH_SIZE
        i2 = i1 + BATCH_SIZE
        f.write('\n'.join('\t'.join(map(str, row)) for row in data_tuples[i1:i2]) + '\n')

print(f"✅ Vector array table saved in {TEMP_TSV_PATH}.")

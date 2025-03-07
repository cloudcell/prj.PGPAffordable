"""
This script is used to create a matrix with rows-vectors from the generated vectorized molecular profiles in json format.
"""
# import duckdb
# import numpy as np
# import json
# import pandas as pd
# from tqdm import tqdm
# import gc


# TEMP_TSV_PATH = "data_tmp/temp_data.tsv"
# BATCH_SIZE = 10 # rows
# NULL = '<NULL>'

# # Connect to DuckDB database
# db_path = "bio_data.duck.db"
# con = duckdb.connect(db_path)

# # Fetch all unique target IDs
# targets_query = "SELECT DISTINCT target_id FROM tbl_actions"
# target_ids = [row[0] for row in con.execute(targets_query).fetchall()]

# # Fetch all vectorized molecular profiles
# vectors_query = "SELECT ChEMBL_id, vector FROM tbl_molecular_vectors"
# vector_data = con.execute(vectors_query).fetchall()  # [:50]

# con.close()

# # Initialize a DataFrame with ChEMBL IDs as rows and target IDs as columns
# vector_array = pd.DataFrame(index=[row[0] for row in vector_data], columns=target_ids, dtype=np.float32).fillna(0.0)

# # Populate the DataFrame with vectorized values
# for chembl_id, vector_json in tqdm(vector_data, desc="Processing molecular vectors"):
#     vector_dict = json.loads(vector_json)
#     for target_id, value in vector_dict.items():
#         if target_id in vector_array.columns:
#             vector_array.at[chembl_id, target_id] = value

# del vector_data
# gc.collect()

# # Insert data into DuckDB with progress bar
# data_tuples = [tuple([chembl_id] + row.tolist()) for chembl_id, row in vector_array.iterrows()]

# header = ['ChEMBL_id'] + list(vector_array.columns)

# del vector_array
# gc.collect()

# with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
#     f.write('\t'.join(map(str, header)) + '\n')
#     for i1 in tqdm(range(int(len(data_tuples) / BATCH_SIZE) + 1), desc="Writing to TSV"):
#         i1 *= BATCH_SIZE
#         i2 = i1 + BATCH_SIZE
#         f.write('\n'.join('\t'.join(map(str, row)) for row in data_tuples[i1:i2]) + '\n')

# print(f"✅ Vector array table saved in {TEMP_TSV_PATH}.")


# -------------------------------------------------------------------------------------------
# the following appears to be the most efficient way to process the data:

import duckdb
import json
from tqdm import tqdm
import gc

TEMP_TSV_PATH = "data_tmp/temp_data.tsv"
BATCH_SIZE = 5  # rows
NULL = '<NULL>'

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all unique target IDs
targets_query = "SELECT DISTINCT target_id FROM tbl_actions"
target_ids = [row[0] for row in con.execute(targets_query).fetchall()]

# Get total count of ChEMBL_ids for tqdm progress bar
total_query = "SELECT COUNT(*) FROM tbl_molecular_vectors"
total_rows = con.execute(total_query).fetchone()[0]

# Stream processing to avoid memory overload
with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
    f.write('\t'.join(['ChEMBL_id'] + list(map(str, target_ids))) + '\n')
    
    cursor = con.execute("SELECT ChEMBL_id, vector FROM tbl_molecular_vectors")
    batch = []
    
    with tqdm(total=total_rows, desc="Processing molecular vectors") as pbar:
        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            
            for chembl_id, vector_json in rows:
                vector_dict = json.loads(vector_json)
                row_values = [vector_dict.get(str(target_id), 0.0) for target_id in target_ids]
                batch.append(f"{chembl_id}\t" + '\t'.join(map(str, row_values)))
            
            f.write('\n'.join(batch) + '\n')
            batch = []
            pbar.update(len(rows))
    
    con.close()
    gc.collect()

print(f"✅ Vector array table saved in {TEMP_TSV_PATH}.")

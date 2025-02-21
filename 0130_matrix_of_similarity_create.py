"""
Create a matrix of ranks from the sparse vectors representing molecular profiles stored in DuckDB.
The matrix is based on cosine similarity between the vectors, where higher values indicate greater similarity.
"""

import duckdb
import numpy as np
import pandas as pd
from tqdm import tqdm

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all molecular vectors from tbl_vector_array
df = con.execute("SELECT * FROM tbl_vector_array").fetchdf()

# Extract ChEMBL IDs and convert dataframe to dictionary
chembl_ids = df["ChEMBL_id"].tolist()  #[:100]
vector_data = df.drop(columns=["ChEMBL_id"]).to_dict(orient="index")

# Initialize dot product similarity matrix DataFrame
similarity_matrix = pd.DataFrame(index=chembl_ids, columns=chembl_ids, dtype=np.float32).fillna(0.0)

# Compute dot product similarity
for i, chembl_id_1 in enumerate(tqdm(chembl_ids, desc="Computing similarity")):
    vec_1 = np.array(list(vector_data[i].values()), dtype=np.float32)
    
    for j, chembl_id_2 in enumerate(chembl_ids[i+1:], start=i+1):
        vec_2 = np.array(list(vector_data[j].values()), dtype=np.float32)
        
        # Compute similarity using dot product (without normalization)
        similarity = np.dot(vec_1, vec_2)
        
        # Store in matrix
        similarity_matrix.at[chembl_id_1, chembl_id_2] = similarity
        similarity_matrix.at[chembl_id_2, chembl_id_1] = similarity
    
    # Set diagonal to self-dot product
    similarity_matrix.at[chembl_id_1, chembl_id_1] = np.dot(vec_1, vec_1)

# Drop the existing table if it exists
con.execute("DROP TABLE IF EXISTS tbl_similarity_matrix;")

# Create a new table for storing the similarity matrix
column_definitions = ", ".join([f'"{col}" FLOAT' for col in chembl_ids])
create_table_query = f"""
    CREATE TABLE IF NOT EXISTS tbl_similarity_matrix (
        ChEMBL_id STRING PRIMARY KEY, {column_definitions}
    )
"""
con.execute(create_table_query)

# Insert data into DuckDB with progress bar
data_tuples = [tuple([chembl_id] + row.tolist()) for chembl_id, row in similarity_matrix.iterrows()]
placeholders = ", ".join(["?"] * (len(chembl_ids) + 1))
insert_query = f"INSERT OR REPLACE INTO tbl_similarity_matrix VALUES ({placeholders})"

for data in tqdm(data_tuples, desc="Inserting similarity matrix into DuckDB"):
    con.execute(insert_query, data)

# Verify insertion
con.sql("SELECT * FROM tbl_similarity_matrix LIMIT 10").show()

# Close connection
con.close()

print("✅ Dot product similarity matrix creation completed and stored in DuckDB.")

# # make a heatmap of the similarity matrix
# import seaborn as sns
# import matplotlib.pyplot as plt

# # Load the similarity matrix from DuckDB
# con = duckdb.connect(db_path)
# similarity_matrix = con.execute("SELECT * FROM tbl_similarity_matrix").fetchdf().set_index("ChEMBL_id")

# # Plot the similarity matrix as a heatmap with labels for every ChEMBL ID
# plt.figure(figsize=(12, 10))
# sns.heatmap(similarity_matrix, cmap="viridis", xticklabels=True, yticklabels=True)
# plt.title("Cosine Similarity Matrix of Molecular Profiles")
# plt.xlabel("ChEMBL ID")
# plt.ylabel("ChEMBL ID")
# plt.show()



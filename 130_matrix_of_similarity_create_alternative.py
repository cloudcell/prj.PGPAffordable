import duckdb
import numpy as np
import json
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt

# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Fetch all molecular vectors from vector_array
df = con.execute("SELECT * FROM vector_array").fetchdf()

# Extract first 100 ChEMBL IDs and subset dataframe
chembl_ids = df["ChEMBL_id"].tolist()[:100]
vector_data = df.set_index("ChEMBL_id").fillna(0.0).loc[chembl_ids].to_dict(orient="index")

# Initialize 100x100 similarity matrix DataFrame
similarity_matrix = pd.DataFrame(index=chembl_ids, columns=chembl_ids, dtype=np.float32).fillna(0.0)

for i, chembl_id_1 in enumerate(tqdm(chembl_ids, desc="Computing similarity")):
    vec_1 = np.array(list(vector_data[chembl_id_1].values()), dtype=np.float32)
    norm_1 = np.linalg.norm(vec_1)  # Compute L2 norm
    
    for j, chembl_id_2 in enumerate(chembl_ids[i+1:], start=i+1):
        vec_2 = np.array(list(vector_data[chembl_id_2].values()), dtype=np.float32)
        norm_2 = np.linalg.norm(vec_2)
        
        # Compute cosine similarity only if norms are non-zero
        if norm_1 > 0 and norm_2 > 0:
            similarity = np.dot(vec_1, vec_2) / (norm_1 * norm_2)
        else:
            similarity = 0.0
        
        similarity_matrix.at[chembl_id_1, chembl_id_2] = similarity
        similarity_matrix.at[chembl_id_2, chembl_id_1] = similarity

    # Set diagonal to 1 (self-similarity)
    similarity_matrix.at[chembl_id_1, chembl_id_1] = 1.0


# Drop the existing table if it exists
con.execute("DROP TABLE IF EXISTS similarity_matrix;")

# Create a new table for storing the similarity matrix
column_definitions = ", ".join([f'"{col}" FLOAT' for col in chembl_ids])
create_table_query = f"""
    CREATE TABLE similarity_matrix (
        ChEMBL_id STRING PRIMARY KEY, {column_definitions}
    )
"""
con.execute(create_table_query)

# Insert data into DuckDB with progress bar
data_tuples = [tuple([chembl_id] + row.tolist()) for chembl_id, row in similarity_matrix.iterrows()]
placeholders = ", ".join(["?"] * (len(chembl_ids) + 1))
insert_query = f"INSERT OR REPLACE INTO similarity_matrix VALUES ({placeholders})"

for data in tqdm(data_tuples, desc="Inserting similarity matrix into DuckDB"):
    con.execute(insert_query, data)

# Verify insertion
con.sql("SELECT * FROM similarity_matrix LIMIT 10").show()

# Close connection
con.close()

print("✅ 100x100 Similarity matrix creation completed and stored in DuckDB.")

# Plot the similarity matrix as a heatmap
con = duckdb.connect(db_path)
similarity_matrix = con.execute("SELECT * FROM similarity_matrix").fetchdf()
con.close()

# Set ChEMBL ID as index
similarity_matrix.set_index("ChEMBL_id", inplace=True)

# Plot the heatmap
plt.figure(figsize=(12, 10))
plt.imshow(similarity_matrix, cmap="viridis", aspect="auto")
plt.colorbar(label="Dot Product Similarity")
plt.title("Dot Product Similarity Matrix of Molecular Profiles")
plt.xlabel("ChEMBL ID")
plt.ylabel("ChEMBL ID")
plt.xticks(range(len(chembl_ids)), chembl_ids, rotation=90)
plt.yticks(range(len(chembl_ids)), chembl_ids)
plt.show()

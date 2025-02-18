"""
Visualize the similarity matrix using a heatmap
"""

import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Load the similarity matrix from DuckDB
df = con.execute("SELECT * FROM similarity_matrix").fetchdf()

# Extract ChEMBL IDs and convert dataframe to dictionary
chembl_ids = df["ChEMBL_id"].tolist()
similarity_matrix = df.drop(columns=["ChEMBL_id"]).to_numpy()

# Create a heatmap
plt.figure(figsize=(12, 10))
sns.heatmap(similarity_matrix, xticklabels=chembl_ids, yticklabels=chembl_ids, cmap="viridis")
plt.title("Similarity Matrix")

plt.show()

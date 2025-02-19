"""
Visualize the similarity matrix using a heatmap
... and write the graphics to a file.
"""

import duckdb
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

output_file = "similarity_matrix3.png"  # Output file name

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Load the similarity matrix from DuckDB
df = con.execute("SELECT * FROM similarity_matrix").fetchdf()

# Extract ChEMBL IDs and convert dataframe to a numpy matrix
chembl_ids = df["ChEMBL_id"].tolist()
similarity_matrix = df.drop(columns=["ChEMBL_id"]).to_numpy()

# Create a heatmap
plt.figure(figsize=(12*3, 10*3))
sns.heatmap(similarity_matrix, xticklabels=chembl_ids, yticklabels=chembl_ids, cmap="viridis")
plt.title("Similarity Matrix")

# Save the figure instead of showing it
plt.savefig(output_file, dpi=300, bbox_inches="tight")

# Close the plot to free memory
plt.close()

print(f"✅ Heatmap saved to {output_file}")

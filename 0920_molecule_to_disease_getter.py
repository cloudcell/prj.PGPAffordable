import duckdb

# Connect to DuckDB
conn = duckdb.connect("bio_data.duck.db")

# Define ChEMBL_id of interest
chembl_id = "CHEMBL956"  # Replace with the desired ChEMBL ID

# Retrieve name and trade names
query_info = """
SELECT name, tradeNames FROM tbl_substances WHERE ChEMBL_id = ?;
"""
info_result = conn.execute(query_info, [chembl_id]).fetchone()

if info_result:
    name, trade_names = info_result
    print(f"ChEMBL ID: {chembl_id}")
    print(f"Name: {name}")
    print(f"Trade Names: {trade_names}\n")
else:
    print(f"No information found for ChEMBL ID: {chembl_id}")
    conn.close()
    exit()

# Retrieve associated diseases
query_diseases = """
SELECT DISTINCT d.disease_id, d.name, d.description
FROM tbl_actions a
JOIN tbl_disease_target dt ON a.target_id = dt.target_id
JOIN tbl_diseases d ON dt.disease_id = d.disease_id
WHERE a.ChEMBL_id = ?;
"""
disease_results = conn.execute(query_diseases, [chembl_id]).fetchall()

# Print diseases
if disease_results:
    print("Associated Diseases:")
    for disease_id, disease_name, description in disease_results:
        print(f"- {disease_name} (ID: {disease_id})")
        if description:
            print(f"  Description: {description}\n")
else:
    print("No associated diseases found.")

# Close connection
conn.close()

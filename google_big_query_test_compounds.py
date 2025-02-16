from google.cloud import bigquery

# Initialize a BigQuery client
client = bigquery.Client()

# Define the Open Targets BigQuery SQL query
query = """
SELECT
  id AS drug_id,
  name AS drug_name
FROM
  `open-targets-prod.platform.molecule`
"""

# Execute the query
query_job = client.query(query)

# Fetch results
results = query_job.result()

# Print the results
print("Available Drug IDs and Names:")
for row in results:
    print(f"ID: {row.drug_id}, Name: {row.drug_name}")

# Save results to a file (optional)
with open("drugs_list.txt", "w") as f:
    for row in results:
        f.write(f"{row.drug_id}, {row.drug_name}\n")

print("Drug list saved to drugs_list.txt")

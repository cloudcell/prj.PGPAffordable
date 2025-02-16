# this file extracts the data from json files and stores it in a database
# The database is created using DuckDB in bio_data.duck.db

import os
import json
import duckdb
import pandas as pd

# Define paths
DATA_DIR = "data/202409XX/molecule"  # Change this to your actual directory path
DUCKDB_PATH = "bio_data.duck.db"
TEMP_CSV_PATH = "temp_data.csv"

# Initialize DuckDB connection
con = duckdb.connect(DUCKDB_PATH)

# Create a list to store parsed data
data_list = []

# Iterate through files in the directory
for filename in os.listdir(DATA_DIR):
    if filename.startswith("part-") and filename.endswith(".json"):  # Adjust based on file format
        file_path = os.path.join(DATA_DIR, filename)
        
        try:
            # Read NDJSON (Newline Delimited JSON)
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    record = json.loads(line)

                    # Extract relevant fields
                    data_list.append({
                        "id": record.get("id"),
                        "canonicalSmiles": record.get("canonicalSmiles"),
                        "inchiKey": record.get("inchiKey"),
                        "drugType": record.get("drugType"),
                        "blackBoxWarning": record.get("blackBoxWarning"),
                        "name": record.get("name"),
                        "yearOfFirstApproval": record.get("yearOfFirstApproval"),
                        "maximumClinicalTrialPhase": record.get("maximumClinicalTrialPhase"),
                        "hasBeenWithdrawn": record.get("hasBeenWithdrawn"),
                        "isApproved": record.get("isApproved"),
                        "tradeNames": ", ".join(record.get("tradeNames", [])),  # Convert list to string
                        "synonyms": ", ".join(record.get("synonyms", [])),  # Convert list to string
                        "crossReferences": json.dumps(record.get("crossReferences", {})),  # Store as JSON string
                        "linkedDiseases": json.dumps(record.get("linkedDiseases", {})),  # Store as JSON string
                        "linkedTargets": json.dumps(record.get("linkedTargets", {})),  # Store as JSON string
                        "description": record.get("description"),
                    })

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Convert list to DataFrame
df = pd.DataFrame(data_list)

# Save DataFrame to a temporary CSV file
df.to_csv(TEMP_CSV_PATH, index=False)

# Create a table in DuckDB and import CSV data
con.execute("""
    CREATE TABLE IF NOT EXISTS molecules (
        id TEXT PRIMARY KEY,
        canonicalSmiles TEXT,
        inchiKey TEXT,
        drugType TEXT,
        blackBoxWarning BOOLEAN,
        name TEXT,
        yearOfFirstApproval INT,
        maximumClinicalTrialPhase FLOAT,
        hasBeenWithdrawn BOOLEAN,
        isApproved BOOLEAN,
        tradeNames TEXT,
        synonyms TEXT,
        crossReferences TEXT,
        linkedDiseases TEXT,
        linkedTargets TEXT,
        description TEXT
    )
""")

con.execute(f"""
    COPY molecules FROM '{TEMP_CSV_PATH}'
    (FORMAT CSV, HEADER TRUE, DELIM ',', QUOTE '"', ESCAPE '"', NULL 'NULL')
""")

# Verify data import
result = con.execute("SELECT * FROM molecules LIMIT 5").fetchdf()
print(result)

# Cleanup
con.close()
os.remove(TEMP_CSV_PATH)

print("Data successfully imported into DuckDB.")

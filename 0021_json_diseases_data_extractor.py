import os
import json
import duckdb

# Define paths
DATA_DIR = "data/202409XX/diseases"  # Change this to your actual directory path
DUCKDB_PATH = "bio_data.duck.db"
TEMP_TSV_PATH = "data_tmp/temp_data.tsv"  # Changed file extension to .tsv
NULL = '<NULL>'  # Define NULL value "string" in temporary TSV file

# Initialize DuckDB connection
con = duckdb.connect(DUCKDB_PATH)

# Create a list to store parsed data
data_list = [[
    "id",
    "code",
    "dbXRefs",
    "name",
    "description",
    "parents",
    "synonyms",
    "ancestors",
    "descendants",
    "children",
    "therapeuticAreas",
    "ontology",
]]

# Iterate through files in the directory
for filename in os.listdir(DATA_DIR):
    if filename.startswith("part-") and filename.endswith(".json"):  # Adjust based on file format
        file_path = os.path.join(DATA_DIR, filename)
        
        try:
            # Read NDJSON (Newline Delimited JSON)
            with open(file_path, "r", encoding="utf-8") as f:
                text = f.read()
            text = text.replace('\\n', ' ')
            for line in text.split('\n'):
                if not line: # for empty last line
                    continue
                line = line.replace('\t', ' ')
                record = json.loads(line)
                # {
                #     "id": str,
                #     "code": str,
                #     "dbXRefs": [str, ...],
                #     "name": str,
                #     "description": str,
                #     "parents": [str, ...],
                #     "synonyms": {
                #         "hasExactSynonym": [str, ...]
                #     },
                #     "ancestors": [str, ...],
                #     "descendants": [str, ...],
                #     "children": [str, ...],
                #     "therapeuticAreas": [str, ...],
                #     "ontology": {
                #         "isTherapeuticArea": bool,
                #         "leaf": bool,
                #         "sources": {"url": str, "name": str}
                #     }
                # }

                # Extract relevant fields
                data_list.append([
                    record["id"],
                    record.get("code", NULL),
                    json.dumps(record.get("dbXRefs", [])),
                    record.get("name", NULL),
                    record.get("description", NULL),
                    json.dumps(record.get("parents", [])),
                    json.dumps(record.get("synonyms", {})),
                    json.dumps(record.get("ancestors", [])),
                    json.dumps(record.get("descendants", [])),
                    json.dumps(record.get("children", [])),
                    json.dumps(record.get("therapeuticAreas", [])),
                    json.dumps(record.get("ontology", {})),
                ])

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Save to tsv file
with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
    f.write('\n'.join('\t'.join(map(str, row)) for row in data_list))

# Copy data into DuckDB with strict mode disabled
con.execute(f"""
    COPY tbl_diseases_tmp FROM '{TEMP_TSV_PATH}'
    (FORMAT CSV, HEADER TRUE, DELIMITER '\t', QUOTE '', ESCAPE '', NULL '{NULL}', AUTO_DETECT FALSE)
""")

# Verify data import
con.sql("SELECT * FROM tbl_diseases_tmp LIMIT 20").show()
print(f'Total: {con.execute("SELECT count(*) FROM tbl_diseases_tmp").fetchone()[0]} rows')

# Cleanup
con.close()
os.remove(TEMP_TSV_PATH)

print("Data successfully imported into DuckDB as TSV.")

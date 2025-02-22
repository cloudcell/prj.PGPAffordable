import os
import json
import duckdb

# Define paths
DATA_DIR = "data/202409XX/knownDrugsAggregated"  # Change this to your actual directory path
DUCKDB_PATH = "bio_data.duck.db"
TEMP_TSV_PATH = "data_tmp/temp_data.tsv"  # Changed file extension to .tsv
NULL = '<NULL>'  # Define NULL value "string" in temporary TSV file

# Initialize DuckDB connection
con = duckdb.connect(DUCKDB_PATH)

# Create a list to store parsed data
data_list = [[
    "drugId",
    "targetId",
    "diseaseId",
    "phase",
    "status",
    "urls",
    "ancestors",
    "label",
    "approvedSymbol",
    "approvedName",
    "targetClass",
    "prefName",
    "tradeNames",
    "synonyms",
    "drugType",
    "mechanismOfAction",
    "targetName",
]]

# Iterate through files in the directory
for filename in os.listdir(DATA_DIR):
    if filename.startswith("part-") and filename.endswith(".json"):  # Adjust based on file format
        file_path = os.path.join(DATA_DIR, filename)
        
        try:
            # Read NDJSON (Newline Delimited JSON)
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.replace('\t', ' ')
                    record = json.loads(line)
                    # {
                    #     "drugId": str,
                    #     "targetId": str,
                    #     "diseaseId": str,
                    #     "phase": float,
                    #     "status": str,
                    #     "urls": [
                    #         {"niceName": str, "url": str},
                    #         ...
                    #     ],
                    #     "ancestors": [str, str, ...],
                    #     "label": str,
                    #     "approvedSymbol": str,
                    #     "approvedName": str,
                    #     "targetClass": [str, str, ...],
                    #     "prefName": str,
                    #     "tradeNames": [str, str, ...],
                    #     "synonyms": [str, str, ...],
                    #     "drugType": str,
                    #     "mechanismOfAction": str,
                    #     "targetName": str
                    # }

                    # Extract relevant fields
                    data_list.append([
                        record["drugId"],
                        record["targetId"],
                        record["diseaseId"],
                        record.get("phase", NULL),
                        record.get("status", NULL),
                        json.dumps(record.get("urls", [])),
                        json.dumps(record.get("ancestors", [])),
                        record.get("label", NULL),
                        record.get("approvedSymbol", NULL),
                        record.get("approvedName", NULL),
                        json.dumps(record.get("targetClass", [])),
                        record.get("prefName", NULL),
                        json.dumps(record.get("tradeNames", [])),
                        json.dumps(record.get("synonyms", [])),
                        record.get("drugType", NULL),
                        record.get("mechanismOfAction", NULL),
                        record.get("targetName", NULL),
                    ])

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Save to tsv file
with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
    f.write('\n'.join('\t'.join(map(str, row)) for row in data_list))

# Copy data into DuckDB with strict mode disabled
con.execute(f"""
    COPY tbl_knownDrugsAggregated FROM '{TEMP_TSV_PATH}'
    (FORMAT CSV, HEADER TRUE, DELIMITER '\t', QUOTE '', ESCAPE '', NULL '{NULL}', AUTO_DETECT FALSE)
""")

# Verify data import
con.sql("SELECT * FROM tbl_knownDrugsAggregated LIMIT 20").show()
print(f'Total: {con.execute("SELECT count(*) FROM tbl_knownDrugsAggregated").fetchone()[0]} rows')

# Cleanup
con.close()
os.remove(TEMP_TSV_PATH)

print("Data successfully imported into DuckDB as TSV.")

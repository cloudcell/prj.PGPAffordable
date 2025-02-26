import os
import json
import duckdb

from tqdm import tqdm

# Define paths
DATA_DIR = "data/202409XX/targets"  # Change this to your actual directory path
DUCKDB_PATH = "bio_data.duck.db"
TEMP_TSV_PATH = "data_tmp/temp_data.tsv"  # Changed file extension to .tsv
NULL = '<NULL>'  # Define NULL value "string" in temporary TSV file

# Initialize DuckDB connection
con = duckdb.connect(DUCKDB_PATH)

# Create a list to store parsed data
data_list = [[
    "id",
    "approvedSymbol",
    "biotype",
    "transcriptIds",
    "canonicalTranscript",
    "canonicalExons",
    "genomicLocation",
    "approvedName",
    "synonyms",
    "symbolSynonyms",
    "nameSynonyms",
    "functionDescriptions",
    "subcellularLocations",
    "obsoleteSymbols",
    "obsoleteNames",
    "proteinIds",
    "dbXrefs",
]]

# Iterate through files in the directory
for filename in tqdm(sorted(os.listdir(DATA_DIR))):
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
                #     "approvedSymbol": str,
                #     "biotype": str,
                #     "transcriptIds": [str, ...],
                #     "canonicalTranscript": {
                #         "id": str,
                #         "chromosome": str,
                #         "start": int,
                #         "end": int,
                #         "strand": str
                #     },
                #     "canonicalExons": [str, ...],
                #     "genomicLocation": {
                #         "chromosome": str,
                #         "start": int,
                #         "end": int,
                #         "strand": int
                #     },
                #     "approvedName": str,
                #     "synonyms": [{"label": str, "source": str}, ...],
                #     "symbolSynonyms": [{"label": str, "source": str}, ...],
                #     "nameSynonyms": [{"label": str, "source": str}, ...],
                #     "functionDescriptions": [str, ...],
                #     "subcellularLocations": [
                #         {
                #             "location": str,
                #             "source": str,
                #             "termSL": str,
                #             "labelSL": str
                #         }
                #     ],
                #     "obsoleteSymbols": [{"label": str, "source": str}, ...],
                #     "obsoleteNames": [{"label": str, "source": str}, ...],
                #     "proteinIds": [{"id": str, "source": str}, ...],
                #     "dbXrefs": [{"id": str, "source": str}, ...]
                # }

                # Extract relevant fields
                data_list.append([
                    record["id"],
                    record.get("approvedSymbol", NULL),
                    record.get("biotype", NULL),
                    json.dumps(record.get("transcriptIds", [])),
                    json.dumps(record.get("canonicalTranscript", {})),
                    json.dumps(record.get("canonicalExons", [])),
                    json.dumps(record.get("genomicLocation", {})),
                    record.get("approvedName", NULL),
                    json.dumps(record.get("synonyms", [])),
                    json.dumps(record.get("symbolSynonyms", [])),
                    json.dumps(record.get("nameSynonyms", [])),
                    json.dumps(record.get("functionDescriptions", [])),
                    json.dumps(record.get("subcellularLocations", [])),
                    json.dumps(record.get("obsoleteSymbols", [])),
                    json.dumps(record.get("obsoleteNames", [])),
                    json.dumps(record.get("proteinIds", [])),
                    json.dumps(record.get("dbXrefs", [])),
                ])

        except Exception as e:
            print(f"Error processing {filename}: {e}")

# Save to tsv file
with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
    f.write('\n'.join('\t'.join(map(str, row)) for row in data_list))

# Copy data into DuckDB with strict mode disabled
con.execute(f"""
    COPY tbl_targets_tmp FROM '{TEMP_TSV_PATH}'
    (FORMAT CSV, HEADER TRUE, DELIMITER '\t', QUOTE '', ESCAPE '', NULL '{NULL}', AUTO_DETECT FALSE)
""")

# Verify data import
con.sql("SELECT * FROM tbl_targets_tmp LIMIT 20").show()
print(f'Total: {con.execute("SELECT count(*) FROM tbl_targets_tmp").fetchone()[0]} rows')

# Cleanup
con.close()
os.remove(TEMP_TSV_PATH)

print("Data successfully imported into DuckDB as TSV.")

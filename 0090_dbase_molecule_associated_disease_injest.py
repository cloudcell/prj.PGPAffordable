
# import os
# import json

# import duckdb
# from tqdm import tqdm



# DATA_DIR = "data/202409XX/molecule"
# db_path = "bio_data.duck.db"

# con = duckdb.connect(db_path)

# files = [filename for filename in os.listdir(DATA_DIR) if filename.endswith(".json")]
# records = []

# for filename in files:
#     file_path = os.path.join(DATA_DIR, filename)
#     with open(file_path, "r", encoding="utf-8") as f:
#         for line in f:
#             line = line.replace('\t', ' ')
#             record = json.loads(line)
#             records.append(record)


# q = 'INSERT OR IGNORE INTO tbl_disease_substance VALUES ($disease_id, $chembl_id)'
# for record in tqdm(records):
#     chembl_id = record['id']
#     for disease_id in record.get('linkedDiseases', {}).get('rows', []):
#         con.execute(q, {'disease_id': disease_id, 'chembl_id': chembl_id})


# print(f'tbl_disease_substance: {con.execute("SELECT count(*) FROM tbl_disease_substance").fetchone()[0]} rows')

# con.close()

# print("✅ Data successfully written to DuckDB")



import os
import json

import duckdb
from tqdm import tqdm

DATA_DIR = "data/202409XX/molecule"
DB_PATH = "bio_data.duck.db"
BATCH_SIZE = 1000  # Commit every 1000 insert statements

con = duckdb.connect(DB_PATH)

files = [filename for filename in os.listdir(DATA_DIR) if filename.endswith(".json")]
records = []

# Read all records
for filename in files:
    file_path = os.path.join(DATA_DIR, filename)
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.replace('\t', ' ')
            record = json.loads(line)
            records.append(record)

# Begin transaction
con.execute("BEGIN TRANSACTION")

insert_stmt = 'INSERT OR IGNORE INTO tbl_disease_substance VALUES ($disease_id, $chembl_id)'
count = 0

# Insert records in batches
for record in tqdm(records, desc="Inserting records"):
    chembl_id = record['id']
    for disease_id in record.get('linkedDiseases', {}).get('rows', []):
        con.execute(insert_stmt, {'disease_id': disease_id, 'chembl_id': chembl_id})
        count += 1

        if count % BATCH_SIZE == 0:
            # Commit every BATCH_SIZE inserts
            con.execute("COMMIT")
            con.execute("BEGIN TRANSACTION")

# Final commit after all inserts are done
con.execute("COMMIT")

row_count = con.execute("SELECT count(*) FROM tbl_disease_substance").fetchone()[0]
print(f"tbl_disease_substance: {row_count} rows")

con.close()
print("✅ Data successfully written to DuckDB")


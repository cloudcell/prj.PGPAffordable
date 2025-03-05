
import os
import json

import duckdb
from tqdm import tqdm



DATA_DIR = "data/202409XX/molecule"
db_path = "bio_data.duck.db"

con = duckdb.connect(db_path)

files = [filename for filename in os.listdir(DATA_DIR) if filename.endswith(".json")]
records = []

for filename in files:
    file_path = os.path.join(DATA_DIR, filename)
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.replace('\t', ' ')
            record = json.loads(line)
            records.append(record)


q = 'INSERT OR IGNORE INTO tbl_disease_substance VALUES ($disease_id, $chembl_id)'
for record in tqdm(records):
    chembl_id = record['id']
    for disease_id in record.get('linkedDiseases', {}).get('rows', []):
        con.execute(q, {'disease_id': disease_id, 'chembl_id': chembl_id})


print(f'tbl_disease_substance: {con.execute("SELECT count(*) FROM tbl_disease_substance").fetchone()[0]} rows')

con.close()

print("✅ Data successfully written to DuckDB")

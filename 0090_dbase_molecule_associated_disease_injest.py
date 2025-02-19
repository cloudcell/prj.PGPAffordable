
import os
import json

import duckdb
from tqdm import tqdm



data_dir = "data_tmp"

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

for filename in tqdm(os.listdir(data_dir)):
    if filename.startswith("mol_disease_") and filename.endswith(".json"):
        file_path = os.path.join(data_dir, filename)
        
        with open(file_path, "r", encoding="utf-8") as f:
            associated_diseases_list = json.load(f)

        # Extract ChEMBL ID
        chembl_id = filename.replace("mol_disease_", "").replace(".json", "")

        for row in associated_diseases_list:
            disease_id = row['id']
            name = row['name']
            description = row.get('description')

            q = 'INSERT OR IGNORE INTO diseases VALUES ($disease_id, $name, $description)'
            params = {'disease_id': disease_id, 'name': name, 'description': description}
            con.execute(q, params)

            q = 'INSERT OR IGNORE INTO disease_substance VALUES ($disease_id, $chembl_id)'
            params = {'disease_id': disease_id, 'chembl_id': chembl_id}
            con.execute(q, params)

print(f'diseases: {con.execute("SELECT count(*) FROM diseases").fetchone()[0]} rows')
print(f'disease_substance: {con.execute("SELECT count(*) FROM disease_substance").fetchone()[0]} rows')

con.close()

print("✅ Data successfully written to DuckDB")

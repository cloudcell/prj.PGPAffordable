
import os
import json

import duckdb
from tqdm import tqdm



data_dir = "data_tmp"

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

files = [filename for filename in os.listdir(data_dir) if filename.startswith("disease_target_") and filename.endswith(".json")]

for filename in tqdm(sorted(files)):
    file_path = os.path.join(data_dir, filename)
    
    with open(file_path, "r", encoding="utf-8") as f:
        associated_targets_list = json.load(f)

    # Extract disease_id
    disease_id = filename.replace("disease_target_", "").replace(".json", "")

    q = '''
    INSERT OR IGNORE INTO tbl_diseases
    SELECT * FROM tbl_diseases_tmp
    WHERE id = $disease_id
    '''
    params = {'disease_id': disease_id}
    con.execute(q, params)

    for row in associated_targets_list:
        target_id = row['target']['id']

        q = '''
        INSERT OR IGNORE INTO tbl_targets
        SELECT * FROM tbl_targets_tmp
        WHERE id = $target_id
        '''
        params = {'target_id': target_id}
        con.execute(q, params)

        q = 'INSERT OR IGNORE INTO tbl_disease_target VALUES ($disease_id, $target_id)'
        params = {'disease_id': disease_id, 'target_id': target_id}
        con.execute(q, params)


con.sql("SELECT * FROM tbl_diseases LIMIT 10").show()
print(f'tbl_diseases: {con.execute("SELECT count(*) FROM tbl_diseases").fetchone()[0]} rows')
print(f'tbl_disease_target: {con.execute("SELECT count(*) FROM tbl_disease_target").fetchone()[0]} rows')

con.close()

print("✅ Data successfully written to DuckDB")

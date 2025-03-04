import os
import json

import duckdb
from tqdm import tqdm


DATA_DIR = "data/202409XX/mechanismOfAction"
db_path = "bio_data.duck.db"

# Connect to DuckDB
con = duckdb.connect(db_path)

# ✅ Ensure `tbl_substances` is populated before proceeding
print("🔄 Populating tbl_substances from tbl_molecules...")
con.execute(
    """
    INSERT OR IGNORE INTO tbl_molecules
    SELECT * FROM tbl_substances;
"""
)
count = con.execute("SELECT COUNT(*) FROM tbl_substances;").fetchone()[0]
print(f"✅ tbl_substances now contains {count} rows.")

# ✅ Ensure `tbl_targets` is populated before proceeding
print("🔄 Populating tbl_targets from tbl_targets_tmp...")
con.execute(
    """
    INSERT OR IGNORE INTO tbl_targets
    SELECT * FROM tbl_targets_tmp;
"""
)
target_count = con.execute("SELECT COUNT(*) FROM tbl_targets;").fetchone()[0]
print(f"✅ tbl_targets now contains {target_count} rows.")


files = [filename for filename in os.listdir(DATA_DIR) if filename.endswith(".json")]
records = []

for filename in files:
    file_path = os.path.join(DATA_DIR, filename)
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.replace('\t', ' ')
            record = json.loads(line)
            records.append(record)


for record in tqdm(records):
    actionType = record['actionType']
    mechanismOfAction = record['mechanismOfAction']

    references = tuple((reference.get("source"), reference.get("urls", [])) for reference in record.get("references", []))

    for chembl_id in record['chemblIds']:
        for target_id in record['targets']:
            action_id = f"{chembl_id}_{target_id}"

            # Insert into tbl_actions table
            q = 'INSERT OR IGNORE INTO tbl_actions VALUES ($action_id, $chembl_id, $target_id, $actionType, $mechanismOfAction)'
            params = {
                'action_id': action_id,
                'chembl_id': chembl_id,
                'target_id': target_id,
                'actionType': actionType,
                'mechanismOfAction': mechanismOfAction,
                }
            con.execute(q, params)

            for ref_source, ref_data in references:
                # Insert into references table
                q = 'INSERT OR IGNORE INTO tbl_refs VALUES ($action_id, $ref_source, $ref_data)'
                params = {'action_id': action_id, 'ref_source': ref_source, 'ref_data': ref_data}
                con.execute(q, params)

# Verify data import
con.sql("SELECT * FROM tbl_actions LIMIT 20").show()
print(f'tbl_actions: {con.execute("SELECT count(*) FROM tbl_actions").fetchone()[0]} rows')
print(f'tbl_refs: {con.execute("SELECT count(*) FROM tbl_refs").fetchone()[0]} rows')
print(f'tbl_targets: {con.execute("SELECT count(*) FROM tbl_targets").fetchone()[0]} rows')
print(f'tbl_substances: {con.execute("SELECT count(*) FROM tbl_substances").fetchone()[0]} rows')
print(f'tbl_substances.name is NULL: {con.execute("SELECT count(*) FROM tbl_substances WHERE name is NULL").fetchone()[0]} rows')
print(f'tbl_actions.actionType is NULL: {con.execute("SELECT count(*) FROM tbl_actions WHERE actionType is NULL").fetchone()[0]} rows')

con.close()

print("✅ Data successfully written to DuckDB")

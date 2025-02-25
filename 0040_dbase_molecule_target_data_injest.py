import os
import json

import duckdb
from tqdm import tqdm


# Connect to DuckDB
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Directory containing JSON files
data_dir = "data_tmp"

files = [filename for filename in os.listdir(data_dir) if filename.startswith("mol_target_") and filename.endswith(".json")]

for filename in tqdm(files):
    file_path = os.path.join(data_dir, filename)
    
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract ChEMBL ID
    drug = data.get("data", {}).get("drug", {})
    chembl_id = filename.replace("mol_target_", "").replace(".json", "")

    # Insert into tbl_substances table
    name = drug.get("name")
    tradeNames = drug.get("tradeNames", [])

    q = '''
    INSERT OR IGNORE INTO tbl_substances
    SELECT * FROM tbl_molecules
    WHERE id = $chembl_id
    '''
    params = {'chembl_id': chembl_id}
    con.execute(q, params)

    # Process mechanisms of action
    for row in drug.get("mechanismsOfAction", {}).get("rows", []):
        actionType = row.get("actionType")
        mechanismOfAction = row.get("mechanismOfAction")

        # Process targets
        for target in row.get("targets", []):
            target_id = target.get("id")

            # Insert into tbl_targets table
            q = '''
            INSERT OR IGNORE INTO tbl_targets
            SELECT * FROM tbl_targets_tmp
            WHERE id = $target_id
            '''
            params = {'target_id': target_id}
            con.execute(q, params)

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

            # Process references
            for reference in row.get("references", []):
                ref_source = reference.get("source")
                ref_data = reference.get("urls", [])

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

import os
import json

import duckdb
from tqdm import tqdm


# Connect to DuckDB
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Directory containing JSON files
data_dir = "data_tmp"

# Process each JSON file
for filename in tqdm(os.listdir(data_dir)):
    if filename.startswith("mol_") and filename.endswith(".json"):
        file_path = os.path.join(data_dir, filename)
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Extract ChEMBL ID
        drug = data.get("data", {}).get("drug", {})
        chembl_id = filename.replace("mol_", "").replace(".json", "")

        # Insert into substances table
        name = drug.get("name")
        tradeNames = drug.get("tradeNames", [])

        q = '''
        INSERT OR IGNORE INTO substances
        SELECT * FROM molecules
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
                target_name = target.get("approvedName")

                # Insert into targets table
                q = 'INSERT OR IGNORE INTO targets VALUES ($target_id, $target_name)'
                params = {'target_id': target_id, 'target_name': target_name}
                con.execute(q, params)

                action_id = f"{chembl_id}_{target_id}"

                # Insert into actions table
                q = 'INSERT OR IGNORE INTO actions VALUES ($action_id, $chembl_id, $target_id, $actionType, $mechanismOfAction)'
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
                    q = 'INSERT OR IGNORE INTO refs VALUES ($action_id, $ref_source, $ref_data)'
                    params = {'action_id': action_id, 'ref_source': ref_source, 'ref_data': ref_data}
                    con.execute(q, params)

# Verify data import
con.sql("SELECT * FROM actions LIMIT 20").show()
print(f'actions: {con.execute("SELECT count(*) FROM actions").fetchone()[0]} rows')
print(f'refs: {con.execute("SELECT count(*) FROM refs").fetchone()[0]} rows')
print(f'targets: {con.execute("SELECT count(*) FROM targets").fetchone()[0]} rows')
print(f'substances: {con.execute("SELECT count(*) FROM substances").fetchone()[0]} rows')
print(f'substances.name is NULL: {con.execute("SELECT count(*) FROM substances WHERE name is NULL").fetchone()[0]} rows')
print(f'actions.actionType is NULL: {con.execute("SELECT count(*) FROM actions WHERE actionType is NULL").fetchone()[0]} rows')

con.close()

print("✅ Data successfully written to DuckDB")

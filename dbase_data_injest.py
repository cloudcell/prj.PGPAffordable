import os
import json

import duckdb
from tqdm import tqdm


# Connect to DuckDB
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Ensure tables exist
con.execute("""
CREATE TABLE IF NOT EXISTS substances (
    ChEMBL_id TEXT PRIMARY KEY,
    name TEXT,
    tradeNames TEXT[]
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS targets (
    target_id TEXT PRIMARY KEY,
    target_approvedName TEXT
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS refs (
    ref_id TEXT PRIMARY KEY,
    ref_source TEXT,
    ref_data TEXT[]
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS actions (
    ChEMBL_id TEXT,
    target_id TEXT,
    actionType TEXT,
    mechanismOfAction TEXT,
    reference_id TEXT,
    FOREIGN KEY (ChEMBL_id) REFERENCES substances(ChEMBL_id),
    FOREIGN KEY (target_id) REFERENCES targets(target_id),
    FOREIGN KEY (reference_id) REFERENCES refs(ref_id)
)
""")

# Directory containing JSON files
data_dir = "data_tmp"

# Process each JSON file
for filename in tqdm(os.listdir(data_dir)[:10]):
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

        q = 'INSERT OR IGNORE INTO substances VALUES ($chembl_id, $name, $tradeNames)'
        params = {'chembl_id': chembl_id, 'name': name, 'tradeNames': tradeNames}
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

                # Process references
                for reference in row.get("references", []):
                    reference_id = f"ref_{chembl_id}_{target_id}"
                    reference_source = reference.get("source")
                    references_data = reference.get("urls", [])

                    # Insert into references table
                    q = 'INSERT OR IGNORE INTO refs VALUES ($reference_id, $reference_source, $references_data)'
                    params = {'reference_id': reference_id, 'reference_source': reference_source, 'references_data': references_data}
                    con.execute(q, params)

                    # Insert into actions table
                    q = 'INSERT INTO actions VALUES ($chembl_id, $target_id, $actionType, $mechanismOfAction, $reference_id)'
                    params = {
                        'chembl_id': chembl_id,
                        'target_id': target_id,
                        'actionType': actionType,
                        'mechanismOfAction': mechanismOfAction,
                        'reference_id': reference_id,
                        }
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

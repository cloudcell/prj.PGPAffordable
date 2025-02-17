import os
import json
import duckdb

# Connect to DuckDB
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Ensure tables exist
con.execute("""
CREATE TABLE IF NOT EXISTS substances (
    ChEMBL_id TEXT PRIMARY KEY,
    name TEXT,
    tradeNames TEXT
)
""")

con.execute("""
CREATE TABLE IF NOT EXISTS targets (
    target_id TEXT PRIMARY KEY,
    target_approvedName TEXT
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

con.execute("""
CREATE TABLE IF NOT EXISTS refs (
    ref_id TEXT PRIMARY KEY,
    ref_source TEXT,
    ref_data JSON
)
""")

# Directory containing JSON files
data_dir = "data_tmp"

# Process each JSON file
for filename in os.listdir(data_dir):
    if filename.startswith("mol_") and filename.endswith(".json"):
        file_path = os.path.join(data_dir, filename)
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Extract ChEMBL ID
        drug = data.get("data", {}).get("drug", {})
        chembl_id = filename.replace("mol_", "").replace(".json", "")

        # Insert into substances table
        name = drug.get("name", "")
        tradeNames = ", ".join(drug.get("tradeNames", [])) if drug.get("tradeNames") else None

        con.execute("INSERT OR IGNORE INTO substances VALUES (?, ?, ?)", (chembl_id, name, tradeNames))

        # Process mechanisms of action
        for row in drug.get("mechanismsOfAction", {}).get("rows", []):
            actionType = row.get("actionType", "")
            mechanismOfAction = row.get("mechanismOfAction", "")

            # Process targets
            for target in row.get("targets", []):
                target_id = target.get("id", "")
                target_name = target.get("approvedName", "")

                # Insert into targets table
                con.execute("INSERT OR IGNORE INTO targets VALUES (?, ?)", (target_id, target_name))

                # Process references
                for reference in row.get("references", []):
                    reference_id = f"ref_{chembl_id}_{target_id}"
                    reference_source = reference.get("source", "")
                    references_json = json.dumps(reference.get("urls", []))

                    # Insert into references table
                    con.execute("INSERT OR IGNORE INTO references VALUES (?, ?, ?)", (reference_id, reference_source, references_json))

                    # Insert into actions table
                    con.execute("INSERT INTO actions VALUES (?, ?, ?, ?, ?)",
                                (chembl_id, target_id, actionType, mechanismOfAction, reference_id))

print("✅ Data successfully written to DuckDB")

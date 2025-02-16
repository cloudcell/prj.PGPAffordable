import duckdb

# Connect to (or create) the DuckDB database
conn = duckdb.connect("bio_data.duck.db")

# Create "substances" table
conn.execute("""
CREATE TABLE IF NOT EXISTS substances (
    ChEMBL_id STRING PRIMARY KEY,
    name STRING,
    tradeNames STRING[]  -- Array to store multiple trade names
);
""")

# Create "targets" table
conn.execute("""
CREATE TABLE IF NOT EXISTS targets (
    target_id STRING PRIMARY KEY,
    target_approvedName STRING
);
""")

# Create "refs" table (must exist before "actions" table)
conn.execute("""
CREATE TABLE IF NOT EXISTS refs (
    ref_id STRING PRIMARY KEY,
    reference_source STRING,
    reference_data JSON
);
""")

# Create "actions" table (now refs exists, so no error)
conn.execute("""
CREATE TABLE IF NOT EXISTS actions (
    ChEMBL_id STRING REFERENCES substances(ChEMBL_id),
    target_id STRING REFERENCES targets(target_id),
    actionType STRING,
    mechanismOfAction STRING,
    ref_id STRING REFERENCES refs(ref_id)  -- Foreign key reference to refs
);
""")

# Create "diseases" table
conn.execute("""
CREATE TABLE IF NOT EXISTS diseases (
    disease_id STRING PRIMARY KEY,
    disease_name STRING
);
""")

# Commit and close connection
conn.close()

print("DuckDB database and tables created successfully!")

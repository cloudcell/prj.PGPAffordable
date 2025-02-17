import duckdb

# Connect to (or create) the DuckDB database
conn = duckdb.connect("bio_data.duck.db")

# Create "substances" table
conn.execute("""
CREATE TABLE IF NOT EXISTS substances (
    ChEMBL_id STRING PRIMARY KEY,
    canonicalSmiles STRING,
    inchiKey STRING,
    drugType STRING,
    blackBoxWarning BOOLEAN,
    name STRING,
    yearOfFirstApproval INT,
    maximumClinicalTrialPhase FLOAT,
    hasBeenWithdrawn BOOLEAN,
    isApproved BOOLEAN,
    tradeNames STRING[],
    synonyms STRING[],
    crossReferences STRING,
    childChemblIds STRING[],
    linkedDiseases STRING[],
    linkedTargets STRING[],
    description STRING
);
""")

conn.execute("""
    CREATE TABLE IF NOT EXISTS molecules (
        id STRING PRIMARY KEY,
        canonicalSmiles STRING,
        inchiKey STRING,
        drugType STRING,
        blackBoxWarning BOOLEAN,
        name STRING,
        yearOfFirstApproval INT,
        maximumClinicalTrialPhase FLOAT,
        hasBeenWithdrawn BOOLEAN,
        isApproved BOOLEAN,
        tradeNames STRING[],
        synonyms STRING[],
        crossReferences STRING,
        childChemblIds STRING[],
        linkedDiseases STRING[],
        linkedTargets STRING[],
        description STRING
    )
""")

# Create "targets" table
conn.execute("""
CREATE TABLE IF NOT EXISTS targets (
    target_id STRING PRIMARY KEY,
    target_approvedName STRING
);
""")

# Create "actions" table (now refs exists, so no error)
conn.execute("""
CREATE TABLE IF NOT EXISTS actions (
    action_id STRING PRIMARY KEY,
    ChEMBL_id STRING,
    target_id STRING,
    actionType STRING,
    mechanismOfAction STRING,
    FOREIGN KEY (ChEMBL_id) REFERENCES substances(ChEMBL_id),
    FOREIGN KEY (target_id) REFERENCES targets(target_id)
)
""")

# Create "refs" table (must exist before "actions" table)
conn.execute("""
CREATE TABLE IF NOT EXISTS refs (
    action_id STRING,
    ref_source STRING,
    ref_data STRING[],
    PRIMARY KEY(action_id, ref_source),
    FOREIGN KEY (action_id) REFERENCES actions(action_id)
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

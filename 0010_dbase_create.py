import duckdb

# Connect to (or create) the DuckDB database
conn = duckdb.connect("bio_data.duck.db")

# Create "tbl_substances" table
conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_substances (
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
    CREATE TABLE IF NOT EXISTS tbl_molecules (
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

# Create "tbl_targets" table
conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_targets (
    target_id STRING PRIMARY KEY,
    target_approvedName STRING
);
""")

# Create "tbl_actions" table
conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_actions (
    action_id STRING PRIMARY KEY,
    ChEMBL_id STRING,
    target_id STRING,
    actionType STRING,
    mechanismOfAction STRING,
    FOREIGN KEY (ChEMBL_id) REFERENCES tbl_substances(ChEMBL_id),
    FOREIGN KEY (target_id) REFERENCES tbl_targets(target_id)
)
""")

# Create "tbl_refs" table
conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_refs (
    action_id STRING,
    ref_source STRING,
    ref_data STRING[],
    PRIMARY KEY(action_id, ref_source),
    FOREIGN KEY (action_id) REFERENCES tbl_actions(action_id)
);
""")

# Create "tbl_diseases" table
conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_diseases (
    disease_id STRING PRIMARY KEY,
    name STRING,
    description STRING
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_disease_target (
    disease_id STRING,
    target_id STRING,
    PRIMARY KEY(disease_id, target_id),
    FOREIGN KEY (disease_id) REFERENCES tbl_diseases(disease_id),
    FOREIGN KEY (target_id) REFERENCES tbl_targets(target_id)
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_disease_substance (
    disease_id STRING,
    ChEMBL_id STRING,
    PRIMARY KEY(disease_id, ChEMBL_id),
    FOREIGN KEY (disease_id) REFERENCES tbl_diseases(disease_id),
    FOREIGN KEY (ChEMBL_id) REFERENCES tbl_substances(ChEMBL_id)
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_action_types (
    actionType STRING PRIMARY KEY,
    value FLOAT
);
""")

conn.execute("""
CREATE TABLE IF NOT EXISTS tbl_knownDrugsAggregated (
    drugId STRING,
    targetId STRING,
    diseaseId STRING,
    phase FLOAT,
    status STRING,
    urls STRING,
    ancestors STRING[],
    label STRING,
    approvedSymbol STRING,
    approvedName STRING,
    targetClass STRING[],
    prefName STRING,
    tradeNames STRING[],
    synonyms STRING[],
    drugType STRING,
    mechanismOfAction STRING,
    targetName STRING
);
""")

# Commit and close connection
conn.close()

print("DuckDB database and tables created successfully!")

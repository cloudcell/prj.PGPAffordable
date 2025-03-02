import duckdb
import numpy as np
import json
from tqdm import tqdm
from collections import defaultdict
import pandas as pd


TEMP_TSV_PATH = "data_tmp/temp_data.tsv"
BATCH_SIZE = 10 # rows
NULL = '<NULL>'


time_start = pd.Timestamp.now()


def save_batch_to_db(con: duckdb.DuckDBPyConnection, batch: list[str]):
    with open(TEMP_TSV_PATH, 'w', encoding='utf-8') as f:
        f.write('\n'.join('\t'.join(map(str, row)) for row in batch))
    con.execute(f"""
        COPY tbl_molecular_vectors FROM '{TEMP_TSV_PATH}'
        (FORMAT CSV, HEADER TRUE, DELIMITER '\t', QUOTE '', ESCAPE '', NULL '{NULL}', AUTO_DETECT FALSE)
    """)


# Connect to DuckDB database
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Create a new table for storing molecular vectors
con.execute('DROP TABLE IF EXISTS tbl_molecular_vectors')
con.execute("""
    CREATE TABLE IF NOT EXISTS tbl_molecular_vectors (
        ChEMBL_id STRING PRIMARY KEY,
        vector JSON
    )
""")

molecule_ids = con.execute("SELECT id FROM tbl_molecules").fetchall()
molecule_ids = [row[0] for row in molecule_ids]

header = ['ChEMBL_id', 'vector']
batch = [header]

for chembl_id in tqdm(molecule_ids):
    q = """
        SELECT a.target_id, at.value
        FROM tbl_actions a
        JOIN tbl_action_types at ON a.actionType = at.actionType
        WHERE a.ChEMBL_id = ?
    """
    target_and_value_list = con.execute(q, [chembl_id]).fetchall()
    target_vector = defaultdict(list)
    for target_id, value in target_and_value_list:
        target_vector[target_id].append(value)
    averaged_vector = {target: np.mean(values) for target, values in target_vector.items()}
    target_ids = list(averaged_vector.keys())
    values = np.array(list(averaged_vector.values()), dtype=np.float32)
    norm = np.linalg.norm(values)
    if norm != 0:
        values /= norm
    vector_json = json.dumps(dict(zip(target_ids, values.tolist())))
    batch.append((chembl_id, vector_json))
    if len(batch) == BATCH_SIZE:
        save_batch_to_db(con, batch)
        batch = [header]

if len(batch) > 1:
    save_batch_to_db(con, batch)


# Verify insertion
con.sql("SELECT * FROM tbl_molecular_vectors LIMIT 10").show()

# Close connection
con.close()

print("✅ Molecular profile vectorization completed and stored in DuckDB.")

time_end = pd.Timestamp.now()
print(f"Time taken: {time_end - time_start}")

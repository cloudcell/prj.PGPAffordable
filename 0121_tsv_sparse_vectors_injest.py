"""
This script is used to create a matrix with rows-vectors from the generated vectorized molecular profiles in json format.
"""
import duckdb
from tqdm import tqdm
import gc


TEMP_TSV_PATH = "data_tmp/temp_data.tsv"
TEMP_TSV_PATH_BATCH = "data_tmp/temp_data_batch.tsv"  # for batch insertion
BATCH_SIZE = 10 # rows
NULL = '<NULL>'

# Connect to DuckDB database and create a huge table with the molecular vectors
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path, read_only=False, config={'max_memory':'64GB'})

# set memory limit
# con.execute("SET memory_limit = '10GB';")  # enable this in the future and test
# con.execute("SET enable_progress_bar = true;")

total = con.execute("SELECT count(*) FROM tbl_molecular_vectors").fetchone()[0]

def save_batch_to_db(con: duckdb.DuckDBPyConnection, batch: list[str]):
    with open(TEMP_TSV_PATH_BATCH, 'w', encoding='utf-8') as f:
        f.write('\n'.join(batch))
    con.execute(f"""
        SET preserve_insertion_order = false;
        SET temp_directory = './tmp/duckdb/';
        
        COPY tbl_vector_array FROM '{TEMP_TSV_PATH_BATCH}'
        (FORMAT CSV, HEADER TRUE, DELIMITER '\t', QUOTE '', ESCAPE '', NULL '{NULL}', AUTO_DETECT FALSE)
    """)

con.execute("DROP TABLE IF EXISTS tbl_vector_array")

with open(TEMP_TSV_PATH, "r", encoding='utf-8') as f:
    header = next(f).strip()

    # Create a new table for the vector array
    column_definitions = ", ".join([f'"{col}" FLOAT' for col in header.split('\t')[1:]])
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS tbl_vector_array (
            ChEMBL_id STRING PRIMARY KEY, {column_definitions}
        )
    """
    con.execute(create_table_query)

    batch = [header]
    for line in tqdm(f, total=total):
        batch.append(line)
        if len(batch) == BATCH_SIZE:
            save_batch_to_db(con, batch)
            batch = [header]
    if len(batch) > 1:
        save_batch_to_db(con, batch)

# Verify insertion
con.sql("SELECT * FROM tbl_vector_array LIMIT 10").show()
print(f'{con.execute("SELECT count(*) FROM tbl_vector_array").fetchone()[0]} rows')
print(f'{len(con.execute("SELECT * FROM tbl_vector_array LIMIT 1").fetchone())} columns')

# Close connection
con.close()

print("✅ Vector array creation completed and stored in DuckDB.")

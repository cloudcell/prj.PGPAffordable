# import os
# import logging
# import datetime as dt
# import duckdb
# import pandas as pd
# from tqdm import tqdm

# # Define paths
# DATA_DIR = "data/202409XX/evidence"
# LOGS_DIR = "logs"
# db_path = "bio_data.duck.db"

# # Ensure log directory exists
# os.makedirs(LOGS_DIR, exist_ok=True)
# logging.basicConfig(
#     filename=os.path.join(LOGS_DIR, dt.datetime.now().isoformat().replace(":", "-") + ".log"),
#     format="%(levelname)s:%(message)s",
#     level=logging.DEBUG,
# )

# # Connect to DuckDB
# con = duckdb.connect(db_path)

# # ✅ Ensure `tbl_diseases` is populated before proceeding
# print("🔄 Populating tbl_diseases from tbl_diseases_tmp...")
# con.execute(
#     """
#     INSERT OR IGNORE INTO tbl_diseases
#     SELECT * FROM tbl_diseases_tmp;
# """
# )
# disease_count = con.execute("SELECT COUNT(*) FROM tbl_diseases;").fetchone()[0]
# print(f"✅ tbl_diseases now contains {disease_count} rows.")

# # Load parquet files
# parquet_files_list = []
# for root, dirs, files in os.walk(DATA_DIR):
#     for fname in files:
#         if fname.endswith(".parquet"):
#             parquet_files_list.append(os.path.join(root, fname))
            
# # delete all but the first 10 files from list
# parquet_files_list = parquet_files_list[:10]

# # Load disease-target associations
# disease_target_list = set()
# for parquet_file in tqdm(parquet_files_list, desc="Processing Parquet Files"):
#     df = pd.read_parquet(parquet_file, columns=["diseaseId", "targetId"])
#     for _, row in df.iterrows():
#         disease_target_list.add((row["diseaseId"], row["targetId"]))

# # Insert data into `tbl_disease_target`
# print("🔄 Inserting data into tbl_disease_target...")
# q = "INSERT OR IGNORE INTO tbl_disease_target VALUES ($disease_id, $target_id)"
# for disease_id, target_id in tqdm(disease_target_list, desc="Inserting Disease-Target Links"):
#     params = {"disease_id": disease_id, "target_id": target_id}
#     try:
#         con.execute(q, params)
#     except Exception as e:
#         logging.error(str(e))

# # Final verification
# disease_target_count = con.execute("SELECT COUNT(*) FROM tbl_disease_target;").fetchone()[0]
# print(f"✅ tbl_disease_target now contains {disease_target_count} rows.")

# # Close connection
# con.close()
# print("✅ Data successfully written to DuckDB")


# exit(0)

import os
import logging
import datetime as dt
import duckdb
import pandas as pd
from tqdm import tqdm

# Define constants
DATA_DIR = "data/202409XX/evidence"
LOGS_DIR = "logs"
DB_PATH = "bio_data.duck.db"
BATCH_SIZE = 5_000  # Optimized batch size for memory efficiency

# Ensure log directory exists
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, dt.datetime.now().isoformat().replace(":", "-") + ".log"),
    format="%(levelname)s:%(message)s",
    level=logging.DEBUG,
)

# Connect to DuckDB
con = duckdb.connect(DB_PATH)

# ✅ Ensure `tbl_diseases` is populated before proceeding
print("🔄 Populating tbl_diseases from tbl_diseases_tmp...")
con.execute(
    """
    INSERT OR IGNORE INTO tbl_diseases
    SELECT * FROM tbl_diseases_tmp;
    """
)
disease_count = con.execute("SELECT COUNT(*) FROM tbl_diseases;").fetchone()[0]
print(f"✅ tbl_diseases now contains {disease_count} rows.")

# Load all Parquet files
parquet_files_list = [
    os.path.join(root, fname)
    for root, _, files in os.walk(DATA_DIR)
    for fname in files if fname.endswith(".parquet")
]

# Prepare batch insert query
print("🔄 Inserting data into tbl_disease_target...")
insert_query = "INSERT OR IGNORE INTO tbl_disease_target VALUES (?, ?)"

# Process files
for parquet_file in tqdm(parquet_files_list, desc="Processing Parquet Files"):
    try:
        rows_batch = []

        # Read Parquet file correctly
        df = pd.read_parquet(parquet_file, columns=["diseaseId", "targetId"])

        for disease_id, target_id in zip(df["diseaseId"], df["targetId"]):
            rows_batch.append((disease_id, target_id))

            # Insert in batches
            if len(rows_batch) >= BATCH_SIZE:
                con.execute("BEGIN TRANSACTION;")
                con.executemany(insert_query, rows_batch)
                con.execute("COMMIT;")
                rows_batch.clear()  # Clear the batch after inserting

        # Insert any remaining data after the loop
        if rows_batch:
            con.execute("BEGIN TRANSACTION;")
            con.executemany(insert_query, rows_batch)
            con.execute("COMMIT;")

    except Exception as e:
        logging.error(f"Error processing {parquet_file}: {str(e)}")

# Final verification
disease_target_count = con.execute("SELECT COUNT(*) FROM tbl_disease_target;").fetchone()[0]
print(f"✅ tbl_disease_target now contains {disease_target_count} rows.")

# Close connection
con.close()
print("✅ Data successfully written to DuckDB")


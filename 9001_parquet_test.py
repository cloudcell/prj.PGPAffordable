import os
import duckdb
import pandas as pd
# import pyarrow.parquet as pq


# open a parquet file from data\202409XX\tmp\part-00000-ea349457-2934-4038-96c6-83470290a1ec.c000.snappy.parquet

# # Connect to DuckDB database
# db_path = "bio_data_test.duck.db"
# con = duckdb.connect(db_path)

# read the whole file
df = pd.read_parquet("data/202409XX/tmp/part-00000-ea349457-2934-4038-96c6-83470290a1ec.c000.snappy.parquet")

# print the first 5 rows showing all columns using two for loops, one for record and one for column, with column names first
for i in range(1):
    for col in df.columns:
        print(col, "\t", df[col][i])
    print()





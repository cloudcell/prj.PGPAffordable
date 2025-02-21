"""
This script is used to check whether manual intervention is required for the activities in the generated intermediary database.
"""

import duckdb


db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# check if new actionType was added
action_type_null_values = con.execute('SELECT * FROM tbl_action_types WHERE value is NULL').fetchall()
if action_type_null_values:
    print(f'WARNING: action_type_null_values: {action_type_null_values}')
    # raise





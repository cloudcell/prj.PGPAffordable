import duckdb

chembl_id = 'CHEMBL621'
k = 200

SHOW_SELF = True  # Ensure self is always listed first if True

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

top_k_list = []
self_similarity = None  # Store self-similarity separately

# Fetch data from DuckDB
res = con.execute("SELECT * FROM similarity_matrix WHERE ChEMBL_id = ?", [chembl_id])
column_names = [column[0] for column in res.description]
values = res.fetchone()

if values is not None:
    for column_name, value in zip(column_names, values):
        # Skip non-numeric values
        if column_name == "ChEMBL_id":
            continue
        
        try:
            similarity_value = float(value)  # Ensure numeric conversion
            
            if column_name == chembl_id:  # Store self-similarity separately
                self_similarity = (column_name, similarity_value)
            else:
                top_k_list.append((column_name, similarity_value))
        
        except ValueError:
            continue  # Skip non-numeric values

    # Sort other similarities in descending order
    top_k_list.sort(key=lambda x: -x[1])
    top_k_list = top_k_list[:k]

    # Always place self-similarity first if SHOW_SELF is True
    if SHOW_SELF and self_similarity is not None:
        top_k_list.insert(0, self_similarity)

    # Print results
    for id, similarity in top_k_list:
        print(f'{id:<14}: {similarity:.5f}')
else:
    print(f'{chembl_id} not found in similarity_matrix')

con.close()

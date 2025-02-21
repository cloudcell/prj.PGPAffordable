import duckdb




chembl_id = 'CHEMBL621'
k = 100

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

top_k_list = []

res = con.sql('SELECT * FROM tbl_similarity_matrix WHERE ChEMBL_id = $chembl_id', params={'chembl_id': chembl_id})
column_names = [column[0] for column in res.description]
values = res.fetchone()

if values is not None:
    for column_name, value in zip(column_names, values):
        if column_name == chembl_id or column_name == 'ChEMBL_id':
            continue
        top_k_list.append((column_name, value))
    top_k_list.sort(key=lambda x: -x[1])
    top_k_list = top_k_list[:k]

    for id, similarity in top_k_list:
        print(f'{id:<14}: {similarity:.3f}')
else:
    print(f'{chembl_id} not found in similarity_matrix')


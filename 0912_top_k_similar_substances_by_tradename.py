import duckdb

# User input for trade name search
trade_name_input = input("Enter a trade name or part of it: ").strip()

k = 200  # Number of top similarities to retrieve
SHOW_SELF = True  # Ensure self is always listed first if True

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Search for matching ChEMBL IDs based on trade name
query = """
    SELECT DISTINCT m.id AS ChEMBL_id, t.trade_name
    FROM tbl_molecules m
    CROSS JOIN UNNEST(m.tradeNames) AS t(trade_name)
    WHERE t.trade_name ILIKE ?
"""
matching_drugs = con.execute(query, [f"%{trade_name_input}%"]).fetchdf()

if matching_drugs.empty:
    print(f"No matches found for trade name containing '{trade_name_input}'.")
    con.close()
    exit()

# If multiple matches, list them and exit
if len(matching_drugs) > 1:
    print("\nMultiple matches found:\n")
    for _, row in matching_drugs.iterrows():
        print(f"ChEMBL ID: {row['ChEMBL_id']} | Trade Name: {row['trade_name']}")
    con.close()
    exit()

# Unique match found, retrieve similarity data
chembl_id = matching_drugs.iloc[0]['ChEMBL_id']
trade_name = matching_drugs.iloc[0]['trade_name']

top_k_list = []
self_similarity = None  # Store self-similarity separately

# Fetch similarity data
res = con.execute("SELECT * FROM tbl_similarity_matrix WHERE ChEMBL_id = ?", [chembl_id])
column_names = [column[0] for column in res.description]
values = res.fetchone()

if values is not None:
    for column_name, value in zip(column_names, values):
        # Skip non-numeric values
        if column_name == "ChEMBL_id":
            continue

        similarity_value = float(value)  # Ensure numeric conversion

        if column_name == chembl_id:  # Store self-similarity separately
            self_similarity = (column_name, similarity_value, trade_name)
        else:
            # Fetch trade name for similarity results
            trade_name_res = con.execute(
                "SELECT DISTINCT t.trade_name FROM tbl_molecules m CROSS JOIN UNNEST(m.tradeNames) AS t(trade_name) WHERE m.id = ?", [column_name]
            ).fetchone()
            associated_trade_name = trade_name_res[0] if trade_name_res else "N/A"
            top_k_list.append((column_name, similarity_value, associated_trade_name))

    # Sort similarities in descending order
    top_k_list.sort(key=lambda x: -x[1])
    top_k_list = top_k_list[:k]

    # Always place self-similarity first if SHOW_SELF is True
    if SHOW_SELF and self_similarity is not None:
        top_k_list.insert(0, self_similarity)

    # Print results
    print(f"\nSimilarity Results for {chembl_id} (Trade Name: {trade_name}):\n")
    print(f"{'ChEMBL ID':<14} {'Similarity':<10} {'Trade Name'}")
    print("-" * 50)

    for id, similarity, tradename in top_k_list:
        print(f"{id:<14} {similarity:.5f}   {tradename}")
else:
    print(f'{chembl_id} not found in similarity_matrix')

con.close()

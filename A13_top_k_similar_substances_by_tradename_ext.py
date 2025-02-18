import duckdb

# User input for trade name or molecule name search
search_input = input("Enter a trade name or molecule name: ").strip()

k = 200  # Number of top similarities to retrieve
SHOW_SELF = True  # Ensure self is always listed first if True
ZERO_THRESHOLD = 0.0000  # Threshold for filtering out low similarity values

db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

# Search for matching ChEMBL IDs based on trade name or molecule name
query = """
    SELECT DISTINCT m.id AS ChEMBL_id, 
           COALESCE(t.trade_name, 'N/A') AS trade_name, 
           COALESCE(m.name, 'N/A') AS molecule_name
    FROM molecules m
    LEFT JOIN (SELECT id, unnest(tradeNames) AS trade_name FROM molecules) t
    ON m.id = t.id
    WHERE t.trade_name ILIKE ? OR m.name ILIKE ?
"""
matching_drugs = con.execute(query, [f"%{search_input}%", f"%{search_input}%"]).fetchdf()

if matching_drugs.empty:
    print(f"No matches found for '{search_input}'.")
    con.close()
    exit()

# If multiple matches, list them and exit
if len(matching_drugs) > 1:
    print("\nMultiple matches found:\n")
    for _, row in matching_drugs.iterrows():
        print(f"ChEMBL ID: {row['ChEMBL_id']} | Trade Name: {row['trade_name']} | Name: {row['molecule_name']}")
    con.close()
    exit()

# Unique match found, retrieve similarity data
chembl_id = matching_drugs.iloc[0]['ChEMBL_id']
trade_name = matching_drugs.iloc[0]['trade_name']
molecule_name = matching_drugs.iloc[0]['molecule_name']

top_k_list = []
self_similarity = None  # Store self-similarity separately

# Fetch similarity data
res = con.execute("SELECT * FROM similarity_matrix WHERE ChEMBL_id = ?", [chembl_id])
column_names = [column[0] for column in res.description]
values = res.fetchone()

if values is not None:
    for column_name, value in zip(column_names, values):
        # Skip non-numeric values
        if column_name == "ChEMBL_id":
            continue

        similarity_value = float(value)  # Ensure numeric conversion

        # Store self-similarity separately
        if column_name == chembl_id:
            self_similarity = (column_name, similarity_value, trade_name, molecule_name)
        elif similarity_value > ZERO_THRESHOLD:  # Filter out low similarity values
            # Fetch trade name & molecule name for similarity results
            trade_name_res = con.execute(
                """SELECT DISTINCT COALESCE(t.trade_name, 'N/A') AS trade_name, 
                          COALESCE(m.name, 'N/A') AS molecule_name 
                   FROM molecules m 
                   LEFT JOIN (SELECT id, unnest(tradeNames) AS trade_name FROM molecules) t 
                   ON m.id = t.id
                   WHERE m.id = ?""", [column_name]
            ).fetchone()
            
            associated_trade_name = trade_name_res[0] if trade_name_res else "N/A"
            associated_molecule_name = trade_name_res[1] if trade_name_res else "N/A"
            top_k_list.append((column_name, similarity_value, associated_trade_name, associated_molecule_name))

    # Sort similarities in descending order
    top_k_list.sort(key=lambda x: -x[1])
    top_k_list = top_k_list[:k]

    # Always place self-similarity first if SHOW_SELF is True
    if SHOW_SELF and self_similarity is not None:
        top_k_list.insert(0, self_similarity)

    # Print results
    print(f"\nSimilarity Results for {chembl_id} (Trade Name: {trade_name}, Name: {molecule_name}):\n")
    print(f"{'ChEMBL ID':<14} {'Similarity':<10} {'Trade Name':<20} {'Molecule Name'}")
    print("-" * 70)

    for id, similarity, tradename, molname in top_k_list:
        print(f"{id:<14} {similarity:.5f}   {tradename:<20} {molname}")

else:
    print(f'{chembl_id} not found in similarity_matrix')

con.close()

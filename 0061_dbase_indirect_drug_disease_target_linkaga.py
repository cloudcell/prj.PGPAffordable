# Creates an association between drugs and targets via drug, disease, and target associations
import duckdb
from tqdm import tqdm


# Connect to DuckDB
db_path = "bio_data.duck.db"
con = duckdb.connect(db_path)

molecule_ids = con.execute("SELECT id FROM tbl_molecules").fetchall()
molecule_ids = [row[0] for row in molecule_ids]

for chembl_id in tqdm(molecule_ids):
    q = '''
    INSERT OR IGNORE INTO tbl_actions (action_id, ChEMBL_id, target_id, actionType, mechanismOfAction)
    SELECT DISTINCT ds.ChEMBL_id || '_' || dt.target_id,
        ds.ChEMBL_id,
        dt.target_id,
        'UNIDENTIFIED',
        'UNIDENTIFIED'
    FROM tbl_disease_substance ds
    JOIN tbl_disease_target dt ON ds.disease_id = dt.disease_id 
    WHERE ds.ChEMBL_id = $chembl_id
    '''
    params = {'chembl_id': chembl_id}
    con.execute(q, params)


con.sql("SELECT * FROM tbl_actions WHERE actionType = 'UNIDENTIFIED' LIMIT 10").show()
print(f'tbl_actions not UNIDENTIFIED:', con.execute("SELECT count(*) FROM tbl_actions WHERE actionType != 'UNIDENTIFIED'").fetchone()[0], 'rows')
print(f'tbl_actions UNIDENTIFIED:', con.execute("SELECT count(*) FROM tbl_actions WHERE actionType = 'UNIDENTIFIED'").fetchone()[0], 'rows')

con.close()

print("✅ Data successfully written to DuckDB")

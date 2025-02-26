from fastapi import FastAPI, Query, HTTPException
import duckdb
from typing import List, Dict

# Initialize FastAPI app
app = FastAPI(title="Molecular Database API", description="API for querying molecular similarity and target data", version="1.0")

# Connect to DuckDB
db_path = "bio_data.duck.db"
conn = duckdb.connect(db_path)

@app.get("/molecules/{chembl_id}", response_model=Dict)
def get_molecule(chembl_id: str):
    """Retrieve details of a molecule by its ChEMBL ID."""
    query = """
        SELECT * FROM tbl_molecules WHERE id = ?
    """
    result = conn.execute(query, [chembl_id]).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Molecule not found")
    columns = [desc[0] for desc in conn.description]
    return dict(zip(columns, result))

@app.get("/similarity/{chembl_id}", response_model=List[Dict])
def get_similarity(chembl_id: str, top_k: int = Query(10, ge=1, le=100)):
    """Retrieve the top-k most similar molecules based on similarity matrix."""
    query = f"""
        SELECT * FROM tbl_similarity_matrix WHERE ChEMBL_id = ?
    """
    result = conn.execute(query, [chembl_id]).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Similarity data not found")
    
    columns = [desc[0] for desc in conn.description]
    similarities = dict(zip(columns, result))
    
    del similarities["ChEMBL_id"]  # Remove self-reference
    top_similar = sorted(similarities.items(), key=lambda x: -x[1])[:top_k]
    return [{"ChEMBL_id": chembl_id, "Similarity": sim} for chembl_id, sim in top_similar]

@app.get("/targets/{target_id}", response_model=Dict)
def get_target(target_id: str):
    """Retrieve details of a target by its ID."""
    query = """
        SELECT * FROM targets WHERE target_id = ?
    """
    result = conn.execute(query, [target_id]).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Target not found")
    columns = [desc[0] for desc in conn.description]
    return dict(zip(columns, result))

@app.get("/diseases/{disease_id}", response_model=Dict)
def get_disease(disease_id: str):
    """Retrieve details of a disease by its ID."""
    query = """
        SELECT * FROM tbl_diseases WHERE id = ?
    """
    result = conn.execute(query, [disease_id]).fetchone()
    if not result:
        raise HTTPException(status_code=404, detail="Disease not found")
    columns = [desc[0] for desc in conn.description]
    return dict(zip(columns, result))

@app.get("/disease_targets/{disease_id}", response_model=List[Dict])
def get_disease_targets(disease_id: str):
    """Retrieve all targets associated with a given disease."""
    query = """
        SELECT t.target_id, t.target_approvedName FROM tbl_disease_target dt
        JOIN tbl_targets t ON dt.target_id = t.id WHERE dt.disease_id = ?
    """
    results = conn.execute(query, [disease_id]).fetchall()
    if not results:
        raise HTTPException(status_code=404, detail="No targets found for this disease")
    columns = [desc[0] for desc in conn.description]
    return [dict(zip(columns, row)) for row in results]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7334)

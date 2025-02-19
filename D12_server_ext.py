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
        SELECT * FROM molecules WHERE id = ?
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
        SELECT * FROM similarity_matrix WHERE ChEMBL_id = ?
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
        SELECT * FROM diseases WHERE disease_id = ?
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
        SELECT t.target_id, t.target_approvedName FROM disease_target dt
        JOIN targets t ON dt.target_id = t.target_id WHERE dt.disease_id = ?
    """
    results = conn.execute(query, [disease_id]).fetchall()
    if not results:
        raise HTTPException(status_code=404, detail="No targets found for this disease")
    columns = [desc[0] for desc in conn.description]
    return [dict(zip(columns, row)) for row in results]

@app.get("/search/molecules", response_model=List[Dict])
def search_molecules(query: str):
    """Search for molecules by partial name or tradename."""
    query_str = """
        SELECT * FROM molecules 
        WHERE name ILIKE ? OR ? = ANY(tradeNames)
    """
    results = conn.execute(query_str, [f"%{query}%", query]).fetchall()
    return [dict(zip([desc[0] for desc in conn.description], row)) for row in results]

@app.get("/search/diseases", response_model=List[Dict])
def search_diseases(query: str):
    """Search for diseases by name or description."""
    query_str = """
        SELECT * FROM diseases 
        WHERE name ILIKE ? OR description ILIKE ?
    """
    results = conn.execute(query_str, [f"%{query}%", f"%{query}%"]).fetchall()
    return [dict(zip([desc[0] for desc in conn.description], row)) for row in results]

@app.get("/search/targets", response_model=List[Dict])
def search_targets(query: str):
    """Search for targets by name or description."""
    query_str = """
        SELECT * FROM targets 
        WHERE target_approvedName ILIKE ?
    """
    results = conn.execute(query_str, [f"%{query}%"]).fetchall()
    return [dict(zip([desc[0] for desc in conn.description], row)) for row in results]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

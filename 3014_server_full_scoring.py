import json
from fastapi import FastAPI, Query, HTTPException
import duckdb
from typing import List, Dict



STATUS_NUM = {
    'Active, not recruiting': 4,
    'Completed': 5,
    'Enrolling by invitation': 3,
    'Not yet recruiting': 1,
    'Recruiting': 2,
    'Suspended': 0,
    'Terminated': 0,
    'Unknown status': 0,
    'Withdrawn': 0,
    'N/A': 0,
}

# Initialize FastAPI app
app = FastAPI(title="Affordable API", description="API for querying molecular similarity and target data", version="1.0")

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
        SELECT * FROM tbl_targets WHERE target_id = ?
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

@app.get("/search/molecules", response_model=List[Dict])
def search_molecules(query: str):
    """Search for molecules by partial name or tradename."""
    query_str = """
        SELECT * FROM tbl_molecules 
        WHERE name ILIKE ? OR ? = ANY(tradeNames)
    """
    results = conn.execute(query_str, [f"%{query}%", query]).fetchall()
    return [dict(zip([desc[0] for desc in conn.description], row)) for row in results]

@app.get("/search/diseases", response_model=List[Dict])
def search_diseases(query: str):
    """Search for diseases by name or description."""
    query_str = """
        SELECT * FROM tbl_diseases 
        WHERE name ILIKE ? OR description ILIKE ?
    """
    results = conn.execute(query_str, [f"%{query}%", f"%{query}%"]).fetchall()
    return [dict(zip([desc[0] for desc in conn.description], row)) for row in results]

@app.get("/search/targets", response_model=List[Dict])
def search_targets(query: str):
    """Search for targets by name or description."""
    query_str = """
        SELECT * FROM tbl_targets 
        WHERE target_approvedName ILIKE ?
    """
    results = conn.execute(query_str, [f"%{query}%"]).fetchall()
    return [dict(zip([desc[0] for desc in conn.description], row)) for row in results]

@app.get("/disease_chembl_similarity/{disease_id}/{chembl_id}", response_model=Dict)
def get_disease_chembl_similarity(disease_id: str, chembl_id: str, top_k: int = Query(10, ge=1, le=100)):
    """Retrieve top-k similar substances for a given disease and ChEMBL ID."""
    
    # Get all target IDs associated with the disease
    target_query = """
        SELECT DISTINCT target_id FROM tbl_disease_target WHERE disease_id = ?
    """
    target_ids = conn.execute(target_query, [disease_id]).fetchall()
    
    if not target_ids:
        raise HTTPException(status_code=404, detail="No targets found for this disease")
    
    target_ids = {tid[0] for tid in target_ids}  # Convert to set
    
    # Fetch all molecule vectors
    vector_query = "SELECT * FROM tbl_vector_array"
    df_vectors = conn.execute(vector_query).fetchdf()
    df_vectors.columns = df_vectors.columns.str.upper()
    
    if "CHEMBL_ID" not in df_vectors.columns:
        raise HTTPException(status_code=500, detail="Error: 'chembl_id' column not found in database schema.")
    
    # Convert vectors into dictionary
    vector_data = df_vectors.set_index("CHEMBL_ID").to_dict(orient="index")
    
    if chembl_id not in vector_data:
        raise HTTPException(status_code=404, detail="ChEMBL ID not found in dataset")
    
    # Create binary mask based on target IDs
    vector_features = list(vector_data[chembl_id].keys())
    mask = [1 if feature in target_ids else 0 for feature in vector_features]
    
    # Apply mask to the reference vector
    vec_ref = [vector_data[chembl_id][feat] * mask[i] for i, feat in enumerate(vector_features)]
    
    similarities = []
    for other_chembl_id, vector in vector_data.items():
        vec = [vector[feat] * mask[i] for i, feat in enumerate(vector_features)]
        norm_product = sum(v**2 for v in vec_ref) ** 0.5 * sum(v**2 for v in vec) ** 0.5
        similarity = sum(vr * v for vr, v in zip(vec_ref, vec)) / (norm_product + 1e-9) if norm_product > 0 else 0
        
        if similarity > 0:
            similarities.append({"ChEMBL ID": other_chembl_id, "Cosine Similarity": similarity})
    
    # Sort results by similarity
    ranked_results = sorted(similarities, key=lambda x: x["Cosine Similarity"], reverse=True)[:top_k]

    query = "SELECT * FROM tbl_knownDrugsAggregated WHERE drugId = ? and diseaseId = ?"
    for i, row in enumerate(ranked_results):
        chembl_id1 = row['ChEMBL ID']
        known_drugs = [{column[0]: json.loads(value) if column[0] == 'urls' else value for column, value in zip(conn.description, row)} 
                       for row in conn.execute(query, [chembl_id1, disease_id]).fetchall()]
        ranked_results[i]['fld_knownDrugsAggregated'] = known_drugs

    for i, row in enumerate(ranked_results):
        chembl_id1 = row['ChEMBL ID']
        query = "SELECT COALESCE(name, 'N/A'), isApproved FROM tbl_substances WHERE chembl_id = ?"
        molecule_name, is_approved = conn.execute(query, [chembl_id1]).fetchone()

        query = "SELECT * FROM tbl_knownDrugsAggregated WHERE drugId = ? and diseaseId = ?"
        known_drugs_aggregated = [{column[0]: value for column, value in zip(conn.description, row)} for row in conn.execute(query, [chembl_id1, disease_id]).fetchall()]
        if known_drugs_aggregated:
            is_url_available = any(row['urls'] for row in known_drugs_aggregated)
            max_phase = max(row['phase'] for row in known_drugs_aggregated)
            max_status_for_max_phase = max((row['status'] for row in known_drugs_aggregated if row['phase'] == max_phase), key=lambda x: STATUS_NUM.get(x, 0))
            if max_status_for_max_phase is None:
                max_status_for_max_phase = 'N/A'
            status_num = STATUS_NUM[max_status_for_max_phase]
        else:
            known_drugs_aggregated = []
            is_url_available = False
            max_phase = 0
            max_status_for_max_phase = 'N/A'
            status_num = 0

        ranked_results[i]['Molecule Name'] = molecule_name
        ranked_results[i]['isUrlAvailable'] = is_url_available
        ranked_results[i]['isApproved'] = is_approved
        ranked_results[i]['phase'] = max_phase
        ranked_results[i]['status'] = max_status_for_max_phase
        ranked_results[i]['status_num'] = status_num
        ranked_results[i]['fld_knownDrugsAggregated'] = known_drugs_aggregated

    reference_drug = next(row for row in ranked_results if row['ChEMBL ID'] == chembl_id)

    ranked_results = [row for row in ranked_results if row['isUrlAvailable'] and row['ChEMBL ID'] != chembl_id]

    ranked_results.sort(key=lambda x: [x['ChEMBL ID'] == chembl_id, x['Cosine Similarity'], x['isApproved'], x['phase'], x['status_num']], reverse=True)

    # Display top-k results based on the top 10-th cosine similarity
    if len(ranked_results) > top_k - 1:
        ref_similarity = ranked_results[top_k - 1]["Cosine Similarity"]
        results_top_k = [row for row in ranked_results if row['Cosine Similarity'] >= ref_similarity]
    else:
        results_top_k = ranked_results

    return {'reference_drug': reference_drug, 'similar_drugs': results_top_k}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7334)

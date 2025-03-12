import os
import json
from fastapi import FastAPI, Query, HTTPException
import duckdb
import numpy as np
from typing import List, Dict



TABLE_IVPE_DIR = 'staging_area_03'
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

    vec_ref = conn.execute('SELECT * FROM tbl_vector_array WHERE ChEMBL_id = ?', [chembl_id]).fetchone()
    if not vec_ref:
        raise HTTPException(status_code=404, detail="ChEMBL ID not found in dataset")
    vec_ref = vec_ref[1:]
    vector_features = tuple(column[0] for column in conn.description[1:])
    mask = np.array([1 if feature in target_ids else 0 for feature in vector_features], dtype=np.float32)
    vec_ref = np.array(vec_ref, dtype=np.float32) * mask
    vec_ref_norm = np.linalg.norm(vec_ref)

    total = conn.execute("SELECT count(*) FROM tbl_vector_array").fetchone()[0]
    all_vectors = conn.execute("SELECT * FROM tbl_vector_array")

    similarities = []
    for _ in range(total):
        other_chembl_id, *vec = all_vectors.fetchone()
        vec = np.array(vec, dtype=np.float32) * mask  # Apply mask to each vector
        norm_product = vec_ref_norm * np.linalg.norm(vec)
        
        similarity = np.dot(vec_ref, vec) / (norm_product + 0) if norm_product > 0 else 0  # Avoid division by zero
        similarity = float(similarity)  # Convert to float for JSON serialization
        similarity = round(similarity, 6)

        if similarity > 0:
            similarities.append({"ChEMBL ID": other_chembl_id, "Similarity": similarity})

    # Sort results by similarity
    ranked_results = sorted(similarities, key=lambda x: x["Similarity"], reverse=True)

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

    # ------------ isApproved OR isUrlAvailable ------------------
    results_top_k_lvl1 = [row for row in ranked_results if (row['isUrlAvailable'] or row['isApproved']) and row['ChEMBL ID'] != chembl_id]
    results_top_k_lvl1.sort(key=lambda x: [x['Similarity'], x['isApproved'], x['isUrlAvailable'], x['phase'], x['status_num'], x['ChEMBL ID']], reverse=True)

    if len(results_top_k_lvl1) > top_k - 1:
        ref_similarity = results_top_k_lvl1[top_k - 1]["Similarity"]
        results_top_k_lvl1 = [row for row in results_top_k_lvl1 if row['Similarity'] >= ref_similarity]

    # ------------ not isApproved AND not isUrlAvailable ------------------
    results_top_k_lvl2 = [row for row in ranked_results if not row['isUrlAvailable'] and not row['isApproved'] and row['ChEMBL ID'] != chembl_id]
    results_top_k_lvl2.sort(key=lambda x: [x['Similarity'], x['phase'], x['status_num'], x['ChEMBL ID']], reverse=True)

    if len(results_top_k_lvl2) > top_k - 1:
        ref_similarity = results_top_k_lvl2[top_k - 1]["Similarity"]
        results_top_k_lvl2 = [row for row in results_top_k_lvl2 if row['Similarity'] >= ref_similarity]

    return {'reference_drug': reference_drug, 'similar_drugs_primary': results_top_k_lvl1, 'similar_drugs_secondary': results_top_k_lvl2}

@app.get("/evidences/{disease_id}/{reference_drug_id}/{replacement_drug_id}", response_model=List)
def get_evidences(disease_id: str, reference_drug_id: str, replacement_drug_id: str):
    q = f'''
    SELECT DISTINCT a.target_id
    FROM tbl_disease_target dt
    JOIN tbl_actions a ON dt.target_id = a.target_id
    WHERE dt.disease_id = ? AND a.ChEMBL_id = ?
    '''
    target_ids = conn.execute(q, [disease_id, reference_drug_id]).fetchall()

    if not target_ids:
        raise HTTPException(status_code=404, detail="No targets found for this disease")

    target_ids = [row[0] for row in target_ids]

    placeholders = ','.join(['?']*len(target_ids))
    q = f'''
    SELECT a.target_id,
        a.actionType,
        a.mechanismOfAction,
        r.ref_source,
        r.ref_data
    FROM tbl_actions a
    LEFT JOIN tbl_refs r ON a.action_id = r.action_id
    WHERE a.ChEMBL_id = ? AND a.target_id IN ({placeholders})
    '''
    rows = conn.execute(q, [replacement_drug_id, *target_ids]).fetchall()

    res = {}
    for target_id, action_type, mechanism_of_action, ref_source, ref_data in rows:
        k = (target_id, action_type, mechanism_of_action)
        if k not in res:
            res[k] = {
                'target_id': target_id,
                'action_type': action_type,
                'mechanism_of_action': mechanism_of_action,
                'refs': []
            }
        if ref_source:
            res[k]['refs'].append({'ref_source': ref_source, 'ref_data': ref_data})

    res = sorted(res.values(), key=lambda x: (x['action_type'] == 'UNIDENTIFIED', x['target_id']))

    return res

@app.get("/table_ivpe", response_model=List[Dict])
def get_table_ivpe():
    files = [fname for fname in sorted(os.listdir(TABLE_IVPE_DIR)) if fname.endswith('.txt')]
    result = []
    for file in files:
        with open(os.path.join(TABLE_IVPE_DIR, file)) as f:
            text = f.read()
        candidate = dict(line.split(':', 1) for line in text.split('\n') if line.strip() and not line.strip().startswith('#'))
        result.append(candidate)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=7334)

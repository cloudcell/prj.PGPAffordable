import os
import logging
import datetime as dt
from hashlib import md5
import json

import requests
from tqdm import tqdm


BASE_URL = 'http://127.0.0.1:7334'
LOGS_DIR = "logs"

os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, dt.datetime.now().isoformat().replace(":", "-") + ".log"),
    format="%(levelname)s: %(message)s",
    level=logging.DEBUG,
)


def get_obj_hash(obj):
    return md5(json.dumps(obj, sort_keys=True).encode('utf-8')).hexdigest()


with open('tests/test_batch_001.txt') as f:
    text = f.read()

for row in tqdm(text.split('\n')[1:]):
    if not row:
        continue
    disease_id, chembl_id, hash_expected, description = row.strip().split()
    res = requests.get(f'{BASE_URL}/disease_chembl_similarity/{disease_id}/{chembl_id}?top_k=100')
    res_json = res.json()

    reference_drug = res_json['reference_drug']
    similar_drugs_primary = res_json['similar_drugs_primary']
    similar_drugs_secondary = res_json['similar_drugs_secondary']

    logging.info(f'reference_drug hash: {get_obj_hash(reference_drug)}')

    logging.info(f'similar_drugs_primary hash: {get_obj_hash(similar_drugs_primary)}')
    for drug in similar_drugs_primary:
        logging.info(f'  {drug["ChEMBL ID"]} hash: {get_obj_hash(drug)}')

    logging.info(f'similar_drugs_secondary hash: {get_obj_hash(similar_drugs_secondary)}')
    for drug in similar_drugs_secondary:
        logging.info(f'  {drug["ChEMBL ID"]} hash: {get_obj_hash(drug)}')

    result_hash = get_obj_hash(res_json)

    logging.info(f'result_hash hash: {get_obj_hash(result_hash)}')

    if hash_expected != result_hash:
        err = f'{disease_id} - {chembl_id}, hash_expected != result_hash: {hash_expected} != {result_hash}'
        logging.error(err)
        print(err)



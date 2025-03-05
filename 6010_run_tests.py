import os
import logging
import datetime as dt
import subprocess
import time
import socket
from hashlib import md5
import json
import requests
from tqdm import tqdm

BASE_URL = 'http://127.0.0.1:7334'
LOGS_DIR = "logs"
SERVER_SCRIPT = "3015_server_full_scoring_optimised.py"  # Update with the actual filename
SERVER_PORT = 7334  # Change this if your server uses a different port

# Detect the local Python environment for both Windows and Linux
if os.name == 'nt':  # Windows
    VENV_PYTHON = os.path.join(os.getcwd(), "venv", "Scripts", "python.exe")
else:  # Linux/Mac
    VENV_PYTHON = os.path.join(os.getcwd(), "venv", "bin", "python")
PYTHON_EXECUTABLE = VENV_PYTHON if os.path.exists(VENV_PYTHON) else "python3"

# Ensure logs directory exists
os.makedirs(LOGS_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOGS_DIR, dt.datetime.now().isoformat().replace(":", "-") + ".log"),
    format="%(levelname)s: %(message)s",
    level=logging.DEBUG,
)

def start_server():
    """Start the server as a subprocess using the local Python environment."""
    logging.info(f"Starting the server using {PYTHON_EXECUTABLE}...")
    
    log_file_path = os.path.join(LOGS_DIR, "server_output.log")
    with open(log_file_path, "w") as log_file:
        server_process = subprocess.Popen(
            [PYTHON_EXECUTABLE, SERVER_SCRIPT],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            env={**os.environ, "PATH": os.path.dirname(PYTHON_EXECUTABLE) + os.pathsep + os.environ["PATH"]}  # Ensure virtual environment is used
        )
    
    return server_process

def is_port_open(port, host="127.0.0.1"):
    """Check if a specific port is open."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)  # Set a short timeout
        return s.connect_ex((host, port)) == 0

def wait_for_server():
    """Wait until the server starts by checking if the port is open."""
    logging.info("Waiting for the server to start...")

    for _ in range(30):  # Try for up to 30 seconds
        if is_port_open(SERVER_PORT):
            logging.info("Server is up and running.")
            return True
        time.sleep(1)
    
    logging.error("Server failed to start in time.")

    # Print server logs for debugging
    log_file_path = os.path.join(LOGS_DIR, "server_output.log")
    with open(log_file_path, "r") as log_file:
        print("===== SERVER LOG OUTPUT =====")
        print(log_file.read())
        print("============================")
    
    return False

# Start the server
server_process = start_server()
if not wait_for_server():
    logging.error("Server did not start. Exiting.")
    exit(1)

def get_obj_hash(obj):
    return md5(json.dumps(obj, sort_keys=True).encode('utf-8')).hexdigest()

with open('tests/test_batch_002_AB.txt') as f:
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
    logging.info(f'result hash: {result_hash}')

    if hash_expected != result_hash:
        err = f'{disease_id} - {chembl_id}, hash_expected != result_hash: {hash_expected} != {result_hash}'
        logging.error(err)
        print(err)

# Cleanup: Stop the server
server_process.terminate()
logging.info("Server terminated.")
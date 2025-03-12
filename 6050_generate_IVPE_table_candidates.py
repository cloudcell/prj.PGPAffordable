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


INPUT_DIR = 'staging_area_01'
OUTPUT_DIR = 'staging_area_02'

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
    filename=os.path.join(LOGS_DIR, "test_" + dt.datetime.now().isoformat().replace(":", "-") + ".log"),
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
else:
    logging.info("Server started successfully.")

for i, fname in enumerate(tqdm([fname for fname in sorted(os.listdir(INPUT_DIR)) if fname.endswith('.txt')])):
    with open(os.path.join(INPUT_DIR, fname)) as f:
        text = f.read()
    disease_id, reference_chembl_id = next(row for row in text.split('\n') if row.strip() and not row.startswith('#')).strip().split()[:2]

    logging.info(f"generate candidates for {disease_id} - {reference_chembl_id}")

    disease_name = requests.get(f'{BASE_URL}/diseases/{disease_id}').json()['name']
    res = requests.get(f'{BASE_URL}/disease_chembl_similarity/{disease_id}/{reference_chembl_id}?top_k=10')
    res_json = res.json()

    results = []
    for p in ('primary', 'secondary'):
        for row in res_json[f'similar_drugs_{p}']:
            result = {
                'similarity': row['Similarity'],
                'disease_id': disease_id,
                'disease_name': disease_name,
                'reference_drug_id': reference_chembl_id, 
                'reference_drug_name': res_json['reference_drug']['Molecule Name'],
                'substitute_drug_id': row['ChEMBL ID'], 
                'substitute_drug_name': row['Molecule Name'],
                'global_patient_population': 'N/A',
                'cost_difference': 'N/A',
                'evidence': f'Phase {row["phase"]} (phase status: {row["status"]})' if row["phase"] else 'N/A',
                'annual_cost_reduction': 'N/A',
            }
            results.append('\n'.join(f'{k}: {v}' for k, v in result.items()))
    with open(os.path.join(OUTPUT_DIR, f'ivpe_case_{i:0>3}.txt'), 'w', encoding='utf-8') as f:
        f.write('\n\n'.join(results))

# Cleanup: Stop the server
server_process.terminate()
logging.info("Server terminated.")
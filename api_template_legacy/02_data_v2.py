import requests
import json
import pandas as pd
from tqdm import tqdm
import logging
import datetime

# Enable Debug Logging
logging.basicConfig(level=logging.DEBUG)

# API URLs
LOGIN_URL = "https://soma.science/api/v1/login"
SESSION_URL = "https://soma.science/api/v1/session"
SEARCH_URL = "https://soma.science/api/v1/search"

# File paths
PATH_METADATA = "./metadata/perturbagens_o.txt"
PATH_OUTPUT = "./data/"
FILENAME_PREFIX = "perturbagen_web_"
REFERENCE_FILE = PATH_OUTPUT + 'reference.tsv'

# Credentials
username = '1234@5678.90'
password = '1234'

# Step 1: Create a persistent session
session = requests.Session()

# Step 2: Authenticate and store session cookies
login_payload = {"username": USERNAME, "password": PASSWORD}
headers = {"Accept": "application/json", "Content-Type": "application/x-www-form-urlencoded"}

response = session.post(LOGIN_URL, headers=headers, data=login_payload)

if response.status_code == 200:
    print("✅ Login successful!")
else:
    print("❌ Login failed:", response.text)
    raise ValueError("Authentication failed.")

# Retrieve session cookie
session_cookie = session.cookies.get_dict()
print("🔑 Session Cookie:", session_cookie)

# Step 3: Retrieve session details
response = session.get(SESSION_URL)

if response.status_code == 200:
    session_data = response.json()
    print("📂 Session Data:", session_data)
else:
    print("❌ Failed to retrieve session data:", response.text)
    raise ValueError("Session retrieval failed.")

# Extract session subfolder name
mySessionSubfolderName = session_data.get("session_temp_sub_folder", "")

# Step 4: Send a search request for an example term
search_payload = {
    "inp_correlation": "any",
    "inp_key_term_classes": [],
    "inp_key_term_end": "",
    "inp_key_term_end_case": False,
    "inp_key_term_end_whole": True,
    "inp_key_term_start": "trazodone",
    "inp_key_term_start_case": False,
    "inp_key_term_start_whole": True,
    "inp_nbr_of_jumps_max": 1,
    "inp_nbr_of_jumps_min": 1,
    "inp_relation": "any",
    "inp_requested_paths": 100,
    "inp_session_subfolder": mySessionSubfolderName
}

print("📨 Sending search request with payload:", json.dumps(search_payload, indent=2))

headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Cookie": f"session={session_cookie.get('session', '')}"
}

response = session.post(SEARCH_URL, headers=headers, json=search_payload)

if response.status_code == 200:
    print("✅ Search request successful!")
    response_data = response.json()
else:
    print("❌ Search request failed:")
    print("Status Code:", response.status_code)
    print("Response:", response.text)
    print("Headers:", response.headers)
    raise ValueError("Failed to send search request.")

print("response_data")
print(response_data)
# Step 5: Fetch data file
edgeDataFileUrl = response_data.get('out_edge_only_datafile_url_full')

if not edgeDataFileUrl:
    raise ValueError("❌ Missing 'out_edge_only_datafile_url_full' in response.")

url = edgeDataFileUrl
r = session.get(url, headers=headers)

if r.status_code != 200:
    raise ValueError("❌ Failed to fetch data file.")

# Save file to disk
with open(REFERENCE_FILE, 'w') as f:
    f.write(r.text)

# Load data to a dataframe
df = pd.read_csv(REFERENCE_FILE, sep='\t', header=None)
print("📊 Loaded Data:")
print(df.head())

# Step 6: Process multiple perturbagens from the metadata file
perturbagens = pd.read_csv(PATH_METADATA, sep="\t", header=None)

counter = 0
start = False

try:
    for i in tqdm(range(len(perturbagens))):
        perturbagen = perturbagens.iloc[i, 0]
        print(f"\n🔍 Searching for perturbagen: {perturbagen}")

        if perturbagen == 'ketamine':
            start = True
        if not start:
            counter += 1
            continue

        # Build search request for perturbagen
        search_payload["inp_key_term_start"] = perturbagen

        response = session.post(SEARCH_URL, headers=headers, json=search_payload)

        if response.status_code != 200:
            with open("error.log", "a") as f:
                f.write(f"❌ Failed to send search request for {perturbagen}\n")
            print(f"❌ Error searching {perturbagen}")
            continue  # Move to the next item instead of stopping the script

        # Fetch data file
        edgeDataFileUrl = response.json().get('out_edge_only_datafile_url_full')
        if not edgeDataFileUrl:
            with open("error.log", "a") as f:
                f.write(f"❌ No file found for {perturbagen}\n")
            continue  # Skip to next iteration

        url = edgeDataFileUrl
        r = session.get(url, headers=headers)

        if r.status_code != 200:
            with open("error.log", "a") as f:
                f.write(f"❌ Failed to fetch data file for {perturbagen}\n")
            continue

        # Save file with a counter-based filename
        suffix = str(counter).zfill(5)
        output_filename = f"{PATH_OUTPUT}{FILENAME_PREFIX}{suffix}.tsv"

        with open(output_filename, 'wb') as f:
            f.write(r.text.encode('utf-8', errors='replace'))

        print(f"✅ Data saved for {perturbagen}: {output_filename}")
        counter += 1

except requests.Timeout as ex:
    print(f"⏳ Timeout error: {ex.request.url}")

now = datetime.datetime.now()
print(f"🎯 Finished at: {now.strftime('%Y-%m-%d %H:%M:%S')}")

# Close session
session.close()

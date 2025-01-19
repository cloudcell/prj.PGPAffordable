

# use API to get the data
# curl -X POST "https://soma.science/api/v1/login" -H  "accept: application/json" -H  "Content-Type: application/x-www-form-urlencoded" -d "username=______________%40gmail.com&password=_________"

import requests
import json
import pandas as pd
from tqdm import tqdm

path_and_filename_i = "./metadata/psd-95-interactive.txt"
path_o = "./analysis_level_a1/"
filename_prefix_o = "perturbagen_web_"
# path_and_filename_ref = path_o + 'reference.tsv'


url = "https://soma.science/api/v1/login"

username = '______________________'
password = '______________________'

payload = {
    'username': username,
    'password': password
    }

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded'
}

if 1:
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code != 200:
        raise ValueError("Failed to authenticate.")

    cookie = response.cookies.get_dict()

    # Get the session data
    url = "https://soma.science/api/v1/session"
    headers = {
        "accept": "application/json",
        "cookie": "session=" + cookie['session']
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise ValueError("Failed to get session data.")

    session_data = response.json()
    url_for_progress_bar = session_data['url_for_progress_bar']
    mySessionSubfolderName = session_data['session_temp_sub_folder']

if 0:
    # Send search request
    url = "https://soma.science/api/v1/search"
    payload = {
        'mySessionSubfolderName': mySessionSubfolderName,
        'inp_key_term_start': 'trazodone',
        'inp_key_term_end': '',
        'inp_nbr_of_jumps': '1',
        'inp_nbr_of_jumps_min': '1',
        'inp_relation': '',
        'inp_correlation': '',
        'inp_key_term_start_whole': 'yes',
        'inp_key_term_start_case': 'no',
        'inp_key_term_end_whole': 'yes',
        'inp_key_term_end_case': 'no'
        }
    response = requests.post(url, headers=headers, data=payload)
    if response.status_code != 200:
        raise ValueError("Failed to send search request.")

    # Fetch data file
    edgeDataFileUrl = response.json()['edgeDataFileUrl']
    url = "https://soma.science" + edgeDataFileUrl

    if 1:
        r = requests.get(url, headers=headers)


    if r.status_code != 200:
        raise ValueError("Failed to fetch data file.")

    # Save file to disk
    with open(path_and_filename_ref, 'w') as f:
        f.write(r.text)

    # Load data to a dataframe
    df = pd.read_csv(path_and_filename_ref, sep='\t', header=None)
    print(df)

# read in the file perturbagens_o.txt
perturbagens = pd.read_csv(path_and_filename_i, sep="\t", header=None)

counter = 0
start = False

try:
    # cycle through the perturbagens and get the data for each one, using tqdm to show the progress bar
    for i in tqdm(range(len(perturbagens))):
        perturbagen = perturbagens.iloc[i,0]
        print("\n" + perturbagen)
        
        if perturbagen == 'Anks1':
            start = True
        if start == False:
            counter += 1
            continue
    
        
        # Send search request
        url = "https://soma.science/api/v1/search"
        payload = {
            'mySessionSubfolderName': mySessionSubfolderName,
            'inp_key_term_start': perturbagen,
            'inp_key_term_end': '',
            'inp_nbr_of_jumps': '1',
            'inp_nbr_of_jumps_min': '1',
            'inp_relation': '',
            'inp_correlation': '',
            'inp_key_term_start_whole': 'yes',
            'inp_key_term_start_case': 'no',
            'inp_key_term_end_whole': 'yes',
            'inp_key_term_end_case': 'no'
            }
        response = requests.post(url, headers=headers, data=payload)
        if response.status_code != 200:
            with(open('error.log', 'w')) as f:
                message = "Failed to send search request for perturbagen " + perturbagen + "."
                f.write(message)
            raise ValueError("Failed to send search request.")
        
        # Fetch data file
        edgeDataFileUrl = response.json()['edgeDataFileUrl']
        url = "https://soma.science" + edgeDataFileUrl
        r = requests.get(url, headers=headers)
        if r.status_code != 200:
            with(open('error.log', 'w')) as f:
                message = "Failed to fetch data file for perturbagen " + perturbagen + "."
                f.write(message)
            raise ValueError("Failed to fetch data file.")
        
        # Save file to disk
        # # clean the perturbagen name from special characters ('+', '-', '/', etc.)
        # perturbagen_clean = perturbagen.replace('+', '_')
        # perturbagen_clean = perturbagen_clean.replace('/', '_')
        # perturbagen_clean = perturbagen_clean.replace('\\', '_')
        
        # create a suffix for the file name with 5 digits
        suffix = str(counter).zfill(5)
        # with open(path_o + filename_prefix_o + perturbagen_clean + ".tsv", 'w') as f:
        with open(path_o + filename_prefix_o + suffix + ".tsv", 'wb') as f:
            # f.write(r.text)
            f.write(r.text.encode('utf-8', errors='replace'))

        counter += 1
    
except requests.Timeout as ex:
    print(f'Timeout: url = {ex.request.url}, data = {ex.request.data}')

import datetime
now = datetime.datetime.now()
print("Finished at: ", now.strftime("%Y-%m-%d %H:%M:%S"))

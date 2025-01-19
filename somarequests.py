# /api/v1/edges_complete_set_status/{session_temp_sub_folder}

import requests
import pandas as pd
from tqdm import tqdm


def get_star(concept_name: str):

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

    # Send search request
    url = "https://soma.science/api/v1/search"
    payload = {
        'mySessionSubfolderName': mySessionSubfolderName,
        'inp_key_term_start': concept_name,
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
    with open('tmp.tsv', 'w') as f:
        f.write(r.text)
    
    url2 = "https://soma.science/api/v1/edges_complete_set_status/" + mySessionSubfolderName
    
    response = requests.get(url2, headers=headers)
    if response.status_code != 200:
        raise ValueError("Failed to get session data.")
    
    print(response.json())
    
    # while response.json()['status'] != "complete":
    #     response = requests.get(url2, headers=headers)
    #     if response.status_code != 200:
    #         raise ValueError("Failed to get session data.")
    #     print(response.json())

    
def get_rays(pairs: pd.DataFrame):
   
    
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

    # create an empty dataframe with length of the pairs dataframe filled with empty strings
    ray_df = pd.DataFrame(columns=["node1", "node2", "datadump"], index=range(len(pairs)))

    for i in tqdm(range(len(pairs))):
        pair = pairs.iloc[i,:]
        
        url = "https://soma.science/api/v1/rlabels"
        
        payload = {
            'node1': pair[0],
            'node2': pair[1],
            }

        response = requests.get(url, headers=headers, params=payload)
        if response.status_code != 200:
            raise ValueError("Failed to send search request.")
        
        
        # use loc to choose the row and the column
        ray_df.loc[i,:] = pair[0], pair[1], response.json()
        
        # ({"node1": pair[1][0], "node2": pair[1][1], "datadump": response.json()}, ignore_index=True)
        # print(response.json())
    
    print("Done.")
    return(ray_df)

    
    
    
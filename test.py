

# use API to get the data
# curl -X POST "https://soma.science/api/v1/login" -H  "accept: application/json" -H  "Content-Type: application/x-www-form-urlencoded" -d "username=______________%40gmail.com&password=_________"

import requests
import json

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

response = requests.request("POST", url, headers=headers, data = payload)
cookie = response.cookies.get_dict()

print(response.text.encode('utf8'))

# return json object

obj_json = json.loads(response.text)

print(obj_json)

# obj_json['token']

# -----------------------

# get the session data
# curl -X GET "https://soma.science/api/v1/session" -H  "accept: application/json"

import requests

url = "https://soma.science/api/v1/session"

headers = {
    "accept": "application/json",
    "cookie": "session=" + cookie['session']
}

response = requests.get(url, headers=headers)

print(response.json())


url_for_progress_bar = response.json()['url_for_progress_bar']
mySessionSubfolderName = response.json()['session_temp_sub_folder']

print(url_for_progress_bar)


url = "https://soma.science/api/v1/search"

payload = {
    'mySessionSubfolderName': mySessionSubfolderName,
    'inp_key_term_start': 'alcohol',
    'inp_key_term_end': 'cancer',
    'inp_nbr_of_jumps': '1',
    'inp_nbr_of_jumps_min': '1',
    'inp_relation': '',
    'inp_correlation': '',
    'inp_key_term_start_whole': 'no',
    'inp_key_term_start_case': 'no',
    'inp_key_term_end_whole': 'no',
    'inp_key_term_end_case': 'no'
    }

# make a request to the url
response = requests.post(url, headers=headers, data = payload)

print(response.text.encode('utf8'))

edgeCompleteDataFileUrl = response.json()['edgeCompleteDataFileUrl']

url = "https://soma.science" + edgeCompleteDataFileUrl

print(url)

# get file using requests

r = requests.get(url, headers=headers)

print(r.text.encode('utf8'))




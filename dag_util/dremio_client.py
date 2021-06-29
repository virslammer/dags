import requests
import json
import urllib.parse
import os
# dremio base url
DREMIO_BASE_URL = "http://10.88.231.37:31220/api/v3/catalog/"
DREMIO_LOGIN_URL = "http://10.88.231.37:31220/apiv2/login"
# dremio credential
USERNAME = "admin"
PASSWORD = "admin1234"
HEADERS = {
    'Content-Type': "application/json"
}
# dremio file default path
DREMIO_DEFAULT_PATH = "minio-source/datalake/landing_zone"


def login_dremio ():
    payload = {"userName": USERNAME, "password": PASSWORD}
    response = requests.request("POST", DREMIO_LOGIN_URL, data=json.dumps(payload), headers=HEADERS)
    response_data = json.loads(response.text)
    return response_data['token']
def promote_file_to_dataset (filename, token):
    HEADERS['Authorization'] = "_dremio"+token
    catalog_by_path_endpoint = DREMIO_BASE_URL + r"by-path/" + DREMIO_DEFAULT_PATH + "/" + filename
    check_file_on_dremio = requests.request("GET", catalog_by_path_endpoint,headers=HEADERS )
    # id = "dremio:/" + DREMIO_DEFAULT_PATH + "/" + filename
    check_file_on_dremio_info = json.loads(check_file_on_dremio.text)
    id = check_file_on_dremio_info['id']
    path = check_file_on_dremio_info['path']
    encode_id = urllib.parse.quote_plus(id)
    # path = DREMIO_DEFAULT_PATH.split("/")
    # path.append("/" +filename)
    # path = os.path.join(DREMIO_DEFAULT_PATH,filename).split("/")
    if check_file_on_dremio_info['entityType'] != 'dataset':
        payload = {
            "entityType": "dataset",
            "type": "PHYSICAL_DATASET",
            "id": encode_id,
            "path": path,
            "format": {
                "type": "Parquet"
            }
        }
        response = requests.request("POST", DREMIO_BASE_URL + encode_id, data=json.dumps(payload), headers=HEADERS)
        response_data = json.loads(response.text)
        return response_data['id']
    else:
        # Todo : update Dataset  - Now: return the id of file
        return id
def add_tag_for_catalog (catalog_id, token, tags):
    payload = {
        "tags": tags
    }
    HEADERS['Authorization'] = "_dremio"+token
    response = requests.request("POST", DREMIO_BASE_URL + catalog_id + "/collaboration/tag", data=json.dumps(payload), headers=HEADERS)
    response_data = json.loads(response.text)
    return response_data
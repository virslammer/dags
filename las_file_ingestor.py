from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor 
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
import lasio
from minio import Minio
from io import BytesIO
from dag_util.minio_client import minio_client
from dag_util.dremio_client import login_dremio, add_tag_for_catalog, promote_file_to_dataset
import pandas as pd
import os
import json
import requests
import urllib.parse

# SETTING  catalog_service , minio and dremio 
FILE_CATALOG_SERVICE_URL =  r"http://10.88.231.37:31624/api/v1/catalog/file/"
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
    
MINIO_URL =  '10.88.231.36:9000'
MINIO_ACCESS_KEY =  'minioadmin'
MINIO_SECRET_KEY =  'minioadmin'
minio_client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY, 
    secure = False
)


# --------------------------------------------------
default_args = {
    'owner': 'dmp',
    'depends_on_past': False,
    'email': ['virslammer@gmail.com'],
    'start_date': datetime(2021,1,1)
}

def _extract_las_file(templates_dict, **kwargs):
    
    BUCKET = "datalake"
    file_path = ""
    if templates_dict['file_name']:
        #do something with the filename
        file_path = templates_dict['file_name']
    else:
        #no filename specified, probably a manual run
        file_path = 'raw_zone/82X30C.LAS'
    response = minio_client.get_object(BUCKET, file_path)
    buff = BytesIO(response.data)
    # read LAS file from minio
    data = {
        'header':{},
        'data':[]
    }
    las = lasio.read(buff.read().decode('UTF-8'))
    data = {
        'file_name': file_path.split('/')[-1],
        'header': {header.mnemonic:header.value for header in las.well},
        'data':{ 
            'channel':[curves.mnemonic for curves in las.curves],
            # 'log_data':[[] for curves in las.curves]
            'log_data':[curves.data.tolist() for curves in las.curves]
            }
    }
    response.close()
    response.release_conn()
    return data
def _add_data_catalog_service(ti):
    headers =  {
                'Content-Type': 'application/json',
                }
    payload = json.dumps({
                    "file_name": ti.xcom_pull(task_ids=['extract_las_file'])[0]['file_name'],
                    "file_type": "las",
                    "ingestor_routines": [],
                    "description": "Well log data",
                    "source": "onewkShop",
                    "tag": ti.xcom_pull(task_ids=['extract_las_file'])[0]['header']
                })
    response = requests.request("POST", FILE_CATALOG_SERVICE_URL, headers=headers, data=payload)
    print(response.text)
def _save_las_file(ti):
    data = ti.xcom_pull(task_ids=['extract_las_file'])
    if not len(data):
        raise ValueError('Data is empty')
    else:
        data = data[0]
    BUCKET = "datalake"
    PREFIX = "landing_zone"
    file_name = data['file_name'].split('.')[0]
    parquet_file_path = os.path.join(PREFIX, file_name  + ".parquet")
    tags = ["_".join([str(key),str(value)]) for key,value in data['header'].items()]
    zip_iterator = zip(data['data']['channel'], data['data']['log_data']) # Covert 2 list -> dict
    log_data_dict = dict(zip_iterator)
    
    df_pandas = pd.DataFrame(log_data_dict)
    print(df_pandas.head())
    f = BytesIO()
    df_pandas.to_parquet(f)
    f.seek(0)
    minio_client.put_object(
        bucket_name=BUCKET,
        object_name=parquet_file_path, 
        length=f.getbuffer().nbytes,
        data=f
    )
    
    return {
        'msg':'Upload success',
        'file_name':file_name + ".parquet",
        'tags':tags
        }

def _promote_dataset_on_dremio(ti):
    token = ti.xcom_pull(task_ids=['get_dremio_token'])[0]
    data = ti.xcom_pull(task_ids=['save_las_file'])
    if not len(data):
        raise ValueError('Data is empty')
    else:
        data = data[0]
    file_name = data['file_name']
    return promote_file_to_dataset(file_name, token)

def _add_tag_on_dremio(ti):
    token = ti.xcom_pull(task_ids=['get_dremio_token'])[0]
    catalog_id = ti.xcom_pull(task_ids=['promote_dataset_on_dremio'])[0]
    data = ti.xcom_pull(task_ids=['save_las_file'])
    if not len(data):
        raise ValueError('Data is empty')
    else:
        data = data[0]
    tags = data['tags']
    return add_tag_for_catalog(catalog_id, token, list(tags))

with DAG('las_ingestor', schedule_interval=None,
        default_args=default_args,
        catchup=False) as dag:
    # Define tasks/operators
    get_dremio_token = PythonOperator(
        task_id='get_dremio_token',
        python_callable=login_dremio,
        
    )
    is_catalog_service_available = HttpSensor(
        task_id="is_catalog_service_available",
        http_conn_id="catalog_service",
        endpoint="/"
    )

    extract_las_file = PythonOperator(
        task_id='extract_las_file',
        provide_context=True,
        python_callable=_extract_las_file,
        templates_dict={'file_name': "{{dag_run.conf['file_name']}}"}
    )

    save_las_file = PythonOperator(
        task_id='save_las_file',
        python_callable=_save_las_file
    )

    add_data_catalog_service = PythonOperator(
        task_id='add_data_catalog_service',
        python_callable=_add_data_catalog_service
    )

    promote_dataset_on_dremio = PythonOperator(
        task_id='promote_dataset_on_dremio',
        python_callable=_promote_dataset_on_dremio
    )
    
    add_tag_on_dremio = PythonOperator(
        task_id='add_tag_on_dremio',
        python_callable=_add_tag_on_dremio
    )

    get_dremio_token >> extract_las_file >> save_las_file >> promote_dataset_on_dremio >> add_tag_on_dremio
    get_dremio_token >> extract_las_file >> add_data_catalog_service
    is_catalog_service_available >>  add_data_catalog_service


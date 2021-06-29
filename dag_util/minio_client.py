from minio import Minio

MINIO_URL =  '10.88.231.36:9000'
MINIO_ACCESS_KEY =  'minioadmin'
MINIO_SECRET_KEY =  'minioadmin'
minio_client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY, 
    secure = False
)
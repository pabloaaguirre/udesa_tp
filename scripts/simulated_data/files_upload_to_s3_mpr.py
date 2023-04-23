import boto3
import pandas as pd
import os

# Get environment variables
USER = os.getenv('ACCESS_KEY')
PASSWORD = os.environ.get('SECRET_KEY')

# Crear una instancia del cliente de S3
s3 = boto3.client("s3",
                  aws_access_key_id=USER,
                  aws_secret_access_key=PASSWORD)

# Definir el nombre del bucket y el nombre del archivo que deseas cargar
bucket_name = "raw-ads-database-tp-programacion-avanzada-mpr"

file_name1 = "ads_views"
file_which1 = "/Users/martin/Library/CloudStorage/OneDrive-Personal/Maestria_Udesa/Materias/Programacion_avanzada/TP_final_programacion/data/ads_views"

file_name2 = "advertiser_ids"
file_which2 = "/Users/martin/Library/CloudStorage/OneDrive-Personal/Maestria_Udesa/Materias/Programacion_avanzada/TP_final_programacion/data/advertiser_ids"

file_name3 = "product_views"
file_which3 = "/Users/martin/Library/CloudStorage/OneDrive-Personal/Maestria_Udesa/Materias/Programacion_avanzada/TP_final_programacion/data/product_views"

# Cargar el archivo en S3
s3.upload_file(file_which1, bucket_name, file_name1)
s3.upload_file(file_which2, bucket_name, file_name2)
s3.upload_file(file_which3, bucket_name, file_name3)

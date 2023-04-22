import boto3
import pandas as pd
import os
import random
import string
from simulate_data import simulate_data

df_advertiser_ids, df_product_views, df_ads_views = simulate_data()

# Get environment variables
USER = os.getenv('ACCESS_KEY')
PASSWORD = os.environ.get('SECRET_KEY')

LOCAL_PATH = "../data/"

s3 = boto3.client("s3",
                  aws_access_key_id=USER,
                  aws_secret_access_key=PASSWORD)

bucket_name = "raw-ads-database-tp-programacion-avanzada"
s3_objects = ["ads_views.csv", "advertiser_ids.csv", "product_views.csv"]

for i in range(len(s3_objects)):

    s3.upload_file(f"{LOCAL_PATH}{s3_objects[i]}", bucket_name, s3_objects[i])

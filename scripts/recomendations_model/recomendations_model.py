import boto3
import pandas as pd
import os

# Get environment variables
USER = os.getenv('ACCESS_KEY')
PASSWORD = os.environ.get('SECRET_KEY')

s3 = boto3.client("s3",
                  aws_access_key_id=USER,
                  aws_secret_access_key=PASSWORD)

bucket_name = "raw-ads-database-tp-programacion-avanzada"
csv_key = 'advertiser_ids.csv'
csv_obj = s3.get_object(Bucket=bucket_name, Key=csv_key)


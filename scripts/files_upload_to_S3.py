import boto3
import pandas as pd
import os
from io import StringIO
from simulate_data import simulate_data

df_advertiser_ids, df_product_views, df_ads_views = simulate_data()
dfs = [df_advertiser_ids, df_product_views, df_ads_views]
s3_objects = ["ads_views.csv", "advertiser_ids.csv", "product_views.csv"]

# Get environment variables
USER = os.getenv('ACCESS_KEY')
PASSWORD = os.environ.get('SECRET_KEY')

s3 = boto3.client("s3",
                  aws_access_key_id=USER,
                  aws_secret_access_key=PASSWORD)

bucket_name = "raw-ads-database-tp-programacion-avanzada"

# Uploading files
for i in range(len(s3_objects)):
    with StringIO() as csv_buffer:

        dfs[i].to_csv(csv_buffer, index=False)

        response = s3.put_object(
            Bucket=bucket_name, Key=s3_objects[i], Body=csv_buffer.getvalue()
        )

        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 put_object response {s3_objects[i]}. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response {s3_objects[i]}. Status - {status}")

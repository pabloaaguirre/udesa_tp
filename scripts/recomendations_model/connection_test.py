import pandas as pd
import boto3
import os
from io import StringIO
import psycopg2

TEMP_DATA_PATH = os.getcwd() + "/temp_data/"

def load_filter_files(bucket_name: str, files_list: list):
    '''
    Reads all files from the database in S3 and filter active advertisers
    '''
    # S3 client
    s3 = boto3.client("s3")
    
    print("Connection ready")

    # Downloading files
    for i in range(len(files_list)):
        s3.download_file(Bucket=bucket_name,
                            Key=files_list[i],
                            Filename=f"{TEMP_DATA_PATH}{files_list[i]}")
    
    print("All files downloaded")

    # Files to DataFrames
    ads_views = pd.read_csv(f"{TEMP_DATA_PATH}{files_list[0]}")
    advertiser_ids = pd.read_csv(f"{TEMP_DATA_PATH}{files_list[1]}")
    product_views = pd.read_csv(f"{TEMP_DATA_PATH}{files_list[2]}")

    # Filtering active advertisers
    ads_views = ads_views.merge(right=advertiser_ids, how="inner", on="advertiser_id")
    product_views = product_views.merge(right=advertiser_ids, how="inner", on="advertiser_id")
    
    # Uploading files
    with StringIO() as csv_buffer:
        ads_views.to_csv(csv_buffer, index=False)
        response = s3.put_object(
            Bucket=bucket_name, Key="airflow/ads_views_filtered.csv",
            Body=csv_buffer.getvalue()
        )

    print("ad_views uploaded")
    
    with StringIO() as csv_buffer:
        product_views.to_csv(csv_buffer, index=False)
        response = s3.put_object(
            Bucket=bucket_name, Key=f"airflow/product_views_filtered.csv",
            Body=csv_buffer.getvalue()
        )

    print("product_views uploaded")


def rds_conn_test():
    engine = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="pepito123",
        host="udesa-database-1.codj3onk47ac.us-east-2.rds.amazonaws.com",
        port="5432")
    
    cursor = engine.cursor()

    cursor.execute("""SELECT * FROM recomendations;""")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

rds_conn_test()
#load_filter_files(bucket_name="raw-ads-database-tp-programacion-avanzada",
#            files_list=["ads_views.csv", "advertiser_ids.csv", "product_views.csv"])
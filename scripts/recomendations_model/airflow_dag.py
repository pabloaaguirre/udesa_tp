# Libraries
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import numpy as np
import pandas as pd
import boto3
import os
from io import StringIO
import psycopg2


TEMP_DATA_PATH = os.getcwd() + "/temp_data/"

with DAG(
    dag_id='recomendations_pipeline',
    schedule_interval=None,
    start_date=datetime.datetime(2022, 4, 1),
    catchup=False,
) as dag:

    def S3_conn():
        # Get environment variables
        USER = os.getenv('ACCESS_KEY')
        PASSWORD = os.environ.get('SECRET_KEY')
        print(os.getcwd())

        s3 = boto3.client("s3",
                        aws_access_key_id=USER,
                        aws_secret_access_key=PASSWORD)
        
        return s3
    
    def rds_conn():
        PASSWORD = os.environ.get('RDS_PASS')
        engine = psycopg2.connect(
            database="postgres",
            user="postgres",
            password=PASSWORD,
            host="udesa-database-1.codj3onk47ac.us-east-2.rds.amazonaws.com",
            port="5432"
        )

        return engine


    def clean_temp_data():
        '''
        Clean existing files in the temp_data folder
        '''
        for file_name in os.listdir(TEMP_DATA_PATH):
            # construct full file path
            file = TEMP_DATA_PATH + file_name
            print(file)
            if os.path.isfile(file):
                print('Deleting file:', file)
                os.remove(file)


    def load_filter_files(bucket_name: str, files_list: list):
        '''
        Reads all files from the database in S3 and filter active advertisers
        '''
        # S3 client
        s3 = S3_conn()

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
    

    def top_product(bucket_name: str):
        """
        Recomendation model based on most visited products of an advertiser
        """
        # S3 client
        s3 = S3_conn()

        # Downloading file
        s3.download_file(Bucket=bucket_name,
                        Key="airflow/product_views_filtered.csv",
                        Filename=f"{TEMP_DATA_PATH}product_views_filtered.csv")
        
        print("All files downloaded")

        # Files to DataFrames
        product_views = pd.read_csv(f"{TEMP_DATA_PATH}product_views_filtered.csv")

        # Top products model
        views_per_product = product_views.groupby(by=["advertiser_id", "product_id"], as_index=False).count()
        views_per_product.rename(columns={"date" : "product_views"}, inplace=True)
        
        max_viewed_product = views_per_product[["advertiser_id", "product_views"]].groupby("advertiser_id").max()
        max_viewed_product.rename(columns={"product_views" : "max_views"}, inplace=True)
        
        views_per_product = views_per_product.merge(right=max_viewed_product,
                                                    how="left",
                                                    on="advertiser_id")

        top_products = views_per_product[views_per_product["product_views"] == views_per_product["max_views"]]
        top_products = top_products.groupby(by=["advertiser_id", "product_id"]).head(1)[["advertiser_id", "product_id"]]
        
        engine = rds_conn()
        cursor = engine.cursor()
        cursor.execute(
            """
            CREATE TABLE [IF NOT EXISTS] recomendations (
                advertiser_id VARCHAR(255) PRIMARY KEY,
                product_id VARCHAR(255),
                model VARCHAR(255)
            );
            """
        )

        cursor.execute(
            """"
            INSERT INTO recomendations(advertiser_id, product_id, model)
            VALUES (advertiser123, product123,top_product);
            """
        )


    ## Tasks -----------------------------------------------------------------------------
    clean_temp_data = PythonOperator(
        task_id='clean_temp_data',
        python_callable=clean_temp_data
    )

    load_filter_files = PythonOperator(
        task_id='load_filter_files',
        python_callable=load_filter_files,
        op_kwargs={
            "bucket_name" : "raw-ads-database-tp-programacion-avanzada",
            "files_list" : ["ads_views.csv", "advertiser_ids.csv", "product_views.csv"]
        }
    )

    clean_temp_data >> load_filter_files

    top_product = PythonOperator(
        task_id="top_product",
        python_callable=top_product,
        op_kwargs={
            "bucket_name" : "raw-ads-database-tp-programacion-avanzada"
        }
    )
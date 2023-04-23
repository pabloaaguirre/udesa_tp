# Libraries
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
import numpy as np
import pandas as pd
import boto3
import os
from io import StringIO


AIRFLOW_PATH = "temp_data/"

with DAG(
    dag_id='recomendations_pipeline',
    schedule_interval=None,
    start_date=datetime.datetime(2022, 4, 1),
    catchup=False,
) as dag:

    def load_filter_files(bucket_name: str, files_list: list):
        '''
        Reads all files from the database in S3 and filter active advertisers
        '''
        
        for file_name in os.listdir(AIRFLOW_PATH):
            # construct full file path
            file = AIRFLOW_PATH + file_name
            if os.path.isfile(file):
                print('Deleting file:', file)
                os.remove(file)

        for i in range(len(files_list)):
            source_s3 = S3Hook("S3_default")
            source_s3.download_file(key=files_list[i],
                                    bucket_name=bucket_name,
                                    local_path=f"{AIRFLOW_PATH}",
                                    preserve_file_name=True,
                                    use_autogenerated_subdir=False)
        print("All files ready")

        ads_views = pd.read_csv(f"{AIRFLOW_PATH}{files_list[0]}")
        advertiser_ids = pd.read_csv(f"{AIRFLOW_PATH}{files_list[1]}")
        product_views = pd.read_csv(f"{AIRFLOW_PATH}{files_list[2]}")

        ads_views = ads_views.merge(right=advertiser_ids, how="inner", on="advertiser_id")
        product_views = product_views.merge(right=advertiser_ids, how="inner", on="advertiser_id")

        ads_views.to_csv(f"{AIRFLOW_PATH}ads_views_filtered.csv")
        product_views.to_csv(f"{AIRFLOW_PATH}product_views_filtered.csv")

        print("filtered files ready")


    load_filter_files = PythonOperator(
        task_id='load_filter_files',
        python_callable=load_filter_files,
        op_kwargs={
            "bucket_name" : "raw-ads-database-tp-programacion-avanzada",
            "files_list" : ["ads_views.csv", "advertiser_ids.csv", "product_views.csv"]
        }
    )

    ads_views_filtered_to_S3 = S3CreateObjectOperator(
        task_id="product_views_filtered_to_S3",
        s3_bucket="raw-ads-database-tp-programacion-avanzada",
        s3_key="airflow/ads_views_filtered.csv",
        data=f"{AIRFLOW_PATH}ads_views_filtered.csv",
        replace=True,
    )

    product_views_filtered_to_S3 = S3CreateObjectOperator(
        task_id="ads_views_filtered_to_S3",
        s3_bucket="raw-ads-database-tp-programacion-avanzada",
        s3_key="airflow/product_views_filtered.csv",
        data=f"{AIRFLOW_PATH}product_views_filtered.csv",
        replace=True,
    )

    load_filter_files >> ads_views_filtered_to_S3
    load_filter_files >> product_views_filtered_to_S3
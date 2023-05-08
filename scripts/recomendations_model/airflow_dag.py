# Libraries
import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
        s3 = boto3.client("s3")

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
        
        print("Dataframes filtered")

        ads_views.to_csv(f"{TEMP_DATA_PATH}ads_views_filtered.csv")
        product_views.to_csv(f"{TEMP_DATA_PATH}product_views_filtered.csv")

        print("Filtered files saved locally")

        # Uploading files
        s3.upload_file(Filename=f"{TEMP_DATA_PATH}ads_views_filtered.csv",
                    Bucket=bucket_name,
                    Key="airflow/ads_views_filtered.csv")

        print("ad_views uploaded")

        s3.upload_file(Filename=f"{TEMP_DATA_PATH}product_views_filtered.csv",
                        Bucket=bucket_name,
                        Key="airflow/product_views_filtered.csv")

        print("product_views uploaded")
    

    def top_product(bucket_name: str):
        """
        Recomendation model based on most visited products of an advertiser
        """
        # S3 client
        s3 = boto3.client("s3")

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
        top_products = top_products.groupby(by=["advertiser_id"]).head(1)
        
        top_products.to_csv(f"{TEMP_DATA_PATH}top_products.csv")

        print("Filtered files saved locally")

        # Uploading files
        s3.upload_file(Filename=f"{TEMP_DATA_PATH}top_products.csv",
                    Bucket=bucket_name,
                    Key="airflow/top_products.csv")

        print("top_products uploaded")


    def top_ctr(bucket_name: str):
        """
        Recomendation model based on products with maximum CTR metric per advertiser
        """
        # S3 client
        s3 = boto3.client("s3")

        # Downloading file
        s3.download_file(Bucket=bucket_name,
                        Key="airflow/ads_views_filtered.csv",
                        Filename=f"{TEMP_DATA_PATH}ads_views_filtered.csv")
        
        print("All files downloaded")

        # Files to DataFrames
        ads_views = pd.read_csv(f"{TEMP_DATA_PATH}ads_views_filtered.csv")

        # Top CTR calculation
        impressions = ads_views[ads_views.type == "impression"][['advertiser_id', 'product_id', 'type']]
        impressions = impressions.groupby(by=["advertiser_id", "product_id"], as_index=False).count()
        impressions.rename(columns={"type" : "impressions"}, inplace=True)

        clicks = ads_views[ads_views.type == "click"][['advertiser_id', 'product_id', 'type']]
        clicks = clicks.groupby(by=["advertiser_id", "product_id"], as_index=False).count()
        clicks.rename(columns={"type" : "clicks"}, inplace=True)

        ctr_data = impressions.merge(clicks, how="left", on=["advertiser_id", "product_id"]).fillna(0)
        ctr_data["CTR"] = ctr_data.clicks / ctr_data.impressions
        max_ctr = ctr_data.groupby(by=["advertiser_id"])["CTR"].max()
        ctr_data = ctr_data.merge(max_ctr, how="left", on="advertiser_id", suffixes=("","_max"))

        top_ctr = ctr_data[ctr_data.CTR == ctr_data.CTR_max]
        top_ctr = top_ctr.groupby("advertiser_id").head(1)
        
        top_ctr.to_csv(f"{TEMP_DATA_PATH}top_ctr.csv")

        print("Filtered files saved locally")

        # Uploading files
        s3.upload_file(Filename=f"{TEMP_DATA_PATH}top_ctr.csv",
                    Bucket=bucket_name,
                    Key="airflow/top_ctr.csv")

        print("top_ctr uploaded")


    def upload_to_database(bucket_name: str):
        '''
        Upload recomendations to RDS database
        '''
         # S3 client
        s3 = boto3.client("s3")

        # Downloading file
        s3.download_file(Bucket=bucket_name,
                        Key="airflow/top_products.csv",
                        Filename=f"{TEMP_DATA_PATH}top_products.csv")
        
        s3.download_file(Bucket=bucket_name,
                        Key="airflow/top_ctr.csv",
                        Filename=f"{TEMP_DATA_PATH}top_ctr.csv")
        
        # CSV to dataframes
        top_products = pd.read_csv(f"{TEMP_DATA_PATH}top_products.csv")
        top_ctr = pd.read_csv(f"{TEMP_DATA_PATH}top_ctr.csv")

        top_products["model"] = "top_products"
        top_ctr["model"] = "top_ctr"

        recomendations = pd.concat([top_products, top_ctr])

        # RDS Connection
        engine = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="pepito123",
            host="udesa-database-1.codj3onk47ac.us-east-2.rds.amazonaws.com",
            port="5432")
        
        cursor = engine.cursor()

        # Create table
        cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS recomendations (
                    advertiser_id VARCHAR(255) PRIMARY KEY,
                    product_id VARCHAR(255),
                    model VARCHAR(255)
                );
                """
            )
        
        # Inserting values
        for i in range(len(recomendations)):

            adv_id = recomendations.advertiser_id.iloc[i]
            prod_id = recomendations.product_id.iloc[i]
            model = recomendations.model.iloc[i]

            cursor.execute(
                f"""
                INSERT INTO recomendations(advertiser_id, product_id, model)
                VALUES ('{adv_id}', '{prod_id}', '{model}');
                """
            )
        
        engine.commit()
        cursor.close()


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

    load_filter_files >> top_product

    top_ctr = PythonOperator(
        task_id="top_ctr",
        python_callable=top_ctr,
        op_kwargs={
            "bucket_name" : "raw-ads-database-tp-programacion-avanzada"
        }
    )

    load_filter_files >> top_ctr

    upload_to_database = PythonOperator(
        task_id="upload_to_database",
        python_callable=upload_to_database,
        op_kwargs={
            "bucket_name" : "raw-ads-database-tp-programacion-avanzada"
        }
    )

    top_product >> upload_to_database
    top_ctr >> upload_to_database
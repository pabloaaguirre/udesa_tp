# Libraries
from datetime import datetime, date, timedelta
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
    schedule_interval= '0 3 * * *',
    start_date=datetime(2023, 5, 5),
    catchup=True,
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
    

    def top_product(bucket_name: str, ds):
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
        
        # Last 30 days filter
        product_views["date"] = pd.to_datetime(product_views["date"]).dt.date
        product_views = product_views[(product_views.date <= date.today()) &
          (product_views.date >= date.today() - timedelta(days=30))]

        # Top products model
        views_per_product = product_views.groupby(by=["advertiser_id", "product_id"], as_index=False).count()
        views_per_product.rename(columns={"date" : "product_views"}, inplace=True)
        views_per_product['ranking'] = views_per_product.sort_values(['advertiser_id','product_views'], ascending=False) \
                    .groupby(['advertiser_id']) \
                    .cumcount() + 1

        top_products = views_per_product[views_per_product.ranking <= 20]
        top_products = top_products[['advertiser_id','product_id','product_views','ranking']]
        print(ds)
        top_products["date"] = ds
        top_products.to_csv(f"{TEMP_DATA_PATH}top_products.csv")

        print("Filtered files saved locally")

        # Uploading files
        s3.upload_file(Filename=f"{TEMP_DATA_PATH}top_products.csv",
                    Bucket=bucket_name,
                    Key="airflow/top_products.csv")

        print("top_products uploaded")

    def top_ctr(bucket_name: str, ds):
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
        
        # Last 30 days filter
        ads_views["date"] = pd.to_datetime(ads_views["date"]).dt.date
        ads_views = ads_views[(ads_views.date <= date.today()) &
          (ads_views.date >= date.today() - timedelta(days=30))]
        
        # Top CTR calculation
        impressions = ads_views[ads_views.type == "impression"][['advertiser_id', 'product_id', 'type']]
        impressions = impressions.groupby(by=["advertiser_id", "product_id"], as_index=False).count()
        impressions.rename(columns={"type" : "impressions"}, inplace=True)

        clicks = ads_views[ads_views.type == "click"][['advertiser_id', 'product_id', 'type']]
        clicks = clicks.groupby(by=["advertiser_id", "product_id"], as_index=False).count()
        clicks.rename(columns={"type" : "clicks"}, inplace=True)

        ctr_data = impressions.merge(clicks, how="left", on=["advertiser_id", "product_id"]).fillna(0)
        ctr_data["CTR"] = ctr_data.clicks / ctr_data.impressions
        ctr_data['ranking'] = ctr_data.sort_values(['advertiser_id','CTR'], ascending=False) \
             .groupby(['advertiser_id']) \
             .cumcount() + 1

        top_ctr = ctr_data[ctr_data.ranking <= 20]
        print(ds)
        top_ctr["date"] = ds
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

        # Create recomedations table if not exists
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS recomendations (
                date DATE,
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                ranking INTEGER,
                model VARCHAR(255)
            );
            """
        )

        # To upload temp recomendations, we create stating_recomendations table
        cursor.execute(
            """
            DROP TABLE IF EXISTS staging_recomendations;
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_recomendations (
                date DATE,
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                ranking INTEGER,
                model VARCHAR(255)
            );
            """
        )

        # Inserting values into staging temp table
        for i in range(len(recomendations)):

            date_id = recomendations.date.iloc[i]
            adv_id = recomendations.advertiser_id.iloc[i]
            prod_id = recomendations.product_id.iloc[i]
            ranking = recomendations.ranking.iloc[i]
            model = recomendations.model.iloc[i]

            cursor.execute(
                f"""
                INSERT INTO staging_recomendations(date, advertiser_id, product_id, ranking, model)
                VALUES (DATE('{date_id}'), '{adv_id}', '{prod_id}', '{ranking}', '{model}');
                """
            )

        # Delete current date from the recomendations table
        cursor.execute(
            '''
            DELETE FROM recomendations AS a
            USING staging_recomendations AS b
            WHERE a.date = b.date
            ;
            '''
        )

        # Insert new rows to recomendations table from staging
        cursor.execute(
            """
            INSERT INTO recomendations
            SELECT
                a.date,
                a.advertiser_id,
                a.product_id,
                a.ranking,
                a.model
            FROM staging_recomendations AS a
                LEFT JOIN recomendations AS b
                    ON a.date = b.date
                    AND a.advertiser_id = b.advertiser_id
                    AND a.product_id = b.product_id
                    AND a.model = b.model
            WHERE b.date IS NULL
            ;
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
        },
        provide_context=True
    )

    load_filter_files >> top_product

    top_ctr = PythonOperator(
        task_id="top_ctr",
        python_callable=top_ctr,
        op_kwargs={
            "bucket_name" : "raw-ads-database-tp-programacion-avanzada"
        },
        provide_context=True 
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
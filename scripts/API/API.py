from fastapi import FastAPI, Path, Query
import psycopg2
import pandas as pd
import numpy as np
import json

app = FastAPI()

@app.get("/recommendations/{advertiser_id}/{model}")
def get_recommendation(advertiser_id: str, model: str):
    '''
    Returns the latest recommendation of the top 20 products for a given advertiser_id
    Params:
    advertiser_id: str, unique ID for the advertiser
    model: str. Possible values ["top_products", "top_ctr"]. It specifies the 
        recommendation model
    '''
    # Connect to the database
    print("start conn")

    conn = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="pepito123",
            host="localhost",
            port="5432")

    print("conn success")

    # Run database query
    sql_statement = f"""
        SELECT
            *
        FROM recomendations
        WHERE date = (SELECT MAX(date) FROM recomendations)
            AND advertiser_id = '{advertiser_id}'
            AND model = '{model}'
    """

    df = pd.read_sql(sql=sql_statement, con=conn, parse_dates=['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    results = json.loads(df[["product_id", "ranking"]].to_json(orient='records'))
    
    return results


@app.get("/stats")
def get_stats():
    '''
    The stats endpoint returns the following statistics:
    total_advertisers: the total number of unique advertisers in the database
    available_dates: all the available dates in the database with recommendations
    product_overlapping: the quantity of products recommended by both models for 
    each advertiser_id and date.
    '''
    # Connect to the database
    print("start conn")

    conn = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="pepito123",
            host="localhost",
            port="5432")

    print("conn success")

    # Run database queries
    # Advertisers count
    advertisers_count_sql = f"""
        SELECT
            COUNT(DISTINCT advertiser_id) AS total_advertisers
        FROM recomendations
        ;
    """
    advertisers_count_df = pd.read_sql(sql=advertisers_count_sql, con=conn, parse_dates=['date'])

    # Available dates
    available_dates_sql = f"""
        SELECT
            DISTINCT date
        FROM recomendations
        ;
    """
    available_dates_df = pd.read_sql(sql=available_dates_sql, con=conn, parse_dates=['date'])
    available_dates_df['date'] = available_dates_df['date'].dt.strftime('%Y-%m-%d')

    # Product recommendation overlapping bewtween models
    # of the las 7 days
    product_overlapping_sql = f"""
        WITH top_products AS (
                SELECT
                    date,
                    advertiser_id,
                    product_id
                FROM recomendations AS a
                WHERE model = 'top_products'
                    AND date >= (SELECT max(date) FROM recomendations) - INTERVAL '7 days'
            ),
            top_ctr AS (
                SELECT
                    date,
                    advertiser_id,
                    product_id
                FROM recomendations AS a
                WHERE model = 'top_ctr'
                    AND date >= (SELECT max(date) FROM recomendations) - INTERVAL '7 days'
            )
        SELECT
            a.date,
            a.advertiser_id,
            COUNT(*) AS qty_product_overlapping
        FROM top_products AS a
            INNER JOIN top_ctr AS b
                ON a.advertiser_id = b.advertiser_id
                AND a.product_id = b.product_id
                AND a.date = b.date
        GROUP BY 1, 2
        ;
    """
    product_overlapping_df = pd.read_sql(sql=product_overlapping_sql, con=conn, parse_dates=['date'])
    product_overlapping_df['date'] = product_overlapping_df['date'].dt.strftime('%Y-%m-%d')
    product_overlapping_json = json.loads(product_overlapping_df.to_json(orient="records"))
    
    results = {
        "total_advertisers" : advertisers_count_df.total_advertisers.tolist()[0],
        "available_dates" : available_dates_df.date.tolist(),
        "product_overlapping" : product_overlapping_json
    }
    
    return results


@app.get("/history/{advertiser_id}/")
def get_history(advertiser_id: str):
    '''
    Returns the history of recommendations for a given advertiser_id
    of the last 7 days.
    Params:
    advertiser_id: str, unique ID for the advertiser
    model: str. Possible values ["top_products", "top_ctr"]. It specifies the recommendation model
    '''
    # Connect to the database
    print("start conn")

    conn = psycopg2.connect(
            database="postgres",
            user="postgres",
            password="pepito123",
            host="localhost",
            port="5432")

    print("conn success")

    # Run database query
    sql_statement = f"""
        SELECT
            *
        FROM recomendations
        WHERE date >= (SELECT MAX(date) FROM recomendations) - INTERVAL '7 days'
            AND advertiser_id = '{advertiser_id}'
    """

    df = pd.read_sql(sql=sql_statement, con=conn, parse_dates=['date'])
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    results = json.loads(df[["date", "product_id", "ranking"]].to_json(orient='records'))
    
    return results


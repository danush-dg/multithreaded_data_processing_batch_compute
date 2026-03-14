
import pandas as pd

import psycopg2
import boto3
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def get_secret():
    secret_name = "redshift-credentials"
    region = "ap-south-1"

    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)

    return json.loads(response["SecretString"])

def connect_redshift():
    creds = get_secret()

    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        database=creds["dbname"],
        user=creds["user"],
        password=creds["password"]
    )

    logger.info("Connected to Redshift Serverless")
    return conn

def fetch_data():
    conn = connect_redshift()

    query = "SELECT * FROM employees"
    df = pd.read_sql(query, conn)

    conn.close()

    logger.info(f"Rows fetched: {len(df)}")
    return df

if __name__ == "__main__":
    data = fetch_data()
    print(data.head(50))
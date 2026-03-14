import pandas as pd
import psycopg2
import boto3
import json
import logging
from config import SECRET_NAME, REGION

logger = logging.getLogger(__name__)


def get_secret():

    client = boto3.client("secretsmanager", region_name=REGION)

    response = client.get_secret_value(
        SecretId=SECRET_NAME
    )

    return json.loads(response["SecretString"])


def connect_redshift():

    creds = get_secret()

    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        database=creds["database"],
        user=creds["username"],
        password=creds["password"],
        connect_timeout=10
    )

    logger.info("Connected to Redshift")

    return conn


def fetch_data():

    conn = connect_redshift()

    query = "SELECT * FROM employees"

    df = pd.read_sql(query, conn)

    conn.close()

    logger.info(f"Rows fetched: {len(df)}")

    return df
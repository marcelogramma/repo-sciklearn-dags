import requests
import json
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
import config as config
from datetime import datetime
from pathlib import Path
import sqlalchemy.exc
import psycopg2
from sqlalchemy import create_engine
from datetime import date
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.dates import WeekdayLocator, DateFormatter, MonthLocator
import matplotlib.ticker as ticker
from matplotlib.ticker import MultipleLocator, FormatStrFormatter, AutoMinorLocator
import numpy as np
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import boto3
import botocore

# credentials
bucket_name = 'ml-dataset-raw-s3'
bucket_key = '2009.csv'
database_name = f"{config.DB_NAME}"
table_name = f"{config.TBL_NAME}"
aws_s3_access_key_id = BaseHook.get_connection('aws_s3_access_key_id').password
aws_s3_secret_access_key = BaseHook.get_connection('aws_s3_secret_access_key').password


def to_postgres():
    print(f"Getting data from {bucket_name }...")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_s3_access_key_id,
        aws_secret_access_key=aws_s3_secret_access_key,
    )
    s3_client.download_file(bucket_name, bucket_key, '2009.csv')
    print(f"Data downloaded from {bucket_name}...")
    print(f"Inserting data into {database_name}...")
    conn = f"{config.engine}"
    cur = conn.cursor()
    cur.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (fl_date date, op_carrier text, op_carrier_fl_num float, origin text, dest text, crs_dep_time float, dep_time float, dep_delay float, taxi_out float, wheels_off float, wheels_on float, taxi_in float, crs_air_time float, arr_time float, arr_delay float, cancelled float, cancellation_code float, diverted float, crs_elapsed_time float, actual_elapsed_time float, air_time float, distance float, carrier_delay float, wheater_delay float, nas_delay float, security_delay float, late_aircraft_delay float, unnamed float;"
    )
    conn.commit()
    with open("2009.csv", "r") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            cur.execute(
                f"INSERT INTO {table_name} VALUES ({row[0]}, {row[1]}, {row[2]}, {row[3]}, {row[4]}, {row[5]}, {row[6]}, {row[7]}, {row[8]}, {row[9]}, {row[10]}, {row[11]}, {row[12]}, {row[13]}, {row[14]}, {row[15]}, {row[16]}, {row[17]}, {row[18]}, {row[19]}, {row[20]}, {row[21]}, {row[22]}, {row[23]}, {row[24]}, {row[25]}, {row[26]}, {row[27]}, {row[28]});"
            )
            conn.commit()
    conn.close()
    print(f"Data inserted into {database_name}...")

DAG_DEFAULT_ARGS = {'owner': 'MG', 'depends_on_past': False, 'start_date': datetime.utcnow(), 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    dag_id='dagextrationdata',
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval='@once',
    catchup=False,
) as dag:

    s3_key_sensor = S3KeySensor(
        task_id='s3_key_sensor',
        bucket_key=bucket_key,
        bucket_name=bucket_name,
        poke_interval=10,
        timeout=60,
        soft_fail=False,
    )

    postgres_operator = PostgresOperator(
        task_id='postgres_operator',
        postgres_conn_id='postgres_default',
        sql=f"SELECT * FROM {table_name};",
    )

    s3_key_sensor >> postgres_operator
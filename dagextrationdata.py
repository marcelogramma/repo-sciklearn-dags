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

# credentials
bucket_name = 'ml-dataset-raw-s3'
bucket_key = '2009.csv'
database_name = f"{config.DB_NAME}"
table_name = f"{config.TBL_NAME}"
aws_s3_access_key_id = BaseHook.get_connection('aws_s3_access_key_id').password
aws_s3_secret_access_key = BaseHook.get_connection('aws_s3_secret_access_key').password

def to_postgres():
    print(f"Getting data from {bucket_name }...")

    con = f"{config.engine}" 
    cs = con.cursor()
    cs.execute('USE DATABASE %s;' % database_name)
 
    copy = (
    "COPY into %s"
    " from s3://%s/%s"
    " credentials = (aws_key_id = '%s' aws_secret_key = '%s')"
    " file_format = (type = csv field_delimiter = ','"
    " field_optionally_enclosed_by = '\"'"
    " skip_header = 1)"
    " on_error = 'continue';"
    % (table_name, bucket_name, bucket_key, aws_s3_access_key_id, aws_s3_secret_access_key)
    )
 
    cs.execute(copy)
    cs.close()

DAG_DEFAULT_ARGS = {'owner': 'MG', 'retries': 0}

with DAG(
    "extract_froms3_to_postgres",
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup = False) as dag:

    from_s3 = S3KeySensor(task_id = 'from_s3_task',
    poke_interval = 60 * 30,
    timeout = 60 * 60 * 12,
    bucket_key = "s3://%s/%s" % (bucket_name, bucket_key),
    bucket_name = None,
    wildcard_match = False,
    dag = dag
    )
    upload_files = PythonOperator(task_id = "upload_to_postgres_task",
    python_callable = to_postgres,
    dag = dag
    )

from_s3 >> upload_files
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
import awswrangler as wr


def extract_load_data():
    print(f"Getting data from {config.BUCKET_RAW}...")
    raw_path = f"s3://{config.BUCKET_RAW}/"
    raw_df = wr.s3.read_csv(path=raw_path)
    print (raw_df)
    result = pd.DataFrame[raw_df]

    print(result)

    con = config.engine
    create_table = con.execute(
        f"CREATE TABLE IF NOT EXISTS {config.TBL_NAME} (id BIGSERIAL PRIMARY KEY, fl_date date, op_carrier text, op_carrier_fl_num float, origin text, dest text, crs_dep_time float, dep_time float, dep_delay float, taxi_out float, wheels_off float, wheels_on float, taxi_in float, crs_air_time float, arr_time float, arr_delay float, cancelled float, cancellation_code float, diverted float, crs_elapsed_time float, actual_elapsed_time float, air_time float, distance float, carrier_delay float, wheater_delay float, nas_delay float, security_delay float, late_aircraft_delay float, unnamed float)"
    )
    create_table.close()
    
    print(f"Data inserted into {config.DB_NAME}...")

DAG_DEFAULT_ARGS = {'owner': 'MG', 'depends_on_past': False, 'start_date': datetime.utcnow(), 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    "extract_load_data",
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval="0 3 * * *",
    catchup = False) as dag:

    from_s3_to_postgres = PythonOperator(task_id="extract_load_data", python_callable=extract_load_data,
    dag = dag
    )

from_s3_to_postgres
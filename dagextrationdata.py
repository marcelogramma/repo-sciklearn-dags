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
    result = list(zip(raw_df['fl_date'], raw_df['op_carrier'], raw_df['op_carrier_fl_num'], raw_df['op_carrier_fl_num'], raw_df['origin'], raw_df['dest'], raw_df['crs_dep_time'], raw_df['dep_time'], raw_df['dep_delay'], raw_df['taxi_out'], raw_df['wheels_off'], raw_df['wheels_on'], raw_df['taxi_in'], raw_df['crs_air_time'], raw_df['arr_time'], raw_df['arr_delay'], raw_df['cancelled'], raw_df['cancellation_code'], raw_df['diverted'], raw_df['crs_elapsed_time'], raw_df['actual_elapsed_time'], raw_df['air_time'], raw_df['distance'], raw_df['carrier_delay'], raw_df['wheater_delay'], raw_df['nas_delay'], raw_df['security_delay'], raw_df['late_aircraft_delay'], raw_df['unnamed']))
#    result = pd.DataFrame(result, columns=['fl_date', 'op_carrier', 'op_carrier_fl_num', 'origin', 'dest', 'crs_dep_time', 'dep_time', 'dep_delay', 'taxi_out', 'wheels_off', 'wheels_on', 'taxi_in', 'crs_air_time', 'arr_time', 'arr_delay', 'cancelled', 'cancellation_code', 'diverted', 'crs_elapsed_time', 'actual_elapsed_time', 'air_time', 'distance', 'carrier_delay', 'wheater_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay', 'unnamed'])
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
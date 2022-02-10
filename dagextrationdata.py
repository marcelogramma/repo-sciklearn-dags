from datetime import datetime, timedelta
from airflow import DAG
import config as config
from datetime import date
from airflow.models import DAG
from airflow.utils.dates import days_ago
import awswrangler as wr
from airflow.operators.python_operator import PythonOperator

######################################################################
#
#                     Extration Data from S3 2009
######################################################################

def extract_load_data2009():
    print(f"Getting data from {config.BUCKET_RAW}...")
    raw_path2009 = f"s3://{config.BUCKET_RAW}/raw/2009.csv"
    raw_df2009 = wr.s3.read_csv(path=raw_path2009)
    print (raw_df2009)
    
    print(f"Writing data to {config.DB_NAME}...")
    
######################################################################
#                          Tranformation data 2009
#     calcular promedio del tiempo de salida por dia por aeropuerto
######################################################################
#    raw_ave_delay = raw_df['DEP_DELAY'], raw_df['ORIGIN'], raw_df['FL_DATE']
    raw_ave_delay2009 = raw_df2009.groupby(['ORIGIN', 'FL_DATE'])['DEP_DELAY'].mean()
    print(f"The average delay by airport for day is")
    print(raw_ave_delay)

######################################################################
#              Load data to Postgres 2009
#               insersion en la DB
######################################################################
    raw_ave_delay2009.to_sql(
        name=config.TBL_NAME,
        con=config.engine,
        schema="public",
        if_exists="replace",
        index=True,
    )
    print(f"Data written to {config.DB_NAME}")

######################################################################
#
#                     Extration Data from S3 2010
######################################################################

def extract_load_data2010():
    print(f"Getting data from {config.BUCKET_RAW}...")
    raw_path2010 = f"s3://{config.BUCKET_RAW}/raw/2010.csv"
    raw_df2010 = wr.s3.read_csv(path=raw_path2010)
    print (raw_df2010)
    
    print(f"Writing data to {config.DB_NAME}...")
    
######################################################################
#                          Tranformation data 2010
#     calcular promedio del tiempo de salida por dia por aeropuerto
######################################################################
#    raw_ave_delay = raw_df['DEP_DELAY'], raw_df['ORIGIN'], raw_df['FL_DATE']
    raw_ave_delay2010 = raw_df2010.groupby(['ORIGIN', 'FL_DATE'])['DEP_DELAY'].mean()
    print(f"The average delay by airport for day is")
    print(raw_ave_delay)

######################################################################
#              Load data to Postgres 2010
#               insersion en la DB
######################################################################
    raw_ave_delay2010.to_sql(
        name=config.TBL_NAME,
        con=config.engine,
        schema="public",
        if_exists="replace",
        index=True,
    )
    print(f"Data written to {config.DB_NAME}")

DAG_DEFAULT_ARGS = {'owner': 'MG', 'depends_on_past': False, 'start_date': datetime.utcnow(), 'retries': 1, 'retry_delay': timedelta(minutes=5)}

with DAG(
    "extract_load_data",
    default_args=DAG_DEFAULT_ARGS,
    schedule_interval="@yearly",
    catchup = False) as dag:

    from_s3_to_postgres = PythonOperator(task_id="extract_load_data", python_callable=extract_load_data,
    dag = dag
    )

from_s3_to_postgres

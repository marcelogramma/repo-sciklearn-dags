from datetime import datetime
from pathlib import Path
import json
from time import sleep
import requests  # type: ignore
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
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import configinc as config


def _data_amzn(**context):
    execution_date = context.get("ds")
    end_point = f"{config.end_point_amzn_dag}"
    print(f"Getting data from {end_point}...")
    r = requests.get(end_point)
    data_amzn = r.json()

    with open(f"data/{execution_date}-{config.SYMBOL_AMZN}.json", "w") as f:
        json.dump(data_amzn, f)

    with open(f"data/{execution_date}-{config.SYMBOL_AMZN}.json", "r") as f:
        read_json_amzn = json.load(f)
        task_amzn = config.engine.execute(
            """
            INSERT INTO prices_from_agm (date, Symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                read_json_amzn["Meta Data"]["3. Last Refreshed"],
                read_json_amzn["Meta Data"]["2. Symbol"],
                read_json_amzn["Time Series (Daily)"][
                    read_json_amzn["Meta Data"]["3. Last Refreshed"]
                ]["1. open"],
                read_json_amzn["Time Series (Daily)"][
                    read_json_amzn["Meta Data"]["3. Last Refreshed"]
                ]["2. high"],
                read_json_amzn["Time Series (Daily)"][
                    read_json_amzn["Meta Data"]["3. Last Refreshed"]
                ]["3. low"],
                read_json_amzn["Time Series (Daily)"][
                    read_json_amzn["Meta Data"]["3. Last Refreshed"]
                ]["4. close"],
                read_json_amzn["Time Series (Daily)"][
                    read_json_amzn["Meta Data"]["3. Last Refreshed"]
                ]["5. volume"],
            ),
        )
        task_amzn.close()


def _data_goog(**context):
    execution_date = context.get("ds")
    end_point = f"{config.end_point_goog_dag}"
    print(f"Getting data from {end_point}...")
    r = requests.get(end_point)
    data_goog = r.json()

    with open(f"data/{execution_date}-{config.SYMBOL_GOOG}.json", "w") as f:
        json.dump(data_goog, f)

    with open(f"data/{execution_date}-{config.SYMBOL_GOOG}.json", "r") as f:
        read_json_goog = json.load(f)
        task_goog = config.engine.execute(
            """
            INSERT INTO prices_from_agm (date, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                read_json_goog["Meta Data"]["3. Last Refreshed"],
                read_json_goog["Meta Data"]["2. Symbol"],
                read_json_goog["Time Series (Daily)"][
                    read_json_goog["Meta Data"]["3. Last Refreshed"]
                ]["1. open"],
                read_json_goog["Time Series (Daily)"][
                    read_json_goog["Meta Data"]["3. Last Refreshed"]
                ]["2. high"],
                read_json_goog["Time Series (Daily)"][
                    read_json_goog["Meta Data"]["3. Last Refreshed"]
                ]["3. low"],
                read_json_goog["Time Series (Daily)"][
                    read_json_goog["Meta Data"]["3. Last Refreshed"]
                ]["4. close"],
                read_json_goog["Time Series (Daily)"][
                    read_json_goog["Meta Data"]["3. Last Refreshed"]
                ]["5. volume"],
            ),
        )
        task_goog.close()


def _data_msft(**context):
    execution_date = context.get("ds")
    end_point = f"{config.end_point_msft_dag}"
    print(f"Getting data from {end_point}...")
    r = requests.get(end_point)
    data_msft = r.json()

    with open(f"data/{execution_date}-{config.SYMBOL_MSFT}.json", "w") as f:
        json.dump(data_msft, f)

    with open(f"data/{execution_date}-{config.SYMBOL_MSFT}.json", "r") as f:
        read_json_msft = json.load(f)
        task_msft = config.engine.execute(
            """
            INSERT INTO prices_from_agm (date, symbol, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                read_json_msft["Meta Data"]["3. Last Refreshed"],
                read_json_msft["Meta Data"]["2. Symbol"],
                read_json_msft["Time Series (Daily)"][
                    read_json_msft["Meta Data"]["3. Last Refreshed"]
                ]["1. open"],
                read_json_msft["Time Series (Daily)"][
                    read_json_msft["Meta Data"]["3. Last Refreshed"]
                ]["2. high"],
                read_json_msft["Time Series (Daily)"][
                    read_json_msft["Meta Data"]["3. Last Refreshed"]
                ]["3. low"],
                read_json_msft["Time Series (Daily)"][
                    read_json_msft["Meta Data"]["3. Last Refreshed"]
                ]["4. close"],
                read_json_msft["Time Series (Daily)"][
                    read_json_msft["Meta Data"]["3. Last Refreshed"]
                ]["5. volume"],
            ),
        )
        task_msft.close()


def _query_for_plots(**context):
    df_amzn = pd.read_sql_query(
        """
        SELECT * FROM prices_from_agm WHERE extract('ISODOW' FROM date) < 6 
        and date >= CURRENT_DATE - 9 AND symbol = 'AMZN'
        """,
        config.engine,
    )
    df_goog = pd.read_sql_query(
        """
        SELECT * FROM prices_from_agm WHERE extract('ISODOW' FROM date) < 6 
        and date >= CURRENT_DATE - 9 AND symbol = 'GOOG'
        """,
        config.engine,
    )
    df_msft = pd.read_sql_query(
        """
        SELECT * FROM prices_from_agm WHERE extract('ISODOW' FROM date) < 6 
        and date >= CURRENT_DATE - 9 AND symbol = 'MSFT'
        """,
        config.engine,
    )
    days = mdates.DayLocator()
    timeFmt = mdates.DateFormatter("%Y-%m-%d %a")
    fig, ax = plt.subplots(figsize=(20, 10))
    df_amzn.plot(x="date", y="volume", ax=ax, label="Amazon", marker="o")
    df_goog.plot(x="date", y="volume", ax=ax, label="Google", marker="o")
    df_msft.plot(x="date", y="volume", ax=ax, label="Microsoft", marker="o")
    ax.legend()
    ax.set_title(f"Daily volume for Last Week - Only business days")
    ax.set_xlabel("Days")
    ax.set_ylabel("Volume x 10M")
    ax.xaxis.set_major_locator(days)
    ax.xaxis.set_major_formatter(timeFmt)
    fig.savefig("/opt/airflow/data/plots/volume.png")

    fig, ax = plt.subplots(figsize=(20, 10))
    df_amzn.plot(x="date", y="open", ax=ax, label="Amazon", marker="o")
    df_goog.plot(x="date", y="open", ax=ax, label="Google", marker="o")
    df_msft.plot(x="date", y="open", ax=ax, label="Microsoft", marker="o")
    ax.legend()
    ax.set_title(f"Daily open for Last Week - Only business days")
    ax.set_xlabel("Days")
    ax.set_ylabel("Open")
    ax.xaxis.set_major_locator(days)
    ax.xaxis.set_major_formatter(timeFmt)
    fig.savefig("/opt/airflow/data/plots/open.png")

    fig, ax = plt.subplots(figsize=(20, 10))
    df_amzn.plot(x="date", y="close", ax=ax, label="Amazon", marker="o")
    df_goog.plot(x="date", y="close", ax=ax, label="Google", marker="o")
    df_msft.plot(x="date", y="close", ax=ax, label="Microsoft", marker="o")
    ax.legend()
    ax.set_title(f"Daily close for Last Week - Only business days")
    ax.set_xlabel("Days")
    ax.set_ylabel("Close")
    ax.xaxis.set_major_locator(days)
    ax.xaxis.set_major_formatter(timeFmt)
    fig.savefig("/opt/airflow/data/plots/close.png")


default_args = {"owner": "MG", "retries": 0}
with DAG(
    "prices_from_AGM",
    default_args=default_args,
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
) as dag:
    all_data_amzn = PythonOperator(task_id="data_amzn", python_callable=_data_amzn)
    all_data_goog = PythonOperator(task_id="data_goog", python_callable=_data_goog)
    all_data_msft = PythonOperator(task_id="data_msft", python_callable=_data_msft)
    all_query_for_plots = PythonOperator(
        task_id="query_for_plots", python_callable=_query_for_plots
    )
    all_data_amzn >> all_data_goog >> all_data_msft >> [all_query_for_plots]

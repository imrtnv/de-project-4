import time
import requests
import json
import pandas as pd
import sqlalchemy
import pendulum
import psycopg2

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

http_conn_id = HttpHook.get_connection('add_conn')
api_key = http_conn_id.extra_dejson.get('X-API-KEY')


postgres_hook_export = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
conn = postgres_hook_export.get_conn()
cur = conn.cursor()

header={
    "X-API-KEY": api_key,
    "X-Nickname": "imartnv",
    "X-Cohort": "2"
    }

def upload_stg_restoraunt(pg_table_load):
    response_restaurants = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?',
    headers=header)
    json_restaurants = json.loads(response_restaurants.content)
    columns = json_restaurants[0].keys() 
    columns_str = ", ".join(columns)
    values = [[value for value in row.values()] for row in json_restaurants]
    query = f"truncate table {pg_table_load}; insert into {pg_table_load} ({columns_str}) values (%s,%s)"
    cur.executemany(query, values)
    conn.commit()


def upload_stg_couriers(pg_table_load):

    cur.execute(f"truncate table {pg_table_load};")
    i=0
    for j in range(300):
        response_couriers = requests.get(f'''https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?
        limit=50&offset={i}''', # точка входа
        headers=header)
        json_couriers = json.loads(response_couriers.content)
        if len(json_couriers)==0:
            break
        else:
            columns = json_couriers[0].keys()
            columns_str = ", ".join(columns)
            values = [[value for value in row.values()] for row in json_couriers]
            i+=50
            query = f"insert into {pg_table_load} ({columns_str}) values (%s,%s)"
            try:
                cur.executemany(query, values)
                conn.commit() 
            except:
                conn.rollback()


def upload_stg_deliveres(pg_table_load):

    for j in range(1000):
        cur.execute(f"select max(delivery_ts)::text from {pg_table_load}")
        max_date = cur.fetchall()
        max_date = str(max_date)[3:22]
        if max_date == '':
            max_date = '2022-06-01 00:00:00'
        response_deliveries = requests.get(f'''https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?
        limit=50&offset=0&sort_field=order_id&from={max_date}''', # точка входа
        headers=header)
        json_deliveries = json.loads(response_deliveries.content)
        if len(json_deliveries)==0:
            break
        else:
            columns = json_deliveries[0].keys()
            columns_str = ", ".join(columns)
            values = [[value for value in row.values()] for row in json_deliveries]
            query = f"insert into {pg_table_load} ({columns_str}) values (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            try:
                cur.executemany(query, values)
                conn.commit() 
            except:
                conn.rollback()

args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}


with DAG(
        'etl_couriers_dag',
        default_args=args,
        catchup=False,
        start_date=pendulum.datetime(2022, 7, 1, tz="UTC"),
        schedule_interval='* 1 * * *',
    	tags=['sprint5', 'project'],
    	is_paused_upon_creation=False
) as dag:

    upload_stg_restoraunt_task = PythonOperator(
        task_id='upload_stg_restoraunt_task',
        python_callable=upload_stg_restoraunt,
        op_kwargs={'pg_table_load': 'stg.restaurants'})

    upload_stg_couriers_task = PythonOperator(
        task_id='upload_stg_couriers_task',
        python_callable=upload_stg_couriers,
        op_kwargs={'pg_table_load': 'stg.couriers'})

    upload_stg_deliveres_task = PythonOperator(
        task_id='upload_stg_deliveres_task',
        python_callable=upload_stg_deliveres,
        op_kwargs={'pg_table_load': 'stg.deliveries'})


    upload_dds_courier_taks = PostgresOperator(
        task_id = 'upload_dds_courier_taks',
        sql = "dds.dm_courier.sql",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_restaurants_taks = PostgresOperator(
        task_id = 'upload_dds_restaurants_taks',
        sql = "dds.dm_restaurants.sql",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_timestamps_taks = PostgresOperator(
        task_id = 'upload_dds_timestamps_taks',
        sql = "dds.dm_timestamps.sql",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )


    upload_dds_delivery_taks = PostgresOperator(
        task_id = 'upload_dds_delivery_taks',
        sql = "dds.dm_delivery.sql",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_orders_taks = PostgresOperator(
        task_id = 'upload_dds_orders_taks',
        sql = "dds.dm_orders.sql",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_fct_sales_taks = PostgresOperator(
        task_id = 'upload_dds_fct_sales_taks',
        sql = "dds.fct_sales.sql",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_cdm_main_report_taks = PostgresOperator(
        task_id = 'upload_cdm_main_report_taks',
        sql = "update_main_report.sql",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

#([upload_stg_restoraunt_task, upload_stg_couriers_task,upload_stg_deliveres_task] >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks] >> [upload_dds_delivery_taks, upload_dds_orders_taks] >> upload_dds_fct_sales_taks)

upload_stg_restoraunt_task >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks]
upload_stg_couriers_task >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks]
upload_stg_deliveres_task >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks]
upload_dds_courier_taks >> [upload_dds_delivery_taks, upload_dds_orders_taks] >> upload_dds_fct_sales_taks >> upload_cdm_main_report_taks
upload_dds_restaurants_taks>> [upload_dds_delivery_taks, upload_dds_orders_taks]
upload_dds_timestamps_taks>> [upload_dds_delivery_taks, upload_dds_orders_taks]
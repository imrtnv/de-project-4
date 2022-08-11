import time
import requests
import json
import pandas as pd
import sqlalchemy
import pendulum

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook

header={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f",
    "X-Nickname": "imartnv",
    "X-Cohort": "2"
    }

def upload_stg_restoraunt(pg_table_load):
    #Создадим подключение для базы приемника
    postgres_hook_export = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    engine = postgres_hook_export.get_sqlalchemy_engine()
    #Получим данные по API
    response_restaurants = requests.get('https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?',
    headers=header)
    #Загрузка данных в таблицу
    restaurants = pd.DataFrame(response_restaurants.json())
    restaurants.to_sql(pg_table_load, engine, schema='stg', if_exists='replace', index=False)

def upload_stg_couriers(pg_table_load):
    #Создадим подключение для базы приемника
    postgres_hook_export = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    engine = postgres_hook_export.get_sqlalchemy_engine()
    #Получим данные по API
    i=0
    res_1=[]
    for j in range(300):
        response_couriers = requests.get(f'''https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?limit=50&offset={i}''',headers=header)
        df = pd.DataFrame(response_couriers.json())
        if df.shape[0]==0:
            break
        else:
            if (len(res_1)==0):
                res_1 = df
            else:
                res_1 = pd.concat([df, res_1])
            i+=50
    #Загрузка данных в таблицу
    res_1.to_sql(pg_table_load, engine, schema='stg', if_exists='replace', index=False)

def upload_stg_deliveres(pg_table_load, pg_table_load_info):
    #Создадим подключение для базы приемника
    postgres_hook_export = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
    engine = postgres_hook_export.get_sqlalchemy_engine()
    #Получим данные по API
    max_date = str(pd.read_sql(f"""select max_time from stg.{pg_table_load_info}""", engine).values)[3:22]
    if max_date == '':
        max_date = '2022-06-01 00:00:00'
    for j in range(1000):
        response_deliveres = requests.get(f'''https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?limit=50&offset=0&sort_field=order_id&from={max_date}''',headers=header)
        df = pd.DataFrame(response_deliveres.json())
        if df.shape[0]==0:
            break
        else:
            max_date = df['delivery_ts'].max()[0:19]
            df.to_sql(pg_table_load, engine, schema='stg', if_exists='append', index=False)
            data = [{'max_time': max_date, 'name': "deliveries"}]
            pd.DataFrame(data).to_sql(pg_table_load_info, engine, schema='stg', if_exists='replace', index=False)


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
        op_kwargs={'pg_table_load': 'restaurants'})

    upload_stg_couriers_task = PythonOperator(
        task_id='upload_stg_couriers_task',
        python_callable=upload_stg_couriers,
        op_kwargs={'pg_table_load': 'couriers'})

    upload_stg_deliveres_task = PythonOperator(
        task_id='upload_stg_deliveres_task',
        python_callable=upload_stg_deliveres,
        op_kwargs={'pg_table_load': 'deliveries',
                    'pg_table_load_info':'info_table_import'})


    upload_dds_courier_taks = PostgresOperator(
        task_id = 'upload_dds_courier_taks',
        sql = """INSERT INTO dds.dm_courier (object_id, courier_name) 
        select c."_id", c."name" 
        from stg.couriers c 
        left join dds.dm_courier dc 
        on c."_id"=dc.object_id 
        where dc.object_id is null""",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_restaurants_taks = PostgresOperator(
        task_id = 'upload_dds_restaurants_taks',
        sql = """INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name) 
        select r."_id", r."name" 
        from stg.restaurants r 
        left join dds.dm_restaurants dr 
        on r."_id"=dr.restaurant_id 
        where dr.restaurant_id is null""",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_timestamps_taks = PostgresOperator(
        task_id = 'upload_dds_timestamps_taks',
        sql = """INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date) 
        select order_ts as ts, extract(year from order_ts) as year, 
        extract(month from order_ts) as month, 
        extract(day from order_ts) as day, order_ts::time as time, order_ts::date as date
        from stg.deliveries d 
        left join dds.dm_timestamps dt 
        on d.order_ts=dt.ts
        where dt.ts is null""",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )


    upload_dds_delivery_taks = PostgresOperator(
        task_id = 'upload_dds_delivery_taks',
        sql = """INSERT INTO dds.dm_delivery (timestamp_id , delivery_id , address) 
        select dt.id, d.delivery_id, d.address 
        from stg.deliveries d 
        left join dds.dm_timestamps dt 
        on d.order_ts=dt.ts 
        left join dds.dm_delivery dd 
        on d.delivery_id=dd.delivery_id 
        where dd.delivery_id is null""",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_orders_taks = PostgresOperator(
        task_id = 'upload_dds_orders_taks',
        sql = """INSERT INTO dds.dm_orders (courier_id , timestamp_id , order_id) 
        select dc.id as courier_id , dt.id as timestamp_id, d.order_id
        from stg.deliveries d
        left join dds.dm_courier dc 
        on dc.object_id=d.courier_id 
        left join dds.dm_timestamps dt 
        on dt.ts=d.order_ts 
        left join dds.dm_orders do2
        on do2.order_id=d.order_id 
        where do2.order_id is null""",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_dds_fct_sales_taks = PostgresOperator(
        task_id = 'upload_dds_fct_sales_taks',
        sql = """INSERT INTO dds.fct_sales (order_id, courier_id , count , total_sum, rate, tip_sum) 
        select do2.id as "order_id", do2.courier_id, count(distinct do2.order_id) as "count", sum(d.sum) as total_sum, avg(d.rate) as rate, sum(d.tip_sum) as tip_sum
        from dds.dm_orders do2 
        left join stg.deliveries d 
        on d.order_id=do2.order_id 
        left join dds.fct_sales fs2 
        on fs2.order_id = do2.id 
        where fs2.order_id is null
        group by 1,2""",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

    upload_cdm_main_report_taks = PostgresOperator(
        task_id = 'upload_cdm_main_report_taks',
        sql = """INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name , settlement_year, settlement_month, orders_count, 
		orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)

        with cte as(
        select t.courier_id, t.courier_name, t."year", t."month", sum(t.orders_count) as orders_count, sum(t.orders_total_sum) as orders_total_sum,
	        sum(t.rate_avg) as rate_avg, sum(order_processing_fee) as order_processing_fee, 
	        sum(case when t.rate_avg < 4 then GREATEST(0.05 * t.orders_total_sum, 100.00)
	        when t.rate_avg < 4.5 and t.rate_avg >= 4 then GREATEST(0.07 * t.orders_total_sum, 150.00)
	        when t.rate_avg < 4.9 and t.rate_avg >= 4.5 then GREATEST(0.08 * t.orders_total_sum, 175.00)
	        when t.rate_avg >= 4.9 then GREATEST(0.10 * t.orders_total_sum, 200.00) end) as courier_order_sum,
	        sum(courier_tips_sum) as courier_tips_sum
        from(
        select do2.courier_id, dc.courier_name, dt."year", dt."month", sum(fs2.count) as orders_count, sum(fs2.total_sum) as orders_total_sum,
    	    avg(fs2.rate) as rate_avg, sum(fs2.total_sum * 0.25) as order_processing_fee, sum(tip_sum) as courier_tips_sum
        from dds.fct_sales fs2 
    	left join dds.dm_orders do2 
            on do2.id=fs2.order_id
	    left join dds.dm_courier dc 
            on do2.courier_id=dc.id 
	    left join dds.dm_timestamps dt 
            on do2.timestamp_id=dt.id 
        group by 1,2,3,4) as t
        group by 1,2,3,4)
        
        select *, (c.courier_order_sum + c.courier_tips_sum * 0.95) as courier_reward_sum
        from cte c
        on conflict (courier_id, settlement_year, settlement_month)
        do update set orders_count = EXCLUDED.orders_count,
            orders_total_sum = EXCLUDED.orders_total_sum,
            rate_avg = EXCLUDED.rate_avg,
            order_processing_fee = EXCLUDED.order_processing_fee,
            courier_order_sum = EXCLUDED.courier_order_sum,
            courier_tips_sum = EXCLUDED.courier_tips_sum,
            courier_reward_sum = EXCLUDED.courier_reward_sum""",
        postgres_conn_id = "PG_WAREHOUSE_CONNECTION"
        )

#([upload_stg_restoraunt_task, upload_stg_couriers_task,upload_stg_deliveres_task] >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks] >> [upload_dds_delivery_taks, upload_dds_orders_taks] >> upload_dds_fct_sales_taks)

upload_stg_restoraunt_task >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks]
upload_stg_couriers_task >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks]
upload_stg_deliveres_task >> [upload_dds_courier_taks, upload_dds_restaurants_taks, upload_dds_timestamps_taks]
upload_dds_courier_taks >> [upload_dds_delivery_taks, upload_dds_orders_taks] >> upload_dds_fct_sales_taks >> upload_cdm_main_report_taks
upload_dds_restaurants_taks>> [upload_dds_delivery_taks, upload_dds_orders_taks]
upload_dds_timestamps_taks>> [upload_dds_delivery_taks, upload_dds_orders_taks]
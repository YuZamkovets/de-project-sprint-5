import requests
import json
import datetime
import time
import psycopg2
import pendulum
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.models.xcom import XCom

log = logging.getLogger(__name__)

# Объявляем переменные для получения данных по API

headers={
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f", # ключ API
    "X-Nickname": "YuZamkovets", # авторизационные данные
    "X-Cohort": "8" # авторизационные данные   
    }

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'

# Загружаем данные из API (курьеры, рестораны, доставки)
def load_couriers_to_stg (ti, url=url, headers=headers):
    method_url = '/couriers'
    table_name = 'couriers_tmp'
    offset = 0
    limit = 1 #загружается по 1 строке, чтобы не писать дополнительный цикл для загрузки в БД
    sort_field='_id'
    sort_direction = 'asc'

    psql_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    while True:
      params = {'limit':limit,'offset':offset,'sort_field':sort_field,'sort_direction':sort_direction}
      data_json = requests.get(url + method_url, headers=headers, params=params).json()
      if len(data_json) == 0:
        conn.commit()
        cur.close()
        conn.close()
        break
      query = f"INSERT INTO stg.api_{table_name}(json_content) VALUES ('{json.dumps(data_json[0], ensure_ascii=False)}'::json)"
      cur.execute(query)
      offset += len(data_json)

    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
    cur.execute("""
    --Загружаем новые записи (ранее не  существовавшие)
insert into stg.api_couriers_raw (json_content,first_load_ts,last_load_ts)
select json_content,load_ts,load_ts
from stg.api_couriers_tmp
where json_content not in (select coalesce(json_content,'') from stg.api_couriers_raw);

--Обновляем даты по ранее загруженным записям
update stg.api_couriers_raw 
set last_load_ts=t1.load_ts
from stg.api_couriers_tmp t1
where api_couriers_raw.json_content=t1.json_content;

--Проставляем даты удаления
update stg.api_couriers_raw 
set delete_ts=t1.max_load_ts
from (select max(load_ts) as max_load_ts from stg.api_couriers_tmp) t1
where api_couriers_raw.json_content 
not in (select json_content from stg.api_couriers_tmp);

truncate table stg.api_couriers_tmp;""")
    conn.commit()
    cur.close()
    conn.close()

def load_restaurants_to_stg (ti, url=url, headers=headers):
    method_url = '/restaurants'
    table_name = 'restaurants_tmp'
    offset = 0
    limit = 1
    sort_field='_id'
    sort_direction = 'asc'

    psql_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()

    while True:
      params = {'limit':limit,'offset':offset,'sort_field':sort_field,'sort_direction':sort_direction}
      data_json = requests.get(url + method_url, headers=headers, params=params).json()
      if len(data_json) == 0:
        conn.commit()
        cur.close()
        conn.close()
        break
      query = f"INSERT INTO stg.api_{table_name}(json_content) VALUES ('{json.dumps(data_json[0], ensure_ascii=False)}'::json)"
      cur.execute(query)
      offset += len(data_json)

    
    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
    cur.execute("""
    --Загружаем новые записи (ранее не  существовавшие)
insert into stg.api_restaurants_raw (json_content,first_load_ts,last_load_ts)
select json_content,load_ts,load_ts
from stg.api_restaurants_tmp
where json_content not in (select coalesce(json_content,'') from stg.api_restaurants_raw);

--Обновляем даты по ранее загруженным записям
update stg.api_restaurants_raw 
set last_load_ts=t1.load_ts
from stg.api_restaurants_tmp t1
where api_restaurants_raw.json_content=t1.json_content;

--Проставляем даты удаления
update stg.api_restaurants_raw 
set delete_ts=t1.max_load_ts
from (select max(load_ts) as max_load_ts from stg.api_restaurants_tmp) t1
where api_restaurants_raw.json_content 
not in (select json_content from stg.api_restaurants_tmp);

truncate table stg.api_restaurants_tmp;""")
    conn.commit()
    cur.close()
    conn.close()

def load_deliveries_to_stg (ti, url=url, headers=headers):
    method_url = '/deliveries'
    table_name = 'deliveries_raw'
    offset = 0
    limit = 50 #максимальный лимит, так как данных много
    sort_field='order_ts'
    sort_direction = 'asc'

    psql_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
# вытаскиваем из таблицы последнюю дату загрузки, если таблица пустая, 
# то считаем последней дату '2023-12-31' и начинаем загружать с 2023-01-01
    max_order_ts = cur.execute("""
    select coalesce(max(order_ts),'2022-12-31'::timestamp) 
    from stg.api_deliveries_raw;""")

    while True:
      params = {'limit':limit,'offset':offset,'sort_field':sort_field,'sort_direction':sort_direction,'from':max_order_ts}
      data_json = requests.get(url + method_url, headers=headers, params=params).json()
      if len(data_json) == 0:
        conn.commit()
        cur.close()
        conn.close()
        break
      for i in range(len(data_json)): #
          query = f"INSERT INTO stg.api_{table_name}(json_content,order_ts) VALUES ('{json.dumps(data_json[i], ensure_ascii=False)}'::json,('{json.dumps(data_json[i], ensure_ascii=False)}'::json->>'order_ts')::timestamp)"
          cur.execute(query)
      offset += len(data_json)

def dds_cdm_insert (ti):
    psql_conn = BaseHook.get_connection('PG_WAREHOUSE_CONNECTION')
    conn = psycopg2.connect(f"dbname='{psql_conn.schema}' port='{psql_conn.port}' user='{psql_conn.login}' host='{psql_conn.host}' password='{psql_conn.password}'")
    cur = conn.cursor()
    cur.execute("""
----------Заполнение таблиц
--Курьеры
truncate table dds.dm_couriers cascade;
insert into dds.dm_couriers
select id,json_content::json->>'_id',json_content::json->>'name'
from stg.api_couriers_raw;

--Доставки (таблица измерений)

insert into dds.dm_deliveries
select id,json_content::json->>'delivery_id',(json_content::json->>'delivery_ts')::timestamp
,json_content::json->>'address'
from stg.api_deliveries_raw
where id not in (select coalesce(id,0) from dds.dm_deliveries);

--Доставки (таблица фактов)

insert into dds.fct_deliveries (delivery_id,order_id,courier_id,rate,sum,tip_sum)
select d.id,o.id,c.id,(stg_d.rate::numeric(14,2))*1.0,stg_d.sum::numeric(14,2),stg_d.tip_sum::numeric(14,2)
from
(select id
,json_content::json->>'delivery_id' as delivery_id
,json_content::json->>'order_id' as order_id
,json_content::json->>'courier_id' as courier_id
,json_content::json->>'rate' as rate
,json_content::json->>'sum' as sum
,json_content::json->>'tip_sum' as tip_sum
from stg.api_deliveries_raw
where id not in (select coalesce(id,0) from dds.dm_deliveries)) as stg_d
inner join dds.dm_deliveries d on stg_d.delivery_id=d.delivery_id 
inner join dds.dm_orders o on stg_d.order_id=o.order_key 
inner join dds.dm_couriers c on stg_d.courier_id=c.courier_id;

--Заполнение витрины
truncate table cdm.dm_courier_ledger cascade;
insert into cdm.dm_courier_ledger (courier_id,courier_name,settlement_year,settlement_month,
orders_count,orders_total_sum,rate_avg,order_processing_fee,
courier_order_sum,courier_tips_sum,courier_reward_sum)
select id,courier_name,year,month,
count(distinct order_id),sum(order_sum),
avg_rate_per_m,
sum(order_sum)*0.25,
sum(courier_order_sum),sum(tip_sum),sum(courier_order_sum)+sum(tip_sum)*0.95
from (
select *,
case when avg_rate_per_m<4 then greatest(100,order_sum*0.05)
when avg_rate_per_m>=4 and avg_rate_per_m<4.5 then greatest(150,order_sum*0.07)
when avg_rate_per_m>=4.5 and avg_rate_per_m<4.9 then greatest(175,order_sum*0.08)
else greatest(200,order_sum*0.10) end as courier_order_sum
from (
select c.id,c.courier_name,t.year,t.month
,fct_d.order_id,fct_d.sum as order_sum,fct_d.tip_sum,rate as rate,
(avg(rate) over (partition by c.id,t.year,t.month)) as avg_rate_per_m --средняя оценка курьера в рамках месяца и года
from dds.dm_couriers c
inner join dds.fct_deliveries fct_d on c.id=fct_d.courier_id
inner join dds.dm_orders o on fct_d.order_id=o.id
inner join dds.dm_timestamps t on o.timestamp_id=t.id) t1
) t2
group by id,courier_name,year,month,avg_rate_per_m
order by id,courier_name,year,month;""")
    conn.commit()
    cur.close()
    conn.close()


# Формируем DAG

dag = DAG(
   dag_id = 'sprint5_project_dag',  
    schedule_interval='* 0 * * *',  # Задаем расписание выполнения дага - каждый день в 0 часов.
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),  # Дата начала выполнения дага. 
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)

t_load_couriers_to_stg = PythonOperator(task_id='load_couriers_to_stg',
                                        python_callable=load_couriers_to_stg,
                                        dag=dag)
t_load_restaurants_to_stg = PythonOperator(task_id='load_restaurants_to_stg',
                                        python_callable=load_restaurants_to_stg,
                                        dag=dag)
t_load_deliveries_to_stg = PythonOperator(task_id='load_deliveries_to_stg',
                                        python_callable=load_deliveries_to_stg,
                                        dag=dag)
t_dds_cdm_insert = PythonOperator(task_id='dds_cdm_insert',
                                        python_callable=dds_cdm_insert,
                                        dag=dag)

t_load_couriers_to_stg >> t_load_restaurants_to_stg >> t_load_deliveries_to_stg >> t_dds_cdm_insert

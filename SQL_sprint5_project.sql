--------------------------------STG------------------------
---Создаем таблицы для исходных данных из API
drop table stg.api_couriers_raw ;
CREATE TABLE stg.api_couriers_raw (
	id serial NOT NULL,
	json_content text NOT NULL,
	CONSTRAINT api_couriers_raw_pkey PRIMARY KEY (id)
);

drop table stg.api_restaurants_raw ;
CREATE TABLE stg.api_restaurants_raw (
	id serial NOT NULL,
	json_content text NOT NULL,
	CONSTRAINT api_restaurants_raw_pkey PRIMARY KEY (id)
);

drop table stg.api_deliveries_raw ;
CREATE TABLE stg.api_deliveries_raw (
	id serial NOT NULL,
	json_content text NOT NULL,
	CONSTRAINT api_deliveries_raw_pkey PRIMARY KEY (id)
);

--------------------------------DDS------------------------

--Таблица с данными курьеров
drop table if exists dds.dm_couriers cascade;
CREATE TABLE dds.dm_couriers
(
    id serial not null constraint dm_couriers_pkey primary key,
    courier_id varchar        NOT NULL,
    courier_name    varchar        NOT NULL
);

--Рестораны (данные, загруженные из АПИ - избыточные, поэтому будет использоать уже существующую таблицу)
--dds.dm_restaurants

--Данные по доставкам

--Таблица измерений
drop table if exists dds.dm_deliveries cascade;
CREATE TABLE dds.dm_deliveries
(
    id serial not null constraint dm_deliveries_pkey primary key,
    delivery_id varchar        NOT NULL,
    delivery_ts timestamp        NOT NULL,
    address varchar        NOT NULL
);

--Таблица фактов
drop table if exists dds.fct_deliveries;
CREATE TABLE dds.fct_deliveries
(
    id serial not null constraint fct_deliveries_pkey primary key,
    delivery_id integer        NOT NULL,
    order_id integer        NOT NULL,
    courier_id integer        NOT NULL,
    rate numeric(14,2)        NOT NULL, --numeric - для корректной работы avg
    sum numeric(14,2)        NOT NULL,
    tip_sum numeric(14,2)        NOT NULL
);

ALTER TABLE dds.fct_deliveries ADD CONSTRAINT fct_deliveries_deliveriy_id_fkey 
FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries (id);
ALTER TABLE dds.fct_deliveries ADD CONSTRAINT fct_deliveries_order_id_fkey 
FOREIGN KEY (order_id) REFERENCES dds.dm_orders (id);
ALTER TABLE dds.fct_deliveries ADD CONSTRAINT fct_deliveries_courier_id_fkey 
FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers (id);

--------------------------------CDM------------------------

-- Создаем витрину данных

CREATE TABLE cdm.dm_courier_ledger (
	id serial NOT NULL,
	courier_id varchar NOT NULL, -- берем данные из нового источника (API), таблицы dds.dm_couriers + связь с заказом, dds.dm_deliveries, dds.fct_deliveries
	courier_name varchar NOT NULL, -- берем данные из нового источника (API), таблицы dds.dm_couriers + связь с заказом, dds.dm_deliveries, dds.fct_deliveries
	settlement_year integer NOT NULL, --Исходя из ТЗ берем дату заказа, а не доставки (существующая таблица dds.dm_order и dds.dm_timestamps)
    settlement_month integer NOT NULL,--Исходя из ТЗ берем дату заказа, а не доставки (существующая таблица dds.dm_order и dds.dm_timestamps)
	orders_count int4 NOT NULL DEFAULT 0, --dds.fct_deliveries
	orders_total_sum numeric(14,2) NOT NULL DEFAULT 0, --dds.fct_deliveries
	rate_avg numeric(14,2) NOT NULL DEFAULT 0,--dds.fct_deliveries
	order_processing_fee numeric(14,2) NOT NULL DEFAULT 0,--dds.fct_deliveries
	courier_order_sum numeric(14,2) NOT NULL DEFAULT 0,--dds.fct_deliveries
	courier_tips_sum numeric(14,2) NOT NULL DEFAULT 0,--dds.fct_deliveries
	courier_reward_sum numeric(14,2) NOT NULL DEFAULT 0--dds.fct_deliveries
);

----Закомменчено, так как выполянется в DAG
/*----------Заполнение таблиц
--Курьеры
truncate table dds.dm_couriers cascade;
insert into dds.dm_couriers
select id,json_content::json->>'_id',json_content::json->>'name'
from stg.api_couriers_raw;

--Доставки (таблица измерений)
truncate table dds.dm_deliveries cascade;
insert into dds.dm_deliveries
select id,json_content::json->>'delivery_id',(json_content::json->>'delivery_ts')::timestamp
,json_content::json->>'address'
from stg.api_deliveries_raw;

--Доставки (таблица фактов)
truncate table dds.fct_deliveries cascade;
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
from stg.api_deliveries_raw) as stg_d
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
order by id,courier_name,year,month;
*/

-------Особенности данных - у всех курьеров всегда стоит одна и та же оценка, поэтому в витрине получилось, что средняя оценка - всегда int
select distinct 
json_content::json->>'courier_id' as courier_id
,json_content::json->>'rate' as rate
from stg.api_deliveries_raw

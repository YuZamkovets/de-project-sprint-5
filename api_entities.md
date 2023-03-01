Описание задачи:

1) Список полей, которые необходимы для витрины.

В витрине должны быть следующие поля:
id — идентификатор записи.
courier_id — ID курьера, которому перечисляем.
courier_name — Ф. И. О. курьера.
settlement_year — год отчёта.
settlement_month — месяц отчёта, где 1 — январь и 12 — декабрь.
orders_count — количество заказов за период (месяц).
orders_total_sum — общая стоимость заказов.
rate_avg — средний рейтинг курьера по оценкам пользователей.
order_processing_fee — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.
courier_order_sum — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).
courier_tips_sum — сумма, которую пользователи оставили курьеру в качестве чаевых.
courier_reward_sum — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).


2) Список таблиц в слое DDS, из которых вы возьмёте поля для витрины. 
Отметьте, какие таблицы уже есть в хранилище, а каких пока нет. Недостающие таблицы вы создадите позднее. Укажите, как они будут называться.


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


3) На основе списка таблиц в DDS составьте список сущностей и полей, которые необходимо загрузить из API. 
Использовать все методы API необязательно: важно загрузить ту информацию, которая нужна для выполнения задачи.

Из API грузим данные as-is, затем выбираем необходимые поля уже на этапе загрузки данных в DDS

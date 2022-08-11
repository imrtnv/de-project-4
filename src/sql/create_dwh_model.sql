CREATE SCHEMA IF NOT exists stg;
CREATE SCHEMA IF NOT exists dds;
CREATE SCHEMA IF NOT exists cdm;

-- Create CDM
drop table if exists cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger(
	id serial4 NOT NULL,
	courier_id int4 NOT NULL,
	courier_name varchar(50) NOT NULL,
	settlement_year int4 NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count int4 NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	rate_avg numeric(14, 2) NOT NULL DEFAULT 0,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_tips_sum numeric(14, 2) NOT NULL DEFAULT 0,
	courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_settlement_report_chek_orders_count CHECK ((orders_count >= 0)),
	CONSTRAINT dm_settlement_report_chek_orders_total_sum CHECK ((orders_total_sum >= 0)),
	CONSTRAINT dm_settlement_report_chek_rate_avg CHECK ((rate_avg >= 0)),
	CONSTRAINT dm_settlement_report_chek_order_processing_fee CHECK ((order_processing_fee >= 0)),
	CONSTRAINT dm_settlement_report_chek_courier_order_sum CHECK ((courier_order_sum >= 0)),
	CONSTRAINT dm_settlement_report_chek_courier_tips_sum CHECK ((courier_tips_sum >= 0)),
	CONSTRAINT dm_settlement_report_chek_courier_reward_sum CHECK ((courier_reward_sum >= 0)),
	CONSTRAINT dm_settlement_report_uniq_restorant UNIQUE (courier_id, settlement_year, settlement_month),
	CONSTRAINT pk_dm_settlement_report_id PRIMARY KEY (id)
);

-- Create DDS
drop table if exists dds.dm_courier cascade;
create table dds.dm_courier(
	id serial constraint dm_courier_pk_id primary key,
	object_id varchar not null,
	courier_name varchar not null
	);

drop table if exists dds.dm_restaurants cascade;
create table dds.dm_restaurants (
	id serial constraint dm_restaurants_pk_id primary key,
	restaurant_id varchar not null,
	restaurant_name varchar not null
	);

drop table if exists dds.dm_timestamps cascade;
create table dds.dm_timestamps (
	id serial constraint dm_timestamps_pk_id primary key,
	ts timestamp ,
	"year" int  constraint dm_timestamps_year_check check("year" >= 2022 and "year" < 2500),
	"month" int  constraint dm_timestamps_month_check check("month" >= 1 and "month" <= 12),
	"day" int  constraint dm_timestamps_day_check check("day" >= 1 and "day" <= 31),
	"time" time,
	"date" date
	);
	
drop table if exists dds.dm_orders cascade;
create table dds.dm_orders (
	id serial constraint dm_orders_pk_id primary key,
	courier_id int not null,
	timestamp_id int not null,
	order_id varchar not null
	);


-- Необязательное поле
drop table if exists dds.dm_delivery cascade;
create table dds.dm_delivery (
	id serial constraint dm_delivery_pk_id primary key,
	timestamp_id int not null,
	delivery_id varchar not null,
	address varchar not null
	);


drop table if exists dds.fct_sales cascade;
create table dds.fct_sales (
	id serial constraint fct_sales_pk_id primary key,
	order_id int not null,
	courier_id int not null ,
	"count" int not null DEFAULT 0 constraint fct_sales_count_check check("count" >= 0),
	total_sum numeric(14,2) not null DEFAULT 0 constraint fct_sales_total_sum_check check(total_sum >= 0),
	rate int not null DEFAULT 0 constraint fct_sales_rate_check check(rate >= 0),
	tip_sum numeric(14,2) not null DEFAULT 0 constraint fct_sales_tip_sum_check check(tip_sum >= 0)
	);

-- alter table dds.dm_orders drop constraint dm_orders_timestamp_id_fkey;
-- alter table dds.dm_orders drop constraint dm_orders_courier_id_fkey;
-- alter table dds.fct_sales drop constraint fct_sales_order_id_fkey;

alter table dds.dm_orders add constraint dm_orders_timestamp_id_fkey foreign key (timestamp_id) references dds.dm_timestamps(id);
alter table dds.dm_orders add constraint dm_orders_courier_id_fkey foreign key (courier_id) references dds.dm_courier(id);
alter table dds.fct_sales add constraint fct_sales_order_id_fkey foreign key (order_id) references dds.dm_orders(id);

-- Create STG
drop table if exists stg.restaurants;
create table stg.restaurants(
	_id text not null,
	name text not null
);

drop table if exists stg.couriers;
create table stg.couriers(
	_id text not null,
	name text not null
);

drop table if exists stg.deliveries;
create table stg.deliveries(
	order_id text not null,
	order_ts timestamp not null,
	delivery_id text not null,
	courier_id text not null,
	address text not null,
	delivery_ts timestamp not null,
	rate int,
	sum numeric(14,2) not null DEFAULT 0,
	tip_sum numeric(14,2) not null DEFAULT 0
);

drop table if exists stg.info_table_import;
CREATE TABLE stg.info_table_import (
	max_time text NULL,
	"name" text NULL
);
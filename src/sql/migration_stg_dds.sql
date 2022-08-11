-- migration stg on dds
INSERT INTO dds.dm_courier (object_id, courier_name) 
select c."_id", c."name" 
from stg.couriers c 
left join dds.dm_courier dc 
on c."_id"=dc.object_id 
where dc.object_id is null


INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name) 
select r."_id", r."name" 
from stg.restaurants r 
left join dds.dm_restaurants dr 
on r."_id"=dr.restaurant_id 
where dr.restaurant_id is null


INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date) 
select order_ts as ts, extract(year from order_ts) as year, 
extract(month from order_ts) as month, 
extract(day from order_ts) as day, order_ts::time as time, order_ts::date as date
from stg.deliveries d 
left join dds.dm_timestamps dt 
on d.order_ts=dt.ts
where dt.ts is null


INSERT INTO dds.dm_delivery (timestamp_id , delivery_id , address) 
select dt.id, d.delivery_id, d.address 
from stg.deliveries d 
left join dds.dm_timestamps dt 
on d.order_ts=dt.ts 
left join dds.dm_delivery dd 
on d.delivery_id=dd.delivery_id 
where dd.delivery_id is null


INSERT INTO dds.dm_orders (courier_id , timestamp_id , order_id) 
select coalesce (dc.id,49) as courier_id , dt.id as timestamp_id, d.order_id
from stg.deliveries d 
left join dds.dm_courier dc 
on dc.object_id=d.courier_id 
left join dds.dm_timestamps dt 
on dt.ts=d.order_ts 
left join dds.dm_orders do2
on do2.order_id=d.delivery_id 
where do2.order_id is null



INSERT INTO dds.fct_sales (order_id, courier_id , count , total_sum, rate, tip_sum) 
select do2.id as "order_id", do2.courier_id, count(distinct do2.order_id) as "count", sum(d.sum) as total_sum, avg(d.rate) as rate, sum(d.tip_sum) as tip_sum
from dds.dm_orders do2 
left join stg.deliveries d 
on d.order_id=do2.order_id 
left join dds.fct_sales fs2 
on fs2.order_id = do2.id 
where fs2.order_id is null
group by 1,2
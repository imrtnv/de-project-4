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
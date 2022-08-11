INSERT INTO dds.dm_delivery (timestamp_id , delivery_id , address) 
select dt.id, d.delivery_id, d.address 
from stg.deliveries d 
left join dds.dm_timestamps dt 
on d.order_ts=dt.ts 
left join dds.dm_delivery dd 
on d.delivery_id=dd.delivery_id 
where dd.delivery_id is null
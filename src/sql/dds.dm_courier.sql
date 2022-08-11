INSERT INTO dds.dm_courier (object_id, courier_name) 
select c."_id", c."name" 
from stg.couriers c 
left join dds.dm_courier dc 
on c."_id"=dc.object_id 
where dc.object_id is null
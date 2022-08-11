INSERT INTO dds.dm_restaurants (restaurant_id, restaurant_name) 
select r."_id", r."name" 
from stg.restaurants r 
left join dds.dm_restaurants dr 
on r."_id"=dr.restaurant_id 
where dr.restaurant_id is null
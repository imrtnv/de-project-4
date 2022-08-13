INSERT INTO dds.dm_timestamps (ts, year, month, day, time, date) 
select order_ts as ts, extract(year from order_ts) as year, 
extract(month from order_ts) as month, 
extract(day from order_ts) as day, order_ts::time as time, order_ts::date as date
from stg.deliveries d 
left join dds.dm_timestamps dt 
on d.order_ts=dt.ts
where dt.ts is null
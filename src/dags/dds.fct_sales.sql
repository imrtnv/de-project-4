INSERT INTO dds.fct_sales (order_id, courier_id , count , total_sum, rate, tip_sum) 
select do2.id as "order_id", do2.courier_id, count(distinct do2.order_id) as "count", sum(d.sum) as total_sum, avg(d.rate) as rate, sum(d.tip_sum) as tip_sum
from dds.dm_orders do2 
left join stg.deliveries d 
on d.order_id=do2.order_id 
left join dds.fct_sales fs2 
on fs2.order_id = do2.id 
where fs2.order_id is null
group by 1,2
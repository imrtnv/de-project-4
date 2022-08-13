INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name , settlement_year, settlement_month, orders_count, 
orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)

with cte as(
select t.courier_id, t.courier_name, t."year", t."month", sum(t.orders_count) as orders_count, sum(t.orders_total_sum) as orders_total_sum,
	sum(t.rate_avg) as rate_avg, sum(order_processing_fee) as order_processing_fee, 
	sum(case when t.rate_avg < 4 then GREATEST(0.05 * t.orders_total_sum, 100.00)
	when t.rate_avg < 4.5 and t.rate_avg >= 4 then GREATEST(0.07 * t.orders_total_sum, 150.00)
	when t.rate_avg < 4.9 and t.rate_avg >= 4.5 then GREATEST(0.08 * t.orders_total_sum, 175.00)
	when t.rate_avg >= 4.9 then GREATEST(0.10 * t.orders_total_sum, 200.00) end) as courier_order_sum,
	sum(courier_tips_sum) as courier_tips_sum
from(
select do2.courier_id, dc.courier_name, dt."year", dt."month", sum(fs2.count) as orders_count, sum(fs2.total_sum) as orders_total_sum,
	avg(fs2.rate) as rate_avg, sum(fs2.total_sum * 0.25) as order_processing_fee, sum(tip_sum) as courier_tips_sum
from dds.fct_sales fs2 
left join dds.dm_orders do2 
	on do2.id=fs2.order_id
left join dds.dm_courier dc 
	on do2.courier_id=dc.id 
left join dds.dm_timestamps dt 
	on do2.timestamp_id=dt.id 
group by 1,2,3,4) as t
group by 1,2,3,4)
        
select *, (c.courier_order_sum + c.courier_tips_sum * 0.95) as courier_reward_sum
from cte c
on conflict (courier_id, settlement_year, settlement_month)
do update set orders_count = EXCLUDED.orders_count,
	orders_total_sum = EXCLUDED.orders_total_sum,
	rate_avg = EXCLUDED.rate_avg,
	order_processing_fee = EXCLUDED.order_processing_fee,
	courier_order_sum = EXCLUDED.courier_order_sum,
	courier_tips_sum = EXCLUDED.courier_tips_sum,
	courier_reward_sum = EXCLUDED.courier_reward_sum
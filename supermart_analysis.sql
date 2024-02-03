-- Customer Segmentations

select * from customer;
select * from sales;

select a.*,b.order_num,b.sales_total,b.total_qunt,b.profit_total
from customer as a
left join (select customer_id,count(distinct order_id) as order_num,sum(sales) as sales_total,sum(quantity) as total_qunt,
		  sum(profit) as profit_total from sales group by customer_id) as b
on a.customer_id =b.customer_id

create table customer_order as (select a.*,b.order_num,b.sales_total,b.total_qunt,b.profit_total
from customer as a
left join (select customer_id,count(distinct order_id) as order_num,sum(sales) as sales_total,sum(quantity) as total_qunt,
		  sum(profit) as profit_total from sales group by customer_id) as b
on a.customer_id =b.customer_id);

select * from customer_order;

select customer_id,customer_name,state,order_num,row_number() over(partition by state order by order_num desc) as row_n
from customer_order;

select * from (select customer_id,customer_name,state,order_num,row_number() over(partition by state order by order_num desc) as row_n
from customer_order) as a where a.row_n<=3;
/*
Marketing Optimization: And for that we will be using Rank & Dense Rank and NTILE
*/
select customer_id,customer_name,state,order_num,
row_number() over(partition by state order by order_num desc) as row_n,
rank() over(partition by state order by order_num desc) as rank_n,
dense_rank() over(partition by state order by order_num desc) as d_rank
from customer_order;

-- NTILE (It devide rows within partition as equally as possible into n groups and assign each row its group number)

select customer_id,customer_name,state,order_num,
row_number() over(partition by state order by order_num desc) as row_n,
rank() over(partition by state order by order_num desc) as rank_n,
dense_rank() over(partition by state order by order_num desc) as d_rank,
ntile(5) over(partition by state order by order_num desc) as tile_n
from customer_order;

/*Revenue Disparity Investigation: top 20% customer from each state
*/
select * from(select customer_id,customer_name,state,order_num,
row_number() over(partition by state order by order_num desc) as row_n,
rank() over(partition by state order by order_num desc) as rank_n,
dense_rank() over(partition by state order by order_num desc) as d_rank,
ntile(5) over(partition by state order by order_num desc) as tile_n
from customer_order) as a where a.tile_n =1; 

-- bottom 20% ocustomer
select * from(select customer_id,customer_name,state,order_num,
row_number() over(partition by state order by order_num desc) as row_n,
rank() over(partition by state order by order_num desc) as rank_n,
dense_rank() over(partition by state order by order_num desc) as d_rank,
ntile(5) over(partition by state order by order_num desc) as tile_n
from customer_order) as a where a.tile_n =5;  

select * from customer_order;
-- Avg revenue wrt states
select customer_id,customer_name,state,sales_total as revenue,
avg(sales_total) over (partition by state) as avg_revenue
from customer_order;

-- customer with less than avg revenue 
select * from (select customer_id,customer_name,state,sales_total as revenue,
			  avg(sales_total) over (partition by state) as avg_revenue
			  from customer_order) as a where a.revenue<a.avg_revenue;

-- Resource Allocation Dilemma:
select Customer_id,customer_name,state,
count(customer_id) over (partition by state) as count_cust
from customer_order;

--Total (Sum window function in SQL)
select * from sales;

create table order_rollup as (select order_id,max(order_date) as order_date, max(customer_id) as customer_id,sum(sales) as sales from sales
group by order_id);

create table order_rollup_state as select a.*,b.state
from order_rollup as a
left join customer as b
on a.customer_id =b.customer_id;

select * from order_rollup_state;

select *, 
sum(sales) over(partition by state) as sales_state_total
from order_rollup_state;

-- Real-time Sales Monitoring:
select *,
sum(sales) over (partition by state) as sales_state_total,
sum(sales) over (partition by state order by order_date) as  running_total
from order_rollup_state where customer_id ='DC-12850';

-- Previous and Next Sales Analysis: using Lag & Lead 

select customer_id,order_date,order_id,sales,
lag(sales,1) over(partition by customer_id order by order_date) as previous_sales,
lag(order_id,1) over(partition by customer_id order by order_date) as previous_order_id
from order_rollup_state where customer_id ='AA-10315';

select customer_id,order_date,order_id,sales,
lead(sales,1) over(partition by customer_id order by order_date) as previous_sales,
lead(order_id,1) over(partition by customer_id order by order_date) as previous_order_id
from order_rollup_state where customer_id ='AA-10315';


select * from customer
select * from sales

-- Performance Evaluation - Lag in Response: Explain

explain select * from customer;

explain select distinct * from customer;
-- schema 
create schema test
create table test.customer as select * from customer;















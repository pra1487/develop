"""
Ankith bansal:
--------------
To print the ratings as ** ** *

select * from movies;

select * from reviews;

with cte as (
select m.genre, m.title, avg(r.rating) as avg_rating, row_number() over(partition by m.genre order by avg(r.rating) desc) as rnum
from movies m inner join reviews r on m.id = r.movie_id
group by m.genre, m.title)
select genre, title, round(avg_rating), repeat('*', round(avg_rating)) as stars from cte
where rnum = 1;

or

select t.genre, t.title, round(t.avrg_rating), repeat('*', round(t.avrg_rating)) as stars from
(select m.genre, m.title, avg(r.rating) as avrg_rating, row_number() over(partition by m.genre order by avg(r.rating) desc) as rnum
from movies m inner join reviews r on m.id = r.movie_id
group by m.genre, m.title) as t
where t.rnum = 1;

---------------------------------------------------

Q) find duplicate emp ids from table:

select emp_id, count(1) from emp group by emp_id having count(1) > 1;

Q) Delete duplicates from emp table:

with cte as (
select *, row_number() over(partition by emp_id order by emp_id) as rnum from emp)
delete from cte where rnum > 1;

Q) Get the employees list who are not present in the department table:

select * from employees where department_id not in (select department_id from dept);

Q) Get the top 2 highest salried employee list.

with cte as (
select *, dense_rank() over(order by salary desc) as dns_rnk from employee)
select * from cte where dns_rnk in (1, 2);

Q) lag and LEAD

with cte as (
select *, lag(salary, 1) over(partition by location order by salary) as previous_sal from employee)
select *, salary-previous_sal as sal_diff from cte;

select *, lead(salary, 1) over(partition by location order by salary) as next_sal from employee;

------------------------------------------------

select * from icc_world_cup;

select team_name, count(win_flag) as matches_played, sum(win_flag) as no_of_wins, count(win_flag)-sum(win_flag) as no_of_loses from
(select Team_1 as team_name, case when Team_1 = Winner then 1 else 0 end as win_flag from icc_world_cup
union all
select Team_2 as team_name, case when Team_2 = Winner then 1 else 0 end as win_flag from icc_world_cup) result
group by team_name;

---------------------------------------------

select * from customer_orders;

with first_visit as
(select customer_id, min(order_date) as first_vd from customer_orders group by customer_id),
visit_flag as (
select co.*, fv.first_vd,
case when co.order_date = fv.first_vd then 1 else 0 end as first_visit_flag,
case when co.order_date != fv.first_vd then 1 else 0 end as repeat_visit_flag
from customer_orders co
inner join first_visit fv on co.customer_id = fv.customer_id order by co.customer_id)
select order_date, sum(first_visit_flag) as first_visit_count, sum(repeat_visit_flag) as repeat_cust_count from visit_flag
group by order_date;


with first_visit as
(select customer_id, min(order_date) as first_vd from customer_orders group by customer_id),
repeat_visit as
( select co.*, fv.first_vd,
case when co.order_date = fv.first_vd then 1 else 0 end as fvfl,
case when co.order_date = fv.first_vd then order_amount else 0 end as fvoamt,
case when co.order_date != fv.first_vd then 1 else 0 end as rvfl,
case when co.order_date != fv.first_vd then order_amount else 0 end as rpamt
from customer_orders co inner join first_visit fv on co.customer_id = fv.customer_id)
select order_date, sum(fvfl) as first_visit_customers_count, sum(fvoamt) as sum_amt, sum(rvfl) as repeat_visit_customer_count,
sum(rpamt) as sum_amt1
from repeat_visit group by order_date;

-----------------------------------------------------------

select * from entries;

with total_visits as
(select name, count(1) as total_visits, group_concat(distinct resources) as resources_used from entries group by name),
floor_visit as (select name, floor, count(1) as no_of_floor_visit,
rank() over(partition by name order by count(1) desc) as rn from entries group by name, floor)
select fv.name, fv.floor as most_visited_floor, tv.total_visits, resources_used
from floor_visit fv inner join total_visits tv on fv.name = tv.name where rn = 1;

-----------------------------------------------------

select * from tasks;

with all_dates as (
select *, row_number() over(partition by state order by date_value) as rn,
date_add(date_value, interval(-row_number() over(partition by state order by date_value)) day) as group_date
from tasks order by date_value)
select min(date_value) as start_date, max(date_value) as end_date, state
from all_dates group by group_date, state order by start_date;


--------------------------------------------------------

select * from cinema;

with cte as (
select *, row_number() over(order by seat_id) as rn,
seat_id-row_number() over(order by seat_id) as grp from cinema where free = 1)
select seat_id from (
select *, count( * ) over(partition by grp) as cnt from cte) A
where cnt > 1;


with cte as (
select c1.seat_id as s1, c2.seat_id as s2 from cinema c1
inner join cinema c2 on c1.seat_id+1 = c2.seat_id
where c1.free =1 and c2.free = 1)
select s1 from cte
union
select s2 from cte;

--------------------------------------------------------------

select * from employees;

select * from (
select *, ascii(email_id) as ascii_email,
rank() over(partition by email_id order by ascii(email_id) desc) as rnk from employees) result where rnk = 1;

---------------------------------------
"""


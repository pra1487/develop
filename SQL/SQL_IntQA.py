"""
select top 10 * from orders_data;

select top 10 order_id, order_date, customer_name, city from orders_data;

select * from orders_data order by order_date;

select * from orders_data order by order_date, profit;

select * from orders_data order by order_date desc, profit;
select * from orders_data order by order_date desc, profit desc;

select top 5 * from orders_data order by sales desc;

select * from orders_data where region = 'central';
select * from orders_data where order_date = '2019-11-22';

select * from orders_data where region = 'Central' and category = 'Technology';
select * from orders_data where region = 'Central' and category = 'Technology' and quantity>2;

select * from orders_data where region = 'Central' or category = 'Technology';

select * from orders_data
where (region = 'Central' or category='Technology') and quantity>5;


select * from orders_data where quantity>=3 and quantity<=5;
select * from orders_data where quantity between 3 and 5;
select * from orders_data where quantity in (3,4,5);
select * from orders_data where quantity=3 or quantity=4 or quantity=5;

--Pattern Matching:
-------------------
select * from orders_data where customer_name like 'A%';
select * from orders_data where customer_name like '%n';
select * from orders_data where customer_name like '[AB]%';
select * from orders_data where customer_name like '%[ed]';
select * from orders_data where customer_name like '_e%';
select * from orders_data where customer_name like '_[ea]%';
select * from orders_data where customer_name like '_[ea]%f';


--Aggregation:
--------------
select sum(sales) as total_sales from orders_data;
select max(sales) as max_sales from orders_data;
select min(sales) as min_sales from orders_data;
select round(AVG(sales),2) as avrg_sales from orders_data;
select (round(sum(sales)/count(*),2)) as avrg_sales from orders_data;
select count(*), count(order_id) from orders_data;

update orders_data set city = null where order_id in ('','');
select * from orders_data where city is null;
select * from orders_data where city is not null;


select count(distinct customer_name) as dist_customers from orders_data;
select distinct category, region from orders_data;
select count(distinct order_id) as order_id, count(distinct order_date) as order_date,
count(distinct city) as city, count(distinct category) as category from orders_data;

select category,count(*) as count from orders_data group by category;
select category, sum(sales) as total_sales from orders_data group by category;
select category, sum(sales) as total_sales, sum(profit) as total_profie from orders_data group by category;

select category, region, sum(sales) as total_sales, sum(profit) as total_profit from orders_data
group by category, region order by category;

select city, sum(sales) as total_sales from orders_data group by city having sum(sales) >= 100
order by total_sales desc;

select city, sum(sales) as total_sales from orders_data group by city having sum(sales) >= 100
order by total_sales desc;

--> from --> where --> group by --> having --> select columns --> order by
select city, sum(sales) as total_sales from orders_data
where region = 'central'
group by city
having sum(sales)>=100
order by total_sales desc;

-- give the list of orders which has return back;
select * from orders_data o
join
returns_data r on o.order_id = r.order_id;

--to fetch the sum of returned sales
select category, sum(sales) as total_returned_sales from orders_data o
join returns_data r on o.order_id=r.order_id group by category;

-- to fetch the return orders wich are with 'wrong items' and from city los-angeles
select * from orders_data o
join returns_data r on o.order_id=r.order_id
where return_reason = 'wrong items' and city = 'Los Angeles';


select * from orders_data o
left join returns_data r on o.order_id = r.order_id
where r.return_reason is null;

select o.*, r.* from orders_data o
join returns_data r on o.order_id=r.order_id
where r.return_reason is null;

--to fetch the total sales for each return_reason.
select r.return_reason, sum(o.sales) as total_sales from orders_data o
left join returns_data r on o.order_id=r.order_id group by r.return_reason;

select *,
case when profit<0 then 'loss'
	when profit>0 then 'Gain' else 'NLNG'
end as profit_Status from orders_data;

select customer_name, len(customer_name) as length from orders_data;

select *, left(order_id,2) as country from orders_data;
select *, left(order_id,2) as country, right(order_id,6) as id from orders_data;

select order_id, SUBSTRING(order_id,4,4) as year from orders_data;
select customer_name, replace(customer_name, 'C', 'P') as new_nm from orders_data;

select order_id, order_date, DATEPART(YEAR, order_date) as Year from orders_data;
select order_id, order_date, DATEPART(YEAR, order_date) as year, DATEPART(MONTH, order_date) as month from orders_data;
select order_id, order_date, DATEPART(QUARTER, order_date) as quarter from orders_data;
select DATEPART(MONTH, order_date) as Month, sum(sales) as total_sales from orders_data group by DATEPART(MONTH, order_date);

select DATEPART(YEAR, order_date) as year,DATENAME(month, order_date) as month_name, sum(sales) as total_sales
from orders_data group by DATEPART(YEAR, order_date),DATENAME(month, order_date);

select order_id, order_date, ship_date, DATEDIFF(DAY,order_date, ship_date) as time_taken from orders_data;

select order_id, order_date, DATEADD(DAY,5,order_date) as delivered_date from orders_data;


Q) Fetch the records from orders_data which are having customer second name starts with 'B'

	select Order_id, order_date, ship_date, customer_name, region, city from (
	select *, Trim(SUBSTRING(customer_name, CHARINDEX(' ', customer_name), len(customer_name))) as second_name from orders_data) result
	where second_name like 'B%'


------------------------------***************--------------------------------------------------

Interview Questions:
====================

1 Find the total playes and total wins from icc_world_cup table:

	with cte1 as
	(select Team_1 as Team_name, case when Team_1=Winner then 1 else 0 end as win_flag from icc_world_cup
	union all
	select Team_2 as Team_name, case when Team_2=Winner then 1 else 0 end as win_flag from icc_world_cup)
	select Team_name, count(win_flag) as Total_played, sum(win_flag) as total_wins from cte1 group by Team_name
	order by total_wins desc;


2  Fetch the number of new and repeat customers count for each date.

	with df1 as (
	select customer_id, min(order_date) as first_visit_date from customer_orders group by customer_id),
	df2 as (
	select distinct co.*,
	case when co.order_date = d1.first_visit_date then 1 else 0 end as fvflag,
	case when co.order_date <> d1.first_visit_date then 1 else 0 end as rpvflag from customer_orders co
	join df1 d1 on co.customer_id=d1.customer_id)
	select order_date, sum(fvflag) as New_cust_count, sum(rpvflag) as repeat_cust_count
	from df2 group by order_date;


3. Find the most visited floor and resources from entries.

	with cte1 as(
	select name, count(1) as total_visits, STRING_AGG(resources,',') as resource_used from entries group by name),
	cte2 as (
	select name, floor, count(1) as Total_Floor_Visit,
	row_number() over(partition by name order by count(1) desc) as rnum from entries group by name, floor)
	select cte1.name, cte1.total_visits, cte2.Total_Floor_Visit as Most_visited_floor, cte1.resource_used
	from cte2 join cte1 on cte2.name=cte1.name where rnum=1 order by name;


4. Fetch the total number of friends, total_friends score greater then 100.

	with Cte1 as(
	select f.Person_id, count(1) as Number_of_friends, sum(p.Score) as Total_friends_score from friend f join person p on f.friend_id=p.Person_id
	group by f.Person_id)
	select Cte1.Person_id, p.Name,p.email, Cte1.Number_of_friends,Cte1.Total_friends_score
	from Cte1 join person p on Cte1.Person_id = p.Person_id where Cte1.Total_friends_score>100;


5. Find the cancellation rate of requests with unbanned user (Both client and driver must not banned)

	select request_at, count(case when status in ('cancelled_by_client','cancelled_by_driver') then 1 else null end) as cancelled_Trips,
	count(1) as Total_trips
	from Trips t join Users u on t.client_id=u.users_id
	join users u1 on t.driver_id=u1.users_id
	where u.banned='No' and u1.banned='No'
	group by request_at;

	Or

	with df1 as (
	select t.*, u.banned as client_banned, u1.banned as driver_banned from Trips t
	join Users u on t.client_id=u.users_id
	join Users u1 on t.driver_id=u1.users_id)
	select request_at, count(case when status in ('cancelled_by_driver', 'cancelled_by_client') then 1 else null end) as Trips_cancelled,
	count(1) as total_trips from df1
	where client_banned='No' and driver_banned='No'
	group by request_at;

6. select * from players;
	select * from matches;

	with player_score as (
	select first_player as player_name, first_score as score from matches
	union
	select second_player as player_name, second_score as score from matches),
	final_scores as(
	select p.group_id, ps.player_name, sum(ps.score) as total_score from player_score ps
	join players p on ps.player_name=p.player_id group by p.group_id, ps.player_name),
	final_ranking as (
	select *, rank() over(partition by group_id order by total_score desc, player_name asc) as rnk from final_scores)
	select * from final_ranking where rnk=1;


7. select * from retail_users;
	select * from retail_orders;
	select * from retail_items;

	with cte1 as(
	select *, rank() over(partition by seller_id order by order_date) as rnum  from retail_orders),
	cte2 as(
	select c1.*, ri.item_brand from cte1 c1
	join retail_items ri on c1.item_id=ri.item_id where c1.rnum=2),
	cte3 as (
	select ru.user_id, c2.*, ru.favorite_brand from retail_users ru left join cte2 c2 on ru.user_id=c2.seller_id)
	select user_id,seller_id, order_date, item_id,item_brand,favorite_brand, buyer_id, case  when item_brand = favorite_brand then 'Yes' else 'No' end as new_col
	from cte3;


8.  select * from tasks;


	with cte1 as(
	select *, ROW_NUMBER() over(partition by state order by date_value) as rnum,
	dateadd(day, -1*ROW_NUMBER() over(partition by state order by date_value), date_value) as group_date from tasks
	)
	select min(date_value) as start_date, max(date_value) as end_date, state
	from cte1 group by group_date, state
	order by min(date_value);

9. select * from spending;

	with cte1 as(
	select spend_date, user_id, max(platform) as platform, sum(amount) as amount
	from spending
	group by spend_date,user_id having count(distinct platform)=1
	union all
	select spend_date,user_id,'both' as platform, sum(amount) as amount from spending
	group by spend_date,user_id having count(distinct platform)=2
	union all
	select spend_date,null as user_id, 'both' as platform, '0' as amount from spending)
	select spend_date, platform,sum(amount) as total_amount, count(distinct user_id) as total_users from cte1
	group by spend_date, platform
	order by spend_date, platform desc;

	or

	select spend_date, user_id, string_Agg(platform,',') as platform, count(distinct user_id) as total_users, sum(amount) as total_amt
	from spending group by spend_date, user_id order by spend_date;


10. How many matches were played by a each player and how many of these were batted and hw many were bowled.

	with df1 as (
	select distinct batter as player_name, matchid, 'batter' as player_type from ipl_cricket
	union
	select distinct bowler as player_name, matchid, 'bowler' as player_type from ipl_cricket)
	select player_name, count(distinct matchid) as total_played,
	sum(case when player_type = 'batter' then 1 else 0 end) as batting_matches,
	sum(case when player_type='bowler' then 1 else 0 end) as bowling_matches from df1
	group by player_name
	order by count(matchid) desc;


11. Identify user sessions. A session is identify as a sequence of activities by a user where the time difference between consecutive events
	is less than or equals to 30 mins. If time difference between two events exceeds 30 mins. its considered the start of a new session.


	with df1 as(
	select *, lag(event_time,1,event_time) over(partition by userid order by event_time) as prev_time,
	datediff(Minute,lag(event_time,1,event_time) over(partition by userid order by event_time), event_time) as time_diff from events),
	df2 as (select *,
	case when time_diff<=30 then 0 else 1 end as session_flag,
	sum(case when time_diff<=30 then 0 else 1 end) over(partition by userid order by event_time) as session_group from df1)
	select userid, min(event_time) as start_time, max(event_time) as end_time,
	count(*) as total_events,
	datediff(minute,min(event_time),max(event_time)) as session_duration
	from df2 group by userid, session_group order by userid, session_group;


12. select * from assessments;

	select experience, count(*) as total_candidates, sum(
	case when (case when sql is null or sql=100 then 1 else 0 end +
	case when algo is null or algo=100 then 1 else 0 end +
	case when bug_fixing is null or bug_fixing = 100 then 1 else 0 end) = 3 then 1 else 0 end) as toal_score_flag from assessments
	group by experience;

13. find the phone numbers which are not having any repeat numbers i.e. all the numbers in the phone number must be uniqe.

	select * from phone_numbers;

	with cte as(
	select *,substring(num,CHARINDEX('-',num)+1,len(num)) as new_num
	from phone_numbers)
	select new_num
	from cte
	cross apply STRING_SPLIT(CAST(CAST(CAST(new_num AS NVARCHAR) AS VARBINARY) AS VARCHAR), CHAR(0))
	group by new_num
	having count(distinct value)=count(value)


14.










"""
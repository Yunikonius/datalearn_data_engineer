# 1 этап - перенос данных из excel в stg-слой

Воспользовался подготовленными скриптами

# 2 этап - формирование таблиц измерений в dm-слое (схема datalearn)

```sql
--классы доставки, индексы не создаем (справочник маленький)
create table datalearn.ship_modes
(
	  id int generated always as identity
	, ship_mode_nm varchar(64) not null
);

insert into datalearn.ship_modes (ship_mode_nm)
select distinct ship_mode from stg.orders;

--клиенты, создаем индекс
create table datalearn.customers
(
	  id int generated always as identity
	, customer_cd varchar(16) not null
	, customer_name varchar(64) not null
);

insert into datalearn.customers (customer_cd, customer_name)
select distinct customer_id, customer_name from stg.orders;

create index idx_customers_id 
on datalearn.customers using btree(id);

--segment, без индекса
create table datalearn.segment
(
	  id int generated always as identity
	, segment_nm varchar(32) not null
);

insert into datalearn.segment (segment_nm)
select distinct segment from stg.orders;

--country
create table datalearn.country
(
	  id int generated always as identity
	, country varchar(32) not null
);

insert into datalearn.country (country)
select distinct country from stg.orders;

create index idx_country_id 
on datalearn.country using btree(id);

--city
create table datalearn.city
(
	  id int generated always as identity
	, city varchar(32) not null
);

insert into datalearn.city (city)
select distinct city from stg.orders;

create index idx_city_id 
on datalearn.city using btree(id);

--state
create table datalearn.state
(
	  id int generated always as identity
	, state varchar(32) not null
);

insert into datalearn.state (state)
select distinct state from stg.orders;

--region
create table datalearn.region
(
	  id int generated always as identity
	, region varchar(32) not null
);

insert into datalearn.region (region)
select distinct region from stg.orders;

--category
create table datalearn.product_category 
(
	  id int generated always as identity
	, category varchar(64) not null
);

insert into datalearn.product_category (category)
select distinct category from stg.orders;

--subcategory
create table product_subcategory
(
	  id int generated always as identity
	, subcategory varchar(64) not null
);

insert into datalearn.product_subcategory (subcategory)
select distinct subcategory from stg.orders;

--link between product_category and product_subcategory
create table datalearn.link_product_category_product_subcategory
(
	  id int generated always as identity
	, product_category_id int not null
	, product_subcategory_id int not null
);

insert into datalearn.link_product_category_product_subcategory (product_category_id, product_subcategory_id)
select distinct
  pc.id as product_category_id
, ps.id as product_subcategory_id
from stg.orders o
join datalearn.product_category pc 
on o.category = pc.category 
join datalearn.product_subcategory ps 
on o.subcategory = ps.subcategory;

--product
create table datalearn.product 
(
	  id int generated always as identity
	, product_cd varchar(32) not null
	, product_name varchar(256) not null
);

insert into datalearn.product (product_cd, product_name)
select distinct product_id, max(product_name) over(partition by product_id) from stg.orders;

select 
  product_cd 
, count(*)
from datalearn.product
group by product_cd 
having count(*) > 1;

--orders 
create table datalearn.orders 
(
	  id int generated always as identity
	, order_cd varchar(64)
	, order_date date
	, ship_date date
);

insert into datalearn.orders (order_cd, order_date, ship_date)
select distinct
  order_id
, order_date
, ship_date
from stg.orders
order by order_date asc; 

create index idx_order_id
on datalearn.orders using btree(id);

select order_cd, count(*)
from datalearn.orders
group by order_cd
having count(*) > 1;

--link between orders and products
create table datalearn.link_orders_products
(
	  id int generated always as identity
	, order_id int not null
	, product_id int not null
	, sales numeric
	, quantity int
	, discount numeric
	, profit numeric
);

insert into datalearn.link_orders_products (order_id, product_id, sales, quantity, discount, profit)
select distinct 
  oo.id as order_id
, p.id as product_id
, o.sales 
, o.quantity 
, o.discount 
, o.profit
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.product p 
on o.product_id = p.product_cd;

create index idx_link_orders_products_order_id
on datalearn.link_orders_products using btree(order_id);

create index idx_link_orders_products_product_id
on datalearn.link_orders_products using btree(product_id);

--postal codes
create table datalearn.postal_codes
(
	  id int generated always as identity
	, postal_code varchar(64)
);

insert into datalearn.postal_codes (postal_code)
select distinct postal_code from stg.orders
where postal_code is not null;

create index idx_postal_code_id
on datalearn.postal_codes using btree(id);

--link between orders and ship modes
create table datalearn.link_order_ship_mode
(
	  id int generated always as identity
	, order_id int not null
	, ship_mode_id int not null
);

insert into datalearn.link_order_ship_mode (order_id, ship_mode_id)
select distinct
  oo.id as order_id
, sm.id as ship_mode_id
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.ship_modes sm 
on o.ship_mode = sm.ship_mode_nm;

create index idx_link_order_ship_mode_order_id
on datalearn.link_order_ship_mode using btree(order_id);

--link between order and segment
create table datalearn.link_order_segment
(
	  id int generated always as identity
	, order_id int not null
	, segment_id int not null
);

insert into datalearn.link_order_segment (order_id, segment_id)
select distinct
  oo.id as order_id
, s.id as segment_id
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.segment s 
on o.segment = s.segment_nm;

create index idx_link_order_segment_order_id
on datalearn.link_order_segment using btree(order_id);

--link between order and country
create table datalearn.link_order_country
(
	  id int generated always as identity
	, order_id int not null
	, country_id int not null
);

insert into datalearn.link_order_country (order_id, country_id)
select distinct
  oo.id as order_id
, c.id as country_id
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.country c 
on o.country = c.country;

create index idx_link_order_country_order_id
on datalearn.link_order_country using btree(order_id);

create index idx_link_order_country_country_id
on datalearn.link_order_country using btree(country_id);

--link between order and city
create table datalearn.link_order_city
(
	  id int generated always as identity
	, order_id int not null
	, city_id int not null
);

insert into datalearn.link_order_city (order_id, city_id)
select distinct
  oo.id as order_id
, c.id as city_id
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.city c 
on o.city = c.city;

create index idx_link_order_city_order_id
on datalearn.link_order_city using btree(order_id);

create index idx_link_order_city_city_id
on datalearn.link_order_city using btree(city_id);

--link between order and state
create table datalearn.link_order_state
(
	  id int generated always as identity
	, order_id int not null
	, state_id int not null
);

insert into datalearn.link_order_state (order_id, state_id)
select distinct
  oo.id as order_id
, s.id as state_id
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.state s 
on o.state = s.state;

create index idx_link_order_state_order_id
on datalearn.link_order_city using btree(order_id);

--link between order and postal_code
create table datalearn.link_order_postal_cd
(
	  id int generated always as identity
	, order_id int not null
	, postal_cd_id int not null
);

insert into datalearn.link_order_postal_cd (order_id, postal_cd_id)
select distinct
  oo.id as order_id
, pc.id as postal_cd_id
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.postal_codes pc 
on o.postal_code = pc.postal_code;

create index idx_link_order_postal_cd_order_id
on datalearn.link_order_postal_cd using btree(order_id);

create index idx_link_order_postal_cd_postal_cd_id
on datalearn.link_order_postal_cd using btree(postal_cd_id);

--link between order and region
create table datalearn.link_order_region
(
	  id int generated always as identity
	, order_id int not null
	, region_id int not null
);

insert into datalearn.link_order_region (order_id, region_id)
select distinct
  oo.id as order_id
, r.id as region_id
from stg.orders o
join datalearn.orders oo 
on o.order_id = oo.order_cd
join datalearn.region r 
on o.region = r.region;

create index idx_link_order_region_order_id
on datalearn.link_order_region using btree(order_id);
```




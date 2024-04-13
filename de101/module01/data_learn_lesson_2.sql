---------------
--** step 1 creating tables in dds-schema
---------------

--1 dict_country +

CREATE TABLE data_learn_dds.dict_country
(
 country_id   serial NOT null primary key,
 country_name text NOT NULL
);

create index idx_dict_country_country_id on data_learn_dds.dict_country using btree (country_id);

insert into data_learn_dds.dict_country (country_name)
select distinct country from data_learn_stg.orders;  

--2 dict_customer_segments +

CREATE TABLE data_learn_dds.dict_customer_segments
(
 customer_segment_id   serial NOT null primary key,
 customer_segment_name text NOT NULL
);

create index idx_dict_customer_segments_customer_segment_id on data_learn_dds.dict_customer_segments using btree (customer_segment_id);

insert into data_learn_dds.dict_customer_segments (customer_segment_name)
select distinct segment from data_learn_stg.orders;

--3 dict_product_category +

CREATE TABLE data_learn_dds.dict_product_category
(
 product_category_id   serial NOT null primary key,
 product_category_name text NOT NULL
);

create index idx_dict_product_category_product_category_id on data_learn_dds.dict_product_category using btree (product_category_id);

insert into data_learn_dds.dict_product_category (product_category_name)
select distinct category from data_learn_stg.orders;

--4 dict_product_sub_category +

CREATE TABLE data_learn_dds.dict_product_sub_category
(
 product_sub_category_id   serial NOT NULL,
 product_sub_category_name text NOT NULL,
 product_category_id       int4 not null references data_learn_dds.dict_product_category(product_category_id)
);

alter table data_learn_dds.dict_product_sub_category add primary key (product_sub_category_id);

CREATE INDEX idx_dict_product_sub_category_product_sub_category_id ON data_learn_dds.dict_product_sub_category
using btree (product_sub_category_id);

insert into data_learn_dds.dict_product_sub_category (product_sub_category_name, product_category_id)
select distinct o.subcategory, c.product_category_id 
from data_learn_stg.orders o
left join data_learn_dds.dict_product_category c 
on o.category = c.product_category_name;

--5 dict_property +

CREATE TABLE data_learn_dds.dict_property
(
 property_id   serial primary key,
 property_name text NOT NULL
);

CREATE INDEX idx_dict_property_property_id ON data_learn_dds.dict_property
using btree (property_id);

insert into data_learn_dds.dict_property (property_name)
values
--('order_postal_code'),
--('order_return_flag'),
('order_date'),
('order_ship_date');

--6 dict_region +

CREATE TABLE data_learn_dds.dict_region
(
 region_id   serial primary key,
 region_name text NOT NULL
);

CREATE INDEX idx_dict_region_region_id ON data_learn_dds.dict_region
using btree (region_id);

insert into data_learn_dds.dict_region (region_name)
select distinct region from data_learn_stg.orders;

--7 dict_ship_modes +

CREATE TABLE data_learn_dds.dict_ship_modes
(
 ship_mode_id   serial primary key,
 ship_mode_name text NOT NULL
);

CREATE INDEX idx_dict_ship_modes_ship_mode_id ON data_learn_dds.dict_ship_modes
using btree (ship_mode_id);

insert into data_learn_dds.dict_ship_modes (ship_mode_name)
select distinct ship_mode from data_learn_stg.orders; 

--8 dict_state +

CREATE TABLE data_learn_dds.dict_state
(
 state_id   serial primary key,
 state_name text NOT NULL
);

CREATE INDEX idx_dict_state_state_id ON data_learn_dds.dict_state
using btree (state_id);

insert into data_learn_dds.dict_state (state_name)
select distinct state from data_learn_stg.orders;

--9 employee +

CREATE TABLE data_learn_dds.employee
(
 employee_id   serial primary key,
 employee_name text NOT NULL,
 region_id     int4 references data_learn_dds.dict_region(region_id)
);

CREATE INDEX idx_employee_employee_id ON data_learn_dds.employee
using btree (employee_id);

insert into data_learn_dds.employee (employee_name, region_id)
select distinct p.person, dr.region_id 
from data_learn_stg.orders o
left join data_learn_stg.people p 
on o.region = p.region
left join data_learn_dds.dict_region dr
on o.region = dr.region_name;

--10 order +

CREATE TABLE data_learn_dds.orders
(
 order_id    serial primary key,
 order_num   text   not null
);

CREATE INDEX idx_order_order_id ON data_learn_dds.orders
using btree (order_id);

insert into data_learn_dds.order (order_num)
select distinct order_id from data_learn_stg.orders;

--11 products +

CREATE TABLE data_learn_dds.products
(
 product_id              serial primary key,
 product_src_id          text NOT NULL,
 product_name            text NOT NULL,
 product_sub_category_id int references data_learn_dds.dict_product_sub_category (product_sub_category_id)
);

CREATE INDEX idx_products_product_id ON data_learn_dds.products
using btree (product_id);

insert into data_learn_dds.products (product_src_id, product_name, product_sub_category_id)
select distinct o.product_id, array_to_string(array_agg(distinct o.product_name), ' // '), c.product_sub_category_id 
from data_learn_stg.orders o
left join data_learn_dds.dict_product_sub_category c 
on o.subcategory = c.product_sub_category_name
group by o.product_id, c.product_sub_category_id;

--12 dict_city +

CREATE TABLE data_learn_dds.dict_city
(
 city_id    serial primary key,
 city_name  text NOT NULL,
 country_id int references data_learn_dds.dict_country(country_id),
 state_id   int references data_learn_dds.dict_state(state_id)
);

create index idx_dict_city_city_id on data_learn_dds.dict_city using btree (city_id);

insert into data_learn_dds.dict_city (city_name, country_id, state_id)
select distinct o.city, dc.country_id, ds.state_id
from data_learn_stg.orders o 
left join data_learn_dds.dict_country dc 
on o.country = dc.country_name 
left join data_learn_dds.dict_state ds 
on o.state = ds.state_name; 

--13 customers +

CREATE TABLE data_learn_dds.customers
(
 customer_id     serial primary key,
 customer_src_id text NOT NULL,
 customer_name   text NOT NULL
);

create index idx_customers_customer_id on data_learn_dds.customers using btree (customer_id);

insert into data_learn_dds.customers (customer_src_id, customer_name)
select distinct customer_id, customer_name
from data_learn_stg.orders;

--14 order_properties

create table data_learn_dds.order_properties
(
	link_id serial primary key,
	order_id int references data_learn_dds.orders(order_id),
	property_id int references data_learn_dds.dict_property(property_id),
	value_int int,
	value_numeric numeric,
	value_date date,
	value_text text
);

create index idx_order_properties_order_id on data_learn_dds.order_properties using btree (order_id);
create index idx_order_properties_property_id on data_learn_dds.order_properties using btree (property_id);

insert into data_learn_dds.order_properties (order_id, property_id, value_date)
select distinct o1.order_id, dp.property_id, o.order_date 
from data_learn_stg.orders o
left join data_learn_dds.orders o1
on o.order_id = o1.order_num
left join data_learn_dds.dict_property dp 
on dp.property_name = 'order_date'; 

insert into data_learn_dds.order_properties (order_id, property_id, value_date)
select distinct o1.order_id, dp.property_id, o.ship_date 
from data_learn_stg.orders o
left join data_learn_dds.orders o1
on o.order_id = o1.order_num
left join data_learn_dds.dict_property dp 
on dp.property_name = 'order_ship_date';

insert into data_learn_dds.order_properties (order_id, property_id, value_text)
select distinct o1.order_id, dp.property_id, o.postal_code
from data_learn_stg.orders o
left join data_learn_dds.orders o1
on o.order_id = o1.order_num
left join data_learn_dds.dict_property dp 
on dp.property_name = 'order_postal_code';

--15 link between order and ship mode +

create table data_learn_dds.link_order_ship_mode 
(
	link_id serial primary key,
	order_id int references data_learn_dds.orders(order_id),
	ship_mode_id int references data_learn_dds.dict_ship_modes(ship_mode_id)
);

create index idx_link_order_ship_mode_order_id on data_learn_dds.link_order_ship_mode using btree(order_id);
create index idx_link_order_ship_mode_ship_mode_id on data_learn_dds.link_order_ship_mode using btree(ship_mode_id);

insert into data_learn_dds.link_order_ship_mode
(order_id, ship_mode_id)
select distinct oo.order_id, sm.ship_mode_id 
from data_learn_stg.orders o
left join data_learn_dds.orders oo 
on o.order_id = oo.order_num 
left join data_learn_dds.dict_ship_modes sm 
on o.ship_mode = sm.ship_mode_name;

--16 link between order and customer +

create table data_learn_dds.link_order_customer 
(
	link_id serial primary key,
	order_id int references data_learn_dds.orders(order_id),
	customer_id int references data_learn_dds.customers(customer_id)
);

create index idx_link_order_customer_order_id on data_learn_dds.link_order_customer using btree(order_id);
create index idx_link_order_customer_customer_id on data_learn_dds.link_order_customer using btree(customer_id);

insert into data_learn_dds.link_order_customer (order_id, customer_id)
select distinct oo.order_id, c.customer_id 
from data_learn_stg.orders o 
left join data_learn_dds.orders oo 
on o.order_id = oo.order_num 
left join data_learn_dds.customers c 
on o.customer_id = c.customer_src_id; 

--17 link between order and product +

create table data_learn_dds.link_order_product 
(
	link_id serial primary key,
	order_id int references data_learn_dds.orders(order_id),
	product_id int references data_learn_dds.products(product_id),
	sales numeric not null,
	quantity int not null,
	discount numeric(3,2) not null,
	profit numeric not null
);

create index idx_link_order_product_order_id on data_learn_dds.link_order_product using btree(order_id);
create index idx_link_order_product_product_id on data_learn_dds.link_order_product using btree(product_id);

insert into data_learn_dds.link_order_product (order_id, product_id, sales, quantity, discount, profit)
select oo.order_id, p.product_id, o.sales, o.quantity, o.discount, o.profit
from data_learn_stg.orders o 
left join data_learn_dds.orders oo 
on o.order_id = oo.order_num 
left join data_learn_dds.products p 
on o.product_id = p.product_src_id;
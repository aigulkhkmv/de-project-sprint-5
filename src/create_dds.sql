-- create couriers table for DDS table
CREATE TABLE dds.dm_couriers (
	id serial4 NOT NULL PRIMARY KEY,
	courier_id varchar NOT NULL,
	courier_name text NOT NULL,
	CONSTRAINT dm_couriers_courier_id_uindex UNIQUE (courier_id)
);

-- create delivery table for DDS data
CREATE TABLE dds.dm_delivery (
	id serial4 NOT NULL PRIMARY KEY,
	delivery_id varchar NOT NULL,
	order_id varchar NOT NULL,
	courier_id int NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT null,
	rate int NOT NULL,
	sum int NOT NULL,
	tip_sum int NOT NULL,
	CONSTRAINT dm_deliveries_delivery_id_uindex UNIQUE (delivery_id),
	CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id),
    CONSTRAINT dm_deliveries_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id)
);

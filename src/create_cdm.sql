CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year date NOT NULL,
	settlement_month int NOT NULL, 
	orders_count int4 NOT NULL DEFAULT 0,
	orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
	rate_avg int4,
	order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
	courier_order_sum numeric(14, 2) DEFAULT 0,
	courier_tips_sum numeric(14, 2) DEFAULT 0,
	courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
	CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (((settlement_month >= 1) AND (settlement_month < 13)))
);

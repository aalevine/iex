/*
public.orders DDL
*/


DROP TABLE IF EXISTS public.orders;

CREATE TABLE IF NOT EXISTS public.orders (
	stock_code			VARCHAR(5) 		NOT NULL,
	order_date			DATE 			NOT NULL,
	order_type			VARCHAR(4)		NOT NULL,
	trading_strategy	VARCHAR(255)	NOT NULL,
	updated_at			TIMESTAMP 		NULL,
	PRIMARY KEY (stock_code, order_date)
);


/*
-- INDEXES WEREN'T BEING UTILIZED
-- Create index on join keys
DROP INDEX IF EXISTS orders_stock_code_order_date_idx
CREATE INDEX orders_stock_code_order_date_idx ON public.orders (stock_code, order_date)
*/


-- Permissions
GRANT SELECT ON public.orders TO data_science;
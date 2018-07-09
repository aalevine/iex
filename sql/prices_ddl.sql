/*
public.prices DDL
*/


DROP TABLE IF EXISTS public.prices;

CREATE TABLE IF NOT EXISTS public.prices (
	stock_code			VARCHAR(5) 	NOT NULL,
	date				DATE 		NOT NULL,
	close				REAL	 	NOT NULL,
	updated_at			TIMESTAMP 	NULL,
	PRIMARY KEY (stock_code, date)
);


/*
-- INDEXES WEREN'T BEING UTILIZED
-- Create index on join keys
DROP INDEX IF EXISTS prices_stock_code_date_idx
CREATE INDEX prices_stock_code_date_idx ON public.prices (stock_code, date)
*/


-- Permissions
GRANT SELECT ON public.prices TO data_science;
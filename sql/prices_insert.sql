/*
public.prices INSERT
*/


-- Staging table for recent execution
DROP TABLE IF EXISTS staging.prices;

CREATE TABLE IF NOT EXISTS staging.prices (
	stock_code 			VARCHAR,
	date				DATE NOT NULL,
	close				REAL NOT NULL,
	PRIMARY KEY (stock_code, date)
);


-- Populate staging table from JSON table
INSERT INTO staging.prices 
SELECT p.*
FROM staging.prices_json l
CROSS JOIN LATERAL 
	json_populate_recordset(null::staging.prices, doc) AS p
;


-- Filter staging table to include only new records (that don't exist in prod)
DELETE
FROM staging.prices 
WHERE (stock_code, date) IN (
		SELECT stock_code, date
		FROM public.prices
	)
;


-- Final insert into prod table
INSERT INTO public.prices
SELECT
	*
	, CURRENT_TIMESTAMP AS updated_at 
FROM staging.prices
;


-- Update statistics
ANALYZE public.prices
;
/*
public.company INSERT
*/


-- Staging table for recent execution
DROP TABLE IF EXISTS staging.company;

CREATE TABLE IF NOT EXISTS staging.company (
	stock_code 			VARCHAR(5) 		PRIMARY KEY,
	company_name		VARCHAR(255) 	NOT NULL,
	exchange			VARCHAR(255) 	NOT NULL,
	sector				VARCHAR(255) 	NOT NULL,
	industry			VARCHAR(255) 	NOT NULL
);


-- Populate staging table from JSON table
INSERT INTO staging.company 
SELECT p.*
FROM staging.company_json l
CROSS JOIN LATERAL 
	json_populate_recordset(null::staging.company, doc) AS p
;


-- Filter staging table to include only new records (that don't exist in prod)
DELETE
FROM staging.company 
WHERE stock_code IN (
		SELECT stock_code
		FROM public.company
	)
;


-- Final insert into prod table
INSERT INTO public.company
SELECT 
	*
	, CURRENT_TIMESTAMP AS updated_at 
FROM staging.company
;


-- Update statistics
ANALYZE public.company
;
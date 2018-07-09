/*
public.company DDL
*/


DROP TABLE IF EXISTS public.company;

CREATE TABLE IF NOT EXISTS public.company (
	stock_code			VARCHAR(5) 		PRIMARY KEY,
	company_name		VARCHAR(255)	NOT NULL,
	exchange			VARCHAR(255)	NOT NULL,
	sector				VARCHAR(255)	NOT NULL,
	industry			VARCHAR(255)	NOT NULL,
	updated_at			TIMESTAMP 		NULL
);


-- Create index on join key
DROP INDEX IF EXISTS company_stock_code_idx;
CREATE INDEX company_stock_code_idx ON public.company (stock_code);


-- Permissions
GRANT SELECT ON public.company TO data_science;
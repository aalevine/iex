/*
public.orders INSERT

Insert new orders (buy/sell/hold) based on SMA (45d) and LMA (180d) movement
	-When the SMA crosses the LMA in an upward direction, create a ‘buy’ order
	-When the SMA crosses the LMA in a downward direction, create a ‘sell’ order
	-If there is insufficient data, or the SMA and LMA are equal, create a ‘hold’ order
*/


-- Staging table for recent execution
DROP TABLE IF EXISTS staging.orders;

CREATE TABLE IF NOT EXISTS staging.orders (
	stock_code		VARCHAR(5) 	NOT NULL,
	date			DATE 		NOT NULL,
	close			REAL		NOT NULL,
	sma				REAL,
	lma				REAL,
	sma_diff		REAL,
	sma_diff_lag	REAL,	
	order_type		VARCHAR(5),
	PRIMARY KEY (stock_code, date)
);


INSERT INTO staging.orders
SELECT 
	stock_code
	, date 
	, close 
	, CASE 
		WHEN days_of_data >= 45
		THEN sma ELSE NULL 
		END AS sma
	, CASE 
		WHEN days_of_data >= 180
		THEN lma ELSE NULL 
		END AS lma
	, CASE 
		WHEN days_of_data >= 180
		THEN sma - lma ELSE NULL 
		END AS sma_diff
	, LAG(CASE 
		WHEN days_of_data >= 180
		THEN sma - lma ELSE NULL 
		END) OVER (
			PARTITION BY stock_code
			ORDER BY date
		) AS sma_diff_lag
FROM (
		SELECT
			stock_code
			, date 
			, close
			-- 45 day moving average
			, AVG(close) OVER ( 
				PARTITION BY stock_code 
				ORDER BY date 
					ROWS BETWEEN 44 PRECEDING AND CURRENT ROW
				) AS sma
			-- 180 day moving average
			, AVG(close) OVER ( 
				PARTITION BY stock_code 
				ORDER BY date 
					ROWS BETWEEN 179 PRECEDING AND CURRENT ROW
				) AS lma
			-- days of available data for each stock
			, COUNT(*) OVER (
				PARTITION BY stock_code 
				ORDER BY date 
					ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				) AS days_of_data
		FROM public.prices
		) x
;


-- apply Golden Cross trading strategy
UPDATE staging.orders 
SET order_type = 
CASE 
	WHEN 
		sma_diff > 0 AND
		sma_diff_lag < 0
	THEN 'buy'
	WHEN 
		sma_diff < 0 AND 
		sma_diff_lag > 0
	THEN 'sell'
	ELSE 'hold'
	END
;


-- Filter staging table to include only new records (that don't exist in prod)
DELETE
FROM staging.orders 
WHERE (stock_code, date) IN (
		SELECT stock_code, order_date
		FROM public.orders
	)
;


-- Final insert into prod table
INSERT INTO public.orders
SELECT 
	stock_code
	, date AS order_date
	, order_type
	, 'Simple Golden Cross' AS trading_strategy -- our only current strategy, hard-coded for now
	, CURRENT_TIMESTAMP AS updated_at
FROM staging.orders	
;


-- Update statistics
ANALYZE public.orders
;
-- Drop all staging tables

-- company
DROP TABLE IF EXISTS staging.company_json;
DROP TABLE IF EXISTS staging.company;

-- prices
DROP TABLE IF EXISTS staging.prices_json;
DROP TABLE IF EXISTS staging.prices;

-- orders
DROP TABLE IF EXISTS staging.orders;
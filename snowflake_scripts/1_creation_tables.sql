-- Use ACCOUNTADMIN to ensure you have permissions to create everything
USE ROLE ACCOUNTADMIN;

-- Create a small 'engine' to run your tasks (XSMALL saves credits)
CREATE OR REPLACE WAREHOUSE CRYPTO_WH 
  WAREHOUSE_SIZE = 'XSMALL' 
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE;

-- Create the Database and Schema
CREATE OR REPLACE DATABASE CRYPTO_DB;
CREATE OR REPLACE SCHEMA CRYPTO_DB.TRANSFORMED;

-- Create the table for your pipeline to land data into
CREATE OR REPLACE TABLE CRYPTO_DB.TRANSFORMED.CRYPTO_HISTORICAL_DATA (
    id STRING,
    symbol STRING,
    name STRING,
    current_price FLOAT,
    market_cap FLOAT,
    market_cap_rank INT,
    total_volume FLOAT,
    high_24h FLOAT,
    low_24h FLOAT,
    price_change_24h FLOAT,
    last_updated TIMESTAMP
);


CREATE OR REPLACE TABLE CRYPTO_INTRA_DAY_DATA (
    id STRING,
    symbol STRING,
    name STRING,
    current_price FLOAT,
    market_cap FLOAT,
    total_volume FLOAT,
    price_change_percentage_24h FLOAT,
    circulating_supply FLOAT,
    total_supply FLOAT,
    ath FLOAT, -- All Time High
    atl FLOAT, -- All Time Low
    last_updated TIMESTAMP
);
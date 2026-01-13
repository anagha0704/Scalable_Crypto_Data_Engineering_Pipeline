-- Step 1: Make sure you are in the right context
USE DATABASE CRYPTO_DB;
USE SCHEMA TRANSFORMED;

-- Step 2: Create the Stage in the RAW schema
-- Replace the URL with your actual CRYPTO_DB.TRANSFORMED.MY_S3_STAGES3 bucket path
CREATE OR REPLACE STAGE MY_S3_STAGE
URL = 's3://YOUR_BUCKET/YOUR_PATH/'
STORAGE_INTEGRATION = CRYPTO_S3_INT
FILE_FORMAT = (TYPE = PARQUET);


-- Step 3: Create the Pipe in the RAW schema
CREATE OR REPLACE PIPE CRYPTO_DB.TRANSFORMED.MYPIPE_HISTORICAL
  AUTO_INGEST = FALSE
  AS
  COPY INTO CRYPTO_DB.TRANSFORMED.CRYPTO_HISTORICAL_DATA (
      ID,
      SYMBOL,
      NAME,
      CURRENT_PRICE,
      MARKET_CAP,
      MARKET_CAP_RANK,
      LAST_UPDATED
  )
  FROM (
    SELECT 
      $1:coin_id::STRING,
      $1:coin_symbol::STRING,
      $1:coin_name::STRING,
      $1:price::FLOAT,
      $1:market_cap::NUMBER,      -- Explicitly cast and map
      $1:market_cap_rank::NUMBER, -- Explicitly cast and map
      $1:timestamp::TIMESTAMP
    FROM @CRYPTO_DB.TRANSFORMED.MY_S3_STAGE
  )
  FILE_FORMAT = (TYPE = 'PARQUET');
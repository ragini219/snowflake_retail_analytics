-- Curated table skeleton for flattened customer records
CREATE TABLE IF NOT EXISTS CURATED.CUSTOMERS_CURATED AS
SELECT
    RAW:"customer_id"::NUMBER AS CUSTOMER_ID,
    RAW:"first_name"::STRING AS FIRST_NAME,
    RAW:"last_name"::STRING AS LAST_NAME,
    RAW:"email"::STRING AS EMAIL,
    RAW:"city"::STRING AS CITY,
    RAW:"country"::STRING AS COUNTRY,
    FILE_NAME,
    LOAD_TIME
FROM CUSTOMERS_RAW
LIMIT 0;

-- Dynamic table for curated orders data with automatic refresh from RAW layer
CREATE OR REPLACE DYNAMIC TABLE CURATED.ORDERS_CURATED
  TARGET_LAG = '1 HOUR'  -- how often Snowflake will refresh
  WAREHOUSE = COMPUTE_WH  -- used during refresh
  COMMENT = 'Curated Orders table automatically refreshed from RAW layer'
AS
SELECT
  RAW:"order_id"::NUMBER       AS ORDER_ID,
  RAW:"customer_id"::NUMBER    AS CUSTOMER_ID,
  RAW:"order_date"::DATE       AS ORDER_DATE,
  RAW:"total_amount"::FLOAT    AS TOTAL_AMOUNT,
  RAW:"status"::STRING         AS STATUS,
  FILE_NAME,
  LOAD_TIME
FROM RAW.ORDERS_RAW;
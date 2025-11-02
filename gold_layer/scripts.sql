-- Create a dynamic table to aggregate order metrics per customer
CREATE OR REPLACE DYNAMIC TABLE GOLD.ORDER_METRICS
  TARGET_LAG = '1 HOUR'
  WAREHOUSE = COMPUTE_WH
AS
SELECT 
  CUSTOMER_ID,
  COUNT(DISTINCT ORDER_ID) AS TOTAL_ORDERS,
  SUM(TOTAL_AMOUNT) AS TOTAL_SPEND
FROM CURATED.ORDERS_CURATED
GROUP BY CUSTOMER_ID;


-- Create a table to store customer churn history
CREATE OR REPLACE TABLE GOLD.CUSTOMER_CHURN_HISTORY (
    RUN_DATE DATE,                 -- Date of the churn calculation
    PERIOD_DAYS NUMBER(5,0),       -- Time period (e.g., 30 days, 60 days)
    TOTAL NUMBER(10,0),            -- Total customers in that period
    CHURNED NUMBER(10,0),          -- Number of customers churned
    RATE NUMBER(5,2)               -- Churn rate percentage
);
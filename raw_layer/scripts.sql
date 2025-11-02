-- Create a raw ingestion table to hold semi-structured customer data

CREATE OR REPLACE TABLE CUSTOMERS_RAW (
  RAW VARIANT,                               -- Stores JSON payload
  FILE_NAME STRING,                          -- Source file name for audit
  LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- Capture load time
);


CREATE OR REPLACE TABLE ORDERS_RAW (
  RAW VARIANT,                               -- Stores JSON payload
  FILE_NAME STRING,                          -- Source file name for audit
  LOAD_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP() -- Capture load time
);

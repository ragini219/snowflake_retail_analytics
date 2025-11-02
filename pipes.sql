-- Create a Snowpipe to automatically load customer data from the GCS stage
CREATE OR REPLACE PIPE CUSTOMERS_PIPE
  AUTO_INGEST = FALSE                                  -- Disable auto-ingest for manual control
  COMMENT = 'Loads order data from GCS stage into RAW.CUSTOMERS_RAW with file metadata'  -- Description of the pipe
AS
COPY INTO RAW.CUSTOMERS_RAW (RAW, FILE_NAME, LOAD_TIME)  -- Target table and columns for loading data
FROM (
    SELECT
        $1 AS RAW,                                   -- JSON payload from the source file
        METADATA$FILENAME AS FILE_NAME,              -- Source file name for tracking
        CURRENT_TIMESTAMP() AS LOAD_TIME              -- Timestamp of the load operation
    FROM @retail_stage/customers                        -- Source data from the customers path in the stage
)
FILE_FORMAT = (FORMAT_NAME = retail_json_fmt)          -- Specify the file format for the COPY operation
ON_ERROR = 'CONTINUE';                                 -- Continue on error instead of failing

-- Create a Snowpipe to automatically load order data from the GCS stage
CREATE OR REPLACE PIPE ORDERS_PIPE
  AUTO_INGEST = FALSE                                  -- Disable auto-ingest for manual control
  COMMENT = 'Loads order data from GCS stage into RAW.ORDERS_RAW with file metadata'  -- Description of the pipe
AS
COPY INTO RAW.ORDERS_RAW (RAW, FILE_NAME, LOAD_TIME)  -- Target table and columns for loading data
FROM (
    SELECT
        $1 AS RAW,                                   -- JSON payload from the source file
        METADATA$FILENAME AS FILE_NAME,              -- Source file name for tracking
        CURRENT_TIMESTAMP() AS LOAD_TIME              -- Timestamp of the load operation
    FROM @retail_stage/orders                           -- Source data from the orders path in the stage
)
FILE_FORMAT = (FORMAT_NAME = retail_json_fmt)          -- Specify the file format for the COPY operation
ON_ERROR = 'CONTINUE';                                 -- Continue on error instead of failing


-- Create or replace a Snowflake task for customer data processing

CREATE OR REPLACE TASK CUSTOMERS_TASK
WAREHOUSE = COMPUTE_WH                           -- Specify compute warehouse to run the task
SCHEDULE = 'USING CRON 0 6 * * * EST'           -- Schedule task to run daily at 6 AM EST
COMMENT = 'Moves new records from RAW -> CURATED layer'
AS

-- Merge operation to sync data between RAW and CURATED layers
MERGE INTO CURATED.CUSTOMERS_CURATED AS tgt     -- Target table in CURATED schema
USING (
    -- Source query that reads from stream and transforms RAW JSON data
    SELECT
        RAW:"customer_id"::STRING AS CUSTOMER_ID,  -- Convert JSON fields to strongly-typed columns
        RAW:"first_name"::STRING AS FIRST_NAME,
        RAW:"last_name"::STRING AS LAST_NAME,
        RAW:"email"::STRING AS EMAIL,
        RAW:"city"::STRING AS CITY,
        RAW:"country"::STRING AS COUNTRY,
        FILE_NAME,                                 -- Metadata columns
        LOAD_TIME
    FROM CUSTOMER_STREAM                          -- Stream captures changes in source table
    WHERE METADATA$ACTION = 'INSERT'              -- Only process new insertions
) AS src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID             -- Join condition for merge

-- Update existing records with new values
WHEN MATCHED THEN UPDATE SET
    tgt.FIRST_NAME = src.FIRST_NAME,
    tgt.LAST_NAME  = src.LAST_NAME,
    tgt.EMAIL      = src.EMAIL,
    tgt.CITY       = src.CITY,
    tgt.COUNTRY    = src.COUNTRY,
    tgt.FILE_NAME  = src.FILE_NAME,
    tgt.LOAD_TIME  = src.LOAD_TIME

-- Insert new records that don't exist in target
WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, CITY, COUNTRY, FILE_NAME, LOAD_TIME
)
VALUES (
    src.CUSTOMER_ID, src.FIRST_NAME, src.LAST_NAME, src.EMAIL, src.CITY, src.COUNTRY, src.FILE_NAME, src.LOAD_TIME
);


-- Activate the task after creation
ALTER TASK CUSTOMERs_TASK RESUME;

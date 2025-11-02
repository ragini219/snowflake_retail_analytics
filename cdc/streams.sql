-- Create a stream to capture changes (inserts, updates, deletes) on the CUSTOMERS_RAW table
-- Streams are used for Change Data Capture (CDC) and provide a log of modifications
-- If stream already exists, it will be replaced due to OR REPLACE clause

CREATE OR REPLACE STREAM CUSTOMER_STREAM
ON TABLE CUSTOMERS_RAW;

SELECT * FROM CUSTOMER_STREAM;
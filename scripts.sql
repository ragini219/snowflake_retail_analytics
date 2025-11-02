-- SQL script for setting up the Retail Analytics database and its components

CREATE DATABASE IF NOT EXISTS RETAIL_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RETAIL_ANALYTICS.RAW;
CREATE SCHEMA IF NOT EXISTS RETAIL_ANALYTICS.CURATED;
CREATE SCHEMA IF NOT EXISTS RETAIL_ANALYTICS.DISCOVERY;

-- Create a file format for JSON files
CREATE OR REPLACE FILE FORMAT retail_json_fmt
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

-- Create a storage integration to connect Snowflake with Google Cloud Storage (GCS)
CREATE OR REPLACE STORAGE INTEGRATION GCS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = GCS
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://retail_analytics_snowflake_2025')
    COMMENT = 'Integration between Snowflake and GCS bucket';

-- Create a notification integration with GCP Pub/Sub
-- Used to enable Snowpipe auto-ingest when a new file lands in GCS
CREATE OR REPLACE NOTIFICATION INTEGRATION gcs_notify_int
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = TRUE
  GCP_PUBSUB_SUBSCRIPTION_NAME = 'projects/gcp-bigquery-pipeline/subscriptions/snowpipe-sub';

/*
For automatic refresh, a notification integration must be configured in Snowflake 
to support auto-ingest via Snowpipe. When a new file lands in the GCS bucket, 
a Pub/Sub topic associated with that bucket will publish an event notification. 
This topic will then be linked to a Pub/Sub subscription, which Snowflake listens 
to through the configured notification integration. Upon receiving the event, 
Snowflake automatically triggers the COPY INTO operation in the associated 
Snowpipe, loading the new data into the target table in near real time.

Currently, the Snowflake account is deployed in the AWS_AP_SOUTHEAST_3 region, 
while the GCS bucket resides in the US multi-region (GCP). Since cross-cloud 
notification integrations are not supported natively, this Pub/Sub–based event 
integration will be implemented in the next phase. For now, file ingestion can 
be handled manually using COPY INTO commands or scheduled Snowflake Tasks.
*/

-- Create a stage to access files in the GCS bucket
CREATE OR REPLACE STAGE retail_stage
    URL = 'gcs://retail_analytics_snowflake_2025/'  -- URL of the GCS bucket
    STORAGE_INTEGRATION = GCS_INTEGRATION            -- Use the defined storage integration
    FILE_FORMAT = (FORMAT_NAME = retail_json_fmt);   -- Specify the file format for the stage


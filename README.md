
This project demonstrates an end-to-end data engineering pipeline built on Snowflake, featuring data ingestion, transformation, and analytics for retail customer insights.
It highlights Snowpipe, Streams, Tasks, and Stored Procedures to automate incremental data processing and customer churn analysis.

Data Flow:

1️⃣ JSON files land in Google Cloud Storage

2️⃣ Snowpipe loads new files into the RAW layer

3️⃣ Streams + Tasks / Dynamic Tables incrementally merge into the CURATED layer

4️⃣ Stored Procedure computes churn metrics

5️⃣ Results are logged in the GOLD layer and refreshed daily

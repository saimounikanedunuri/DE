Large pharma companies:
Manufacture drugs in multiple plants
Sell across regions & distributors
Store data in different formats (CSV, JSON, Parquet)
Process TBs of data daily

But here’s the real pain 👇
Silent Quality & Revenue Leakage in Pharma Sales Pipelines
What’s going wrong?
Pipelines run successfully
No job failures
No alerts 🚫
BUT data is silently corrupted
Quantity sold > quantity produced
Expired medicines being sold
Storage temperature violations
QC failures still entering analytics
Business teams discover issues weeks later
Finance reports mismatch
Regulatory audits fail
Revenue leakage
Compliance risks (FDA, CDSCO)

💥 Root cause
Most pipelines focus on:
❌ Technical success
❌ Not business correctness

Impact Area -	Real Impact
Compliance	- FDA audits can shut plants
Finance	- Wrong revenue numbers
Operations -	Overstock / stockouts
Brand	- Patient safety risk
--------------------------------------------------------------
Requirement
Design a config-driven, scalable pharma data pipeline that:
Detects silent data corruption
Enforces business data quality rules
Works on TB-scale data
Produces trusted analytics-ready data
-----------------------------------------------------------
Solution Overview
Raw Pharma Data (CSV / TBs)
        ↓
Schema & Config Validation
        ↓
Business Data Quality Checks
        ↓
Data Cleaning & Standardization
        ↓
Business Metrics Calculation
        ↓
Parquet Curated Layer (Trusted Data)
-------------------------------------------------------
Problem 1: Silent Data Corruption

✔️ Solved by data_quality.py

Pipeline success ≠ Data correctness
✅ Problem 2: No Centralized Rules

✔️ Solved by config.py

Rules hardcoded in code = risky
Config-driven = scalable

✅ Problem 3: Difficult Audits
✔️ Solved by logs.py

Every step logged → traceability

✅ Problem 4: Slow Analytics

✔️ Solved by CSV → Parquet
Columnar storage → faster queries → lower cost

“This project lays the foundation for regulatory-grade lineage and auditability.”

The problem this project is solving:
In pharma pipelines, jobs often run successfully but the data is silently corrupted. This leads to incorrect sales reporting, compliance risks, and delayed issue detection.

I designed a config-driven PySpark pipeline that enforces business data quality rules like quantity validation, expiry checks, QC status verification, and storage condition validation before data reaches 
analytics. The pipeline converts raw CSV data into curated Parquet datasets suitable for large-scale analytics.
------------------------------------------
🗓️ DAILY JOBS (Most common)
Used when:
Sales data arrives every day
Manufacturing batches generated daily
Regulatory reporting needs daily freshness

In project:
👉 Daily pharma sales & batch data ingestion
Example:
“Every night at 1 AM, ingest yesterday’s sales data from all plants.”

🗓️ WEEKLY JOBS
Used when:
Aggregations or summaries are needed
Weekly compliance reports
Data reconciliation

In project:
👉 Weekly region-wise sales summary
Example:
“Every Monday, generate weekly sales & QC compliance report.”

🗓️ BI-WEEKLY JOBS
Used when:
Vendor reconciliation
Inventory sync with distributors

In pharma:
👉 Distributor stock alignment

🗓️ MONTHLY JOBS (VERY IMPORTANT)
Used when:
Finance closes books
Regulatory submissions
KPI reporting

In project:
👉 Monthly revenue, expired drug analysis

Example:
“At month-end, calculate total revenue & expired stock impact.”

🗓️ QUARTERLY / HALF-YEARLY / YEARLY
Used when:
Audits
FDA / CDSCO submissions
Executive reporting

In project:
👉 Compliance & audit datasets

----------------------------------------------
🧑‍💻 DAILY TASKS
✅ 1. Monitor pipelines
Check if jobs ran successfully
Look at logs
Check record counts
“Job success doesn’t mean data success.”

✅ 2. Handle failures
Failures can be:
Data quality failure
Schema mismatch
Late-arriving data
File naming issues

Your tasks:
Debug logs
Fix logic
Rerun pipelines

✅ 3. Enhance data quality rules
Business says:
“Now check humidity as well.”
You:
Update data_quality.py
Update config.py
Deploy

✅ 4. Handle new requirements
Examples:
New column added
New plant onboarded
New region added
You:
Update schema
Update transformations
Backfill historical data

✅ 5. Optimization
Even if jobs succeed:
Reduce runtime
Optimize joins
Improve partitioning
Convert formats (CSV → Parquet)

✅ 6. Support & RCA (Root Cause Analysis)
When business asks:
“Why sales dropped last week?”
You:
Trace data
Check pipeline logic
Validate source data

------------------------------------------
We had scheduled pharma data pipelines running at different frequencies based on business needs.
Daily jobs handled raw sales and manufacturing data ingestion, while weekly and monthly jobs generated aggregated and compliance-ready datasets.

As a Python data engineer, my role involved building, monitoring, enhancing, and automating these pipelines, ensuring data quality, handling failures, and supporting analytics and regulatory reporting.
--------------------------------------------
start
  ↓
extract_raw_data
  ↓
run_data_quality_checks
  ↓
clean_transform_data
  ↓
write_parquet
  ↓
end

---------------------------------------
🚨 Real Failure Scenarios
Scenario 1: Data quality failure
DAG fails at run_data_quality_checks
Downstream tasks don’t run
Business notified

Scenario 2: Late data arrival
Upstream system delays file
DAG retries
Eventually succeeds
-----------------------------------
We built an automated pharma data pipeline orchestrated using Airflow.
Airflow handled scheduling, retries, dependencies, and monitoring, while Python and PySpark handled extraction, data quality, transformations, and Parquet loading.
Different DAG schedules were used for daily ingestion, weekly summaries, and monthly compliance reporting.
---------------------------------
            ┌────────────┐
            │  Sources   │
            │ CSV / DB   │
            │ Kafka      │
            └─────┬──────┘
                  ↓
           ingestion/
                  ↓
          RAW (Bronze)
                  ↓
           quality/
                  ↓
          cleaning/
                  ↓
       CURATED (Silver)
                  ↓
           business/
                  ↓
        MART (Gold)

-------------------------------
Our pipeline follows a layered architecture. Ingestion loads source data into the raw layer without modification. Quality checks validate raw data before it is cleaned and standardized into the curated layer. 
Business transformations and aggregations are then applied to create mart-level datasets for analytics.
------------------------------

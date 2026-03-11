### 𝗠𝗮𝗻𝗮𝗴𝗲𝗿𝗶𝗮𝗹 / 𝗕𝗲𝗵𝗮𝘃𝗶𝗼𝗿𝗮𝗹

#### Q-1 Design a real-time data processing system for customer transactions.
```bash
Client Apps → API Gateway → Stream Ingestion → Stream Processing → Storage → Analytics / Actions

1. Data Ingestion Layer
Handles incoming transaction events (payments, orders, clicks)
Tools: Apache Kafka (most common),Amazon Kinesis
Features:High throughput,Partitioning for scalability,Durable event storage

2. Stream Processing Layer
Frameworks:Apache Flink (true real-time),Apache Spark Streaming (micro-batch)

3. Storage Layer
Amazon S3
Data lake for analytics & ML training
Analytical Storage ,Snowflake

[Client]
   ↓
[API Gateway]
   ↓
[Kafka] → [Flink Processing] → [Redis / Cassandra]
                           → [S3 Data Lake]
                           → [Analytics Warehouse]
```

#### Q-2 Compare DynamoDB vs. RDS, batch vs. stream processing, and handling schema evolution ? 
```bash
| Feature              | DynamoDB                         | RDS                                           |
| -------------------- | -------------------------------- | --------------------------------------------- |
| **Type**             | NoSQL (key-value / document)     | Relational (SQL)                              |
| **Schema**           | Flexible / schema-less           | Fixed schema                                  |
| **Scaling**          | Automatic horizontal scaling     | Vertical + limited horizontal (read replicas) |
| **Performance**      | Single-digit ms latency at scale | Higher latency under heavy load               |
| **Transactions**     | Limited (but supported)          | Strong ACID support                           |
| **Query Capability** | Simple queries (PK, indexes)     | Complex joins, aggregations                   |
| **Best For**         | High-throughput, real-time apps  | Complex business logic, reporting             |
| **Cost Model**       | Pay-per-request                  | Instance-based pricing                        |

When to Use DynamoDB
Real-time transaction ingestion
Massive scale (millions of requests/sec)
Simple access patterns (lookup by ID)

When to Use RDS
Financial systems needing strict consistency
Complex queries & joins
Reporting-heavy workloads

Use DynamoDB for real-time processing, and RDS (or warehouse) for analytics & reporting.
✅ Batch Processing Use Cases
Daily reports
Billing cycles
Historical analytics

✅ Stream Processing Use Cases
Fraud detection
Real-time alerts
Live dashboards
```

#### Q-3 Discuss strategies for handling high-latency issues in data pipelines ? 
```bash
1. Common Causes of High Latency
-> Network delays (cross-region traffic)
-> Backpressure in queues (e.g., Apache Kafka lag)
-> Slow processing (CPU / memory bottlenecks)
-> Inefficient storage reads/writes
-> Serialization/deserialization overhead
-> Large batch sizes or poorly tuned windows

2. Strategies to Reduce Latency
-> Partitioning & Parallelism
Increase Kafka partitions → more parallel consumers
Ensure even key distribution (avoid “hot partitions”)

-> Compression & Serialization
Use efficient formats:
Avro / Protobuf instead of JSON
Reduce payload size

-> Handle Backpressure
```

#### Q-4 Explain how to manage pipeline overloads and ensure data integrity ? 
```bash
1. What Causes Pipeline Overload?
-> Sudden traffic spikes (flash sales, peak hours)
-> Slow downstream systems (DB, APIs)
-> Uneven partitioning (hot keys)
-> Insufficient consumers or compute resources
-> Backpressure buildup (e.g., in Apache Kafka)

2. Strategies to Handle Pipeline Overloads
A. Backpressure Management
B. Load Shedding (Controlled Data Dropping)
Drop non-critical events (e.g., logs, analytics)
Keep critical transactions (payments, orders)

Example:
Fraud detection events → MUST keep
Clickstream analytics → can drop

C. Buffering & Queueing
Use durable queues (Kafka)
Too much buffering → increased latency

D. Autoscaling
Use Kubernetes or cloud autoscaling

E. Partitioning Strategy
```

#### Q-5 Tell me about a time you handled a production issue under pressure. How did you manage it ?
```bash
We had a production ETL pipeline built on Databricks processing raw JSON files from Amazon S3 into curated 
Delta tables.

During month-end reporting, dashboards suddenly showed missing data for several hours, and finance escalated 
immediately.

I was responsible for:
Identifying why the pipeline failed
Restoring data quickly
Ensuring no corruption in Delta tables
Meeting reporting deadlines

1. Immediate Triage
-> Checked Databricks job runs → found failures in the Silver layer
-> Error: Schema mismatch due to new column added in raw JSON
-> The job failed because schema evolution was not enabled

2. Backfill & Recovery
-> Reprocessed failed raw files
-> Validated row counts between Bronze → Silver → Gold
-> Used Delta time travel to confirm no corrupted writes

Example :- AWS + Snowflake + Kafka (Real-Time Incident)
Situation: 
We had a real-time transaction pipeline:
Events streamed via Apache Kafka
Running on Amazon Web Services
Loaded into Snowflake for analytics

During a high-traffic campaign, alerts triggered:
Kafka consumer lag spiking
Snowflake ingestion latency > 10 minutes
Fraud dashboard delayed

Task 
Restore real-time ingestion while ensuring:
No duplicate transactions
No data loss
Fraud systems remain accurate

1️⃣ Rapid Diagnosis
Checked Kafka metrics → consumers falling behind
Identified Snowflake warehouse was overloaded
COPY INTO operations were queuing

2️⃣ Immediate Mitigation
Scaled Snowflake warehouse up (larger compute cluster)
Increased Kafka consumer parallelism
Prioritized fraud topic over analytics topic

3️⃣ Integrity Controls
Verified Kafka offsets were not committed prematurely
Confirmed idempotent writes in Snowflake (dedupe on transaction_id)
Monitored DLQ for failed records

4️⃣ Long-Term Fix
Introduced micro-batching instead of per-event loads
Enabled auto-scaling for Snowflake warehouse
Added alert for warehouse queue depth
```

#### Q-6 How do you explain technical solutions to non-technical clients ?
```bash
Focus on what problem we’re solving, not how

Example:
“Instead of saying we built a pipeline using Apache Kafka, I start with:
‘We built a system that processes your transactions instantly so you can detect fraud in real time.’”

Data pipeline → “assembly line”
Streaming → “live traffic updates”
Batch → “end-of-day report”

We implemented a real-time pipeline using Databricks for transaction processing.
Right now, your reports are delayed by hours. We’re building a system that updates them almost instantly—so you 
can detect issues like fraud or sales spikes as they happen.
```

#### Q-7 Explain the bronze-silver-gold architecture in a data lakehouse. Why is this layering important ?
```bash
Raw Data → Cleaned Data → Business-Ready Data
(Bronze)    (Silver)        (Gold)

Bronze Layer — Raw Data
Purpose:
Store raw, unmodified data exactly as it arrives.
Characteristics:
JSON / CSV / streaming data
No transformations
Append-only
May contain duplicates, nulls, bad records

Silver Layer — Cleaned & Structured

Purpose:
Transform Bronze into validated, structured datasets.
Typical Operations:
Remove duplicates
Fix data types
Handle nulls
Enforce schema
Basic joins (e.g., add user info)

Example:
Cast amount to decimal
Standardize timestamps
Drop corrupt records

Now data is:
Queryable
Consistent
Trusted
👉 Silver = Operational data layer

Gold Layer — Business-Ready Data

Purpose:
Optimized for analytics and reporting.
Typical Operations:
Aggregations (daily revenue, user metrics)
KPIs
Denormalized tables
BI-ready datasets
```

#### Q-8 How would you optimize Glue job for large files processing ?
```bash
1️⃣ Increase Parallelism (Repartition Data)
df = df.repartition(100)
2️⃣ Use Columnar Formats Instead of CSV/JSON
df.write.parquet("s3://data-bucket/output/")
3️⃣ Push Down Predicate Filtering
4️⃣ Enable Job Bookmarking
5️⃣ Enable Glue Auto Scaling
6️⃣ Use DynamicFrames to DataFrames Conversion
7️⃣ Use Broadcast Join for Small Tables
```

#### Q-9 How can you trigger Glue job automatically on S3 file arrival ? 
```bash
S3 Upload
   ↓
S3 Event Notification
   ↓
AWS Lambda Trigger
   ↓
Start AWS Glue Job
   ↓
ETL Processing

-> Create Lambda Function

import boto3

def lambda_handler(event, context):
    
    glue = boto3.client('glue')
    
    response = glue.start_job_run(
        JobName='my_glue_job'
    )
    
    return response

-> Pass S3 File Path to Glue Job
import boto3

def lambda_handler(event, context):

    s3_path = event['Records'][0]['s3']['object']['key']
    
    glue = boto3.client('glue')

    glue.start_job_run(
        JobName='my_glue_job',
        Arguments={
            '--input_file': s3_path
        }
    )
```

#### Q-10  How will you read data from S3 bucket and write into another bucket using Glue Job ? 
```bash
1️⃣ Initialize Glue Job
import sys
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init("s3_to_s3_job", {})

2️⃣ Read Data from Source S3 Bucket
source_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": ["s3://source-bucket/input/"]},
    format="csv",
    format_options={"withHeader": True}
)
```

#### Q-11 What is AWS Glue? How is it different from Databricks ? 
```bash
AWS Glue is a serverless ETL service from AWS used to prepare and transform data for analytics.

The main difference between AWS Glue and Databricks:

AWS Glue → Serverless ETL service mainly for data integration

Databricks → Unified data analytics platform built on Spark with advanced features like ML, notebooks, 
and optimized performance

Use Databricks when:
You need big data processing
Machine learning pipelines
Real-time streaming
Advanced Spark optimization
```

#### Q-12 
```bash
```

#### Q-13
```bash
```

#### Q-14 How do you implement audit logging for a data pipeline using SQL procedures and triggers?
```bash
Audit Pipeline Example Flow
Data Pipeline
      ↓
Insert / Update / Delete
      ↓
Trigger Fires
      ↓
Stored Procedure Executes
      ↓
Audit Log Table Updated

Benefits of Audit Logging
Data lineage tracking
Regulatory compliance
Debugging pipeline errors
Monitoring data changes

To implement audit logging in a data pipeline, I create an audit table to store change history. Then I use SQL triggers 
on target tables to capture INSERT, UPDATE, and DELETE operations. The trigger calls a stored procedure that writes old 
values, new values, user, and timestamp into the audit table. This ensures every data change in the pipeline is logged automatically.
```

#### Q-15  You have a huge text file, how would you replicate a given row "n" number of times, write a code for this ?
```bash
For a huge text file, I would process it line by line using streaming instead of loading it into memory. When the target 
row is reached, I would write that row n times to the output file, ensuring the solution works efficiently for very large datasets.
```

#### Q-16 How is deployment done in your project ? Explain about development/testing etc. ?
```bash
1️⃣ Development Environment (DEV)
Create dbt models
Write SQL transformations
Develop Airflow DAGs
Create staging tables in Snowflake
Test logic with sample data

2️⃣ Version Control (Git)
Main Branch (Production)
      ↑
Pull Request
      ↑
Feature Branch (Developer)

3️⃣ Testing / QA Environment

4️⃣ CI/CD Pipeline
Jenkins
GitHub Actions

5️⃣ Production Deployment
Airflow DAG
     ↓
Load data into Snowflake
     ↓
Run dbt transformations
     ↓
Create final analytics tables
```

#### Q-17 Explain dimensional modeling and how you would design a sales fact table with product and customer dimensions ?
```bash
Components of Dimensional Modeling :-
1. Fact Table
Stores numeric measurements.

sales_amount
quantity
discount

2. Dimension Table
Stores descriptive attributes used for filtering and grouping.

product name
customer city
product category
```

#### Q-18 How would you design a fact table for an e-commerce platform ?
```bash
                     Dim_Date
                        |
                        |
Dim_Customer ---- Fact_Order_Items ---- Dim_Product
                        |
                        |
                   Dim_Payment
                        |
                        |
                   Dim_Promotion

Fact table → contains measures

Dimension tables → contain descriptive attributes
```

#### Q-19 How would you design a data warehouse for a retail business using Synapse ?
```bash
1️⃣ Data Sources (Retail Systems)
Sales Transactions
Customer Data
Product Catalog
Inventory Updates
Store Data

2️⃣ Data Ingestion Layer (AWS)
Raw data is ingested into Amazon S3.
Batch file uploads (CSV/JSON)
API ingestion
Streaming ingestion

3️⃣ Workflow Orchestration
Pipelines are orchestrated using Apache Airflow.
Airflow responsibilities:
Trigger ingestion pipelines
Load data to Snowflake
Run dbt models
Monitor pipeline failures

4️⃣ Data Warehouse Layer
S3 Raw Data
      ↓
Snowflake Stage
      ↓
Staging Tables

5️⃣ Data Transformation with dbt

7️⃣ Data Flow Architecture
Retail Systems
      ↓
Amazon S3 (Raw Data Lake)
      ↓
Apache Airflow (Orchestration)
      ↓
Snowflake (Data Warehouse)
      ↓
dbt (Transformations & Models)
      ↓
Analytics / BI Tools
```

#### Q-20 How would you handle late-arriving data in a batch ETL pipeline in pyspark ?
```bash
1️⃣ Use Event Time vs Ingestion Time
event_time → when the event actually happened
ingestion_time → when the data arrived

from pyspark.sql.functions import col
df_late = df.filter(col("event_time") < col("ingestion_time"))

2️⃣ Sliding Window Reprocessing
from pyspark.sql.functions import current_date, date_sub, col

df = spark.read.parquet("s3://data/events")

df_filtered = df.filter(
    col("event_date") >= date_sub(current_date(), 3)
)

3️⃣ Upsert / Merge Logic
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "s3://data/target")

target.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

 4️⃣ Partition-Based Reprocessing
 df = spark.read.parquet("s3://data/events/year=2026/month=03/day=10")
```
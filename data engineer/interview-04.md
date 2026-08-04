#### Q-1 how to handle HDFS (Hadoop Distributed File System) fault tolerance ? 
```bash
All about making sure data is safe and accessible even when hardware fails (which will happen in distributed systems)

How HDFS Handles Fault Tolerance

1. Block Replication (Core Concept)
Files in HDFS are split into blocks (default ~128 MB).
Each block is replicated across multiple nodes (default replication factor = 3).

Block A → Node1, Node2, Node3

2. Rack Awareness
HDFS places replicas intelligently across different racks.

Typical placement:

1 replica → same node
2nd replica → different rack
3rd replica → same rack as 2nd

✔ Protects against:

Node failure ✅
Rack failure (power/network) ✅

3. Heartbeat Mechanism
Each DataNode sends heartbeat signals to NameNode (every ~3 seconds).

❌ If heartbeat stops:

NameNode marks node as dead
Triggers replication of missing blocks

4. Block Reports
DataNodes periodically send block metadata to NameNode.

✔ Helps NameNode:

Track which blocks exist
Detect missing or corrupt blocks

5. Automatic Re-Replication
When a node fails:
NameNode detects under-replicated blocks
New replicas are created on other healthy nodes

✔ Fully automatic — no manual intervention needed

🧠 Real-World Failure Scenarios
Scenario 1: DataNode crashes
✔ Handled by:
Replication
Re-replication

Scenario 2: Rack failure
✔ Handled by:
Rack-aware placement

Scenario 3: Corrupt data block
✔ Handled by:
Checksum + replica fetch

Scenario 4: NameNode failure
✔ Handled by:
HA (Active/Standby + ZooKeeper)
```

#### Q-2 I can draw a simple architecture diagram or give a real-world production example (Kafka → Spark → Snowflake).
```bash
        ┌────────────────────┐
        │   Data Sources     │
        │ (Apps, APIs, DBs)  │
        └─────────┬──────────┘
                  │
                  ▼
        ┌────────────────────┐
        │      Kafka         │
        │ (Topics/Partitions)│
        └─────────┬──────────┘
                  │
        (Streaming Consumption)
                  │
                  ▼
        ┌──────────────────────────┐
        │   Spark Structured       │
        │       Streaming          │
        │ - Transformations        │
        │ - Validation             │
        │ - Deduplication          │
        │ - Checkpointing          │
        └─────────┬────────────────┘
                  │
        ┌─────────┴──────────┐
        │                    │
        ▼                    ▼
┌───────────────┐    ┌────────────────┐
│   DLQ Kafka   │    │   Snowflake    │
│ (Bad Records) │    │ (Final Tables) │
└───────────────┘    └────────────────┘
```

#### Q-3 Normalization vs Denormalization ? 
```bash
```

#### Q-4 low offset and high offset in kafka  ? 
```bash
In Kafka, low offset and high offset define the range of messages available in a partition, where the low offset 
is the earliest available message and the high offset is the latest written message.

Partition:
Offsets → 5, 6, 7, 8, 9

Low Offset  = 5
High Offset = 9
Messages 0–4 → already deleted (retention)
Messages 5–9 → currently available

Consumer Perspective
Consumer reads between:
Current offset (where it is now)
High offset (latest data available)

👉 Lag = High Offset - Consumer Offset
```

#### Q-5 Difference between processing time and event time in kafka ? 
```bash
Processing time = time when the system (consumer/Spark) processes the event

Event created at: 10:00 AM  
Kafka processes it at: 10:05 AM  
→ Processing Time = 10:05 AM

✅ Characteristics:
Based on system clock
Simple and fast
No handling of delays

Event time = actual time when the event occurred (inside the data itself)
Event created at: 10:00 AM  
Arrives late at: 10:05 AM  
→ Event Time = 10:00 AM

✅ Characteristics:
Comes from data (timestamp field)
Handles late-arriving data
More accurate for analytics
```

#### Q-6 what is factless fact table ? 
```bash
A factless fact table is a type of fact table that does not contain any numeric measures—it only stores relationships 
or events.

👉 A fact table without metrics (like sales, amount, quantity)
👉 It only captures “what happened” or “what exists”

Why Use Factless Fact Table?
Track events (attendance, login, participation)
Track relationships (student-course, customer-product eligibility)

🧩 Types of Factless Fact Tables
1. ✅ Event Tracking

👉 Example: Student Attendance
Student_ID | Date_ID | Class_ID
--------------------------------
101        | 20240201 | Math
102        | 20240201 | Science

✔️ No measure column
✔️ Each row = event happened

| Feature  | Fact Table            | Factless Fact Table         |
| -------- | --------------------- | --------------------------- |
| Measures | Yes (sales, amount)   | ❌ No                       |
| Purpose  | Quantitative analysis | Event/relationship tracking |
| Example  | Sales data            | Attendance                  |

           dim_student
                |
                |
dim_date --- attendance_fact --- dim_class
```

#### Q-7 how to how do manage schema evaluation as a part of pipeline make a step by step two section point as a snowflake dbt airflow and aws and databricks and azure data engineer ?
```bash
Step 1: Ingest Data
Receive data from APIs, databases, or streaming sources using AWS services such as Kinesis or S3.
Land all incoming data in the Bronze (Raw) layer.
Preserve the original data for replay and auditing.

Step 2: Detect Schema Changes
Compare the incoming schema with the existing schema stored in the metadata repository (for example, AWS Glue Data Catalog).

Step 3: Validate the Schema

Before loading data:

Check required columns exist.
Validate data types.
Verify primary keys and timestamps.
Ensure mandatory fields are not null.

If validation fails:

Move the file or records to a quarantine location.
Log the error.
Notify the data engineering team.

Step 4: Handle Schema Evolution

Follow these rules:

✅ Add new nullable columns automatically.
✅ Populate missing optional columns with NULL.

Step 5: Load Data into Snowflake
Load validated data into staging tables.
Keep staging tables close to the raw schema.

Step 6: Transform Using dbt

Create dbt models:

Staging: Standardize column names and data types.
Intermediate: Apply business logic.
Mart: Build reporting-ready tables.

Step 7: Orchestrate with Airflow

Create an Airflow DAG with tasks such as:

Ingest data
Detect schema changes
Validate schema
Load to Snowflake
Run dbt models
Execute dbt tests
Publish curated tables
Send success or failure notifications

Step 8: Monitor Schema Drift

Continuously monitor:

New columns
Dropped columns
Data type changes
Null percentage
Row counts
Data freshness
```

#### Q-8 database sharding and partitioning ?
```bash
| Feature          | Partitioning             | Sharding                                |
| ---------------- | ------------------------ | --------------------------------------- |
| Split Data       | Within the same database | Across multiple databases/servers       |
| Purpose          | Performance              | Scalability                             |
| Physical Storage | Same DB server           | Multiple DB servers                     |
| Transparency     | Usually handled by DB    | Often handled by application/middleware |
| Complexity       | Lower                    | Higher                                  |

Sharding divides data across multiple database servers.
Shard 1 → Customers 1-1M
Shard 2 → Customers 1M-2M
Shard 3 → Customers 2M-3M	

Partitioning divides a large table into smaller logical partitions within the same database to improve query 
performance and manageability. Sharding distributes data across multiple database servers to achieve horizontal 
scalability and handle very large workloads. Partitioning improves performance on a single database, while 
sharding allows the system to scale beyond the capacity of a single server.
```

#### Q-9 Spark is an in-memory compute engine then why do we need cache in apache spark ?
```bash
Instead of repeatedly reading data from disk, Spark keeps frequently used data in RAM, where it can be 
processed much more quickly.

cache() tells Spark to keep the DataFrame in memory so later operations don't need to reread the source.

Example

Suppose you have a 10 GB sales dataset.

Read 10 GB once
↓
Store in RAM
↓
Filter
↓
Group By
↓
Join
↓
Write output
```

#### Q-10 Different compression techniques such as snappy, biz2 and LZO. And which one to choose ?
```bash
| Compression     |     Speed | Compression Ratio | CPU Usage |    Splittable | Best Use Case           |
| --------------- | --------: | ----------------: | --------: | ------------: | ----------------------- |
| **Snappy**      | Very Fast |            Medium |       Low |           Yes | Fast ETL, analytics     |
| **Bzip2 (bz2)** |      Slow |              High |      High |           Yes | Maximum storage savings |
| **LZO**         | Very Fast |        Low–Medium |       Low | Yes (indexed) | Real-time processing    |

snappy(100 GB raw data → ~35–50 GB compressed) 
Bzip2 (bz2) (100 GB raw data → ~20–30 GB compressed)
LZO (100 GB raw data → ~45–60 GB compressed)
```

#### Q-11 How do you ensure the pipeline does not repeat a similar kind of failure ?
```bash
A. Retry Strategy with Backoff

For transient failures like: API timeout Temporary DB issue Network issue
I configure retries

retries=3,
retry_delay=timedelta(minutes=5),
retry_exponential_backoff=True

B. Idempotent Pipeline Design

One of the biggest causes of repeated failures is duplicate processing after reruns.

C. Failure Isolation

When a pipeline fails, I don’t just rerun it. I first identify the root cause using logs and monitoring, 
then implement preventive fixes. In Airflow, I use retries with backoff, idempotent DAG design, and alerts. 
In Snowflake, I use transactional loads, MERGE logic, validations, and monitoring through query history. 
In AWS pipelines, I rely on DLQs, checkpointing, CloudWatch alerts, and autoscaling. The goal is to ensure 
the same failure becomes non-repeatable rather than repeatedly firefighting it.
```

#### Q-12  tell me when to use delta lake , data warehouse , sql and no sql ?
```bash
:- SQL Database – For Transactional Applications

Use when:

Data has clear relationships.
ACID transactions are required.
Data consistency is critical.

:- NoSQL Database – For Flexible and High-Scale Data

Use when:

Schema changes frequently.
Large-scale applications require horizontal scaling.
Data is semi-structured or unstructured.

:- Data Warehouse – For Business Reporting

Use when:

Creating dashboards.
Supporting BI tools.
Running SQL-based analytics.
Managing curated business data.

Popular Cloud Data Warehouses

Snowflake
Google Cloud BigQuery
Amazon Web Services Redshift
Microsoft Azure Synapse Analytics

:- Delta Lake – For Data Lakes and Big Data

Cloud Example (AWS)

Kinesis -> Amazon S3-> Databricks (Delta Lake)->Snowflake ->

Cloud Example (Azure)

IoT Devices -> Azure Event Hubs -> Azure Data Lake Storage -> Databricks + Delta Lake -> Silver Tables
-> Gold Tables
```

#### Q-13 expalin serveless and monolith artecutuire and how lambda work ?  
```bash
monolith :-

A monolithic architecture is an application where all components are built and deployed as a single unit.

                Monolithic Application
      +--------------------------------------+
      |                                      |
      |  User Login                          |
      |  Product Catalog                     |
      |  Shopping Cart                       |
      |  Payment                             |
      |  Order Management                    |
      |  Notification                        |
      |                                      |
      +--------------------------------------+
                     │
                     ▼
                  Database

How it works :- 
All features run in one application.
If you change one feature, you typically redeploy the entire application.
All modules usually share the same database.

serveless :-

In a serverless architecture, you don't manage servers directly. You write small functions, and the cloud provider runs them when events occur.

For AWS, this is commonly done using Amazon Web Services Lambda.

Customer Uploads Image
          │
          ▼
     Amazon S3
          │
     (Event Trigger)
          │
          ▼
      AWS Lambda
          │
          ▼
Resize Image
          │
          ▼
Store Processed Image
```

#### Q-14 daily my data incoming 80 gb but someday suddenly 100 gb  then how to handle this situation ?
```bash
Scenario :- 
Daily Input = 80 GB
Executors = 8
Executor Memory = 8 GB (Your job finishes in 20 minutes.)

One day, due to a festival, month-end processing, or a data replay, the input becomes: (100 GB)

If you do nothing, you may see:

Longer execution time
Executor OutOfMemory (OOM) errors
Increased shuffle spill to disk
Failed stages due to memory pressure

Option 1: Enable Dynamic Allocation (Recommended)

Yesterday
80 GB
↓
8 Executors

Today
100 GB
↓
Spark requests
10 Executors

spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=4
spark.dynamicAllocation.maxExecutors=20

Option 2: Increase Partitions

80 GB
↓
80 Partitions
↓
1 GB per partition

100 GB
↓
Still 80 Partitions
↓
1.25 GB per partition

df = df.repartition(120)

They respond by:

-> Dynamic allocation increases executors from 8 → 12.
-> Input partitions increase from 80 → 120.
-> No code changes are required.
-> Job finishes successfully.
```

#### Q-15 How do you decide which should go to the Data Warehouse and which should be treated as an external table ?
```bash
Rule of Thumb

Put data in the Data Warehouse when:

It is frequently queried.
Used in dashboards and reports.
Requires fast response times.
Requires joins with other warehouse tables.
Needs data modeling (star schema, dimensions, facts).
Business users access it regularly.

Use External Tables when:

Data is very large.
Accessed infrequently.
Mostly used for ad-hoc analysis.
Stored in a data lake (S3, ADLS, OneLake).
You want to avoid copying data.
Raw or semi-structured data (JSON, Parquet, CSV).

ad-hoc (Ad-hoc means "for a specific purpose or as needed" rather than being pre-planned or recurring.
In data engineering and analytics, an ad-hoc query is a query that a user runs to answer a one-time business question.)

SELECT
    order_date,
    SUM(sales_amount)
FROM fact_sales
GROUP BY order_date;
```

#### Q-16 My file is compressed with Snappy in S3 and its size is 3 GB. How many partitions will Spark create ? 
```bash
It depends on the file format. If it's a Snappy-compressed Parquet file, Spark can split it and, with the default spark.sql.files.maxPartitionBytes of 128 MB, 
it will create approximately 24 partitions (3072 MB ÷ 128 MB).
```

#### Q-17 What happens if the timestamp or watermark value is incorrect ?
```bash
If the timestamp or watermark value is incorrect, the pipeline may process duplicate records, miss valid records, process data out of order, or incorrectly discard 
late-arriving events. The impact depends on whether the timestamp is used for incremental loading or event-time processing.
```

#### Q-18 How did you handle failures or recovery scenarios in the pipeline ?
```bash
1. Orchestration and Retries (Used Apache Airflow to orchestrate end-to-end workflows.)
2. Checkpointing in Streaming Pipelines (For Databricks Structured Streaming, enabled checkpointing to store offsets and state information.)
3. Incremental Processing with Watermarks (Used watermark columns such as last_updated_timestamp for incremental data loads.)
4. Idempotent Data Loading (Used MERGE (UPSERT) operations in Snowflake instead of simple INSERT statements.)
5. Error Logging and Monitoring (Monitored pipeline execution using Airflow logs, Databricks job logs, and Snowflake query history.)
6. Data Validation (Performed schema validation, null checks, duplicate detection, and record count validation before loading data into target tables.)
7. Transaction Management (Ensured that target tables were updated only after all transformations completed successfully.)
8. Recovery Strategy (After resolving the root cause, I restarted only the failed Airflow task or Databricks job.)
```

#### Q-19
```bash
```

#### Q-20
```bash
```
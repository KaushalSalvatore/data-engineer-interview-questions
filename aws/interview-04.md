#### Q-1 What is the AWS Glue Data Catalog ?
```bash
The AWS Glue Data Catalog is a centralized metadata repository that stores:
Table definitions
Database schemas
Column names & data types
Partition information
Data location (like S3 path)

It does NOT store the actual data — only metadata about the data.
Actual Data → Stored in S3
Schema Info → Stored in Data Catalog

Example table metadata:

Database: sales_db
Table: orders
Columns:
  order_id (string)
  amount (double)
  region (string)
Partitions:
  year
  month
Location:
  s3://sales-data/

S3 (Raw Data)
     ↓
Glue Crawler
     ↓
Glue Data Catalog
     ↓
Athena / Redshift / EMR
```

#### Q-2 Explain the types of triggers in AWS Glue ? 
```bash
Condition Met → Trigger Fires → Job/Crawler Runs
There are three main types of triggers.

There are three main types of triggers.
1️⃣ Scheduled Trigger
Uses:
Cron expressions
Fixed intervals

Example:
Run ETL every night at 1 AM
Run crawler every 30 minutes

Daily ETL → 1:00 AM → Transform S3 raw data → Store cleaned data

2️⃣ On-Demand Trigger
You start it via:
AWS Console
AWS CLI
SDK/API

Best for:
✔ Testing
✔ Debugging
✔ Manual reprocessing

3️⃣ Conditional Trigger (Most Powerful)

Job succeeds
Job fails
Multiple jobs complete

Job A → Job B → Job C
```

#### Q-3 What is a Job Bookmark in AWS Glue ?
```bash
A Job Bookmark is a mechanism that:Tracks previously processed data so that future job runs only process new 
or changed data.

Run 1 → Process all files
Run 2 → Process only new files
Run 3 → Process only newly added files

s3://sales-data/
  file1.csv
  file2.csv

Run 1 → processes both files
Later → file3.csv added

Run 2 → processes only file3.csv
```

#### Q-4 How does AWS Glue integrate with other AWS services ?
```bash
1️⃣ Storage Integration
S3 (Raw) → Glue ETL → S3 (Processed)

2️⃣ Data Warehouse Integration
Amazon Redshift

3️⃣ Query Services

6️⃣ Database Integration
Amazon RDS

7️⃣ Event & Orchestration Integration
EventBridge → Glue Job → Step Functions → Redshift
```

#### Q-5 What are the main use cases for AWS Glue ?
```bash
1️⃣ Data Lake ETL Processing (Most Common)
S3 (Raw Data) → Glue ETL → S3 (Cleaned/Curated Data)

2️⃣ Data Warehouse Loading
S3 → Glue Transform → Redshift

3️⃣ Incremental Data Processing
```

#### Q-6 How does AWS Glue work with data lakes ?
```bash
A data lake is: A centralized storage system that holds raw, structured, semi-structured, and unstructured 
data at scale.

Amazon S3
S3 = storage
Glue = processing + metadata management

1️⃣ Metadata Layer (Schema Management)
👉 AWS Glue Data Catalog
This stores:
Table definitions
Column names
Data types
Partitions
S3 locations

2️⃣ Schema Discovery (Crawlers)
Scan S3 → Detect schema → Create tables in Data Catalog

3️⃣ ETL / Data Transformation Layer
S3 (Raw) → Clean → Join → Aggregate → S3 (Curated)
```

#### Q-7 What are DynamicFrames in AWS Glue ?
```bash
A DynamicFrame is:A distributed collection of data records designed for ETL, built on top of Spark, 
but with flexible schema handling.

It’s similar to a Spark DataFrame but more tolerant of messy data.

Under the hood, Glue runs on Apache Spark — but instead of working directly with Spark DataFrames, 
Glue introduces DynamicFrames.

{"id": 1, "name": "John"}
{"id": "2", "name": "Alice", "extra": "value"}

Key Features of DynamicFrames
1️⃣ Schema Flexibility
2️⃣ Built-in ETL Transforms
3️⃣ Easy Conversion
```

#### Q-8 How does AWS Glue optimize ETL performance ?
```bash
1️⃣ Distributed Processing (Spark-Based Engine)
So it automatically:
Distributes data across workers
Executes tasks in parallel
Handles shuffling and partitioning
Auto-scales compute resources

2️⃣ Automatic Scaling
Glue dynamically provisions:
Workers (DPUs)
Memory

3️⃣ Columnar Format Optimization
Glue works well with columnar formats like:
Parquet
ORC

4️⃣ Partitioning Support
s3://sales/year=2026/month=03/
```

#### Q-9 What are the limitations of AWS Glue ?
```bash
1️⃣ Cold Start & Job Startup Time
Glue jobs don’t start instantly.
Job initialization can take 1–3 minutes
Spark environment needs to spin up

2️⃣ Cost Can Add Up
Glue pricing is based on:
DPUs (Data Processing Units)
Job duration

The main limitations of AWS Glue include job startup latency, limited real-time processing capabilities, 
reduced customization compared to EMR, potential performance issues with small files, debugging constraints, 
and service limits on concurrency. It is best suited for batch ETL workloads rather than low-latency streaming 
or deeply customized Spark environments.
```

#### Q-10 What is the difference between Glue’s Data Catalog and traditional metadata stores ?
```bash
The AWS Glue Data Catalog is a fully managed, serverless metadata repository that stores:
Table definitions
Column schemas
Partition information
Data locations (usually in S3)

| Feature          | Glue Data Catalog          | Traditional Metadata Store |
| ---------------- | -------------------------- | -------------------------- |
| Management       | Fully managed              | Self-managed               |
| Scalability      | Auto-scaled                | Manual scaling             |
| Integration      | Native AWS services        | Manual integration         |
| Schema discovery | Automated (Crawlers)       | Mostly manual              |
| Security         | Lake Formation integration | DB-level security          |
| Cost model       | Pay-per-use                | Infrastructure cost        |
```

#### Q-11 What is a Glue Partition Index, and why is it used ?
```bash
A Glue Partition Index is a feature of the AWS Glue Data Catalog that improves the performance of partition filtering in large partitioned tables.

Normally, when a query filters partitions, the Data Catalog:
Fetches all partition metadata
Then filters them
If your table has millions of partitions, that step becomes slow.

The Problem It Solves

s3://logs/year=2024/month=01/day=01/
...
s3://logs/year=2026/month=12/day=31/

If you have:
3 years
12 months
30 days
Possibly hourly partitions

SELECT * FROM logs
WHERE year = 2026 AND month = 3;

Without partition index:
Glue scans entire partition metadata list
Then filters
Slow for large tables

What Partition Index Does
Partition Index creates an index on selected partition columns.

Index lookup → Directly fetch matching partitions
```

#### Q-12 How does AWS Glue handle incremental data updates ?
```bash
1️⃣ Job Bookmarks (Primary Mechanism)
A Job Bookmark allows Glue to:
Track previously processed data
Process only new or modified records in the next run

Run 1 → Process file1.csv, file2.csv
Run 2 → Only process file3.csv

2️⃣ Partition-Based Incremental Processing
s3://sales/year=2026/month=03/day=05/

3️⃣ Predicate Pushdown
push_down_predicate = "year = '2026' AND month = '03'"

4️⃣ CDC (Change Data Capture) Pattern
Glue itself doesn’t automatically generate CDC logs, but it can process CDC data from:
Databases exporting change logs
Streaming systems
Timestamp-based columns

5️⃣ Upserts & Merge Logic
For incremental updates that require:
Insert new rows
Update existing rows
```

#### Q-13 What is AWS Glue Workflows ?
```bash
A Glue Workflow is:A collection of Glue jobs, crawlers, and triggers organized into a coordinated ETL 
pipeline.

Crawler → Job A → Job B → Job C

A Glue Workflow typically includes:
1️⃣ Jobs (ETL transformations)
2️⃣ Crawlers (schema discovery)
3️⃣ Triggers (control execution flow)

Step 1: Crawler
        ↓
Step 2: Clean Job
        ↓
Step 3: Aggregate Job
        ↓
Step 4: Load to Warehouse
```

#### Q-14 How does AWS Glue integrate with AWS Step Functions ?
```bash
Why Integrate Glue with Step Functions?
✔ Orchestrate multiple AWS services
✔ Add complex branching logic
✔ Add retries, timeouts, parallelism
✔ Handle failure scenarios gracefully

So instead of:
Crawler → Glue Job → Done

You can build:
API Call → Lambda → Glue Job → Validation → SNS → Archive
```

#### Q-15 What is the difference between Glue DynamicFrames and Spark DataFrames ?
```bash
| DynamicFrame                        | Spark DataFrame           |
| ----------------------------------- | ------------------------- |
| Glue-specific abstraction           | Native Spark abstraction  |
| Schema flexible                     | Schema strict             |
| Built for messy data                | Built for structured data |
| Handles semi-structured data easily | Needs predefined schema   |
```

#### Q-16 What are AWS Glue Blueprints ?
```bash
In AWS Glue, Blueprints are:
Predefined, reusable templates that automatically generate ETL workflows.
Instead of manually creating:
Crawlers
Jobs
Triggers
Workflows

A Blueprint creates them for you based on your inputs.
Think of it like:
“Give me source + target → I’ll build the pipeline structure.”

Why Use Blueprints?

✔ Faster pipeline setup
✔ Standardization across teams
✔ Reduced manual configuration
✔ Reusable ETL patterns
✔ Enforces best practices
```

#### Q-17 How does Glue handle schema conflicts ?
```bash
In AWS Glue, schema conflicts usually happen when:
Same column has different data types
Columns are missing in some files
Nested JSON fields vary
Partition schemas differ

1️⃣ DynamicFrames = Built for Schema Drift
✔ Allow missing columns
✔ Allow extra columns
✔ Handle inconsistent types


2️⃣ ResolveChoice (Key Feature)
3️⃣ Crawlers & Schema Evolution
When using Glue Crawlers:
New columns → automatically added
Removed columns → not automatically deleted
Type changes → may create conflicts

4️⃣ Partition Schema Conflicts
Partition A → price: int
Partition B → price: double

Glue may:
Promote to common type (e.g., double)
Or create a conflict
```

#### Q-18 What is AWS Glue Flex ?
```bash
Instead of running your job immediately at full dedicated capacity, Glue Flex:
Uses spare AWS compute capacity at a lower cost, but with variable start time.
So you trade speed for cost savings.

Why Does It Exist?
Normally, Glue jobs run in:
Standard execution mode
Starts quickly
Dedicated resources
Higher cost

Glue Flex:
Uses available spare capacity
May delay job start
Up to ~30–35% cheaper (varies by region)
Great for non-urgent workloads.
```

#### Q-19  What are AWS Glue Tags ?
```bash
In AWS Glue, tags are:
Key–value metadata labels attached to Glue resources.
They help you organize, control, and track your Glue resources.

You can tag:
Glue Jobs
Crawlers
Workflows
Connections
Development endpoints

Key: Environment   Value: Production
Key: Team          Value: DataEngineering
Key: CostCenter    Value: Finance
```

#### Q-20 How do Glue Streaming Jobs process late-arriving data ?
```bash
Late data = records that arrive after their event time window has passed.
Event time: 10:00 AM
Arrival time: 10:07 AM
Window: 5 minutes

This happens due to:
Network delays
Device buffering
Retries
Distributed systems lag

How Glue Streaming Handles It
1️⃣ Event-Time Processing
.withWatermark("event_time", "10 minutes")

2️⃣ Watermarking (The Key Concept)
df.withWatermark("event_time", "10 minutes") \
  .groupBy(window("event_time", "5 minutes")) \
  .count()

3️⃣ Windowed Aggregations
10:00–10:05 window
```
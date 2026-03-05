#### Q-1 What is the role of a data engineer at AWS ?
```bash
A Data Engineer at AWS designs and builds scalable data platforms using AWS services like S3, Glue, 
EMR, and Redshift. The role involves building batch and streaming pipelines, ensuring data quality and 
security, optimizing performance and cost, and maintaining highly reliable data systems in production.
```

#### Q-2 What are the common challenges faced by AWS data Engineers ?
```bash
Slow queries in Amazon Redshift
Massive scans on Amazon S3
Poor partitioning in data lakes
Shuffle-heavy Spark jobs on Amazon EMR

Common challenges for AWS Data Engineers include handling large-scale data processing, optimizing performance 
and cost, managing IAM and security complexities, dealing with schema evolution, debugging distributed systems, 
and ensuring reliable monitoring and governance. Cost-performance trade-offs and production reliability are often 
the biggest ongoing challenges.
```

#### Q-3 What exactly is Amazon S3 ?
```bash
Amazon S3 is a scalable object storage service used to store and retrieve large volumes of unstructured or 
structured data. It is highly durable, secure, and commonly used to build data lakes and support analytics 
workloads in AWS environments.    

Bucket
   └── Folder (optional logical path)
         └── File (object)
        
s3://company-data/raw/2026/sales.csv
```

#### Q-4 What does Amazon EC2 do ?
```bash
Amazon EC2 (Elastic Compute Cloud) is a service from Amazon Web Services that lets you run virtual servers 
in the cloud.

Virtual machines (called instances)
CPU, RAM, storage
Operating system (Linux/Windows)
Networking

Amazon EC2 is a cloud computing service that provides scalable virtual servers. It allows users to run applications, 
process data, and host services without managing physical hardware, with flexible instance types and pay-as-you-go 
pricing.
```

#### Q-5 What is Amazon Redshift ?
```bash
Amazon Redshift is a fully managed cloud data warehouse service provided by Amazon Web Services used for 
large-scale analytics and reporting.

Redshift is where you store huge amounts of structured data and run very fast SQL queries for analytics.

Amazon Redshift is a fully managed, columnar cloud data warehouse that uses massively parallel processing to run 
high-performance analytical SQL queries on large datasets. It is commonly used for business intelligence and reporting 
workloads.

COPY sales
FROM 's3://bucket/sales/'
IAM_ROLE 'role-arn'
FORMAT AS PARQUET;
```

#### Q-6 What is Amazon Glue, and how does it make the Extract, Transform, and Load (ETL) process easier ?
```bash
Amazon Glue is a serverless ETL service that helps automate data discovery, transformation, and loading. 
It simplifies ETL by eliminating infrastructure management, automatically detecting schemas using crawlers, 
and executing Spark-based transformation jobs that scale automatically.

Traditional ETL setup requires: Provisioning servers,Installing Spark/Hadoop,Managing clusters,Scheduling jobs,
Handling scaling
Glue removes most of that headache.

1. Glue Data Catalog
This is like a metadata store.
Stores table definitions
Maintains schema
Tracks data location (usually in S3)
Enables schema discovery

2. Glue Crawlers
Scan your data in S3
Automatically detect schema
Create tables in the Data Catalog

3. Glue Jobs
These are the ETL jobs.
Written in Python or Scala
Under the hood → runs on Apache Spark
Fully serverless (no cluster management)

4. Glue Triggers
Schedule jobs
Create job dependencies
Build workflows

Raw Data (S3)
     ↓
Glue Crawler (detect schema)
     ↓
Glue Job (transform)
     ↓
Curated Data (S3 / Redshift)
```

#### Q-7 How do data engineering migrations benefit from the use of AWS DMS (Database Migration Service) ?
```bash
It moves data from source → target while keeping the source database running.
1. Minimal Downtime (Biggest Benefit)
DMS supports:
Full initial load
Then ongoing replication using CDC (Change Data Capture)

So:
It copies existing data.
Then continuously syncs changes.
During cutover → almost zero downtime.

On-Prem DB
    ↓
DMS
    ↓
S3 (raw layer)
    ↓
Glue / EMR
    ↓
Redshift
```

#### Q-8 How does AWS Glue support schema evolution in data engineering?
```bash
What Is Schema Evolution?

New columns added
Columns removed
Data type changes
Nested JSON structure changes

1. Glue Data Catalog Handles Schema Changes
The Glue Data Catalog stores metadata about tables (columns, types, partitions).
When schema changes:
Crawlers can detect new columns
It updates the table definition automatically
Downstream tools (Athena, Redshift Spectrum) see updated schema

2. Glue Crawlers Detect New Columns
{
  "id": 1,
  "name": "Damon"
}

{
  "id": 1,
  "name": "Damon",
  "city": "Mumbai"
}

Detects new column city
Updates table metadata
No manual schema editing required
```

#### Q-9 What is AWS Glue Spark Runtime, and how does it utilize Apache Spark for distributed data processing ?
```bash
AWS Glue Spark Runtime is the managed Apache Spark execution environment used by Glue to run ETL jobs. It provisions 
Spark drivers and executors automatically using DPUs, enabling distributed and parallel data processing without 
requiring cluster management. It leverages Spark’s partitioning, fault tolerance, and in-memory processing capabilities 
while integrating tightly with AWS services like S3 and IAM.

AWS Glue Spark Runtime is the managed Apache Spark environment that runs your Glue ETL jobs.

When you create a Glue job, AWS automatically spins up a managed Apache Spark cluster behind the scenes to 
execute your code.

What Happens When You Run a Glue Job: 
1. Glue provisions compute resources (DPUs)
2. A Spark driver is launched
3. Executors are created
4. Data is processed in parallel
5. Results written to S3 / Redshift / etc.

DPU = Data Processing Unit
1 DPU roughly includes:
4 vCPUs
16 GB memory
More DPUs → more Spark executors → more parallelism.
You configure how many DPUs your job uses.
```

#### Q-10 Explain the difference between Amazon RDS and Amazon Redshift ? 
```bash
| Feature      | Amazon RDS                    | Amazon Redshift             |
| ------------ | ----------------------------- | --------------------------- |
| Purpose      | Transactional database (OLTP) | Analytical warehouse (OLAP) |
| Workload     | Day-to-day app operations     | Reporting & analytics       |
| Data Volume  | GB–TB                         | TB–PB                       |
| Storage Type | Row-based                     | Columnar                    |
| Query Type   | Fast single-row queries       | Complex aggregations        |

Application → RDS (transactions)
            ↓
        ETL Pipeline
            ↓
        Redshift (analytics)
            ↓
        BI Dashboard
```

#### Q-11 What is IAM, and why is it important ?
```bash
AWS Identity and Access Management (IAM) is a service that helps you securely control access to AWS services and 
resources. IAM allows you to manage users, groups, and roles with fine-grained permissions. It’s important because 
it helps enforce the principle of least privilege, ensuring users only have access to the resources they need, thereby 
enhancing security and compliance.
```

#### Q-12 What is Amazon CloudWatch, and what are its main components ?
```bash
CloudWatch monitors your AWS resources and applications in real time.
1. Track performance
2. Monitor logs
3. Set alerts
4. Automatically respond to issues

Why Is CloudWatch Important?
1. EC2 crashes
2. Glue job errors
3. Redshift CPU spikes
4. Lambda timeout
5. Disk space fills up

Main Components of CloudWatch :

1. Metrics
Metrics are numerical measurements over time.
EC2 CPU utilization
RDS memory usage
Lambda invocation count
Glue job duration

2. Alarms
CPU > 85% for 5 minutes → send alert
Disk space < 10% → notify admin
Error rate spikes → rollback deployment

3. Logs
CloudWatch Logs collects and stores log data.

4. Events (Now EventBridge Integration)
1. Trigger actions based on system events
2. Run Lambda when EC2 stops
3. Trigger pipeline when file uploaded to S3
```

#### Q-13 What is AWS Lambda, and how does it enable serverless computing ?
```bash
AWS Lambda is a compute service provided by
Amazon Web Services that lets you run code without provisioning or managing servers.

You upload your code, and Lambda runs it automatically when triggered.

S3 file uploaded
        ↓
Lambda triggered
        ↓
Process file
        ↓
Store result

JSON file lands in S3
Lambda validates schema
Sends data to SQS
Triggers downstream ETL

Lambda is great for:
Lightweight transformations
Event-driven processing
Automation tasks
```

#### Q-14 What is Elastic Load Balancing (ELB) in AWS ?
```bash
Amazon Web Services that automatically distributes incoming application traffic across multiple targets.
ELB makes sure no single server gets overloaded.

User Traffic → Single EC2 → Crash
User Traffic
     ↓
Load Balancer
  ↙   ↓   ↘
EC2   EC2   EC2

How ELB Works
1️⃣ Users send requests
2️⃣ Load balancer receives them
3️⃣ It checks which server is healthy
4️⃣ Forwards request to that server

Users
   ↓
Route 53 (DNS)
   ↓
Application Load Balancer
   ↓
Auto Scaling Group (EC2 instances)
   ↓
RDS Database
```

#### Q-15 Explain how you would choose between Amazon RDS, Amazon DynamoDB, and Amazon Redshift for a data-driven application ? 
```bash
When to Choose Amazon RDS : 
✔ You need relational database features
✔ Strong ACID transactions
✔ Joins and structured schema
✔ Moderate scale (GB–few TB)
✔ Traditional application backend

Example use cases:
E-commerce app
Banking system
ERP system
User authentication database

RDS is best for:
👉 OLTP (Online Transaction Processing)

SELECT * FROM orders WHERE order_id = 101;

When to Choose Amazon DynamoDB

Use DynamoDB when:

✔ You need very high scale
✔ Millisecond latency
✔ Flexible schema
✔ No complex joins
✔ Massive concurrent traffic

Best for:
Gaming leaderboards
IoT applications
Session management
Real-time APIs

It’s a NoSQL key-value/document database.

When to Choose Amazon Redshift

Use Redshift when:
✔ You need large-scale analytics
✔ Complex joins across billions of rows
✔ Aggregations
✔ BI dashboards
✔ Historical reporting

Best for:

Business Intelligence
Data warehousing
Sales analysis
Marketing analytics

This is OLAP (Online Analytical Processing).

Application → RDS (transactions)
           → DynamoDB (sessions/cache)
           → ETL → Redshift (analytics)
```

#### Q-16 What are the main components of Amazon Redshift ?
```bash
At a high level, Redshift is a cluster-based, MPP (Massively Parallel Processing) system.

1️⃣ Leader Node
Think of this as the brain of the cluster 🧠
What it does:
Receives SQL queries from clients
Parses and optimizes queries
Creates execution plans
Distributes work to compute nodes
Aggregates final results

Important:
👉 It does NOT store user data
👉 It only manages query coordination

2️⃣ Compute Nodes
These are the workers ⚙️
What they do:
Store data
Execute queries
Perform joins, aggregations, sorting

Redshift distributes data across compute nodes to process queries in parallel.
This is why it’s fast for large analytics workloads.

3️⃣ Node Slices
Inside each compute node are slices.
Each slice gets a portion of data
Executes part of the query
Runs in parallel

Example:
If a node has 4 slices → it can process 4 partitions simultaneously.
This is where true parallelism happens.

4️⃣ Columnar Storage
Redshift stores data in columnar format, not row-based.

Why this matters:
✔ Faster aggregation
✔ Reads only required columns
✔ Better compression

5️⃣ Massively Parallel Processing (MPP)
Query
 ↓
Leader Node
 ↓
Compute Nodes (parallel execution)
 ↓
Results combined
```

#### Q-17 What types of data sources can you load into Amazon Redshift ?
```bash
Redshift can ingest data from almost anywhere — databases, files, streams, APIs, data lakes.
Amazon Redshift can ingest data from multiple sources including Amazon S3 files in formats like CSV, JSON, 
and Parquet using the COPY command. It can load from relational databases such as RDS and on-prem systems 
using AWS DMS or ETL tools. It also supports streaming ingestion from Kinesis and Kafka. Additionally, Redshift 
Spectrum allows querying data directly from S3 data lakes without loading it into the cluster.
```

#### Q-18 How does Amazon Redshift handle data compression and distribution ?
```bash
1️⃣ How Redshift Handles Data Compression
Redshift uses columnar storage.
That alone enables high compression because:
Data in a column is similar
Repeated values compress well
Only required columns are scanned

Column-Level Encoding

2️⃣ How Redshift Handles Data Distribution
Distribution controls how data is spread across compute nodes.
This is critical for joins and query speed.
Redshift distributes data across nodes using:

DISTSTYLE KEY
DISTKEY (customer_id)

In distributed systems, slow queries usually happen because of:
👉 Data shuffling between nodes
If distribution keys are chosen correctly:
Minimal data movement
Parallel joins
Faster aggregations
```

#### Q-19  What are some best practices for optimizing query performance in Amazon Redshift ?
```bash
1️⃣ Choose the Right Distribution Style
Bad distribution = massive data shuffling = slow queries.
✔ Use DISTKEY for large fact tables joined frequently
✔ Use ALL for small dimension tables
✔ Use EVEN if no clear join pattern

Goal:
👉 Minimize data movement between nodes.

2️⃣ Choose Proper Sort Keys
WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31'

3️⃣ Use Compression (Encoding)
✔ Use automatic compression during COPY
✔ Run ANALYZE COMPRESSION before large loads

4️⃣ Use COPY Instead of INSERT
COPY table_name
FROM 's3://bucket/file'
IAM_ROLE 'role';

6️⃣ Avoid SELECT *

7️⃣ Use Materialized Views
```

#### Q-20 How does Amazon Redshift handle concurrency and scalability ?
```bash
1️⃣ Concurrency Handling
Redshift uses Workload Management (WLM) to control concurrent queries.

Workload Management (WLM)
WLM allows you to:
Create query queues
Allocate memory per queue
Set concurrency limits
Prioritize workloads

Automatic WLM (Recommended)
Modern Redshift uses automatic WLM:
Dynamically allocates memory
Adjusts concurrency automatically
Uses ML to optimize resource usage

Concurrency Scaling
This is a big one.
Redshift can automatically add temporary clusters when concurrency increases.

If 100 users hit at once:
Main cluster continues processing
Redshift spins up additional clusters
Queries routed automatically
Clusters shut down when load drops
This feature is called Concurrency Scaling.

2️⃣ Scalability in Redshift
Horizontal Scaling (Add Nodes)

Redshift is an MPP system.
You can:
Increase number of compute nodes
Distribute data across more nodes
More nodes = more parallel processing.

RA3 Nodes (Compute & Storage Separation)
Modern clusters use RA3 nodes.
Benefits:
Compute and storage separated
Managed storage automatically scales
No need to overprovision compute for storage
```
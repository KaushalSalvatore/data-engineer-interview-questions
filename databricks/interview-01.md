#### Q-1 Unity Catalog in Databricks ?
```bash
Unity Catalog is the central governance layer inside Databricks.

It manages:
🔐 Data access control
📊 Metadata (tables, schemas, catalogs)
🧬 Data lineage
📜 Audit logs
🌍 Cross-workspace sharing

The security + control center for all your data in Databricks.
```

#### Q-2 What do you understand about Databricks ?
```bash
A central workshop where raw data comes in, gets cleaned, analyzed, and turned into insights or AI models—without 
needing separate tools for each step.

Databricks is a unified data platform that lets teams:
Process large-scale data
Build machine learning models
Run analytics and dashboards
Collaborate across data teams

It runs on major cloud providers like:
AWS
Azure (as Azure Databricks)
Google Cloud

Key Concepts

1. Lakehouse Architecture
Databricks popularized the Lakehouse concept:
Combines a data lake (cheap storage, flexible)
With a data warehouse (structured, fast querying)

This is powered by:
Delta Lake – adds reliability, ACID transactions, and versioning to data lakes

2. Apache Spark Integration
3. Collaborative Workspace
Notebooks (Python, SQL, Scala, R)
Shared dashboards
Real-time collaboration

4. Machine Learning & AI
5. Data Engineering & ETL
Used to:
Clean and transform raw data (ETL/ELT)
Build data pipelines
Schedule jobs
```

#### Q-3 databricks vs snowflake ? 
```bash
| Aspect     | Databricks                      | Snowflake                              |
| ---------- | ------------------------------- | -------------------------------------- |
| Core Idea  | Data + AI platform (lakehouse)  | Cloud data warehouse (analytics-first) |
| Best For   | Data engineering, ML, big data  | BI, SQL analytics, reporting           |
| Users      | Data engineers, data scientists | Data analysts, business users          |
| Complexity | More flexible, but harder       | Easier, more plug-and-play             |

1. Architecture Difference (Biggest Concept)
Databricks → Lakehouse
Combines data lake + warehouse
Handles structured + unstructured data
Built on Apache Spark
Uses Delta Lake

👉 Great for:
Raw data (logs, images, streaming)
AI/ML pipelines

Snowflake → Data Warehouse
Fully managed SaaS platform
Optimized for structured data + SQL
Separates storage & compute cleanly

👉 Great for:
Dashboards
Business reporting

2. Use Case Split (Very Important)
-> Choose Databricks if you want:
Machine learning / AI pipelines
Real-time streaming data
Complex ETL pipelines
Data science workflows

-> Choose Snowflake if you want:
Fast SQL analytics
BI dashboards (Power BI, Tableau)
Data sharing across teams

3. Performance (Depends on Workload)
-> Databricks
Faster for:
ETL pipelines
Big data processing
ML workloads

-> Snowflake
Faster for:
SQL queries
Business analytics
High concurrency

4. Governance & Security

-> Snowflake
Strong out-of-the-box governance
Easier compliance setup

-> Databricks
Flexible governance (Unity Catalog)
More customizable, but requires setup
```

#### Q-4 What is the core architecture of Databricks ?
```bash
Databricks has two main layers:

1. Control Plane (Managed by Databricks)
UI (workspace, notebooks)
Job scheduling
Cluster management
Security & access control
👉 This layer is fully managed by Databricks.

2.  Data Plane (In Your Cloud Account)
Actual data storage (S3 / ADLS / GCS)
Compute clusters (VMs running Spark)
Data processing happens here
👉 This ensures:
You own your data
Better security & compliance


Core Building Blocks 
A. Storage Layer (Data Lake)
Stores raw and processed data
Uses cloud storage:
AWS S3

Delta Lake
Adds:
ACID transactions
Schema enforcement
Time travel (versioning)

B. Compute Layer
Powered by:
Apache Spark

Key features:
Distributed processing across clusters
Auto-scaling

Supports:
Batch processing
Streaming

👉 Clusters can be:
Interactive (for notebooks)
Job clusters (for pipelines)

C. Workspace Layer
Collaborative notebooks (Python, SQL, Scala, R)
Dashboards
Version control

D. ML & AI Layer
Includes:
MLflow (experiment tracking)
Feature Store
Model serving APIs

E. Governance Layer
Unity Catalog (central governance)
Data access control
Lineage tracking
```

#### Q-5 How to set up and manage clusters ?
```bash
1. How to Create a Cluster
Step 1: Go to Clusters
In Databricks UI → Click Compute / Clusters

Step 2: Click “Create Cluster”

Key Configuration Settings
1. Cluster Mode
Single Node → for testing
Standard → most common
High Concurrency → many users (BI tools)

2. Databricks Runtime (Very Important)
Pre-installed environment with Spark + libraries
👉 Example:
Latest runtime (recommended)
ML runtime (for AI projects)

3. Node Type (VM Size)
Defines CPU, RAM
👉 Example:
Small → dev work
Large → big data processing

4. Workers (Scaling)
Fixed size → manual
Autoscaling → recommended

👉 Example:
Min: 2
Max: 8

5. Auto Termination
Stops cluster after inactivity
👉 Example:
30 minutes (saves cost)

6. Access Mode
Single user
Shared
No isolation

3. Types of Clusters (Important)

🧪 1. All-Purpose Cluster
Interactive use
Notebooks
Development

⚙️ 2. Job Cluster
Created automatically for jobs
Terminates after completion

👉 Best for production pipelines

👥 3. High-Concurrency Cluster
Multiple users
BI tools (Power BI, Tableau)

💰 4. Cost Optimization Tips (Very Important)
Use autoscaling
Enable auto-termination
Prefer job clusters for pipelines
Avoid idle clusters
```

#### Q-6 List the best practices for ETL processes in Databricks ? 
```bash
1. Use Medallion Architecture (Must Follow)
2. Use Delta Lake for Storage
3. Build Incremental ETL Pipelines (MERGE , change data capture)
4. Optimize Spark Jobs
5. Partition Data Properly
6. Ensure Data Quality in Silver Layer
7. Make Pipelines Idempotent (Meaning:Running the same job multiple times gives the same result)
(Use MERGE instead of INSERT , Avoid duplicate writes)
8. Automate with Workflows
9. Implement Data Governance
```

#### Q-7 How to maintain data security ?
```bash
1. Use Centralized Governance (Unity Catalog)
Centralizes access control
Tracks data lineage
Manages permissions at:Catalog,Schema,Table,Column level

2. Role-Based Access Control (RBAC)
Give users only what they need (Principle of Least Privilege)
Analyst → read-only access
Engineer → read + write
Admin → full control

3. Authentication & Identity Management
Integrate with:
Azure AD / AWS IAM / Google IAM

4. Data Encryption (Must Have)
At Rest:
Data stored in encrypted form (cloud storage)

5. Data Masking & Column-Level Security
Protect sensitive data like:
Emails
Phone numbers
Financial data

6. Data Isolation (Separate environments:)
Dev
Test
Prod
```

#### Q-8 How to optimize the performance of Databricks ?
```bash
1. Use Delta Lake Optimizations
Key features to use:
OPTIMIZE → compacts small files
Z-ORDER → improves query performance
OPTIMIZE sales ZORDER BY (customer_id);

2. Fix the Small Files Problem
Too many small files = slow queries ❌
Solution:
Use OPTIMIZE
Use Auto Optimize (Databricks feature)
👉 Ideal file size:
~100MB–1GB per file

3. Partition Data Properly
Partition by:
Date (most common)
Region / category

4. Optimize Spark Jobs
Best practices:
Use broadcast joins for small tables
Avoid unnecessary shuffles
Filter early (WHERE clause first)
Select only required columns

5. Use Caching Smartly
df.cache()

6. Tune Cluster Configuration
Right-size clusters:
Too small → slow
Too large → costly
Use:
Autoscaling
Latest runtime
```

#### Q-9 How to implement CI/CD pipelines in Databricks ?
```bash
-> CI (Continuous Integration)
Developers push code → automatically:
Validate
Test
Build

-> CD (Continuous Deployment)
Automatically deploy code to:
Dev → QA → Production

-> Core Components of Databricks CI/CD
You typically use:
Git (GitHub / Azure DevOps / GitLab)
Databricks Repos
CI/CD tools (Azure DevOps, GitHub Actions)
Databricks CLI / REST APIs

Developer commits code
        ↓
CI Pipeline runs tests
        ↓
Build artifacts created
        ↓
CD deploys to Dev
        ↓
Test → QA validation
        ↓
Deploy to Production
```

#### Q-10 What are Delta Live Tables (DLT) and how do they benefit ETL pipelines ?
```bash
DLT is a framework where you:
Define what transformations should happen
Databricks handles:
Execution
Dependency management
Error handling
Monitoring

It is built on top of:
Delta Lake
Uses Apache Spark under the hood

How DLT Works (Concept)
@dlt.table
def clean_sales():
    return spark.read.table("bronze.sales").filter("amount > 0")

Types of Tables in DLT
1. Streaming Tables
For real-time data

2. Materialized Views
For batch processing

3. Temporary Views
Intermediate transformations

1. Simplifies ETL Development
❌ Traditional:
Write ingestion
Write transformations
Manage orchestration manually

✅ DLT:
Just define transformations
Everything else is handled automatically
```

#### Q-11 What is the Photon ?
```bash
Photon is a vectorized query engine in Databricks that accelerates SQL and Spark workloads by using a 
C++ execution layer, improving performance and reducing cost.

1. Faster Performance
Uses vectorized execution (processes data in batches instead of row-by-row)
Optimized for:Joins, Aggregations, Scans

2. Works with Spark
Fully compatible with:
Apache Spark

Normal Spark = Reading one line at a time 📄
Photon = Reading entire pages at once 📚

Enable Photon
When creating a SQL warehouse or cluster:
Turn ON Photon

You write normal SQL queries, and Photon automatically accelerates them.
```

#### Q-12 What is Lakehouse Architecture in Databricks and why is it important ?
```bash
Data Lake
Stores raw data
Cheap and scalable
Supports structured + unstructured data

Data Warehouse
Structured data
Fast SQL queries
Strong governance
```

#### Q-13 What is Serverless Compute in Databricks and how does it benefit enterprises ?
```bash
Serverless compute in Databricks is a fully managed compute model where infrastructure provisioning, scaling, 
and maintenance are handled automatically, allowing users to focus only on writing queries and processing data.
```

#### Q-14 How does Unity Catalog enhance data governance in large organizations ?
```bash
Unity Catalog is a centralized governance solution in Databricks that provides fine-grained access control, 
data lineage, and auditing across all data assets, ensuring secure and compliant data usage in large 
organizations.

1. Centralized Access Control
Manage permissions in one place (not per workspace)
👉 Controls at:
Catalog
Schema
Table
Column level

2. Fine-Grained Security (RBAC)
Role-Based Access Control (RBAC)
👉 Example:
Analyst → read-only
Engineer → read/write
Admin → full access

3. Data Lineage (Very Important)
Tracks:
Where data comes from
How it is transformed
Where it is used
👉 Helps:
Debug pipelines
Understand data flow
Meet compliance requirements
```

#### Q-15 How does Databricks support real-time analytics and streaming pipelines in modern data architectures ?
```bash
Data Source (Kafka / APIs / Logs)
        ↓
Ingestion (Auto Loader / Streaming)
        ↓
Processing (Spark Structured Streaming)
        ↓
Storage (Delta Lake tables)
        ↓
Real-time Dashboards / ML / APIs

1. Structured Streaming (Core Engine)
df = spark.readStream.format("kafka").load()

Step 1: Ingest Streaming Data (Bronze Layer)

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define schema
schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Read streaming data (example: Kafka or JSON files)
raw_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .load("/mnt/raw/orders")

# Write to Bronze Delta table
raw_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/checkpoints/bronze_orders") \
    .table("bronze.orders")

Streaming Source → Bronze → Silver → Gold → Dashboard

⚡ Key Concepts Used
✔️ Streaming Read
spark.readStream
✔️ Streaming Write
writeStream
✔️ Checkpointing
Stores progress → ensures fault tolerance
✔️ Watermarking
Handles late-arriving data
```

#### Q-16 What is a Databricks job cluster ?
```bash
A Job Cluster in Databricks is a temporary (ephemeral) cluster that is automatically created to run a specific job 
and terminated once the job finishes.
🔄 Workflow
Job Trigger → Cluster Created → Job Runs → Cluster Terminated

Key Features
-> Ephemeral (Temporary)
Exists only during job execution

-> Fully Automated
No manual cluster management

-> Cost Efficient 💰
No idle time → pay only when used

-> Isolated Execution
Each job runs in its own environment
No interference with other workloads
```

#### Q-17 What is DBFS ?
```bash
DBFS is a distributed file system in Databricks that provides a unified interface to access and manage data stored 
in underlying cloud storage like S3 or ADLS.

DBFS is a distributed file system in Databricks that acts as an abstraction layer over cloud storage like 
S3 or ADLS, allowing users to access data using simple file paths and integrate seamlessly with Spark.

How DBFS Works (Databricks File System)
DBFS is not actual storage
It sits on top of:
AWS S3
Azure Data Lake Storage (ADLS)
Google Cloud Storage
👉 It makes cloud storage look like a local file system
```

#### Q-18 What actions should I take to resolve the issues I'm having with Azure Databricks ?
```bash
Step 1: Identify the Type of Issue
❌ Job failure
🐢 Slow performance
🔐 Permission / access issue
⚙️ Cluster not starting
📊 Incorrect data

Step 2: Check Logs & Error Messages
Step 3: Debug Spark Execution
Step 4: Validate Data Issues
Step 5: Optimize Performance Issues
Step 6: Restart / Reconfigure Cluster
Step 9: Monitor Jobs

Most Databricks issues are related to data quality, improper partitioning, or cluster misconfiguration, so I 
focus on those areas first.
```

#### Q-19 How do you handle streaming data in Databricks?
```bash 
In Databricks, I handle streaming data using Spark Structured Streaming for real-time processing, ingest data from 
sources like Kafka or cloud storage, apply transformations, and store results in Delta Lake tables. I also use checkpointing
and watermarking to ensure fault tolerance and handle late-arriving data.

1. Ingest Streaming Data
Sources:
Kafka
Event Hubs
Cloud storage (Auto Loader)

df = spark.readStream.format("kafka").load()

2. Process Data in Real-Time
Apply transformations:
Filtering
Joins
Aggregations

3. Handle Late Data (Watermarking)
df.withWatermark("timestamp", "10 minutes")
Late data is handled correctly
Memory is managed efficiently

4. Write to Delta Tables
processed_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/mnt/checkpoints") \
    .table("silver.data")

5. Use Checkpointing (Very Important)
Stores progress of stream
👉 Ensures:
Fault tolerance
No data loss

6. Build Medallion Architecture
Bronze → raw stream
Silver → cleaned data
Gold → aggregated insights

Key Features Databricks Provides
🚀 Structured Streaming
🔁 Incremental Processing
🔐 Exactly-Once Semantics
📊 Unified Batch + Streaming
⚙️ Auto Scaling

🏢 Real Example
👉 E-commerce app:
User clicks → event generated
Data streamed via Kafka
Databricks processes in real time

Updates:
Live dashboard
Recommendation system

I ensure fault tolerance using checkpointing and handle late-arriving data using watermarking, while leveraging
Delta Lake for exactly-once processing.
```

#### Q-20 What is the purpose of the Delta Lake Change Data Feed (CDF)?
```bash
Delta Lake Change Data Feed allows us to track row-level changes such as inserts, updates, and deletes in a 
table, enabling efficient incremental data processing and downstream data synchronization.

🔄 To process only the changed data instead of the entire dataset
```
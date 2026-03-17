#### Q-1 what is difference in distributed process and storage ?
```bash
Distributed Processing : Distributed processing means splitting a computational task across multiple machines 
(nodes) that work together to process data or perform calculations.

Example:

Running parts of a large simulation on different servers.
A MapReduce job where:
Map tasks process chunks of data in parallel.
Reduce tasks aggregate the results.

Examples of technologies:
Apache Spark, Hadoop MapReduce

Distributed Storage : Distributed storage means splitting and storing data across multiple physical or virtual 
storage systems (nodes, disks, or servers), often with replication for reliability.

Example:

A file is split into blocks stored on different servers.
Cloud object storage like Amazon S3 or Google Cloud Storage.

Benefits:

High availability and fault tolerance (data is replicated).
Scalability (add more storage nodes to handle more data).
Improved access speed (data can be read from the nearest node).

Examples of technologies:

HDFS (Hadoop Distributed File System), Amazon S3, Google File System, Ceph, Cassandra 
(for distributed databases).
```

#### Q-2 Difference in RDD , DataSet , DataFrame ?
```bash
RDD (Resilient Distributed Dataset)
RDD is a low-level distributed collection of objects with no schema and no optimization, so it’s more flexible 
but slower.
DataFrame is a higher-level abstraction with schema, optimized by Catalyst and Tungsten, which makes it much 
faster and easier to use.
Dataset combines the benefits of RDD and DataFrame by providing schema and compile-time type safety, but it’s 
available only in Scala and Java.
In real projects, we mostly use DataFrames because they give the best performance with less code.

Which one do you use and why ?
I mostly use DataFrames because they are optimized, easy to write, and work well with Spark SQL.
I use RDD only when I need low-level transformations or unstructured data.
Dataset is useful in Scala when type safety is required.”
```

#### Q-3 AWS step function or Airflow which i have to choice for scheduling jobs ?
```bash
The choice between AWS Step Functions and Airflow depends on the workload and environment.
If the jobs are AWS-native, event-driven, and relatively simple workflows, I prefer AWS Step Functions because 
it’s serverless, highly reliable, and requires minimal maintenance.
If the workflows are complex, involve many dependencies, conditional logic, retries, or span across multiple systems 
and clouds, then Airflow is a better choice because it provides more flexibility and rich scheduling features.

Step Functions are great for orchestrating AWS services.
Airflow excels at complex data pipelines and scheduling logic.
```

#### Q-4 What is Dimensional Modeling ?  
```bash
Dimensional modeling is a data-warehouse design technique used to organize data for analytics and reporting.
It structures data into fact tables that store measurable business metrics and dimension tables that store 
descriptive attributes.
The goal is to make queries simple, fast, and easy for business users to understand.

1. Fact Table
Fact tables store quantitative measures like sales amount, quantity, or revenue, along with foreign keys to 
dimensions.

Examples:
sales_amount
order_count
profit

2️. Dimension Table
Dimension tables store descriptive attributes used for filtering and grouping.

Examples:
Customer (name, age, city)
Product (category, brand)
Date (day, month, year)
```

#### Q-5 Design Dimensional modeling for social media ? 
```bash
For social media, I would use dimensional modeling with fact tables to capture user activities like posts, 
likes, comments, and shares, and dimension tables to describe users, content, time, and platform attributes.
The design focuses on analytics such as engagement, growth, and content performance.

                    Dim_User
          ┌────────────────────────┐
          │ user_id (PK)           │
          │ username               │
          │ age                    │
          │ gender                 │
          │ country                │
          │ signup_date            │
          └──────────┬─────────────┘
                     │
                     │
Dim_Time      ┌───────▼───────────┐       Dim_Post
┌──────────┐  │Fact_User_Activity │  ┌───────────────┐
│ time_id  │  │-------------------│  │ post_id (PK)  │
│ date     │  │ user_id (FK)      │  │ post_type     │
│ day      │◄─┤ post_id (FK)      ├─►│ category      │
│ month    │  │ time_id (FK)      │  │ created_date  │
│ year     │  │ device_id (FK)    │  └───────────────┘
└──────────┘  │                   │
              │ like_count        │
              │ comment_count     │
              │ share_count       │
              │ view_count        │
              └─────────┬─────────┘
                        │
                        │
                 Dim_Device
          ┌────────────────────────┐
          │ device_id (PK)         │
          │ device_type            │
          │ OS                     │
          │ app_version            │
          └────────────────────────┘
```

#### Q-6 how to explain upstream and downstream clearly and confidently ? 
```bash
In a data pipeline, upstream refers to the systems or processes that provide input data, while downstream refers to 
the systems or processes that consume the output data.
Any change or failure upstream can impact downstream processes.

Source Systems → Spark ETL → Data Warehouse → BI Dashboard
   (Upstream)                    (Downstream)

Upstream → Data producers
Downstream → Data consumers
Impact rule → Upstream issues propagate downstream

For example, in a Spark ETL pipeline, the source database or Kafka topic is upstream.
The Spark job itself is in the middle, and the data warehouse tables, dashboards, or reports that use the output 
are downstream.
```

#### Q-7 what is Backpressure in streaming in kafka and how to handle backfill ? 
```bash
Backpressure happens when: Producers send data faster than consumers can process it
Producer → 10,000 msgs/sec
Consumer → 2,000 msgs/sec
➡️ Remaining 8,000 msgs/sec accumulate → lag = backpressure

1. Scale Consumers (Horizontal Scaling)
Increase number of consumers in a consumer group
Kafka will rebalance partitions
Rule: consumers ≤ partitions

2. Increase Partitions
More partitions = more parallelism

3. Optimize Consumer Processing
Batch processing instead of one-by-one
Tune configs:

Backfill in Kafka : 
Backfill = Processing historical data (old data) again.

When it happens:
New consumer added
Bug fix requires reprocessing
Data warehouse rebuild
Late-arriving data

How to Handle Backfill Safely
1. Reset Offsets
kafka-consumer-groups --reset-offsets
```

#### Q-8 How would you handle schema evolution in a data lake (e.g., a new column gets added in incoming JSON files) ?
```bash
You have JSON data landing in a data lake (e.g., Amazon S3):
Old :-
{
  "user_id": "123",
  "amount": 100
}
New :-
{
  "user_id": "123",
  "amount": 100,
  "currency": "INR"
}

1. Schema-on-Read (Recommended for Data Lakes)
SELECT 
  user_id,
  amount,
  COALESCE(currency, 'INR') AS currency
FROM transactions;

2. Schema Versioning in Data
{
  "schema_version": 2,
  "user_id": "123",
  "amount": 100,
  "currency": "INR"
}
```

#### Q-9 if in project we using hive and want to convert in redshift so how to handle schema conversation problem. because schema semantic is different (hint schema conversation tool SCT in AWS) ? 
```bash
We used AWS SCT to convert Hive schema to Redshift. SCT handled most DDL conversion, but we manually fixed data types, 
flattened complex structures like arrays/maps, and redesigned partitioning using sort/dist keys. Data was migrated via 
S3 using COPY command, followed by validation and query optimization.

Pro Tips
Use Parquet for faster migration
Avoid small files problem
Pre-clean data before migration
Automate validation scripts
```

#### Q-10 What is the best way to schedule daily pipeline with failure retry and notification alerts ?
```bash
Scheduler (Airflow)
      ↓
Trigger DAG (Daily)
      ↓
Task 1: Extract Data
      ↓
Task 2: Transform Data
      ↓
Task 3: Load Data
      ↓
Success / Failure Notification

The best way to schedule a daily pipeline with retries and alerts is to use an orchestration tool like Apache Airflow. 
I would create a DAG scheduled daily, configure task retries using retry and retry_delay, and enable failure 
notifications through email or Slack. Airflow manages task dependencies, monitoring, logging, and automatic 
retries, making it ideal for reliable production pipelines.
```

#### Q-11 How will you design a parameterized pipeline for dynamic data ingestion from multiple files ?  
```bash
Source Systems
 (CSV / JSON / Parquet / APIs)
        │
        ▼
Landing Zone (S3 / Data Lake)
        │
        ▼
Metadata Table (Config Driven)
        │
        ▼
Orchestration Layer (Airflow)
        │
        ▼
Dynamic Ingestion Engine (Spark)
        │
        ▼
Data Quality Checks
        │
        ▼
Target Storage
(Snowflake / Data Warehouse)
```

#### Q-12 What is the best way to handle null values during data transformation ? 
```bash
1. Replace Null Values with Default Values
SELECT COALESCE(salary,0) AS salary
FROM employees;

2. Remove Rows with Null Values
3. Apply Conditional Logic
4. Use Statistical Imputation (Mean Median Mode)
```

#### Q-13 Your job is failing due to OOM (Out of Memory) on the last shuffle step — how will you debug and optimize this issue ?
```bash
If a Spark job fails with an OOM error during the shuffle stage, I first check the Spark UI to identify the 
failing stage and memory usage. Then I optimize by increasing shuffle partitions, repartitioning data based on 
join keys, and reducing data size before the shuffle. I also check for data skew and apply techniques like salting 
if needed. If one dataset is small, I use broadcast joins to avoid shuffling. Finally, I tune executor memory 
and cluster resources if required.
```

#### Q-14 What are some best practices for monitoring and logging PySpark jobs ? 
```bash
1. Use Spark UI for Job Monitoring
Job stages and tasks
Execution tim
Shuffle read/write metrics
Executor memory usage

2. Implement Structured Logging
import logging

logger = logging.getLogger("pyspark_job")

logger.info("Starting data transformation")
logger.error("Data load failed")

3. Track Job Metrics
Record counts
Processing time
Input/output data size
Failed records

4. Integrate with Monitoring Tools
Grafana

5. Configure Alerts and Notifications
Email
Slack
PagerDuty
```

#### Q-15 How do you deploy PySpark applications in a production environment ?
```bash
pyspark_project/
 ├── main.py
 ├── config/
 │    └── config.yaml
 ├── utils/
 │    └── transformations.py
 ├── requirements.txt

 Git Repository
      ↓
CI/CD Pipeline
      ↓
Build PySpark Package
      ↓
Airflow Scheduler
      ↓
spark-submit
      ↓
Spark Cluster (YARN/Kubernetes)
      ↓
Data Lake / Warehouse
```

#### Q-16 Write a PySpark code to find the top 3 customers with the highest revenue per region ?
```bash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, rank
from pyspark.sql.window import Window

# Create Spark session
spark = SparkSession.builder.appName("TopCustomersPerRegion").getOrCreate()

# Sample data
data = [
    ("North", "C1", 500),
    ("North", "C2", 700),
    ("North", "C3", 300),
    ("North", "C4", 900),
    ("South", "C1", 400),
    ("South", "C2", 800),
    ("South", "C3", 200),
    ("South", "C4", 600)
]

columns = ["region", "customer_id", "revenue"]

df = spark.createDataFrame(data, columns)

# Aggregate revenue per customer per region
revenue_df = df.groupBy("region", "customer_id") \
               .agg(sum("revenue").alias("total_revenue"))

# Define window partitioned by region
window_spec = Window.partitionBy("region").orderBy(col("total_revenue").desc())

# Rank customers
ranked_df = revenue_df.withColumn("rank", rank().over(window_spec))

# Filter top 3 customers per region
top_customers = ranked_df.filter(col("rank") <= 3)

top_customers.show()
```

#### Q-17 Steps you would take to tune a slow running spark application ?
```bash
1. Analyze Spark UI
2. Optimize Data Partitioning
3. Reduce Data Shuffling
4. Use Broadcast Joins
5. Handle Data Skew
6. Cache Reused Data
7. Optimize File Formats
```

#### Q-18 Which storage level in persist is optimized for storage ?
```bash
the persist() method allows you to store DataFrames or RDDs in memory or disk using different storage levels.

from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_ONLY_SER)

| Storage Level           | Description                                                       |
| ----------------------- | ----------------------------------------------------------------- |
| **MEMORY_ONLY**         | Stores deserialized objects in memory (fast but uses more memory) |
| **MEMORY_ONLY_SER**     | Stores serialized objects in memory (memory efficient)            |
| **MEMORY_AND_DISK**     | Stores in memory, spills to disk if memory is insufficient        |
| **MEMORY_AND_DISK_SER** | Serialized in memory and disk                                     |
| **DISK_ONLY**           | Stores data only on disk                                          |
```

#### Q-19 How do you optimize Spark jobs to run faster when dealing with terabytes of data ?
```bash
```

#### Q-20 Explain optimization techniques in Spark ?
```bash
1. Use Efficient File Formats
Parquet
ORC

2. Partitioning & Data Layout Optimization

3. Reduce Shuffle (Most Important 🔥)
Shuffle = disk + network + memory.
Techniques:Filter before join,Avoid unnecessary groupBy,Avoid distinct unless required
Use map-side operations

4. Join Optimization
-> Broadcast Join
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")
-> Handle Data Skew (Key salting)
-> Tune Spark Configurations
spark.sql.shuffle.partitions
executor memory
executor cores
driver memory

5. Caching & Persistence
```
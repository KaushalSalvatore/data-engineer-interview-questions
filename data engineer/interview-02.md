#### Q-1 What is the difference between partitioning and bucketing in PySpark ?
```bash
Partitioning in PySpark : Partitioning physically divides data into folders based on column values.
df.write.partitionBy("country").parquet("path")
path/
  country=India/
  country=US/
  country=UK/

Bucketing in PySpark : Bucketing distributes data into a fixed number of files using a hash function.
df.write.bucketBy(10, "user_id").saveAsTable("users_bucketed")

How it works
Spark hashes user_id
Distributes rows into 10 fixed buckets
Creates 10 files regardless of unique values

| Partitioning                         | Bucketing                              |
| ------------------------------------ | -------------------------------------- |
| Divides data by column value         | Divides data by hash of column         |
| Creates folders                      | Creates fixed number of files          |
| Best for filtering                   | Best for joins                         |
| Number of partitions depends on data | Number of buckets is fixed             |
| No shuffle optimization for join     | Can avoid shuffle if bucketed same way |

Use Partitioning When:
Column has few distinct values
Queries use WHERE filters
Example: year, month, country

Use Bucketing When:
Joining large tables on a column
Column has high cardinality (like user_id)
You want shuffle optimization
```

#### Q-2 How do you handle data skewness in distributed data processing ?
```bash
Data skew happens when one (or a few) partition(s) get disproportionately large amount of data, 
usually during:
Joins
GroupBy
Aggregations
Shuffle operations

Example:
If 70% of records have country = 'India', then during a groupBy(country) or join on country → one 
partition gets overloaded.

Result?
One task runs forever
Others finish quickly
Job time = slowest task time
Possible OOM

How To Handle Data Skew in Distributed Processing
1. Repartitioning Smartly
df.repartition(200, "user_id")

2. Salting (Most Common Interview Answer)
Problem : Joining on a key where one key appears too many times.
Solution : Add random salt to break large key into smaller chunks.

from pyspark.sql.functions import rand, floor
df1 = df1.withColumn("salt", floor(rand()*10))
df2 = df2.withColumn("salt", explode(array([lit(i) for i in range(10)])))
df_joined = df1.join(df2, ["user_id", "salt"])

3. Broadcast Join (When One Table is Small)
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "user_id")
```

#### Q-3 Given a large table with millions of records, how would you optimize a query that uses multiple joins and filters?
```bash
1. Filter As Early As Possible (Push Down Data Reduction)
2. Choose the Right Join Strategy
BroadcastHashJoin (If one table is small)
SortMergeJoin
ShuffleHashJoin
3. Handle Data Skew (Very Important)
4. Use Efficient File Format & Partition Pruning
5. Avoid Unnecessary Columns
6. Cache If Intermediate Used Multiple Times
```

#### Q-4 Describe a situation where a pipeline failed in production. How did you troubleshoot it ?
```bash
In one project, a scheduled ETL pipeline failed due to a Spark OutOfMemory error. I checked the orchestration 
logs to identify the failing stage, then analyzed the Spark UI and found severe data skew during a join. A new 
data source had introduced highly imbalanced keys. I applied a temporary fix by increasing executor memory and 
shuffle partitions to restore service quickly. Then I implemented a permanent solution using adaptive query 
execution and repartitioning to handle skew. I also added monitoring to prevent recurrence.
```

#### Q-5 How do you implement incremental data loads in your pipelines?
```bash
Incremental Load : - We process only new or changed records since the last successful run.

1. Timestamp-Based Incremental Load
If source table has:
created_at
updated_at
last_modified_ts
```

#### Q-6 Explain CDC and how you have implemented it ? 
```bash
Change Data Capture (CDC) is a technique to capture only incremental changes (INSERT, UPDATE, DELETE) 
from a source system instead of reloading full data every time.
Instead of: Loading 100M rows daily
We:Load only changed rows

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/delta/orders")
(deltaTable.alias("target")
 .merge(
     sourceDF.alias("source"),
     "target.id = source.id"
 )
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())
```

#### Q-7 Develop a program to read a CSV file, extract unique values from a column, and save the results in a new file and dataframe ? 
```bash
1️⃣ Using PySpark (Best for Databricks / Big Data)
from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder.appName("UniqueValues").getOrCreate()

# Read CSV file
df = spark.read.csv("input_file.csv", header=True, inferSchema=True)

# Extract unique values from a column
unique_df = df.select("column_name").distinct()

# Show results
unique_df.show()

# Save results to a new CSV file
unique_df.write.mode("overwrite").csv("output_unique_values", header=True)

2️⃣ Using Pandas (Best for Small Files)
import pandas as pd

# Read CSV file
df = pd.read_csv("input_file.csv")

# Extract unique values
unique_values = df["column_name"].drop_duplicates()

# Convert to dataframe
unique_df = pd.DataFrame(unique_values)

# Save to new CSV file
unique_df.to_csv("unique_values.csv", index=False)

print(unique_df)
```

#### Q-8 What is the difference between a temporary view and a global view in Spark?
```bash
A temporary view is session-scoped and accessible only within the current Spark session, while a global temporary 
view is accessible across multiple sessions within the same application.

| Feature    | Temporary View                | Global Temporary View           |
| ---------- | ----------------------------- | ------------------------------- |
| Scope      | Single session                | Multiple sessions               |
| Visibility | Only current notebook/session | All notebooks/sessions          |
| Lifetime   | Ends when session ends        | Ends when application ends      |
| Naming     | Direct name                   | Must use `global_temp` database |

Temporary View :-
df.createOrReplaceTempView("sales_view")
SELECT * FROM sales_view;

Global Temporary View
df.createOrReplaceGlobalTempView("sales_view")
SELECT * FROM global_temp.sales_view;

The main difference is scope: temporary views are session-specific and disappear when the session ends, whereas 
global temporary views are shared across sessions and persist for the lifetime of the Spark application, and must 
be accessed using the global_temp database.
```

#### Q-9 AWS vs Azure servises Mapping Cheat Sheet
```bash
💻 Compute

| AWS               | Azure            |
| ----------------- | ---------------- |
| EC2               | Virtual Machines |
| Auto Scaling      | VM Scale Sets    |
| Lambda            | Azure Functions  |
| Elastic Beanstalk | App Service      |

📦 Storage
| AWS     | Azure           |
| ------- | --------------- |
| S3      | Blob Storage    |
| EBS     | Managed Disks   |
| EFS     | Azure Files     |
| Glacier | Archive Storage |

🗄️ Databases
| AWS      | Azure                      |
| -------- | -------------------------- |
| RDS      | Azure SQL Database         |
| Aurora   | Azure SQL Managed Instance |
| DynamoDB | Cosmos DB                  |
| Redshift | Synapse Analytics          |

📊 Data Engineering / Big Data ⭐
| AWS              | Azure                          |
| ---------------- | ------------------------------ |
| EMR              | HDInsight                      |
| Glue             | Data Factory                   |
| Kinesis          | Event Hubs                     |
| Athena           | Synapse Serverless SQL         |
| Lake Formation   | Azure Data Lake Storage (ADLS) |
| Databricks (AWS) | Azure Databricks               |

🔐 Identity & Security
| AWS     | Azure                  |
| ------- | ---------------------- |
| IAM     | Azure Active Directory |
| Cognito | Azure AD B2C           |
| KMS     | Key Vault              |
```

#### Q-10 In Snowflake, you are implementing a data loading strategy for a high-volume streaming scenario where data arrives continuously and needs to be available for querying within minutes. Which combination of Snowflake features provides the optimal solution ?
```bash
The optimal solution is:👉 Snowpipe + Streams + Tasks

-> Why this combination is best for near real-time streaming:
1. Snowpipe
Automatically ingests data as soon as it lands in cloud storage
Event-driven (via notifications) → low latency (seconds to minutes)
Eliminates need for manual or scheduled loads

2. Streams
Tracks incremental changes (CDC) in loaded tables
Allows you to process only new or changed data, not full reloads

3. Tasks
Automates downstream transformations
Can run on a schedule or trigger-based workflow
Enables continuous pipelines (ELT pattern)

-> How they work together:
1. Data lands in cloud storage (e.g., S3)
2. Snowpipe ingests it automatically into staging tables
3. Streams capture newly ingested data
4. Tasks process and transform that data into final tables
```

#### Q-11 Your ETL pipeline processes financial data where data quality is critical. You need to implement comprehensive validation that checks for referential integrity, business rule compliance, and statistical anomalies. Which approach provides the most robust validation framework ? 
```bash
A layered validation framework combining constraints, business rules, and anomaly detection with strong logging 
and alerting.

A single technique is not enough for financial-grade data quality. You need multiple layers of validation working 
together:

1. Structural & Referential Integrity
Enforce primary/foreign key constraints (where possible)
Validate relationships between tables (e.g., transactions ↔ accounts)
Catch missing or orphaned records early

2. Business Rule Validation
Implement custom validation logic (e.g., balance ≥ 0, transaction limits)
Use rule engines or validation functions for maintainability
Example: reject trades outside allowed thresholds

3. Statistical / Anomaly Detection
Detect outliers using:
Z-score / standard deviation
Historical trend comparisons
Volume spikes or unusual patterns

4. Logging, Auditing, and Alerting
Capture detailed validation errors (row-level + rule-level)
Maintain audit trails for compliance
Trigger alerts for critical failures
```

#### Q-12 Your Python ETL application needs robust error handling for various failure scenarios: network timeouts, data format errors, memory issues, and external service failures. Which error handling strategy provides the best resilience ?
```bash
A robust ETL system must handle different failure types differently, not with a one-size-fits-all approach.
1. Use Specific Exception Handling
Catch granular exceptions instead of generic ones:
TimeoutError, ConnectionError → network issues
ValueError, TypeError → data format issues
MemoryError → resource constraints
Create custom exceptions for ETL-specific failures

2. Implement Retries with Exponential Backoff
Retry only transient failures (e.g., network, external APIs)
Use exponential backoff + jitter to avoid overload

3. Centralized Logging & Alerting
Log:
Error type
Context (job ID, record ID)
Stack trace
Send alerts for critical failures
```

#### Q-13 In Snowflake's multi-cluster warehouse architecture, you notice that your ETL jobs are experiencing queue wait times during peak hours. Your warehouse is set to auto-suspend after 10 minutes and auto-resume on query. What is the most cost-effective solution to reduce wait times while maintaining performance ?
```bash
Use multi-cluster auto-scaling to handle concurrency spikes instead of resizing the warehouse — it reduces queue 
time while staying cost-efficient.

Cost-effectiveness:
You only pay for extra clusters when they are actually used
Keeps auto-suspend (10 min) intact → avoids idle cost
Avoids over-provisioning a large warehouse all the time
```

#### Q-14   Your database query performance degrades significantly when filtering on a composite column (first_name + last_name). The table has 100M+ rows and the query is part of a critical ETL process. Which indexing strategy would provide the best performance improvement?
```bash
A composite index on (first_name, last_name) provides the most efficient access path and significantly reduces 
scan time for large datasets.

CREATE INDEX idx_name ON table(first_name, last_name);
```

#### Q-15 In SQL query optimization, which approach typically provides the best performance improvement for large dataset joins?
```bash
The best approach is: Adding appropriate indexes on join columns.
Proper indexing on join keys is the single most impactful optimization for large joins.

most effective clause for improving query performance when filtering large datasets:
The most effective clause is the WHERE clause

WHERE → filters before grouping/aggregation ✅
HAVING → filters after aggregation (less efficient)
DISTINCT → removes duplicates but adds overhead
ORDER BY → sorts data (expensive, not filtering)
```

#### Q-16 explain me i want to load 100 gb data everyday what would be cluster configuration in spark , databrick configuration and snowflake configration ? 
```bash
Spark Cluster Configuration :-
Rule of thumb
1 core ≈ 2–4 GB RAM
1 executor = 4–5 cores (avoid too large executors)
Partition size ≈ 128–256 MB

👉 Recommended Cluster 
| Component           | Config             |
| ------------------- | ------------------ |
| Executors           | 8–12               |
| Cores per executor  | 4                  |
| Memory per executor | 16–24 GB           |
| Total cores         | 32–48              |
| Driver              | 4 cores, 16 GB RAM |

Why this works
100 GB / 128 MB ≈ 800 partitions
40 cores → can process in parallel efficiently
Good balance of shuffle + memory

spark.executor.instances=10
spark.executor.cores=4
spark.executor.memory=20g
spark.sql.shuffle.partitions=400
spark.default.parallelism=400
spark.memory.fraction=0.6
spark.sql.adaptive.enabled=true

☁️ Databricks Configuration
| Setting      | Value                 |
| ------------ | --------------------- |
| Cluster Mode | Autoscaling           |
| Min Workers  | 4                     |
| Max Workers  | 12                    |
| Node Type    | i3.xlarge / r5.xlarge |
| Runtime      | Latest LTS (Spark 3+) |


Key Databricks Features to enable
Auto-scaling ✅
Photon Engine (huge performance boost)
Delta Lake (default storage)

Delta Optimization
OPTIMIZE table_name
ZORDER BY (important_column)

VACUUM table_name RETAIN 168 HOURS;

Snowflake Configuration :-
| Workload           | Warehouse Size |
| ------------------ | -------------- |
| 100 GB/day (light) | Small          |
| Moderate ETL       | Medium         |
| Heavy joins        | Large          |

CREATE WAREHOUSE etl_wh
WITH WAREHOUSE_SIZE = 'MEDIUM'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

Loading Strategy
Best approach:
Use Snowpipe (for near real-time)
Or COPY INTO (batch loads)

COPY INTO table_name
FROM @stage/path
FILE_FORMAT = (TYPE = PARQUET);

🧠 Performance Tips
Use clustering keys for large tables
Prefer Parquet over CSV
Use multi-cluster warehouse for concurrency

Databricks → if doing heavy Spark ETL + ML
Snowflake → if mostly SQL analytics
Spark (EMR) → if cost optimization is priority
```

#### Q-17 What is Delta Lake ? Delta table , Schema in DataFrame and Schema Evolution?
```bash
👉 Delta Lake is a storage layer built on top of data lakes (like S3/ADLS) that brings database-like 
features to big data.
Delta Lake is an open-source storage layer that adds ACID transactions, schema enforcement, and versioning 
on top of data lakes.

A Delta Table is just a table stored in Delta Lake format.

A Delta table is a table stored in Delta Lake format, where data is stored as Parquet files and changes 
are tracked using a transaction log, enabling versioning and reliable updates.

Schema in DataFrame : Schema defines the structure of a DataFrame, including column names, data types, and nullability.
id: int
name: string
age: int

Schema Evolution : Schema evolution allows us to modify the table structure, such as adding new columns,
without breaking existing data or pipelines.
```

#### Q-18 i have 8 columns requirement is 12 table so how to do it how pipeline effect explain me how to handle in databricks and snowflake ? 
```bash
Existing table → 8 columns
New requirement → 12 columns

1. How to Handle in Databricks

df.write.format("delta") \
  .option("mergeSchema", "true") \
  .mode("append") \
  .save("path")

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

What happens internally
New columns automatically added to table
Old records → NULL for new columns
No pipeline failure ✅

📊 Downstream Impact
BI dashboards may break
ETL jobs expecting 8 columns may fail

👉 Fix:
Version your tables (silver → gold layers)
Communicate schema change

2. How to Handle in Snowflake
✅ Step 1: Alter Table

ALTER TABLE table_name
ADD COLUMN new_col1 STRING,
ADD COLUMN new_col2 STRING,
ADD COLUMN new_col3 STRING,
ADD COLUMN new_col4 STRING;

Step 2: Update Load Process
COPY INTO table_name
FROM @stage
FILE_FORMAT = (TYPE = PARQUET);
```

#### Q-19 ingestion daily data into delta table but some data data is duplicate so how we can handle this and store only unique. show me both approch databicks and snowflake ? 
```bash
1. In Databricks (Delta Lake)

Approach 1: dropDuplicates (simple)

df = df.dropDuplicates(["id"])   # or multiple columns

df.write.format("delta") \
  .mode("append") \
  .save("path")

Approach 2: Window-based dedup (best practice)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window = Window.partitionBy("id").orderBy(col("updated_at").desc())

df_clean = df.withColumn("rn", row_number().over(window)) \
             .filter("rn = 1") \
             .drop("rn")
          
Approach 3: MERGE (Production-grade ✅)

MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *

❄️ 2. In Snowflake
✅ Approach 1: DISTINCT / QUALIFY (basic)

SELECT DISTINCT * FROM source_table;

SELECT *
FROM source_table
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1;

🔥 Approach 2: MERGE (Best Practice)
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
  t.col1 = s.col1,
  t.col2 = s.col2
WHEN NOT MATCHED THEN INSERT (
  id, col1, col2
) VALUES (
  s.id, s.col1, s.col2
);
```

#### Q-20 two large data set customer data transaction data 10 gb and 20 db how join efficiently and avoid skewness Show original results
```bash
I usually start with AQE and only move to salting if skew is still impacting performance.

Case 1: If one dataset is small enough (< ~10 GB compressed)
👉 Use Broadcast Join
from pyspark.sql.functions import broadcast
df = transactions.join(broadcast(customers), "customer_id")
✅ Why?
Avoids shuffle completely
Fastest join

But in your case:
10 GB + 20 GB → both large
👉 Broadcast likely NOT feasible

🔥 Approach 1: Salting (Most Important)
Used when some keys (like customer_id) are heavily skewed
from pyspark.sql.functions import rand, floor
transactions = transactions.withColumn("salt", floor(rand()*10))

Approach 2: Skew Join Optimization (Easy win)
spark.sql.adaptive.enabled = true
spark.sql.adaptive.skewJoin.enabled = true

🔥 Approach 3: Repartition before join
df1 = customers.repartition("customer_id")
df2 = transactions.repartition("customer_id")

df = df1.join(df2, "customer_id")

🔥 Approach 4: Filter skewed keys separately
👉 Advanced trick:

Identify skewed keys
Process separately
Union results

⚡ Step 4: Optimize Shuffle
spark.sql.shuffle.partitions = 400-800

👉 Best practical approach:

Enable AQE (auto skew handling)
Repartition on join key
If skew exists → use salting
Tune shuffle partitions
```
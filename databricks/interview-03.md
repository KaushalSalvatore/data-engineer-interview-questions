#### Q-1 if i have 5 excutore and my 1 excutore fail then what happed ?
```bash
Apache Spark, if one executor fails, the job does not fail immediately. Spark automatically recovers by re-running 
the failed tasks on another available executor.

Assume:

5 executors running
One executor crashes 💥

1. Executor failure is detected
Driver notices executor is lost
All tasks running on that executor are marked FAILED

2. Tasks are retried
👉 Spark will:
Reassign failed tasks to other executors (remaining 4)

✔ Important:
Spark works on tasks, not executors
Only failed tasks are re-run, not the whole job

3. Data is recomputed (Lineage concept)
👉 If data was lost, Spark uses:
RDD/DataFrame lineage
✔ Meaning:
Spark knows how data was created and can recompute it

4. Shuffle data case (important edge case)
If executor stored shuffle data:
That data is lost ❌
Spark will:
Re-run previous stage to regenerate shuffle data

🔹 5. Job continues (usually)
👉 As long as:
Enough resources are available
Failures are within retry limit
✔ Job completes successfully

When will job FAIL?

Spark will fail if:
Too many retries exceeded
Multiple executors fail repeatedly
Critical stage cannot be recomputed

spark.task.maxFailures (default = 4) (Retry Configuration)

👉 Think of a team of 5 workers:
One worker leaves suddenly
Manager (driver) redistributes work to others
Work still gets done, just a bit slower
```

#### Q-2 i config 5 executor 4 executor running completed and 1 is running from a long time what  would be the reason running long and how to solve that
```bash
Why 1 Executor Runs Longer?

1. Data Skew (Most Common)
👉 One partition has huge data, others are small

Partition 1 → 1GB
Partition 2 → 1GB
Partition 3 → 1GB
Partition 4 → 50GB  ← problem

2. Uneven Partitioning
Poor partition strategy
Not enough partitions
One executor gets more work

3. Skewed Join
👉 Happens during joins:
One key appears too frequently

How to Debug
👉 “I use Spark UI to identify the root cause.”

Check:
✅ Stage View
Look for one task taking much longer
✅ Task Metrics
Input size per task
Shuffle read/write
✅ Executor Tab
CPU / memory usage

How to Fix (Important for interview)
✅ 1. Fix Data Skew
✅ 2. Repartition Data
✅ 3. Use Adaptive Query Execution (AQE)
✅ 4. Increase Parallelism
✅ 5. Cache Intermediate Data
✅ 6. Check Join Strategy


Reason:
- Data skew (most common)
- Uneven partitions
- Skewed joins
- Slow node

Fix:
- Repartition
- Salting
- AQE
- Broadcast join
```

#### Q-3 mechanism for read and flatten the data in json in spark ? 
```bash
🔹 Step 1: File is split into partitions
Large JSON file → divided across executors
Each executor processes a chunk

🔹 Step 2: JSON parsing
Spark uses an internal JSON parser
Converts JSON → structured DataFrame

Case 1: Flatten Struct
df.select("id", "address.city", "address.zip")

🔹 Case 2: Flatten Array (explode)
Flatten Array (explode)
from pyspark.sql.functions import explode

df.select("id", explode("phones").alias("phone"))

Case 3: Nested Struct + Array
df.select(
    "id",
    "address.city",
    explode("phones").alias("phone")
)

4. Best Practices (important for interview)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([...])
spark.read.schema(schema).json("file.json")
```

#### Q-4 how many number of partitions will be created when we do wide dependency transformation ?
```bash
wide transformations create a shuffle, and the number of partitions in the next stage is not fixed—it depends on 
the shuffle partition configuration or the transformation used.

Wide transformation = shuffle happens
Examples:
groupBy
join
reduceByKey
distinct

Number of partitions = spark.sql.shuffle.partitions
Default value:
200

No. 200 is the default value of spark.sql.shuffle.partitions, but the actual runtime partitioning can be affected 
by Adaptive Query Execution. AQE can coalesce small shuffle partitions and optimize the number of partitions based on 
the actual data.
```

#### Q-5 i have  100 core in machine so how many partitions will be  there ? 
```bash
Cores ≠ Partitions
Cores → parallel execution
Partitions → units of data/work

If you have 100 cores
👉 This means:
You can run 100 tasks in parallel at a time

But how many partitions?
👉 It depends on the situation:

✅ Case 1: Wide transformation (like groupBy)
👉 Default:
Partitions = 200 (spark.sql.shuffle.partitions)

✅ Case 2: Reading a file
👉 Depends on:
File size
Block size (e.g., HDFS/S3 ~128MB)
Example:
1TB file → ~8000 partitions

🔥 Best Practice (Important!)
👉 Ideal partitions:
Partitions ≈ 2 to 4 × number of cores

For 100 cores: 👉 Recommended: 200 to 400 partitions

Why not 100 partitions?
👉 If partitions = 100:
Each core gets 1 task
No backup tasks if skew happens

👉 If partitions = 300:
Better load balancing
Handles slow tasks better

What if partitions are too low?
CPUs idle ❌
Poor performance ❌

⚠️ What if too many partitions?
Too many small tasks
Scheduling overhead

Cores define parallelism, while partitions define how work is divided. Ideally, we keep partitions higher than 
cores to ensure efficient utilization
```

#### Q-6 stateless and staefull stream processing in databricks ? 
```bash
Stateless stream processing processes each event independently without remembering previous events. Operations such as filter, select, withColumn, and data transformations are stateless because the output depends only on the current record. 

Stateful stream processing maintains information across micro-batches, so the output depends on both current and past events. Examples include aggregations (groupBy), windowed aggregations, stream-stream joins, session windows, and duplicate removal with watermarks.
```

#### Q-7 What is Liquid Clustering ?
```bash
Liquid Clustering is a Delta Lake feature that organizes data inside files based on one or more frequently filtered columns without creating physical partitions.

customer_id=1001/
customer_id=1002/

Liquid Clustering keeps the data in optimized files where similar values are grouped together.
Spark can skip many files using metadata, reducing the amount of data read.

| Partitioning                              | Liquid Clustering                                          |
| ----------------------------------------- | ---------------------------------------------------------- |
| Creates physical folders                  | Does not create folders                                    |
| Best for low-cardinality columns          | Best for high-cardinality columns                          |
| Manual partition selection                | Automatically organizes data                               |
| Too many partitions can hurt performance  | Avoids millions of small partitions                        |
| Commonly used for `date`, `year`, `month` | Commonly used for `customer_id`, `product_id`, `device_id` |
```

#### Q-8 What do you mean by a metadata-driven pipeline? Can you explain with an Azure Data Factory and Databricks example ? 
```bash
Instead of hardcoding every table/file/pipeline rule, we store the configuration in metadata tables, and one generic pipeline reads that metadata and decides what to process.

1. Traditional pipeline
Suppose you have 100 tables:

SQL Table 1 → ADF → ADLS → Databricks
SQL Table 2 → ADF → ADLS → Databricks
SQL Table 3 → ADF → ADLS → Databricks
...
SQL Table 100 → ADF → ADLS → Databricks
You might create 100 pipelines or lots of hardcoded activities.

2. Metadata-driven approach

pipeline_metadata
--------------------------------------------------------
source_table | target_table | load_type   | watermark
--------------------------------------------------------
customers    | customers    | FULL        | NULL
orders       | orders       | INCREMENTAL | updated_at
products     | products     | INCREMENTAL | updated_at
payments     | payments     | INCREMENTAL | modified_at

one generic pipeline :-

Metadata Table ->  ADF -> Read configuration -> tables (orders , products , customers) -> ADLS -> Databricks

If tomorrow you add transactions you can often add a metadata row rather than building an entirely new pipeline.

Azure SQL -> Metadata Table -> ADF -> Lookup Activity -> ForEach / Parameters -> ADLS Gen2 -> 

Databricks (Bronze , Silver , Gold) -> Synapse -> Power BI

What would metadata contain ?

CREATE TABLE pipeline_metadata (
    pipeline_id        INT,
    source_system      VARCHAR,
    source_schema      VARCHAR,
    source_table       VARCHAR,
    target_path        VARCHAR,
    target_table       VARCHAR,
    load_type          VARCHAR,
    watermark_column   VARCHAR,
    primary_key        VARCHAR,
    is_active          BOOLEAN
);

Step 1
How ADF uses metadata ->
SELECT *
FROM pipeline_metadata
WHERE is_active = true;
It gets: customers orders payments products

Step 2
ADF uses a ForEach.
ForEach table
     │
     ├── customers
     ├── orders
     ├── payments
     └── products

Step 3 :-
ADF passes parameters to Databricks.
source_table = orders
load_type = INCREMENTAL
watermark_column = updated_at
target_path = /bronze/orders/
```

#### Q-9 How do you monitor and troubleshoot failed jobs in Azure Databricks ?
```bash
First, I check the Databricks Jobs/Workflows page to identify whether the failure is at the job, task, or 
cluster level.

Second, I check the driver and executor logs to identify the actual exception. For example, whether it's an 
OutOfMemoryError, schema mismatch, permission issue, missing file, or Spark shuffle failure.

Third, I use the Spark UI to analyze failed stages. I specifically look for data skew, excessive shuffle, long-running tasks, 
spill to disk, and executor failures.

Fourth, I check the input data and upstream pipeline. For example, if ADF is responsible for loading files into ADLS, I verify 
whether the expected files arrived and whether the schema changed.

Finally, after identifying the root cause, I fix the issue, rerun only the failed task or appropriate job, and monitor the 
next execution. For production, I also configure retries, alerts, logging, and job dependencies so failures are detected 
automatically.

1. Check Databricks Job status
          ↓
2. Identify failed task
          ↓
3. Check driver/executor logs
          ↓
4. Check Spark UI
          ↓
5. Validate input files/schema
          ↓
6. Check ADLS/Unity Catalog permissions
          ↓
7. Identify root cause
          ↓
8. Fix + rerun
          ↓
9. Monitor successful completion

Common failures I would check :-

| Error                  | What I investigate                                  |
| ---------------------- | --------------------------------------------------- |
| `OutOfMemoryError`     | Data volume, partitioning, caching, executor memory |
| `FetchFailedException` | Shuffle, executor failure, network/resource issues  |
| Data skew              | Spark UI, uneven partition sizes                    |
| Schema mismatch        | Source schema vs Delta table schema                 |
| File not found         | ADLS path, upstream ADF pipeline                    |
| Permission denied      | Unity Catalog/storage permissions                   |
| Job timeout            | Long stages, inefficient joins, excessive shuffle   |
| Cluster terminated     | Driver/executor logs, cluster configuration         |
| Concurrent update      | Multiple jobs writing to same Delta table           |
| Bad records            | Data-quality checks and source data                 |
```

#### Q-10 Two jobs simultaneously update the same Delta table. What happens ?
```bash
The key concept is Optimistic Concurrency Control (OCC) 

Suppose both jobs start at the same time:
Delta Table = version 10

Job A ────────┐
              ├──> reads version 10
Job B ────────┘

then :-

Job A → modifies customer 101
Job B → modifies customer 101

Step 1 — Job A commits first 

Version 10 -> Job A -> Version 11 

Step 2 — Job B tries to commit
Job B started from version 10, but the table is now version 11.
Delta checks whether Job A changed data that conflicts with Job B.
If there is a conflict:
Job B → COMMIT FAILED
          ↓
Concurrent modification/conflict
Delta does not simply overwrite Job A's changes. Delta uses optimistic concurrency control and validates 
conflicts during commit.

Delta Lake supports concurrent writes using optimistic concurrency control. Suppose two jobs read the same Delta 
table version and both try to update it. Delta allows both jobs to proceed without locking the table. At commit time, 
it checks whether another transaction has modified data that conflicts with the current transaction. If there is no 
conflict, the transaction commits as a new Delta version. If both jobs modify the same data, the later transaction can 
fail with a concurrent modification conflict rather than silently overwriting the first transaction.

If the jobs modify independent data, they may both succeed, and modern Databricks also provides row-level concurrency for supported tables to reduce unnecessary conflicts. In production, I would minimize overlapping writes, use appropriate 
partition or predicate filters, and implement retry logic for transient concurrency failures.
```

#### Q-11 Compare watermark, CDC and CDF ?
```bash
Watermark = “Which records should I pick next? WHERE last_updated > previous_watermark
CDC = “What exactly changed in the source? INSERT / UPDATE / DELETE
CDF = “What changes happened in my Delta table? Delta Table → Changes

CDF
Suppose Bronze Delta is already updated using CDC.
Instead of scanning the entire Bronze table every time:
10 million rows
you can consume only the changes recorded by CDF:
620 changed records
This makes downstream incremental processing much more efficient.
```

#### Q-12 A CDC pipeline was down for 8 hours. How do you recover ?
```bash
1. First check: Why did the pipeline fail?
Before recovery, I identify the failure:- Kafka/CDC connector failure?, Source database connectivity?, Databricks job failure?
ADLS permission issue?, Schema change?, Target Delta/MERGE failure?, Checkpoint corruption?, Infrastructure issue?

2. Find the last successful CDC position: This is the most important step.
SQL Server → LSN
Oracle     → SCN
MySQL      → Binlog position / GTID
Kafka      → Topic + Partition + Offset

3. Check whether the CDC log is still available
4. Resume from the checkpoint
5. What about duplicate records?
6. Preserve CDC ordering
7. Bronze is extremely useful here
8. After catching up, perform reconciliation

The interviewer wants to hear these 7 keywords:

Checkpoint → Offset/LSN → Retention → Replay → Idempotency → Ordering → Reconciliation
```

#### Q-13 Explain OPTIMIZE vs VACUUM vs clustering ?
```bash
| Feature        | What it does                                             | Main purpose                            |
| -------------- | -------------------------------------------------------- | --------------------------------------- |
| **OPTIMIZE**   | Combines small files into larger files                   | Improve query/write performance         |
| **VACUUM**     | Deletes old, unreferenced data files                     | Free storage                            |
| **Clustering** | Organizes data based on columns commonly used in filters | Improve data skipping/query performance |

OPTIMIZE = Organize files
VACUUM = Remove old files
Clustering = Organize data by query patterns
```

#### Q-14 Explain managed tables vs external tables in Unity Catalog ? 
```bash
1. Managed Table

CREATE TABLE main.banking.customer (
    customer_id BIGINT,
    name STRING,
    balance DECIMAL(18,2)
)
USING DELTA;

I didn't specify a storage path.

Databricks stores the table data in the configured managed storage location associated with the Unity Catalog 
metastore/catalog/schema hierarchy.

Unity Catalog
     │
     ├── Catalog
     │      │
     │      └── Schema
     │             │
     │             └── Managed Table
     │                    │
     │                    └── Data files
     │                         managed storage

What does Databricks manage :- Table metadata ,table location, access permissions, underlying data lifecycle

2. External Table :- 

CREATE TABLE main.banking.customer_ext
USING DELTA
LOCATION 'abfss://raw@bankstorage.dfs.core.windows.net/customer/';

Now Unity Catalog knows:

Table:
main.banking.customer_ext

Location:
ADLS Gen2/customer/

Managed:
DROP TABLE
   ↓
Metadata + managed data lifecycle

External:
DROP TABLE
   ↓
Table registration removed
   ↓
External files remain
```

#### Q-15 Design an end-to-end production CDC architecture ? 
```bash
We have an OLTP banking database containing customer and account transactions. Whenever a customer is inserted, 
updated, or deleted, we need to capture those changes and make them available in our cloud data platform with minimum 
latency.

I land the raw CDC events into ADLS Gen2 Bronze as immutable Delta data. I preserve metadata such as operation type, 
event timestamp, source sequence/LSN, ingestion timestamp and event ID because these are important for ordering, 
auditing and replay.

In Databricks, I validate the schema, handle bad records, deduplicate events and process them into Silver. For the 
target current-state table, I use Delta MERGE for INSERT, UPDATE and DELETE operations. If the business requires 
historical tracking, I implement SCD Type 2 instead of simply overwriting the record.

Important CDC metadata :-
I don't store only business columns. I also capture metadata such as:

customer_id
name
balance 

operation
event_timestamp
ingestion_timestamp
source_system
transaction_id
LSN / SCN
batch_id

operation = I = insert 
D = delete
U = update
```

#### Q-16 You have 5 TB data in ADLS and your Databricks job takes 3 hours. How will you optimize it?
```bash
Partition pruning
Column pruning
Parquet/Delta
Small files
Shuffle
Data skew
Broadcast join
Caching only when required
Cluster sizing
Photon where applicable
```

#### Q-17 how to get data from API and store in dataframe ?
```bash
example 1 :-

import requests
response = requests.get("https://api.company.com/employees")
data = response.json()
df = spark.createDataFrame(data)
df.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/delta/employees")

example 2 :-

import requests
from pyspark.sql.types import *
url = "https://api.company.com/employees"
response = requests.get(url)
data = response.json()
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", IntegerType(), True)
])
df = spark.createDataFrame(data, schema)
```

#### Q-18 A team has a Databricks job that needs frequent joins among a large fact table and many dimension tables. How will they optimize the join operations for better performance ?
```bash
1️⃣ Use Broadcast Joins
from pyspark.sql.functions import broadcast
df = fact_df.join(broadcast(dim_df), "key")

Why it helps:
No shuffle of large fact table
Much faster joins
Ideal for star schema

2️⃣ Optimize Join Order
fact → dim1 → dim2 → dim3 (bad)
fact → dim1 → dim2 → dim3 (better)

3️⃣ Use Partitioning / Repartitioning
fact_df = fact_df.repartition("customer_id")
dim_df = dim_df.repartition("customer_id")

4️⃣ Handle Data Skew (Very Important ⚠️)
If some keys are very frequent → skewed joins → slow tasks.
Techniques:
Salting
Skew join hints in Spark

df = fact_df.join(dim_df.hint("skew"), "key")

5️⃣ Use Delta Table Optimizations (Databricks Specific)
a) Z-Ordering
OPTIMIZE table_name ZORDER BY (join_key)
b) Data Skipping
Delta automatically skips irrelevant files.

6️⃣ Use Caching for Reused Tables
dim_df.cache()

7️⃣ Choose Correct Join Type
Avoid unnecessary joins:
Use inner join if possible
Avoid wide joins when not needed

8️⃣ Enable Adaptive Query Execution (AQE)
spark.conf.set("spark.sql.adaptive.enabled", "true")
Benefits:
Converts sort-merge join → broadcast join automatically
Handles skew dynamically

9️⃣ Use Bucketing (Advanced)
CREATE TABLE fact
CLUSTERED BY (customer_id) INTO 100 BUCKETS

Benefit:
Avoid shuffle during joins

🔟 Reduce Data Before Join
filtered_fact = fact_df.filter("date > '2025-01-01'")
```

#### Q-19 A Databricks notebook is not running at its full potential due to big shuffle operations. How to identify and resolve this issue ?
```bash
🔍 1️⃣ How to Identify Shuffle Issues
A. Spark UI (Most Important)

Look at:
👉 Stages Tab
Check for stages with large shuffle read/write
Look for:
“Shuffle Read Size”
“Shuffle Write Size”

👉 Symptoms of Shuffle Problem
One stage taking much longer than others
Huge shuffle read (GBs/TBs)
Tasks stuck or slow

B. DAG Visualization
df.explain(True)

Look for:
Exchange → indicates shuffle
SortMergeJoin → expensive join
Aggregate → can cause shuffle

C. Skew Detection

In Spark UI:
Some tasks take much longer than others
One partition much larger

👉 This means data skew

🛠️ 2️⃣ How to Resolve Shuffle Issues
1. Use Broadcast Joins 🚀
2. Repartition Smartly
3. Reduce Data Before Shuffle
4. Handle Data Skew ⚠️
5. Tune Shuffle Partitions
spark.conf.set("spark.sql.shuffle.partitions", 200)
6. Enable Adaptive Query Execution (AQE)
7. Avoid Wide Transformations When Possible
8. Cache Intermediate Results
9. Use Delta Optimizations (Databricks)
```

#### Q-20 Write a PySpark code to process streaming data from kafka in Databricks ?
```bash
1️⃣ Basic Kafka → Spark Streaming → Console
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# Kafka value is in binary → convert to string
df_string = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Simple transformation
df_transformed = df_string.select(
    col("key"),
    col("value")
)

# Write to console (for testing)
query = df_transformed.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()

🧾 2️⃣ Kafka JSON Data Processing (Real Use Case)

example :- {"user_id": 1, "amount": 100, "timestamp": "2026-04-01"}

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Define schema
schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

# Read Kafka stream
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value to string
df_string = df.selectExpr("CAST(value AS STRING)")

# Parse JSON
df_json = df_string.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Example transformation
df_agg = df_json.groupBy("user_id").sum("amount")

# Write to console
query = df_agg.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start()

query.awaitTermination()

->  Common Mistakes to Avoid

❌ Not using checkpointing
❌ Using complete mode unnecessarily
❌ Not defining schema (causes slow inference)
❌ Ignoring watermarking for late data
```
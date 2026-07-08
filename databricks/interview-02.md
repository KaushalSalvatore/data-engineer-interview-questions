#### Q-1 In the Medallion Architecture (widely used in Databricks), data flows through three layers: Bronze → Silver → Gold. Each layer has a clear responsibility ? 
```bash
1. Bronze Layer (Raw Data)

What it does:
Stores raw, unprocessed data
Exact copy of source data

-> Characteristics:
No cleaning
No transformations
Append-only (keep history)

-> Example:
Logs from apps
CSV/JSON files from APIs
Database dumps

-> Stored using:
Delta Lake

Purpose: -> “Keep the original truth”

2. Silver Layer (Cleaned Data)
🔹 What it does:
Cleans and standardizes data

🔹 Operations:
Remove duplicates
Handle missing values
Fix data types
Join datasets

🔹 Example:
Clean customer table
Validated transactions

👉 Processed using: Apache Spark

Purpose:“Make data reliable and usable”

3. Gold Layer (Business Data)
🔹 What it does:
Creates business-ready datasets

🔹 Operations:
Aggregations
KPIs
Metrics
Reporting tables

🔹 Example:
Total sales per month
Revenue by region
Customer lifetime value

🧠 Purpose: “Make data useful for decision-making”
```

#### Q-2 How do you schedule jobs in Databricks?
```bash
Step 1: Create a Job
Go to Workflows → Jobs → Create Job
Add:
Job name
Task (Notebook / Python / JAR)

Step 2: Define Task
Choose:
Notebook
Python script
SQL query
👉 Example:
ETL notebook

Step 3: Configure Cluster
Choose:
Job cluster (recommended)
Existing cluster

Step 4: Set Schedule
👉 Use:
Simple schedule (daily, hourly)
OR
Cron expression

Types of Scheduling
1. Time-Based Scheduling
2. Cron-Based Scheduling
3. Event-Based Trigger (Advanced)
```

#### Q-3 Describe the Databricks Runtime ? 
```bash
Databricks Runtime (DBR) is a pre-configured, optimized environment in Databricks that includes
Apache Spark
Optimized libraries
Performance enhancements
Built-in integrations

Databricks Runtime is an optimized version of Apache Spark provided by Databricks, which includes performance
enhancements, pre-installed libraries, and integrations to efficiently run data engineering, analytics, and machine 
learning workloads.
```

#### Q-4 Explain the role of a driver in a Spark cluster ? 
```bash
The driver in a Spark cluster is the main process that runs the user application, creates the Spark session, 
builds the execution plan, and coordinates tasks across worker nodes.

Driver = Manager 👨‍💼
Executors = Workers 👷

Driver (Brain 🧠)
   ↓
Executors (Workers 💪)
   ↓
Process Data

                Spark Application

             +----------------------+
             |      Driver          |
             |----------------------|
             | SparkContext         |
             | DAG Scheduler        |
             | Task Scheduler       |
             | Collect Result       |
             +----------+-----------+
                        |
          ------------------------------
          |             |              |
      Executor 1    Executor 2    Executor 3
      (4 GB RAM)    (4 GB RAM)    (4 GB RAM)
          |             |              |
     Task1 Task2    Task3 Task4    Task5 Task6

Driver Memory : 

-> Creating SparkSession
-> Reading your code
-> Creating execution plans (DAG)
-> Scheduling tasks
-> Receiving results from executors
-> Holding objects created in the driver program

spark = SparkSession.builder.getOrCreate()

df = spark.read.csv("sales.csv")

Executor Memory :- 

-> Read partitions
-> Execute tasks
-> Store cached data
-> Perform joins
-> Perform aggregations
-> Shuffle data

df.groupBy("country").sum("sales")
```

#### Q-5 What is Auto Loader ?
```bash
Auto Loader is a Databricks feature that efficiently ingests new data files from cloud storage in an incremental and 
scalable way, using Structured Streaming.

Cloud Storage → Auto Loader → Spark Processing → Delta Table

Purpose of Auto Loader : 📥 Automatically detect and load new files only (no reprocessing)

Auto Loader is preferred for building reliable Bronze layer ingestion pipelines in Medallion architecture.
```

#### Q-6 How do you manage cluster configurations in Databricks ?
```bash
I manage cluster configurations in Databricks by selecting the appropriate cluster type, runtime version, node size, 
autoscaling settings, and Spark configurations. I also use job clusters for production workloads and monitor performance 
to optimize resource usage.

1. Choose Cluster Type
👉 Options:
All-purpose cluster
Development / notebooks

Job cluster
Production jobs (recommended)

2. Select Databricks Runtime
Choose version based on:
Stability (LTS)
ML vs Standard
Photon support

👉 Example:
Latest LTS for production

3. Configure Node Type (VM Size)
CPU / Memory selection

👉 Example:
Small jobs → small nodes
Large ETL → high-memory nodes

4. Enable Autoscaling
Set:
Min workers
Max workers

Right-sizing clusters and minimizing shuffle operations are key to achieving optimal performance and 
cost efficiency.
```

#### Q-7 How do you implement data lineage in Databricks ?
```bash
Data lineage means tracking: Where data comes from → how it is transformed → where it is used

In Databricks, I implement data lineage using Unity Catalog, which automatically tracks data flow across tables, 
notebooks, and jobs. I also follow structured ETL design and naming conventions to ensure traceability.

Unity Catalog : 
Automatically tracks:
Table dependencies
Column-level lineage
Job and notebook relationships
```

#### Q-8 What are the best practices for optimizing Delta Lake performance ?
```bash
To optimize Delta Lake performance, I focus on file compaction using OPTIMIZE, applying Z-ordering for data skipping, 
proper partitioning, avoiding small files, leveraging caching, and tuning Spark configurations. I also use incremental 
processing and monitor query performance regularly.

🚀 Best Practices for Optimizing Delta Lake

1. Use OPTIMIZE (File Compaction)
Problem:
Too many small files → slow reads
use : 
OPTIMIZE table_name;

Benefits:
Merges small files into larger ones
Improves read performance

2. Use Z-Ordering (Data Skipping)
OPTIMIZE table_name
ZORDER BY (customer_id);

👉 Benefits:
Faster filtering & joins
Reads fewer files

3. Proper Partitioning
Partition on:
Date
Region
Frequently filtered columns

4. Use Caching for Repeated Queries

Combining partitioning with Z-ordering gives the best performance for large datasets.
```

#### Q-9 How do you handle schema evolution in a Parquet file ?
```bash
1. Enable Schema Merging in Spark
df = spark.read.option("mergeSchema", "true").parquet("/data/path")

2. Add New Columns Safely
Old schema:
id, name

New data:
id, name, age

3. Enforce Schema During Read
from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

df = spark.read.schema(schema).parquet("/data")

4. Handle Data Type Changes Carefully
❌ Example:
int → string

👉 Solution:
Cast explicitly:
df.withColumn("age", col("age").cast("string"))

5. Avoid Column Renaming

In Parquet, schema evolution is handled by enabling schema merging in Spark, enforcing schemas during reads, 
and carefully managing changes like adding new columns. However, due to limitations in Parquet, Delta Lake is 
preferred for robust schema evolution and enforcement.
```

#### Q-10 What is watermarking in streaming data processing ?
```bash
Watermarking is a mechanism in streaming systems that defines how long to wait for late-arriving data before 
processing results, helping manage state and ensure correctness in aggregations.

Why Do We Need Watermarking?
In real-world streaming:
Data doesn’t always arrive on time ❌
Some records come late ⏱️

👉 Without watermarking:
Aggregations may be incorrect
Memory usage grows indefinitely

How Watermarking Works
df.withWatermark("event_time", "10 minutes")
👉 Meaning:
Spark will wait up to 10 minutes for late data
After that → late data is ignored

🔄 Example Scenario
| Event Time | Arrival Time   |
| ---------- | -------------- |
| 10:00      | 10:00 ✅        |
| 10:05      | 10:06 ✅        |
| 10:10      | 10:25 ❌ (late) |

👉 With watermark = 10 minutes:
Data arriving after 10 mins → dropped

Use Case: Aggregation
df.withWatermark("timestamp", "10 minutes") \
  .groupBy(window("timestamp", "5 minutes")) \
  .count()

Watermarking is especially important for stateful operations like windowed aggregations and joins in 
streaming pipelines.
```

#### Q-11 what is Z ordering in databricks ?  
```bash
Z-Ordering is a technique in Delta Lake that co-locates related data in the same files based on specified columns, 
improving data skipping and query performance.

Why Do We Need Z-Ordering?
When data is stored in a data lake:
Data is spread across many files
Queries scan many unnecessary files
👉 Result:
Slow queries

What Z-Ordering Does
Z-Ordering:
Reorders data inside files
Groups similar values together

👉 So queries can:
Skip irrelevant files
Read only required data

Without Z-Ordering
| customer_id | region | amount |
| ----------- | ------ | ------ |
| 101         | US     | 100    |
| 202         | India  | 200    |
| 101         | US     | 150    |
| 303         | UK     | 300    |

With Z-Ordering on customer_id
| customer_id | region | amount |
| ----------- | ------ | ------ |
| 101         | US     | 100    |
| 101         | US     | 150    |
| 202         | India  | 200    |
| 303         | UK     | 300    |

How to Apply Z-Ordering
OPTIMIZE sales
ZORDER BY (customer_id);
```

#### Q-12 what are schema enforement and schema evolution , and why are they critical ? 
```bash
Schema enforcement means: Data written to a table must strictly match the defined schema
If schema doesn’t match → write fails ❌

Schema evolution means:Ability to change schema over time without breaking existing data

Why They Are Critical (Databricks Context)
1. Data Quality (Schema Enforcement)
Prevents bad or corrupt data
Ensures consistent structure

2. Flexibility (Schema Evolution)
Business requirements change
New fields get added (e.g., new user attributes)

3. Reliable Data Pipelines
ETL jobs don’t break unexpectedly
Controlled schema changes

4. Backward Compatibility
Old data still works
New schema doesn’t break existing queries
```

#### Q-13 managed vs unmamnaged table explain with scenario ?
```bash
A managed table is one where : Databricks (or Spark) controls both metadata AND data
(Fully controlled by Databricks)

🔹 When to Use
✔ Temporary or intermediate data
✔ ETL processing tables
✔ When you don’t care about underlying files

An external table is one where: Databricks manages only metadata, but data stays in your storage
Data stored in:
Amazon S3
Azure Data Lake Storage
Dropping table → data is NOT deleted
You control storage

CREATE TABLE sales_external (
    id INT,
    amount DOUBLE
)
LOCATION 's3://my-bucket/sales/';
```

#### Q-14 how do you implement exception handling in Spark ?
```bash
1. 🧱 Driver-Level Exception Handling
👉 Used for:

Job orchestration
Reading configs
Triggering actions

try:
    df = spark.read.parquet("path")
    df.show()
except Exception as e:
    print(f"Error occurred: {e}")

✔ Catches errors like:

File not found
Syntax issues
Job submission failures

2. ⚙️ Executor-Level Exception Handling (Inside Transformations)
Spark executes transformations on worker nodes (executors), so errors inside functions must be handled there.

def safe_divide(x):
    try:
        return 10 / x
    except Exception:
        return None   # or default value

rdd = spark.sparkContext.parallelize([1, 2, 0, 4])
result = rdd.map(safe_divide).collect()

3. 🚫 Handling Bad Records (Production Approach)
Option 1: Using dropMalformed / permissive

df = spark.read.option("mode", "PERMISSIVE").json("data.json")

Modes:

PERMISSIVE (default) → keeps bad records
DROPMALFORMED → drops bad rows
FAILFAST → fails immediately

4. 🔁 Retry Mechanism (Spark Built-in Fault Tolerance)
```

#### Q-15 ETL pipeline is failed and nothing in trigger any error in airflow and CloudWatch and how i debug and what to do next in this situation ? 
```bash
In such cases, the issue is usually not a system failure but a logic or data issue, so I focus on tracing data flow and 
improving observability to catch silent failures early.

✅ “I trace data across stages, validate assumptions, and add checks to prevent silent failures”

-> Step 1: Verify if it’s really a failure
“First, I confirm whether the pipeline actually failed or if the issue is downstream.”

Check target tables / warehouse
Compare expected vs actual data

📌 Real-life example:
Sales dashboard shows zero revenue → pipeline “looks successful” but data is missing

-> Step 2: Check data at each stage (most important)

“Then I trace the data step-by-step through the pipeline.”

Source → Staging → Transform → Target
Run queries to validate counts

📌 Example:

Source has 1M rows
Staging has 1M
Final table has 0 → issue in transformation step

-> Step 3: Check Airflow DAG behavior

👉 “Even if Airflow shows success, I check task-level behavior.”

Look for:

Skipped tasks
Short-circuit operators
Conditional branching
Incorrect dependencies

📌 Example:

A task marked SUCCESS but actually skipped due to condition

-> Step 4: Check for silent failures (very common)

👉 “Many pipelines fail silently due to logic issues rather than system errors.”

Things to check:

⚠️ Data filtering mistakes
Wrong WHERE condition
Join condition removing rows (like your SQL example earlier 😉)
⚠️ Empty data loads
Source delivered empty file
API returned no data
⚠️ Schema mismatch
Column type change → data dropped silently

📌 Example:

API returned empty JSON → pipeline ran successfully but loaded nothing

🔍 Step 5: Re-run components manually

👉 “I isolate and rerun individual steps.”

Run SQL manually
Execute script locally
Trigger single Airflow task

📌 Example:

Transformation query returns 0 rows → root cause found
```

#### Q-16 how to build CDC data pipeline in databrivks if we have different source of data one is mongo db and second is sql or other database ?
```bash
To build a CDC pipeline in Databricks for multiple sources like MongoDB and SQL databases, I use a streaming-based 
architecture with a bronze–silver–gold design, where changes are captured from each source, standardized using Delta 
Lake, and merged into target tables using CDC logic.

Step 1: Capture CDC from sources
✅ MongoDB
Use:
MongoDB Change Streams
👉 Send changes to:
Kafka OR directly to cloud storage (JSON logs)

✅ SQL databases (MySQL/Postgres)
Use:
Debezium
👉 Reads:
binlog / WAL → pushes to Kafka

Step 2: Ingest into Databricks
df = spark.readStream.format("kafka") \
    .option("subscribe", "cdc_topic") \
    .load()

🔹 Step 3: Bronze Layer (Raw Data)
df.writeStream \
  .format("delta") \
  .option("checkpointLocation", "/chk/bronze") \
  .table("bronze_cdc")

Step 4: Normalize & Transform (Silver Layer)
👉 Problem:
MongoDB = nested JSON
SQL CDC = structured
👉 Solution:
Flatten + standardize schema

Step 5: Apply CDC using MERGE (Gold Layer)
MERGE INTO target_table t
USING silver_cdc s
ON t.id = s.id
WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
 
#### Q-17 read mode and write mode in spark (what is default read and write mode) ? 
```bash
Read Modes 
1. PERMISSIVE (Default)
👉 This is the default read mode
Keeps all records
Bad records → put in a special column (_corrupt_record)
Missing fields → set to NULL

spark.read.option("mode", "PERMISSIVE").json("file.json")

👉 Behavior:
No failure
Data is not lost
Errors are captured

2. DROPMALFORMED
Drops bad records completely
spark.read.option("mode", "DROPMALFORMED").json("file.json")

3. FAILFAST
Immediately throws error if bad record found
spark.read.option("mode", "FAILFAST").json("file.json")

2. Write Mode in Spark
1. error / errorifexists (Default)
👉 Default write mode
Throws error if data already exists

df.write.mode("error").save("path")

2. overwrite
Replaces existing data
df.write.mode("overwrite").save("path")

3. append
Adds new data to existing data
df.write.mode("append").save("path")

4. ignore
Does nothing if data exists
df.write.mode("ignore").save("path")
```

#### Q-18 Avro ,orc and Parque file format  difference ? 
```bash

```

#### Q-19 executor memory or driver memory explain memory distribution if i have 1 TB data ?
```bash
Sales.csv (E-commerce Sales)

Size = 1 TB

Cluster Size : 

Driver = 8 GB

Executor1 = 16 GB,Executor2 = 16 GB,Executor3 = 16 GB,Executor4 = 16 GB

Step 1 :- Driver creates

df = spark.read.csv("sales.csv")

Driver stores only :- (Location of file,Schema,Execution plan)
Not the actual 1 TB.

Step 2 :- Spark divides file

Partition 1 = 250 GB
Partition 2 = 250 GB
Partition 3 = 250 GB
Partition 4 = 250 GB

Each executor gets one partition.

Executor1 → Partition1
Executor2 → Partition2
Executor3 → Partition3
Executor4 → Partition4

Step 3 :- You execute

df.filter("country='India'")

Each executor filters its own partition

Executor1
250 GB
↓
10 GB

Same for all executors.
Driver only receives progress updates.

Step 4 :- You execute
df.count()

Executors compute local counts.

Executor1
Count = 25 Million

Executor2
Count = 27 Million

Executor3
Count = 24 Million

Executor4
Count = 26 Million

Executors send only numbers.

Driver receives :- 25M,27M,24M,26M

Driver adds them :- 102 Million

Driver never loads 1 TB.

Example 2: collect() 

step : 1 df.collect()

Executors send all rows back to Driver.

Executor1
250 GB
↓
Driver

Executor2
250 GB
↓
Driver

Executor3
250 GB
↓
Driver

Executor4
250 GB
↓
Driver

Driver now needs
250 + 250 + 250 + 250 = 1 TB RAM

But Driver has only = 8 GB

Result :- OutOfMemoryError

Example 3: Cache
df.cache()

Where is data cached?
Answer:
Executor memory

| Operation            | Driver Memory                     | Executor Memory |
| -------------------- | --------------------------------- | --------------- |
| SparkSession         | ✅ Yes                             | ❌ No            |
| Execution Plan (DAG) | ✅ Yes                             | ❌ No            |
| Reading CSV          | Metadata only                     | Actual data     |
| Filter               | No                                | Yes             |
| Join                 | No                                | Yes             |
| Aggregation          | Final result only                 | Yes             |
| Cache/Persist        | No                                | Yes             |
| Broadcast Variable   | Creates and sends                 | Stores a copy   |
| `count()`            | Final count                       | Computes counts |
| `collect()`          | Receives all data (can cause OOM) | Sends data      |
| Write Parquet        | Coordinates job                   | Writes files    |

Question: Your Spark job fails with java.lang.OutOfMemoryError: Java heap space on the Driver. Why?

Possible causes:

Using collect() on a large DataFrame.
Converting a large DataFrame to Pandas with toPandas().
Storing large Python objects on the driver.
Returning too much data from executors to the driver.

Fixes:

Avoid collect() on large datasets; use show(), take(n), or write results to storage instead.
Process data in a distributed way rather than pulling it to the driver.
Increase driver memory if appropriate:

Question: When should you increase executor memory?

Increase executor memory when executors run out of memory during:

Large joins
Aggregations (groupBy)
Sorting
Caching/persisting large datasets
Shuffle-intensive operations
```

#### Q-20
```bash
```
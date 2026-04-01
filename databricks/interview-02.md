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

#### Q-15
```bash
```

#### Q-16
```bash
```

#### Q-17
```bash
```

#### Q-18
```bash
```

#### Q-19
```bash
```

#### Q-20
```bash
```
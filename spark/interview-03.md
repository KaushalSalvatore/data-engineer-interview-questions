#### Q-1 Explain the difference between Spark's dynamic and static allocation, and when you might choose one ?
```bash
Static allocation → Fixed number of executors

Dynamic allocation → Executors increase or decrease based on workload

1. Static Allocation

--num-executors 5
--executor-cores 4
--executor-memory 8G

Here:

Spark will always use 5 executors
Even if workload is small
Even if workload increases later

Characteristics

Fixed resources
Predictable performance
Simpler configuration
Can waste resources if job is small
May be slow if workload suddenly increases

2. Dynamic Allocation

More tasks are pending → Spark adds executors
Executors are idle → Spark removes executors

spark.dynamicAllocation.enabled=true
```

#### Q-2 How can you calculate Executor Memory in Spark ?
```bash
Calculating executor memory is about proper resource planning so that:
Executors don’t run out of memory (OOM)
Cluster resources are efficiently utilized
You avoid too much GC overhead

Step 1: Understand Cluster Resources

Assume you know:
Total RAM per worker node
Number of CPU cores per worker
Number of worker nodes

Example:
1 worker node
64 GB RAM
16 cores

----------------------------------------------------------------------------
Step 2: Reserve Memory for OS & Hadoop

You should not use 100% memory.

Rule of thumb:
Reserve 1–2 cores
Reserve 10–15% memory for OS & system daemons

Example:
64 GB → keep ~8 GB for OS

Available = 56 GB

----------------------------------------------------------------------------
Step 3: Decide Number of Executors per Node

Best practice:
3–5 executors per node (commonly 4)

Why?
Too many → overhead & GC pressure
Too few → poor parallelism

Let’s choose:
4 executors per node

----------------------------------------------------------------------------
Step 4: Calculate Executor Memory

Available memory per node = 56 GB
Executors per node = 4

Executor memory ≈

56 / 4 = 14 GB per executor

Now subtract memory overhead.

----------------------------------------------------------------------------
Step 5: Consider Memory Overhead

Spark adds memory overhead for:
Shuffle
Serialization
Python processes
JVM metadata

Default:
max(384MB, 10% of executor memory)

For 14GB executor:
10% of 14GB ≈ 1.4GB
So actual usable executor memory ≈
14GB - 1.4GB = ~12.6GB

----------------------------------------------------------------------------
Final Configuration
--executor-memory 14G
--executor-cores 3 or 4
--num-executors total_across_cluster

----------------------------------------------------------------------------
General Formula

If:

Node memory = M
Reserved memory = R
Executors per node = E

Then:
Executor memory = (M - R) / E
Then subtract 10% overhead.

----------------------------------------------------------------------------
Important: Memory Types in Spark

Spark executor memory is divided into:

1️ Execution Memory
2️ Storage Memory
3️ User Memory
4️ Reserved Memory

----------------------------------------------------------------------------
You have 5 nodes, each with 64GB RAM and 16 cores. How would you configure Spark?

Reserve 8GB per node
Usable memory = 56GB
Use 4 executors per node
Executor memory = 14GB
Executor cores = 3–4
Total executors = 5 × 4 = 20
```

#### Q-3
```bash
```

#### Q-4
```bash
```

#### Q-5
```bash
```

#### Q-6 How would you approach integrating data from an external API into your PySpark workflow ?
```bash
```

#### Q-7 Explain how you would use PySpark to join data from a Hive table and a Kafka stream ?
```bash
```

#### Q-8 How would you integrate data from an external API into your PySpark pipeline ?
```bash
```

#### Q-9 Given a requirement to process and transform data from various sources like CSV, JSON, and Parquet files, how would you handle this in a PySpark job ?
```bash
```

#### Q-10 How would you design and implement an ETL pipeline using PySpark to extract data from an RDBMS, transform it, and load it into a data warehouse ?
```bash
```

#### Q-11 Describe the steps you would take to implement a solution in PySpark for processing real-time sensor data to detect anomalies ?
```bash
```

#### Q-12 How would you set up a real-time data pipeline using PySpark and Kafka to process streaming data ?
```bash
```

#### Q-13 When joining two large datasets causes out-of-memory errors, what strategies would you use to optimize the join operation ?
```bash
Large Joins Cause OOM 
Joins trigger shuffle
Data is redistributed across executors
Large partitions can overload executor memory
Skew can make one executor hold massive data


```

#### Q-14 If your PySpark job is running slower than expected due to data skew, how would you identify and resolve the issue ?
```bash
1. Check key distribution manually
df.groupBy("join_key") \
  .count() \
  .orderBy("count", ascending=False) \
  .show()

2. Enable Adaptive Query Execution (Best Modern Fix)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

3. Broadcast Join (If One Table Is Small)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

4. Salting Technique (Manual Fix for Heavy Keys)
from pyspark.sql.functions import rand, concat, lit

df = df.withColumn(
    "salted_key",
    concat(col("key"), lit("_"), (rand()*10).cast("int"))
)
```

#### Q-15 How would you flatten a dataset with nested JSON structures into a tabular format using PySpark ?
```bash
{"order_id": 1001,"customer": {"name": "Damon","location": {"city": "Mumbai","country": "India"}},
  "items": [{"product": "Laptop", "price": 800},{"product": "Mouse", "price": 20}]}

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

spark = SparkSession.builder \
    .appName("FlattenNestedJSON") \
    .getOrCreate()
df = spark.read.json("input/orders.json")
df.printSchema()
# Flatten Struct Columns
df_struct_flat = df.select(
    col("order_id"),
    col("customer.name").alias("name"),
    col("customer.location.city").alias("city"),
    col("customer.location.country").alias("country"),
    col("items")
)
# Explode Array Column
df_exploded = df_struct_flat.withColumn(
    "item",
    explode(col("items"))
)
#Extract Fields from Exploded Struct
final_df = df_exploded.select(
    "order_id",
    "name",
    "city",
    "country",
    col("item.product").alias("product"),
    col("item.price").alias("price")
)
```

#### Q-16 You have a dataset with user activity logs that contain missing values and inconsistent data types. How would you clean and standardize this dataset using PySpark?
```bash
First I inspect the schema and null distribution. Then I standardize data types, handle missing values, clean 
invalid records, normalize formats, remove duplicates, and finally validate the output.

1️⃣ Create Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, trim, lower, to_timestamp
from pyspark.sql.types import IntegerType

spark = SparkSession.builder \
    .appName("UserActivityCleaning") \
    .getOrCreate()

2️⃣ Read Raw Data
df = spark.read.csv("input/user_logs.csv", header=True)

3️⃣ Standardize Column Names
df = df.toDF(*[c.strip().lower() for c in df.columns])

4️⃣ Fix Data Types
df = df.withColumn("user_id", col("user_id").cast(IntegerType())) \
       .withColumn("duration", col("duration").cast(IntegerType())) \
       .withColumn("event_time", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss"))

5️⃣ Handle Missing Values
df = df.dropna(subset=["user_id", "event_time"])

6️⃣ Remove Duplicates
df = df.dropDuplicates()
```

#### Q-17 Write a PySpark job to accomplish a specific data processing task, such as filtering or aggregating data ?
```bash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum
# 1️⃣ Create Spark Session
spark = SparkSession.builder \
    .appName("SalesAggregationJob") \
    .getOrCreate()
# 2️⃣ Read Input Data (CSV example)
df = spark.read.csv(
    "input/sales.csv",
    header=True,
    inferSchema=True
)
# 3️⃣ Filter Data (amount > 1000)
filtered_df = df.filter(col("amount") > 1000)
# 4️⃣ Aggregate Data (Total sales per country)
agg_df = filtered_df.groupBy("country") \
    .agg(_sum("amount").alias("total_sales"))
# 5️⃣ Write Output
agg_df.write.mode("overwrite").parquet("output/total_sales_by_country")
# 6️⃣ Stop Spark Session
spark.stop()
```

#### Q-18 How do you handle resource management and scheduling within a PySpark application ?
```bash
To handle resource management and scheduling in a PySpark application, I properly configure executor memory, cores,
and number of executors based on workload size. I tune shuffle partitions, enable dynamic allocation when appropriate, 
and use cluster managers like YARN or Kubernetes for fair scheduling. I also optimize partitions and joins to avoid 
resource waste and monitor usage through Spark UI and cluster metrics.

--num-executors 10
--executor-memory 4G
--executor-cores 2
--driver-memory 2G

num-executors → How many workers
executor-memory → Memory per worker
executor-cores → Parallel tasks per worker
```

#### Q-19 Can you share some best practices for monitoring and logging PySpark jobs effectively ?
```bash
1. Use Proper Logging in Code
2. Monitor Spark UI Metrics
3. Centralized Log Management (Amazon Cloud, WatchELK, StackDatadog)
```

#### Q-20 What is your approach to deploying PySpark applications in a production setting ?
```bash
Step 1: Write Clean Code
Step 2: Remove Hardcoding
Step 3: Package Dependencies (requirements.txt)
Step 4: Choose Where to Run (Amazon EMR Databricks Kubernetes)
Step 5: Schedule the Job (Apache Airflow AWS Step Functions)
How do you handle failures : Retry policies, checkpointing, idempotent writes, and monitoring alerts.
How do you ensure data quality : Validation checks before and after transformation
```
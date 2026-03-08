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

#### Q-3 - You need to join two large datasets, but the join operation is causing out-of-memory errors. What strategies would you use to optimize this join ?
```bash
1. Use Broadcast Join (If One Table Is Small)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "id", "inner")

2. Repartition Before Join
df1 = df1.repartition(200, "id")
df2 = df2.repartition(200, "id")
result = df1.join(df2, "id")

3. Filter Data Before Joining
filtered_df1 = df1.filter("status = 'ACTIVE'")
filtered_df2 = df2.select("id", "name")

result = filtered_df1.join(filtered_df2, "id")

4. Handle Data Skewc(Salting technique)

5. Use Bucketing
df.write.bucketBy(100, "id").saveAsTable("table_bucketed")
```

#### Q-4 Write PySpark code to remove duplicates based on multiple columns ? 
```bash
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RemoveDuplicates").getOrCreate()

data = [
    (1, "Alice", "alice@gmail.com"),
    (2, "Bob", "bob@gmail.com"),
    (3, "Alice", "alice@gmail.com"),
    (4, "Charlie", "charlie@gmail.com")
]

columns = ["id", "name", "email"]

df = spark.createDataFrame(data, columns)

# Remove duplicates based on name and email
df_clean = df.dropDuplicates(["name", "email"])

df_clean.show()

-----------------------------------------------------------------------------------
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

window_spec = Window.partitionBy("name", "email").orderBy(col("id").desc())

df_clean = df.withColumn("rn", row_number().over(window_spec)) \
             .filter(col("rn") == 1) \
             .drop("rn")
```

#### Q-5 Write Pyspark code to implement SCD logic ?
```bash
-> SCD Type 1 (Overwrite Old Data)

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SCD_Type1").getOrCreate()

# Existing dimension table
dim_data = [
    (1, "Alice", "Delhi"),
    (2, "Bob", "Mumbai")
]

dim_df = spark.createDataFrame(dim_data, ["id","name","city"])

# Incoming source data
src_data = [
    (1, "Alice", "Pune"),
    (3, "Charlie", "Bangalore")
]

src_df = spark.createDataFrame(src_data, ["id","name","city"])

# Overwrite changes
final_df = src_df.unionByName(dim_df).dropDuplicates(["id"])

final_df.show()
-----------------------------------------------------------------
-> SCD Type 2 (Maintain History)

from pyspark.sql.functions import col, lit, current_date
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SCD_Type2").getOrCreate()

# Existing dimension table
dim_data = [
    (1, "Alice", "Delhi", "2024-01-01", None, "Y"),
    (2, "Bob", "Mumbai", "2024-01-01", None, "Y")
]

dim_cols = ["id","name","city","start_date","end_date","current_flag"]
dim_df = spark.createDataFrame(dim_data, dim_cols)

# Source data
src_data = [
    (1, "Alice", "Pune"),
    (2, "Bob", "Mumbai"),
    (3, "Charlie", "Bangalore")
]

src_df = spark.createDataFrame(src_data, ["id","name","city"])

# Join source with dimension
joined_df = src_df.join(dim_df, "id", "left")

# Detect changed records
changed_df = joined_df.filter(col("city") != col("dim_df.city"))

# Expire old records
expired_df = dim_df.join(changed_df, "id") \
    .withColumn("end_date", current_date()) \
    .withColumn("current_flag", lit("N"))

# Insert new records
new_records = src_df.withColumn("start_date", current_date()) \
    .withColumn("end_date", lit(None)) \
    .withColumn("current_flag", lit("Y"))

# Final dimension table
final_dim = expired_df.unionByName(new_records)

final_dim.show()
```

#### Q-6 Write a PySpark code to read CSV file from S3 Bucket and convert it into Parquet format ? 
```bash
1️⃣ Basic PySpark Code
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSV_to_Parquet") \
    .getOrCreate()

# Read CSV file from S3
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://my-bucket/input/data.csv")

# Write as Parquet
df.write \
    .mode("overwrite") \
    .parquet("s3a://my-bucket/output/data_parquet")

2️⃣ With AWS Credentials (If Required)
```

#### Q-7 Explain how you would use PySpark to join data from a Hive table and a Kafka stream ?
```bash
Kafka → Spark Structured Streaming → Join with Hive table → Output

Step 1: Enable Hive Support
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("StreamHiveJoin") \
    .enableHiveSupport() \
    .getOrCreate()

Step 2: Read Static Hive Table
hive_df = spark.table("analytics.user_profile")

Step 3: Read Streaming Data from Kafka
stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

Step 4: Perform Stream–Static Join
joined_df = parsed_stream.join(
    hive_df,
    on="user_id",
    how="left"
)
```

#### Q-8 How would you integrate data from an external API into your PySpark pipeline ?
```bash
To integrate external API data into a PySpark pipeline, I avoid calling the API on the driver. Instead, 
I use distributed techniques like mapPartitions or foreachBatch to call the API per partition. I also handle 
rate limits, retries, and deduplicate keys before making calls. In production, if the API data is reusable, 
I prefer ingesting it separately into storage and performing a join within Spark for better scalability.
```

#### Q-9 Given a requirement to process and transform data from various sources like CSV, JSON, and Parquet files, how would you handle this in a PySpark job ?
```bash
High-Level Approach

1. Read each source with proper configuration
2. Enforce a consistent schema
3. Standardize column names and types
4. Handle nulls & bad records
5. Apply business transformations
6. Write to a target format (usually Parquet/warehouse)

Step 1: Create Spark Session
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MultiSourceETL") \
    .getOrCreate()

Step 2: Define a Common Schema
from pyspark.sql.types import *

common_schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_time", TimestampType(), True)
])

Step 3: Read Each Source Properly
csv_df = spark.read \
    .option("header", True) \
    .schema(common_schema) \
    .csv("input/data.csv")
```

#### Q-10 How would you approach integrating data from an external API into your PySpark workflow ?
```bash
To integrate external API data into a PySpark workflow, I avoid making API calls on the driver. Instead, 
I either pre-ingest API data into storage and join it within Spark, or use distributed methods like mapPartitions 
for batch processing and foreachBatch for streaming. I also handle rate limiting, retries, and deduplicate keys 
before calling the API to ensure scalability and fault tolerance.
```

#### Q-11 How would you design and implement an ETL pipeline using PySpark to extract data from an RDBMS, transform it, and load it into a data warehouse ?
```bash
I would extract data from the RDBMS using Spark’s JDBC connector with partitioned reads for parallelism. 
For large tables, I would implement incremental extraction using a watermark column like updated_at. During 
transformation, I would apply data cleaning, deduplication, type casting, and business logic validations. 
Before loading, I would perform data quality checks. Finally, I would load the data into a warehouse using 
staging tables and MERGE operations for upserts. The pipeline would be orchestrated using Airflow and monitored 
with proper logging and audit tracking.
```

#### Q-12 How would you set up a real-time data pipeline using PySpark and Kafka to process streaming data ?
```bash
Producer → Kafka → PySpark Structured Streaming → Sink (DB/Data Lake)
```

#### Q-13 When joining two large datasets causes out-of-memory errors, what strategies would you use to optimize the join operation ?
```bash
Why Large Joins Cause OOM(OutOfMemory) :-

Joins trigger shuffle
Data is redistributed across executors
Large partitions can overload executor memory
Skew can make one executor hold massive data

Step 1: Identify Root Cause
Before fixing, I check: :-

Spark UI → shuffle read size
Task memory usage
Whether one key is skewed
Size of both datasets
Join type (inner, left, etc.)
Never blindly increase memory first.

Strategies to Optimize Large Joins :

1. Use Broadcast Join (If One Table Is Smaller)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

2. Handle Data Skew
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

3. Increase Shuffle Partitions
spark.conf.set("spark.sql.shuffle.partitions", 400)

4. Repartition Before Join
df1 = df1.repartition("join_key")
df2 = df2.repartition("join_key")

5. Filter Early (Push Down Filtering)
df1_filtered = df1.filter("date >= '2025-01-01'")

6. Select Only Required Columns
df1 = df1.select("id", "key", "value")
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
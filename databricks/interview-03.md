#### Q-1
```bash
```

#### Q-2
```bash
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

#### Q-6
```bash
```

#### Q-7
```bash
```

#### Q-8
```bash
```

#### Q-9
```bash
```

#### Q-10
```bash
```

#### Q-11
```bash
```

#### Q-12
```bash
```

#### Q-13
```bash
```

#### Q-14
```bash
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
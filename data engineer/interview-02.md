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

#### Q-7 Implement a Python class that dynamically creates a hierarchical data structure from a flat table with ID, ParentID, and Value columns ? 
```bash
```

#### Q-8 Develop a program to read a CSV file, extract unique values from a column, and save the results in a new file and dataframe ? 
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

#### Q-18
```bash
```

#### Q-19
```bash
```

#### Q-20
```bash
```
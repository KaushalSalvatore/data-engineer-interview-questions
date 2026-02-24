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

#### Q-18
```bash
```

#### Q-19
```bash
```

#### Q-20
```bash
```
#### Q-1 How do you create a SparkSession in PySpark? What are its main uses ?
```bash
SparkSession is the entry point to using the Spark functionality, and it’s created using the 
SparkSession.builder API.

Its main uses include:

Interacting with Spark SQL to process structured data.
Creating DataFrames.
Configuring Spark properties.
Managing SparkContext and SparkSession lifecycle.

from pyspark.sql import SparkSession
     
spark = SparkSession.builder \
         .appName("MySparkApp") \
         .master("local[*]") \
         .getOrCreate()	
```

#### Q-2 Describe the different ways to read data into PySpark ?
```bash
df_from_csv = spark.read.csv("my_file.csv", header=True)
df_from_parquet = spark.read.parquet("my_file.parquet")
df_from_json = spark.read.json("my_file.json")
```

#### Q-3 How do you handle missing data in PySpark ?
```bash
We can drop rows or columns containing missing values using the method .dropna().
df_from_csv.dropna(how="any")

We can fill missing data with a specific value or use interpolation methods with the method .fillna().
df_from_parquet.fillna(value=2)

We can impute missing values using statistical methods, such as mean or median, using Imputer.
from pyspark.ml.feature import Imputer
imputer = Imputer(strategy="median", inputCols=["price","rooms"], outputCols=["price_imputed","rooms_imputed"])
model = imputer.fit(df_from_json)
df_imputed = model.transform(df_from_json)
```

#### Q-4 Describe performing joins in PySpark ?
```bash
Pyspark allows us to perform several types of joins: inner, outer, left, and right joins. 
By using the .join() method.

# How to inner join two datasets
df_from_csv.join(df_from_json, on="id", how="inner")

# How to outer datasets
df_from_json.join(df_from_parquet, on="product_id", how="outer")
```

#### Q-5 Explain the concept of lazy evaluation in PySpark. How does it impact performance ?
```bash
Spark does not execute transformations immediately.
It waits until it sees an action, then it builds and runs the full execution plan at once.

Lazy evaluation in PySpark means that transformations are not executed immediately. Spark builds a DAG of transformations 
and only executes them when an action is triggered. This allows Spark to optimize the execution plan using the Catalyst 
optimizer, reduce data movement, and improve performance.

df = spark.read.csv("data.csv")
df_filtered = df.filter(df.age > 30)
df_selected = df_filtered.select("name", "age")

Why Lazy Evaluation is Important
1️. Optimization :
Read only required columns
Apply filter early
Optimize joins
Reduce shuffle

2. Reduced Data Movement
Spark avoids unnecessary computation and shuffling.

3. Fault Tolerance
Spark keeps lineage (DAG).
If a partition fails, it recomputes only the lost part.
```

#### Q-6 What is the role of partitioning in PySpark? How can it improve performance ?
```bash
Partitioning in PySpark is about how data is divided across the cluster.
Every Spark DataFrame/RDD is split into partitions, and each partition is processed in parallel by an executor core.

So partitioning directly controls:
Parallelism
Shuffle behavior
Resource utilization
Job performance

A partition is a chunk of data.

If you have:
100GB dataset
10 partitions
Each partition ≈ 10GB (roughly).

Each partition is processed by one task.
More partitions → more parallel tasks (up to cluster capacity).

Partitioning in PySpark determines how data is distributed across executors. Proper partitioning improves parallelism, 
reduces shuffle during joins and aggregations, prevents data skew, and enables partition pruning during reads. 
By aligning partition strategy with query patterns, we can significantly reduce execution time and optimize resource 
utilization.

Why Partitioning Matters for Performance

1️. Enables Parallelism

If you have:
4 cores
4 partitions → full parallel usage
1 partition → only 1 core used → slow
1000 partitions → overhead from too many small tasks

2. Reduces Shuffle Cost (Very Important)

Shuffle is expensive because:
Data moves across network
Disk I/O increases
Execution slows down

3. Avoids Data Skew

Data skew happens when:
One partition has huge data
Others are small

Result:
One task runs very long
Other executors idle
```

#### Q-7 Explain the concept of broadcast variables in PySpark and provide a use case  ?
```bash
Broadcast variables in PySpark allow us to distribute a read-only variable to all executors only once, instead of sending 
it with every task. They are commonly used to optimize joins by broadcasting small tables to avoid expensive shuffles, 
resulting in faster execution and reduced network overhead.

Why Broadcast Variables Are Needed

In Spark:

Driver sends tasks to executors.
If a normal variable is used inside transformations, Spark serializes and sends it with every task.
If the variable is large → it gets sent multiple times → inefficient.

Broadcast variable solves this by:
Sending the variable to each executor only once, and caching it there.

How Broadcast Works

1. Driver creates broadcast variable.
2. Spark distributes it to all executors.
3. Executors keep it in memory.
4. Tasks reuse the same copy.

Most Important Use Case: Broadcast Join

If you join:
Large table (1TB)
Small table (10MB)

Normally Spark performs shuffle join:
Both datasets are shuffled
Expensive network cost

Instead, Spark can broadcast the small table:

from pyspark.sql.functions import broadcast
df_join = large_df.join(broadcast(small_df), "id")

Now:

Small table is sent to all executors
Large table is NOT shuffled
Join happens locally on each executor
Much faster
This is called a Broadcast Hash Join.
```

#### Q-8 Explain the concept of window functions in PySpark and provide an example ? 
```bash
Window functions in PySpark perform calculations across a group of related rows while preserving the original row count. 
Unlike groupBy, they do not collapse rows. A window specification includes partitioning, ordering, and optionally a frame. 
Common use cases include ranking, running totals, lag/lead analysis, and moving averages.
```

#### Q-9 What is the purpose of checkpoints in PySpark ?
```bash
Checkpointing in PySpark is used to truncate the lineage of an RDD or DataFrame by saving it to reliable storage. 
This helps prevent very long dependency chains, improves fault recovery time, and stabilizes iterative or streaming 
workloads. Unlike caching, checkpointing removes lineage and provides stronger fault tolerance.  

Example Scenario

Suppose you run:
100 transformations in loop
A failure happens at step 95

Without checkpoint:
Spark recomputes from step 1
With checkpoint at step 50:

Spark recomputes from step 50 only

Huge performance difference.
```

#### Q-10 What is a Catalyst optimizer in Spark, and how does it work ?
```bash
The Catalyst Optimizer is Spark SQL’s built-in query optimization engine.

It is responsible for taking your SQL or DataFrame code and transforming it into an efficient execution plan.

In simple terms:

Catalyst figures out the fastest way to run your query.

If you write:
df.filter(df.age > 30).filter(df.salary > 5000)

Catalyst combines filters into one:
WHERE age > 30 AND salary > 5000

catalyst optimizer can optimized UDF :- No, the Catalyst Optimizer in Apache Spark cannot effectively optimize UDFs, 
especially regular (black-box) UDFs.

Avoid UDFs whenever possible — use built-in functions so Catalyst can optimize the query.

Catalyst works on logical plans and understands:
Built-in Spark SQL functions
Expressions (like filters, joins, aggregations)

👉 But UDFs are treated as black box
```

#### Q-11 How can you perform incremental processing with PySpark ?
```bash
1. Using a Timestamp (Most Common Method)
last_processed = "2026-02-07 00:00:00"

df = spark.read.parquet("s3://source/")
incremental_df = df.filter(df.updated_at > last_processed)

2️. Using Partition-Based Incremental Loads
s3://data/year=2026/month=02/day=08/
df = spark.read.parquet("s3://data/year=2026/month=02/day=08/")

3. Incremental Processing with Delta Lake (Very Common in Databricks)
Delta Lake supports:
MERGE (Upsert)
Change Data Feed (CDF)
```

#### Q-12 What are the different ways to handle row duplication in a PySpark DataFrame ?
```bash
1. dropDuplicates() (Most Common)
df = df.dropDuplicates()
2. distinct()
df = df.distinct()
3. Using Window Function (Best for Controlled Deduplication)
4. Using groupBy + Aggregation
```

#### Q-13 What is SparkConf in PySpark? List a few attributes of SparkConf ? 
```bash
SparkConf is the configuration object used to set and manage Spark application settings before creating a 
SparkContext or SparkSession.

SparkConf is used to configure how your Spark application runs — including memory, cores, executors, app name, 
and cluster settings

Common Attributes of SparkConf
1. Application Configuration
| Property         | Description                                    |
| ---------------- | ---------------------------------------------- |
| `spark.app.name` | Name of the Spark application                  |
| `spark.master`   | Cluster manager (local, yarn, k8s, standalone) |

2. Executor Configuration
| Property                        | Description                  |
| ------------------------------- | ---------------------------- |
| `spark.executor.instances`      | Number of executors          |
| `spark.executor.memory`         | Memory per executor          |
| `spark.executor.cores`          | Number of cores per executor |
| `spark.executor.memoryOverhead` | Extra off-heap memory        |

3. Driver Configuration
| Property              | Description   |
| --------------------- | ------------- |
| `spark.driver.memory` | Driver memory |
| `spark.driver.cores`  | Driver cores  |

4. Shuffle Configuration
| Property                       | Description                  |
| ------------------------------ | ---------------------------- |
| `spark.sql.shuffle.partitions` | Number of shuffle partitions |
| `spark.shuffle.compress`       | Compress shuffle data        |

5.Dynamic Allocation
| Property                               | Description                       |
| -------------------------------------- | --------------------------------- |
| `spark.dynamicAllocation.enabled`      | Enable/disable dynamic allocation |
| `spark.dynamicAllocation.minExecutors` | Minimum executors                 |
| `spark.dynamicAllocation.maxExecutors` | Maximum executors                 |
```

#### Q-14 What would happen if we lose RDD partitions due to the failure of the worker node ?
```bash
If we lose RDD partitions due to a worker node failure, Spark does not lose the entire dataset permanently.

If an RDD partition is lost due to worker node failure, Spark automatically recomputes the lost partitions using the 
RDD’s lineage graph. Since RDDs are immutable and maintain a record of transformations, Spark can regenerate only the 
missing partitions without restarting the entire job. This mechanism provides fault tolerance in Spark.
```

#### Q-15 What are the different approaches for creating RDD in PySpark ?
```bash
1. sparkContext.parallelize()
list = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd=spark.sparkContext.parallelize(list)

2. sparkContext.textFile()
rdd_txt = spark.sparkContext.textFile("/path/to/textFile.txt")

3. sparkContext.wholeTextFiles()
rdd_whole_text = spark.sparkContext.wholeTextFiles("/path/to/textFile.txt")

4. sparkContext.emptyRDD
empty_rdd_string = spark.sparkContext.emptyRDD[String]
```

#### Q-16 What are the profilers in PySpark ?
```bash
Tools that help us find why a Spark job is slow or using too much memory.
Think of profilers as a health check tool for your Spark job.

When you run a Spark job, you can open: (http://localhost:4040)

It shows:
How many jobs ran
How long each stage took
Which task is slow
Memory usage
Shuffle data size

python code :- 
import cProfile
cProfile.run("main()")
``` 

#### Q-17 How is Apache Spark different from MapReduce ?
```bash
1. Processing Model

MapReduce
Works in two strict phases: Map → Reduce
After each phase, intermediate data is written to disk (HDFS)
Not flexible beyond map and reduce steps

Spark
Uses a DAG (Directed Acyclic Graph) execution engine
Supports multiple transformations (map, filter, join, groupBy, etc.)
Keeps intermediate data in memory whenever possible

Spark is more flexible and optimized.

2. Performance
MapReduce: Disk-based → Slower
Spark: In-memory processing → Much faster (10–100x faster for iterative workloads)

3. Iterative & Machine Learning Workloads
MapReduce:
Poor for iterative algorithms
Each iteration reads/writes to disk

Spark:
Designed for iterative processing
Data can stay cached in memory
Ideal for ML and graph processing

4. Streaming Support
MapReduce:
Batch processing only

Spark:
Supports Structured Streaming
Can process near real-time data

5. Fault Tolerance
MapReduce:
Uses data replication in HDFS

Spark:
Uses RDD lineage to recompute lost data
```

#### Q-18 What is Broadcast ? 
```bash
Broadcast join is used when one dataset is small enough to fit in memory, allowing Spark to distribute it to all 
executors and avoid expensive shuffles.

Problem :-
When joining two tables:
Large table → 1 billion rows
Small table → 10k rows

Normally Spark:
Shuffles both tables across cluster
Expensive
Slow

What Broadcast Does
Broadcast sends the small table to every executor.
So:
No shuffle of large table
Faster join
Less network cost

from pyspark.sql.functions import broadcast

large_df = spark.read.parquet("transactions")
small_df = spark.read.parquet("countries")

result = large_df.join(
    broadcast(small_df),
    "country_id",
    "left"
)

By default,  only 10 MB of data can be broadcasted.
spark.sql.autoBroadcastJoinThreshold can be increased up to 8GB
```

#### Q-19 collect_list() and collect_set() ? 
```bash
These are aggregation functions used with groupBy().

1. collect_list()
Collects values into a list
Keeps duplicates
Order not guaranteed

from pyspark.sql.functions import collect_list
df.groupBy("user_id") \
  .agg(collect_list("product_id").alias("products"))

2. collect_set()
Collects unique values
Removes duplicates

from pyspark.sql.functions import collect_set
df.groupBy("user_id") \
  .agg(collect_set("product_id").alias("unique_products"))
```

#### Q-20 managed vs external table ? 
```bash
A Managed Table (also called Internal Table) means:
The system manages both metadata AND data files.

👉 Metadata deleted
👉 Data files also deleted

An External Table means:
The system manages only metadata, NOT the actual data files.
```
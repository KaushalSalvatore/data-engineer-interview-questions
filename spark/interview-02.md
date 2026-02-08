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

Partitioning in PySpark determines how data is distributed across executors. Proper partitioning improves parallelism, reduces 
shuffle during joins and aggregations, prevents data skew, and enables partition pruning during reads. By aligning partition 
strategy with query patterns, we can significantly reduce execution time and optimize resource utilization.

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
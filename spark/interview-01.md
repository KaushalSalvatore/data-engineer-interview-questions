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

#### Q-7 Difference between client mode and cluster mode ? 
```bash
1. Client Mode

You run spark-submit from your laptop
Driver runs on your laptop
Executors run on cluster nodes

Flow:
Your machine (Driver) → Cluster Manager → Executors

Characteristics:

Driver logs appear on your local machine
If your machine disconnects → job fails
Good for development and debugging

spark-submit --master yarn --deploy-mode client app.py

2. Cluster Mode

In cluster mode, the driver runs inside the cluster.

Example:
You submit job
Cluster manager launches driver on a worker node
Executors also run in cluster

Flow:
Cluster Manager → Driver (inside cluster) → Executors

Characteristics:
Driver runs inside cluster
Job continues even if you disconnect
Suitable for production
Logs are inside cluster (YARN/K8s UI)

spark-submit --master yarn --deploy-mode cluster app.py
```

#### Q-8 What are the different types of cluster managers available in Spark ?
```bash
cluster manager is responsible for allocating resources (CPU, memory) and launching executors for your Spark 
application.

1. Standalone Cluster Manager
This is Spark’s built-in cluster manager.
Comes with Spark
Simple to set up
Good for small clusters or testing
Not commonly used in large enterprise environments

Architecture:
Master node
Worker nodes

Best for:
Development
Small-scale deployments

2. Hadoop YARN (Yet Another Resource Negotiator)

Very common in on-prem Hadoop environments.
Used in Hadoop clusters
Manages resources for multiple applications (not just Spark)
Supports multi-tenant environments

Two modes:
Client mode
Cluster mode

Best for:
Enterprises using Hadoop ecosystem

3. Apache Mesos (Less Common Now)

General-purpose cluster manager.
Can run multiple distributed systems
Supports Spark, Hadoop, Kafka, etc.

Not widely used today compared to YARN or Kubernetes.

4. Kubernetes (K8s)

Modern and increasingly popular.
Container-based resource management
Cloud-native
Works well in AWS, Azure, GCP
Used heavily in modern data platforms

Best for:
Cloud environments
Microservices architecture
Containerized deployments
```

#### Q-9 What is a Spark Driver, and what are its responsibilities ?
```bash
The Spark Driver is the central coordinator of a Spark application. It creates the SparkSession, builds the execution 
plan (DAG), splits it into stages and tasks, requests resources from the cluster manager, distributes tasks to executors, 
monitors execution, and collects results. It does not process data itself but orchestrates the entire workflow.

Every Spark application has one driver process, and it is responsible for planning, coordinating, and monitoring the 
execution of your job.

Think of it as the brain of the Spark application.

Key Responsibilities of the Spark Driver
1. Maintains SparkSession / SparkContext
2. Converts Code into Execution Plan (DAG Creation)
3. Requests Resources from Cluster Manager
4. Distributes Tasks to Executors
5. Collects Results
```

#### Q-10 how can optimize quries in if they are take long time ?
```bash
When a Spark query takes a long time, I optimize it by first analyzing the execution plan using explain().
Then I focus on reducing data size early with filters and column pruning, choosing the right join strategy 
like broadcast joins, handling data skew, optimizing partitions using repartition or coalesce, caching reused 
data, and selecting efficient file formats like Parquet.
I also tune Spark configurations when required. The main goal is to reduce shuffle, I/O, and unnecessary computation.

1. Analyze the Query Plan
df.explain(true) (check joins, shuffles, and scan operations.)

2. Reduce Data Early (Big Win)
Apply filter before joins
Select only required columns

df.select("id", "salary").filter("salary > 50000") (“Less data early means less shuffle later.”)

3. Optimize Joins (Very Important)
Use Broadcast Join for small tables

from pyspark.sql.functions import broadcast
df.join(broadcast(dim_df), "id")
“Broadcast joins avoid shuffle and are the fastest join strategy.”

4. Partition Optimization
Use repartition for even distribution
Use coalesce before writing output
“Too many or too few partitions hurt performance.”

5. Use Efficient File Formats
Prefer Parquet / ORC
Enable partition pruning
```

#### Q-11 which function contribute in action and which contribute in transformation ?
```bash
In Spark, transformations are lazy operations that create a new RDD or DataFrame, while actions trigger 
the execution and return results or write data.

Functions like map, filter, select, repartition, and coalesce are transformations.

Functions like count, collect, show, and write are actions because they trigger the Spark job.

Transformations (Lazy – No Execution)  RDD Transformations
“Transformations build the DAG but don’t execute immediately.”

Actions (Trigger Execution)
Actions trigger Spark to execute the DAG and return results or persist output.”
```

#### Q-12 functions in spark coalesce and repartition ?
```bash
repartition and coalesce are used to change the number of partitions in Spark.
repartition can increase or decrease partitions and always causes a full shuffle, so it’s more expensive.
coalesce is mainly used to reduce the number of partitions and avoids shuffle by default, so it’s more 
efficient.
In practice, I use repartition when I need even data distribution and coalesce when reducing partitions before 
writing output.

I use repartition when increasing partitions or when data is skewed and I need uniform distribution.
I use coalesce when decreasing partitions, especially before writing to files, to avoid creating many small files.
```

#### Q-13 how to check SparkData frame code chache ? 
```bash
We can check whether a Spark DataFrame is cached by using is_cached in PySpark or by checking the Spark UI storage 
tab. Programmatically, df.is_cached returns true if the DataFrame is cached.

df.cache()
print(df.is_cached)
```

#### Q-14 diffrence in cache and persist in spark ?
```bash
cache and persist are used in Spark to store DataFrames or RDDs in memory to avoid recomputation.
cache is a shorthand for persist with the default storage level, which is memory only.
persist gives more control by allowing different storage levels like memory and disk.
Both are lazy and take effect only after an action is triggered.

df.cache()
df.count()   # action triggers caching

from pyspark import StorageLevel
df.persist(StorageLevel.MEMORY_AND_DISK) # df_from_csv.persist(storageLevel=StorageLevel.DISK_ONLY)
df.count()

“I use cache for quick reuse and persist when I need control over memory and disk usage.”
```

#### Q-15 In which scenario broadcast join can result in out of memory error ?
```bash
Broadcast join can cause an out-of-memory error when the table being broadcast is too large to fit into the 
executor memory.
Since the broadcasted dataset is copied to every executor, if its size exceeds available memory or if there 
are many executors, Spark can run out of memory.

“If the broadcast DataFrame is larger than the executor’s available memory, it leads to OOM.”

Example:

Default auto-broadcast threshold is 10 MB
Broadcasting a 1–2 GB table → ❌ OOM
```

#### Q-16 how do you optimize this spark job to handle the skewed data ?
```bash
To optimize a Spark job with skewed data, I first identify the skewed keys using the execution plan and Spark UI.
Then I reduce the impact by using techniques like salting the skewed keys, applying broadcast joins where applicable, 
repartitioning on better keys, enabling Adaptive Query Execution, and handling skewed keys separately.
The goal is to balance the workload across executors and avoid long-running tasks.

I enable AQE so Spark can automatically handle skewed joins and optimize shuffle partitions at runtime.

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

#### Q-17 what do we do in salting techniques ?
```bash
Salting converts a data skew problem into a data distribution problem that Spark can parallelize.

In the salting technique, we add an extra random value, called a salt, to the skewed join key.
This breaks a single hot key into multiple keys so that the data is distributed across multiple 
partitions during the join.
After the join, we can remove the salt to get the correct results.

Salting spreads hot keys across multiple partitions.

from pyspark.sql.functions import rand, concat, col
df1 = df1.withColumn("salt", (rand()*10).cast("int"))
df1 = df1.withColumn("join_key_salt", concat(col("key"), col("salt")))
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
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
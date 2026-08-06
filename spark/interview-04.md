#### Q-1 spill in spark ?
```bash
Spill = Memory overflow → Data written to disk.

If the data being processed is larger than the available executor memory, Spark spills intermediate data to disk.

Example:

Executor Memory = 4 GB
Data to process = 10 GB

        Executor
    -----------------
    Memory (4 GB)
    -----------------
          |
 Memory Full
          |
          ▼
 Spill to Local Disk
          |
          ▼
 Continue Processing

Operations That Commonly Cause Spill

1. Shuffle
2. Sort
3. Join
4. Aggregation

| Cause                      | Why It Spills                               | Solution                                              |
| -------------------------- | ------------------------------------------- | ----------------------------------------------------- |
| Large `groupBy()`          | Shuffle data doesn't fit in memory          | Increase partitions, filter early, tune memory        |
| Large `join()`             | Shuffle join creates huge intermediate data | Use broadcast join, optimize join order               |
| `orderBy()`                | Global sort requires shuffle                | Avoid unnecessary sorts, use `sortWithinPartitions()` |
| Data skew                  | One partition becomes too large             | Salting, AQE, repartitioning                          |
| Too little executor memory | Executors can't hold intermediate data      | Increase executor memory                              |
| Too few partitions         | Each partition becomes very large           | Increase `spark.sql.shuffle.partitions`               |
| Excessive caching          | Memory is consumed by cached data           | Cache selectively and unpersist when done             |
```

#### Q-2 tumbling window and sliding window ? 
```bash
A Tumbling Window divides the stream into fixed-size, non-overlapping windows.

Window Size = 5 minutes
Time
00:00 ───── 00:05 ───── 00:10 ───── 00:15

 Window 1     Window 2     Window 3

from pyspark.sql.functions import window

df.groupBy(
    window("timestamp", "5 minutes")
).count()


A Sliding Window also has a fixed duration, but it overlaps because it moves forward at a smaller interval (the slide).
An event can belong to multiple windows.

00:00 ---------------- 00:10

      00:05 ---------------- 00:15

            00:10 ---------------- 00:20

from pyspark.sql.functions import window

df.groupBy(
    window("timestamp", "10 minutes", "5 minutes")
).count()

Real-World Examples

How many orders were placed every 5 minutes ? (Tumbling Window)

How many transactions occurred in the last 10 minutes, updated every minute? (Sliding Window) 
```

#### Q-3 If you perform five actions in Spark, how many DAGs are created?
```bash
Normally, five actions create five separate Spark jobs, and each job has its own DAG. If the actions use the same 
DataFrame without caching, 
Spark recomputes the transformations for every action. If the DataFrame is cached or persisted, Spark still 
creates five DAGs, but after the first action 
the later jobs reuse the cached data instead of recomputing the lineage.
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
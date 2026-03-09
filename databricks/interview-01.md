#### Q-1 Unity Catalog in Databricks ?
```bash
Unity Catalog is the central governance layer inside Databricks.

It manages:
🔐 Data access control
📊 Metadata (tables, schemas, catalogs)
🧬 Data lineage
📜 Audit logs
🌍 Cross-workspace sharing

The security + control center for all your data in Databricks.
```

#### Q-2 How do you optimize Spark jobs to run faster when dealing with terabytes of data ?
```bash
```

#### Q-3 Explain Slowly Changing Dimensions (SCD Type 2). How would you implement it in Spark ?
```bash
```

#### Q-4 Explain optimization techniques in Spark ?
```bash
1. Use Efficient File Formats
Parquet
ORC

2. Partitioning & Data Layout Optimization

3. Reduce Shuffle (Most Important 🔥)
Shuffle = disk + network + memory.
Techniques:Filter before join,Avoid unnecessary groupBy,Avoid distinct unless required
Use map-side operations

4. Join Optimization
-> Broadcast Join
from pyspark.sql.functions import broadcast
df_large.join(broadcast(df_small), "id")
-> Handle Data Skew (Key salting)
-> Tune Spark Configurations
spark.sql.shuffle.partitions
executor memory
executor cores
driver memory

5. Caching & Persistence
```

#### Q-5 Write a PySpark code to process streaming data from Event Hub in Databricks ?
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
### ADLS

#### Q-1 What is ADLS Gen2,?
```bash
ADLS Gen2 is Microsoft's cloud-based data lake storage service built on top of Azure Blob Storage.
It is designed to store large volumes of structured, semi-structured, and unstructured data.

ADLS Gen2 is essentially Azure Blob Storage enhanced with hierarchical namespace and analytics-oriented capabilities.

Hierarchical Namespace :-
container/file1
container/file2
container/folder1/file3

Can we enable Hierarchical Namespace after creating the storage account :- I normally enable hierarchical namespace 
while creating the ADLS Gen2 account because it is a fundamental storage-account configuration and should be planned 
before production migration.

storage account structure in ADLS Gen2 :-
Storage Account — the top-level namespace (e.g., mydatalakeacct ), globally unique, defines region, redundancy, and
performance tier.
Container / Filesystem — logical grouping (e.g., raw , curated , landing ). Equivalent to a "bucket" in AWS S3 terms.
Directories — nested folders within a container, real objects when HNS is enabled.
Files/Blobs — the actual data objects (Parquet, CSV, JSON, Delta files, etc.)

A full path looks like: abfss://curated@mydatalakeacct.dfs.core.windows.net/sales/2026/09/05/data.parquet
```

#### Q-2 What is the difference between RBAC and ACL in ADLS ?
```bash
RBAC controls access at Azure resource level.(Storage Blob Data Reader, Storage Blob Data Contributor, Storage Blob Data Owner)
ACL provides more granular filesystem-level access. (/raw/customer)
```

#### Q-3 How would you implement incremental ingestion into ADLS ?
```bash
customer_id
name
address
updated_at

I maintain a watermark: last_processed_timestamp

ADF query :-
SELECT *
FROM customer
WHERE updated_at > '2026-09-04 00:00:00'
AND updated_at <= '2026-09-05 00:00:00';
``` 

#### Q-4 How do you handle schema drift in ADLS?
```bash
Suppose today's file:
customer_id
name
salary

Tomorrow:
customer_id
name
salary
email

Depending on requirements, I can:

Detect schema change.
Compare against expected schema.
Log the change.
Allow additive columns if approved.
Reject breaking changes.
Notify the support/data owner.
```

#### Q-5 What is lifecycle management in ADLS ?
```bash
0-30 days
   |
Hot

31-90 days
   |
Cool

> 90 days
   |
Archive/Delete depending on policy
```

#### Q-6 What is the difference between soft delete, versioning, and snapshots in ADLS Gen2?
```bash
```

#### Q-7  How would you prevent accidental deletion of critical Gold-layer data by an engineer with write access?
```bash
```

#### Q-8 What is predicate pushdown, and how does file format choice affect it?
```bash
```

#### Q-9 How do you optimize read/write throughput when loading large volumes of data from ADLS into Snowflake or Databricks?
```bash
```

#### Q-10 What is Data Lake Storage lifecycle management, and how would you configure it for a medallion architecture?
```bash
```

#### Q-11 What's the difference between a Copy Activity and a Data Flow (Mapping Data Flow) in ADF when writing to ADLS?
```bash
```

#### Q-12 How would you set up a Snowflake external stage pointing to an ADLS Gen2 container, and load data with COPY INTO ?
```bash
```

#### Q-13 Your ADF pipeline that writes to ADLS is failing intermittently with a "403 Forbidden" error, but only during certain hours of the day. How do you investigate?
```bash
```

#### Q-14 You need to migrate 50 TB of historical data from an on-prem NAS to ADLS Gen2 with minimal downtime. What approach do you take?
```bash
```

#### Q-15 Two different teams both need write access to the same Bronze container, but you need to prevent them from overwriting each other's files. How do you design this?
```bash
```

#### Q-16 Your organization wants to reduce ADLS costs by 30% without impacting query performance on active data. What levers do you pull?
```bash
```

#### Q-17 You run df.write.parquet(path) in Spark and get an error: "Path already exists." How do you resolve it, and what are the trade-offs of each fix ?
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
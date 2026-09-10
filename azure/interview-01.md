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

#### Q-6 What is the difference between soft delete, versioning, and snapshots in ADLS Gen2 ?
```bash
1. Soft Delete :- A developer accidentally deleted a Bronze-layer file. Instead of permanently losing it, I can 
recover it during the soft-delete retention period.

User deletes file
       ↓
File appears deleted
       ↓
Underlying deleted data is retained
       ↓
Restore if required

2. Versioning :- Versioning keeps previous versions when a blob is modified or overwritten. If somebody accidentally 
overwrites Version 3 with incorrect data, you can restore an earlier version.

Versioning = history of changes to a blob.

customer.csv
Version 1 → 1000 customers

customer.csv
Version 2 → 1200 customers

customer.csv
Version 3 → 1500 customers

3. Snapshots :- A snapshot is a point-in-time copy/state of a blob that you can use for recovery.
10:00 AM → Snapshot 1
12:00 PM → Snapshot 2
03:00 PM → Snapshot 3
If something goes wrong after 3 PM, you can use an earlier snapshot to recover the required state.
Snapshot = "What did this data look like at this point in time"
```

#### Q-7 How would you prevent accidental deletion of critical Gold-layer data by an engineer with write access ?
```bash
1. Don't give Delete permission unnecessarily   
2. Separate production and development access
3. Enable soft delete
4. Enable versioning where appropriate
5. Use CI/CD and service principals for production writes
6. Enable auditing and monitoring
7. Have a recovery strategy
```

#### Q-8 What is predicate pushdown, and how does file format choice affect it ?
```bash
Predicate pushdown is an optimization where Spark pushes filter conditions closer to the data source. Instead 
of reading the complete dataset and filtering afterward, the source can eliminate irrelevant data during the scan. 
File format has a major impact. CSV and JSON generally provide limited opportunities for predicate pushdown, while 
columnar formats like Parquet and ORC support it much better because they maintain column-level metadata and statistics. 
In Databricks, Delta tables use Parquet underneath and can additionally benefit from data skipping and other Delta optimizations. This reduces I/O and improves query performance.
```

#### Q-9 How do you optimize read/write throughput when loading large volumes of data from ADLS into Snowflake or Databricks ?
```bash
1. Optimize the data in ADLS first 
I prefer Parquet/Delta instead of CSV/JSON for large-volume processing.

2. Avoid too many small files
256 MB – 1 GB per file

3. Optimize Databricks read performance
4. Control Spark parallelism
```

#### Q-10 What is Data Lake Storage lifecycle management, and how would you configure it for a medallion architecture ?
```bash
If a Bronze file hasn't been modified for 30 days, move it to Cool. After 90 days, move it to Archive. 
Delete it after 7 years.

Old files -> Cool -> Archive -> Delete
```

#### Q-11 What's the difference between a Copy Activity and a Data Flow (Mapping Data Flow) in ADF when writing to ADLS ?
```bash
Copy Activity = move data
Mapping Data Flow = transform data

1. Copy Activity :- Copy Activity is mainly used for data ingestion/movement from source to destination.
Azure SQL ->  Copy Activity -> ADLS Gen2 -> bronze/customer/

2. Mapping Data Flow :- Mapping Data Flow is used when I need transformations while processing the data.
Azure SQL -> Mapping Data Flow  -> Filter  -> Join -> Column -> Derived Column -> Aggregate -> ADLS
```

#### Q-12 How would you set up a Snowflake external stage pointing to an ADLS Gen2 container, and load data with COPY INTO ?
```bash
CREATE STORAGE INTEGRATION azure_adls_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'AZURE'
ENABLED = TRUE
AZURE_TENANT_ID = '<azure-tenant-id>'
STORAGE_ALLOWED_LOCATIONS = (
    'azure://mystorageaccount.blob.core.windows.net/data/'
);


Once the stage is created, I verify access using LIST and then load the files into a RAW table using COPY INTO. For example, 
I can point COPY INTO to a specific date partition such as /sales/2026/09/, which supports incremental loading. Before production loading, I can use VALIDATION_MODE to identify file errors. I also monitor COPY history and Snowflake load 
metadata to identify failed files and avoid unnecessarily reloading files.
```

#### Q-13 Your ADF pipeline that writes to ADLS is failing intermittently with a "403 Forbidden" error, but only during certain hours of the day. How do you investigate ?
```bash
1. First understand the architecture 
Azure Data Factory ->  ADLS Gen2 -> /raw/customer/ 

ADF Copy Activity fails:
ADF
 |
 |---- 10:00 AM → SUCCESS
 |
 |---- 11:00 AM → 403
 |
 |---- 12:00 PM → 403
 |
 |---- 01:00 PM → SUCCESS

 2. Step 1 — Check the exact ADF error
 3. Step 2 — Compare successful vs failed runs
 4. Step 3 — Check ADLS firewall/network rules
 5. Why would firewall cause only certain hours?
 6. Step 4 — Check the Integration Runtime
 7 — Check whether something changes during those hours

 If an ADF pipeline writing to ADLS Gen2 intermittently gets 403 only during certain hours, I wouldn't immediately 
 assume an RBAC issue because a static permission problem would normally fail consistently. I would first capture 
 the exact error code, request ID, activity run ID, timestamp and Integration Runtime from the failed Copy Activity.

Then I would compare a successful run and a failed run to identify what changed — Integration Runtime, identity, 
network path, target path or workload. My first checks would be the ADLS storage firewall and networking configuration, including public network access, selected networks, VNet rules, private endpoints and any scheduled security policies.
Microsoft identifies network restrictions and insufficient identity permissions as key causes of ADLS Gen2 403 errors.

Next, I would verify the ADF managed identity or service principal has the required RBAC role, such as Storage Blob Data Contributor for a destination, and I would also check ADLS Gen2 ACLs and execute/write permissions on the directory 
hierarchy.
```

#### Q-14 You need to migrate 50 TB of historical data from an on-prem NAS to ADLS Gen2 with minimal downtime. What approach do you take ?
```bash
First, I would assess the migration

Before copying anything, I would collect:

Total size: 50 TB
Number of files
Average file size
File types: CSV, Parquet, PDF, images, ZIP, etc.
NAS protocol: SMB/NFS
Current network bandwidth
Network latency
Number of files modified per day
Whether files continue changing during migration
Folder structure
Security/ACL requirements
Business downtime window

I would split the migration into two phases
Phase 1 — Bulk migration
Day 1 ──────────────── Day 5
       Bulk Migration

NAS ───────────────────────► ADLS
     50 TB historical data

Phase 2 — Delta / changed-file migration
Files created after initial copy
Files modified after initial copy
Files deleted after initial copy

ADF pipeline design :- I would create a metadata-driven migration pipeline instead of creating hundreds of individual Copy Activities.
```

#### Q-15 Two different teams both need write access to the same Bronze container, but you need to prevent them from overwriting each other's files. How do you design this ?
```bash
I would not give both teams unrestricted write access to the same physical directory. I would give them separate landing 
folders inside the same Bronze container and control access using Microsoft Entra groups + ADLS Gen2 ACLs.

1. My preferred design
Team A = Customer Data
Team B = Transaction Data

bronze/
│
├── team-a/
│   ├── customer/
│   └── account/
│
└── team-b/
    ├── transaction/
    └── payment/

Another important design: immutable Bronze :-

/bronze/team-a/customer/
    ingest_date=2026-09-10/
       customer_1001_20260910.csv

    ingest_date=2026-09-11/
       customer_1001_20260911.csv   
```

#### Q-16 Your organization wants to reduce ADLS costs by 30% without impacting query performance on active data. What levers do you pull ?
```bash
Keep frequently queried data in Hot, automatically move older/infrequently accessed data to Cool/Cold, clean up unnecessary versions/snapshots, optimize file sizes and formats, and review redundancy.
```

#### Q-17 You run df.write.parquet(path) in Spark and get an error: "Path already exists." How do you resolve it, and what are the trade-offs of each fix ?
```bash
Default behavior = “I won't accidentally destroy existing data.”

Option 1 — overwrite
df.write \
  .mode("overwrite") \
  .parquet("/data/customer")

Option 2 — append
df.write \
  .mode("append") \
  .parquet("/data/customer")

Option 3 — ignore
df.write \
  .mode("ignore") \
  .parquet("/data/customer")

Option 4 — Write to a unique/partitioned path
/data/customer/date=2026-09-08/
/data/customer/date=2026-09-09/
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
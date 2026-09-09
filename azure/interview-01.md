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
```

#### Q-14 You need to migrate 50 TB of historical data from an on-prem NAS to ADLS Gen2 with minimal downtime. What approach do you take ?
```bash
```

#### Q-15 Two different teams both need write access to the same Bronze container, but you need to prevent them from overwriting each other's files. How do you design this ?
```bash
```

#### Q-16 Your organization wants to reduce ADLS costs by 30% without impacting query performance on active data. What levers do you pull ?
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
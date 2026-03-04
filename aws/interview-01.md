#### Q-1 What is the role of a data engineer at AWS ?
```bash
A Data Engineer at AWS designs and builds scalable data platforms using AWS services like S3, Glue, 
EMR, and Redshift. The role involves building batch and streaming pipelines, ensuring data quality and 
security, optimizing performance and cost, and maintaining highly reliable data systems in production.
```

#### Q-2 What are the common challenges faced by AWS data Engineers ?
```bash
Slow queries in Amazon Redshift
Massive scans on Amazon S3
Poor partitioning in data lakes
Shuffle-heavy Spark jobs on Amazon EMR

Common challenges for AWS Data Engineers include handling large-scale data processing, optimizing performance 
and cost, managing IAM and security complexities, dealing with schema evolution, debugging distributed systems, 
and ensuring reliable monitoring and governance. Cost-performance trade-offs and production reliability are often 
the biggest ongoing challenges.
```

#### Q-3 What exactly is Amazon S3 ?
```bash
Amazon S3 is a scalable object storage service used to store and retrieve large volumes of unstructured or 
structured data. It is highly durable, secure, and commonly used to build data lakes and support analytics 
workloads in AWS environments.    

Bucket
   └── Folder (optional logical path)
         └── File (object)
        
s3://company-data/raw/2026/sales.csv
```

#### Q-4 What does Amazon EC2 do ?
```bash
Amazon EC2 (Elastic Compute Cloud) is a service from Amazon Web Services that lets you run virtual servers 
in the cloud.

Virtual machines (called instances)
CPU, RAM, storage
Operating system (Linux/Windows)
Networking

Amazon EC2 is a cloud computing service that provides scalable virtual servers. It allows users to run applications, 
process data, and host services without managing physical hardware, with flexible instance types and pay-as-you-go 
pricing.
```

#### Q-5 What is Amazon Redshift ?
```bash
Amazon Redshift is a fully managed cloud data warehouse service provided by Amazon Web Services used for 
large-scale analytics and reporting.

Redshift is where you store huge amounts of structured data and run very fast SQL queries for analytics.

Amazon Redshift is a fully managed, columnar cloud data warehouse that uses massively parallel processing to run 
high-performance analytical SQL queries on large datasets. It is commonly used for business intelligence and reporting 
workloads.

COPY sales
FROM 's3://bucket/sales/'
IAM_ROLE 'role-arn'
FORMAT AS PARQUET;
```

#### Q-6 What is Amazon Glue, and how does it make the Extract, Transform, and Load (ETL) process easier ?
```bash
Amazon Glue is a serverless ETL service that helps automate data discovery, transformation, and loading. 
It simplifies ETL by eliminating infrastructure management, automatically detecting schemas using crawlers, 
and executing Spark-based transformation jobs that scale automatically.

Traditional ETL setup requires: Provisioning servers,Installing Spark/Hadoop,Managing clusters,Scheduling jobs,
Handling scaling
Glue removes most of that headache.

1. Glue Data Catalog
This is like a metadata store.
Stores table definitions
Maintains schema
Tracks data location (usually in S3)
Enables schema discovery

2. Glue Crawlers
Scan your data in S3
Automatically detect schema
Create tables in the Data Catalog

3. Glue Jobs
These are the ETL jobs.
Written in Python or Scala
Under the hood → runs on Apache Spark
Fully serverless (no cluster management)

4. Glue Triggers
Schedule jobs
Create job dependencies
Build workflows

Raw Data (S3)
     ↓
Glue Crawler (detect schema)
     ↓
Glue Job (transform)
     ↓
Curated Data (S3 / Redshift)
```

#### Q-7 How do data engineering migrations benefit from the use of AWS DMS (Database Migration Service) ?
```bash
It moves data from source → target while keeping the source database running.
1. Minimal Downtime (Biggest Benefit)
DMS supports:
Full initial load
Then ongoing replication using CDC (Change Data Capture)

So:
It copies existing data.
Then continuously syncs changes.
During cutover → almost zero downtime.

On-Prem DB
    ↓
DMS
    ↓
S3 (raw layer)
    ↓
Glue / EMR
    ↓
Redshift
```

#### Q-8 How does AWS Glue support schema evolution in data engineering?
```bash
What Is Schema Evolution?

New columns added
Columns removed
Data type changes
Nested JSON structure changes

1. Glue Data Catalog Handles Schema Changes
The Glue Data Catalog stores metadata about tables (columns, types, partitions).
When schema changes:
Crawlers can detect new columns
It updates the table definition automatically
Downstream tools (Athena, Redshift Spectrum) see updated schema

2. Glue Crawlers Detect New Columns
{
  "id": 1,
  "name": "Damon"
}

{
  "id": 1,
  "name": "Damon",
  "city": "Mumbai"
}

Detects new column city
Updates table metadata
No manual schema editing required
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
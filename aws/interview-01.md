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

#### Q-9 What is AWS Glue Spark Runtime, and how does it utilize Apache Spark for distributed data processing ?
```bash
AWS Glue Spark Runtime is the managed Apache Spark execution environment used by Glue to run ETL jobs. It provisions 
Spark drivers and executors automatically using DPUs, enabling distributed and parallel data processing without 
requiring cluster management. It leverages Spark’s partitioning, fault tolerance, and in-memory processing capabilities 
while integrating tightly with AWS services like S3 and IAM.

AWS Glue Spark Runtime is the managed Apache Spark environment that runs your Glue ETL jobs.

When you create a Glue job, AWS automatically spins up a managed Apache Spark cluster behind the scenes to 
execute your code.

What Happens When You Run a Glue Job: 
1. Glue provisions compute resources (DPUs)
2. A Spark driver is launched
3. Executors are created
4. Data is processed in parallel
5. Results written to S3 / Redshift / etc.

DPU = Data Processing Unit
1 DPU roughly includes:
4 vCPUs
16 GB memory
More DPUs → more Spark executors → more parallelism.
You configure how many DPUs your job uses.
```

#### Q-10 Explain the difference between Amazon RDS and Amazon Redshift ? 
```bash
| Feature      | Amazon RDS                    | Amazon Redshift             |
| ------------ | ----------------------------- | --------------------------- |
| Purpose      | Transactional database (OLTP) | Analytical warehouse (OLAP) |
| Workload     | Day-to-day app operations     | Reporting & analytics       |
| Data Volume  | GB–TB                         | TB–PB                       |
| Storage Type | Row-based                     | Columnar                    |
| Query Type   | Fast single-row queries       | Complex aggregations        |

Application → RDS (transactions)
            ↓
        ETL Pipeline
            ↓
        Redshift (analytics)
            ↓
        BI Dashboard
```

#### Q-11 What is IAM, and why is it important ?
```bash
AWS Identity and Access Management (IAM) is a service that helps you securely control access to AWS services and 
resources. IAM allows you to manage users, groups, and roles with fine-grained permissions. It’s important because 
it helps enforce the principle of least privilege, ensuring users only have access to the resources they need, thereby 
enhancing security and compliance.
```

#### Q-12 What is Amazon CloudWatch, and what are its main components ?
```bash
```

#### Q-13 What is AWS Lambda, and how does it enable serverless computing ?
```bash
```

#### Q-14 What is Elastic Load Balancing (ELB) in AWS ?
```bash
```

#### Q-15 Explain how you would choose between Amazon RDS, Amazon DynamoDB, and Amazon Redshift for a data-driven application ? 
```bash
```

#### Q-16 What are the main components of Amazon Redshift ?
```bash
```

#### Q-17 What types of data sources can you load into Amazon Redshift ?
```bash
```

#### Q-18 How does Amazon Redshift handle data compression and distribution ?
```bash
```

#### Q-19  What are some best practices for optimizing query performance in Amazon Redshift ?
```bash
```

#### Q-20 How does Amazon Redshift handle concurrency and scalability ?
```bash
```
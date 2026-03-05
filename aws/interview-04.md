#### Q-1 What is the AWS Glue Data Catalog ?
```bash
The AWS Glue Data Catalog is a centralized metadata repository that stores:
Table definitions
Database schemas
Column names & data types
Partition information
Data location (like S3 path)

It does NOT store the actual data — only metadata about the data.
Actual Data → Stored in S3
Schema Info → Stored in Data Catalog

Example table metadata:

Database: sales_db
Table: orders
Columns:
  order_id (string)
  amount (double)
  region (string)
Partitions:
  year
  month
Location:
  s3://sales-data/

S3 (Raw Data)
     ↓
Glue Crawler
     ↓
Glue Data Catalog
     ↓
Athena / Redshift / EMR
```

#### Q-2 Explain the types of triggers in AWS Glue ? 
```bash
Condition Met → Trigger Fires → Job/Crawler Runs
There are three main types of triggers.

There are three main types of triggers.
1️⃣ Scheduled Trigger
Uses:
Cron expressions
Fixed intervals

Example:
Run ETL every night at 1 AM
Run crawler every 30 minutes

Daily ETL → 1:00 AM → Transform S3 raw data → Store cleaned data

2️⃣ On-Demand Trigger
You start it via:
AWS Console
AWS CLI
SDK/API

Best for:
✔ Testing
✔ Debugging
✔ Manual reprocessing

3️⃣ Conditional Trigger (Most Powerful)

Job succeeds
Job fails
Multiple jobs complete

Job A → Job B → Job C
```

#### Q-3 What is a Job Bookmark in AWS Glue ?
```bash
```

#### Q-4 How does AWS Glue integrate with other AWS services ?
```bash
```

#### Q-5 What are the main use cases for AWS Glue ?
```bash
```

#### Q-6 How does AWS Glue work with data lakes ?
```bash
```

#### Q-7 What are DynamicFrames in AWS Glue ?
```bash
```

#### Q-8 How does AWS Glue optimize ETL performance ?
```bash
```

#### Q-9 What are the limitations of AWS Glue ?
```bash
```

#### Q-10 What is the difference between Glue’s Data Catalog and traditional metadata stores ?
```bash
```

#### Q-11 What is a Glue Partition Index, and why is it used ?
```bash
```

#### Q-12 How does AWS Glue handle incremental data updates ?
```bash
```

#### Q-13 What is AWS Glue Workflows ?
```bash
```

#### Q-14 How does AWS Glue integrate with AWS Step Functions ?
```bash
```

#### Q-15 What is the difference between Glue DynamicFrames and Spark DataFrames ?
```bash
```

#### Q-16 What are AWS Glue Blueprints ?
```bash
```

#### Q-17 How does Glue handle schema conflicts ?
```bash
```

#### Q-18 What is AWS Glue Flex ?
```bash
```

#### Q-19  What are AWS Glue Tags ?
```bash
```

#### Q-20 How do Glue Streaming Jobs process late-arriving data ?
```bash
```
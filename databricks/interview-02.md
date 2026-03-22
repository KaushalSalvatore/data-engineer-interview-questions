#### Q-1 In the Medallion Architecture (widely used in Databricks), data flows through three layers: Bronze → Silver → Gold. Each layer has a clear responsibility ? 
```bash
1. Bronze Layer (Raw Data)

What it does:
Stores raw, unprocessed data
Exact copy of source data

-> Characteristics:
No cleaning
No transformations
Append-only (keep history)

-> Example:
Logs from apps
CSV/JSON files from APIs
Database dumps

-> Stored using:
Delta Lake

Purpose: -> “Keep the original truth”

2. Silver Layer (Cleaned Data)
🔹 What it does:
Cleans and standardizes data

🔹 Operations:
Remove duplicates
Handle missing values
Fix data types
Join datasets

🔹 Example:
Clean customer table
Validated transactions

👉 Processed using: Apache Spark

Purpose:“Make data reliable and usable”

3. Gold Layer (Business Data)
🔹 What it does:
Creates business-ready datasets

🔹 Operations:
Aggregations
KPIs
Metrics
Reporting tables

🔹 Example:
Total sales per month
Revenue by region
Customer lifetime value

🧠 Purpose: “Make data useful for decision-making”
```

#### Q-2 How do you schedule jobs in Databricks?
```bash
```

#### Q-3 Describe the Databricks Runtime ? 
```bash
```

#### Q-4 Explain the role of a driver in a Spark cluster ? 
```bash
```

#### Q-5 What is Auto Loader ?
```bash
```

#### Q-6 How do you manage cluster configurations in Databricks ?
```bash
```

#### Q-7 How do you implement data lineage in Databricks ?
```bash
```

#### Q-8 What are the best practices for optimizing Delta Lake performance ?
```bash
```

#### Q-9 How do you handle schema evolution in a Parquet file ?
```bash
```

#### Q-10 What is watermarking in streaming data processing ?
```bash
```

#### Q-11 what is Z ordering in databricks ?  
```bash
Z-Ordering is a technique in Delta Lake that co-locates related data in the same files based on specified columns, 
improving data skipping and query performance.

Why Do We Need Z-Ordering?
When data is stored in a data lake:
Data is spread across many files
Queries scan many unnecessary files
👉 Result:
Slow queries

What Z-Ordering Does
Z-Ordering:
Reorders data inside files
Groups similar values together

👉 So queries can:
Skip irrelevant files
Read only required data

Without Z-Ordering
| customer_id | region | amount |
| ----------- | ------ | ------ |
| 101         | US     | 100    |
| 202         | India  | 200    |
| 101         | US     | 150    |
| 303         | UK     | 300    |

With Z-Ordering on customer_id
| customer_id | region | amount |
| ----------- | ------ | ------ |
| 101         | US     | 100    |
| 101         | US     | 150    |
| 202         | India  | 200    |
| 303         | UK     | 300    |

How to Apply Z-Ordering
OPTIMIZE sales
ZORDER BY (customer_id);
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
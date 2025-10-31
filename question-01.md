#### Q-1 what is difference in distributed process and storage
```bash
Distributed Processing : Distributed processing means splitting a computational task across multiple machines (nodes) that work together to process data or perform calculations.

Example:

Running parts of a large simulation on different servers.
A MapReduce job where:
Map tasks process chunks of data in parallel.
Reduce tasks aggregate the results.

Examples of technologies:
Apache Spark, Hadoop MapReduce

Distributed Storage : Distributed storage means splitting and storing data across multiple physical or virtual storage systems (nodes, disks, or servers), often with replication for reliability.

Example:

A file is split into blocks stored on different servers.
Cloud object storage like Amazon S3 or Google Cloud Storage.

Benefits:

High availability and fault tolerance (data is replicated).
Scalability (add more storage nodes to handle more data).
Improved access speed (data can be read from the nearest node).

Examples of technologies:

HDFS (Hadoop Distributed File System), Amazon S3, Google File System, Ceph, Cassandra (for distributed databases).

```
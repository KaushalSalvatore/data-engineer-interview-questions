#### Q-1 What is Apache Kafka ?
```bash
Apache Kafka is a distributed streaming platform that allows for publishing, subscribing to, storing, and 
processing streams of records in real-time. It's designed to handle high-throughput, fault-tolerant, and 
scalable data pipelines. Kafka is often used for building real-time data pipelines and streaming applications.

While RabbitMQ focuses on real-time message delivery without storing messages long-term, Kafka's retention policy 
supports more complex, data-driven applications.  

Common use cases for Kafka include application tracking, log aggregation, and messaging, though it lacks traditional 
database features like querying and indexing. Its strength lies in handling real-time data streams, making it 
indispensable for distributed systems and real-time analytics.
```

#### Q-2 What are the key components of Kafka ?
```bash
Producer: A producer is any entity that sends messages (also called records) to Kafka topics. Producers 
are responsible for creating and pushing messages into Kafka brokers. Producers can publish messages to
specific partitions within a topic and can also apply message batching and compression.

Consumer: A consumer is an entity that reads messages from Kafka topics. Consumers subscribe to one or more 
Kafka topics and process the messages they receive. Consumers typically belong to a consumer group, which 
ensures that each message is processed by one consumer in the group.

Brokers: Kafka brokers are the servers that store and manage messages. They also handle the distribution of 
partitions across a Kafka cluster. Brokers ensure that data is stored, replicated, and made available for 
consumers. A Kafka cluster consists of multiple brokers working together.

Topics: A topic is a logical channel to which producers send messages and consumers read from. Each topic is 
split into multiple partitions for scalability and parallelism. Topics allow data to be organized logically, 
and each topic can hold messages of a particular kind (e.g., user events, logs).

Partitions: Kafka topics are split into partitions, which allow Kafka to scale horizontally. Partitions enable 
parallelism by allowing different consumers to read different parts of the topic simultaneously. Each partition 
has an ordered sequence of messages.

Zookeeper: Kafka relies on Apache Zookeeper to manage distributed coordination tasks such as leader election for 
partitions, tracking offsets, and maintaining cluster metadata. Zookeeper ensures that Kafka brokers are aware of 
each other’s status and configuration.

Kafka Connect: Kafka Connect is a framework for connecting Kafka to external systems such as databases, file systems, 
and other message queues. It provides pre-built connectors and allows developers to build custom connectors.

Kafka Streams: Kafka Streams is a client library for building real-time stream processing applications on top of Kafka. 
It provides simple APIs for consuming, transforming, and producing data in real-time.
```

#### Q-3 What is a topic in Kafka ?
```bash
A topic in Kafka is a category or feed name to which records are published. Topics in Kafka are always multi-subscriber
that is, a topic can have zero, one, or many consumers that subscribe to the data written to it. Topics are split into 
partitions for improved scalability and parallel processing.

Key characteristics of Kafka topics:
Partitioning
Durability
Topic Names
Retention Policy
```

#### Q-4 What is a partition in Kafka ?
```bash
A partition is an ordered, immutable sequence of records that is continually appended to. Each partition is a structured 
commit log, and records in the partitions are each assigned a sequential id number called the offset. Partitions allow 
Kafka to scale horizontally and provide parallel processing capabilities.
```

#### Q-5 What is the role of ZooKeeper in Kafka ?
```bash
ZooKeeper is used for managing and coordinating Kafka brokers. It serves as a centralized service for maintaining 
configuration information, naming, providing distributed synchronization, and providing group services. ZooKeeper 
keeps track of the status of Kafka cluster nodes, Kafka topics, and partitions.
```

#### Q-6 What is a broker in Kafka ?
```bash
A broker is a Kafka server that runs in a Kafka cluster. It receives messages from producers, assigns offsets 
to them, and commits the messages to storage on disk. It also services consumers, responding to fetch requests
for partitions and responding with the messages that have been published.

Key responsibilities of a Kafka broker:
Message Storage
Partition Management
Cluster Coordination
Serving Clients
```

#### Q-7 How does Kafka ensure fault tolerance ?
```bash
Kafka ensures fault tolerance through data replication. Each partition is replicated across a configurable number 
of servers for fault tolerance. One of the servers is designated as the leader, which handles all read and write 
requests for the partition, while the others are followers that passively replicate the leader.

In Kafka, data is stored across multiple nodes in the cluster. There is a high probability of one of the nodes 
failing. Fault tolerance means that the system is protected and available even when nodes in the cluster fail.
```

#### Q-8  What is the difference between a Kafka consumer and consumer group ?
```bash
A Kafka consumer is an application that reads data from Kafka topics. A consumer group is a set of consumers that 
work together to consume data from one or more topics. The key difference is that each message is delivered to one 
consumer instance within each subscribing consumer group. This allows for parallel processing and load balancing 
of topic consumption.
```

#### Q-9 What is the purpose of the offset in Kafka ?
```bash
The offset is a unique identifier of a record within a partition. It denotes the position of the consumer in the 
partition. Kafka maintains this offset per partition, per consumer group, allowing each consumer group to read 
from a different position in the partition. This enables Kafka to provide both queue and publish-subscribe messaging 
models.
```

#### Q-10 How does Kafka support scalability ?
```bash
Kafka supports scalability through partitioning and distributed processing. Topics can be partitioned across multiple 
brokers, allowing for parallel processing. Consumers can be grouped to read from multiple partitions simultaneously. 
Brokers can be added to a cluster to increase capacity, and the cluster can be scaled without downtime.
```

#### Q-11 How does Kafka handle data retention ?
```bash
Kafka handles data retention through configurable retention policies. These can be based on time (e.g., retain data 
for 7 days) or size (e.g., retain up to 1GB per partition). After the retention limit is reached, old messages
are discarded. Kafka also supports log compaction for topics where only the latest value for each key is needed.

Kafka’s message retention policy

Time-based Retention: The most common retention policy, which is based on time. Kafka can be configured to 
retain messages for a specified period, such as 7 days. Messages older than the retention period are deleted 
automatically. 

Size-based Retention: Kafka can also retain messages based on the total size of the log for each partition. 
When the log size exceeds a specified limit, Kafka will delete the oldest messages to free up space.
```

#### Q-12 What is the difference between a Kafka consumer and a Kafka streams application ?
```bash
A Kafka consumer is a client that reads data from Kafka topics and processes it in some way. It's typically 
used for simple consumption scenarios. A Kafka Streams application, on the other hand, is a more sophisticated 
client that can consume, process, and produce data back to Kafka. It provides a DSL for complex stream processing 
operations like filtering, transforming, aggregating, and joining streams.
```

#### Q-13 How does Kafka handle message serialization and deserialization ?
```bash
Kafka producers and consumers can be configured with serializers and deserializers for keys and values. Common 
formats include String, Integer, and Avro. For complex objects, custom serializers and deserializers can be 
implemented.

Application Object
       |
       | Serialize
       v
Producer
       |
       | Bytes
       v
Kafka Topic
       |
       | Bytes
       v
Consumer
       |
       | Deserialize
       v
Application Object

Kafka brokers only store bytes. Therefore, before sending data to Kafka, the producer must convert the object into a byte array.

The consumer receives the byte array and converts it back into an object.

Why do we need serialization in Kafka?

Because Kafka brokers can only store and transmit byte arrays. Serialization converts application objects into bytes before sending, and deserialization converts bytes back into application objects after receiving.
```

#### Q-14 How does Kafka handle message retention across multiple data centers ?
```bash
Kafka can handle message retention across multiple data centers through a feature called MirrorMaker. MirrorMaker 
is a stand-alone tool for copying data between Kafka clusters. It consumes from one cluster and produces to another, 
allowing for replication of data across different data centers. This can be used for disaster recovery, geographic 
distribution of data, or aggregating data from multiple sources into a central location.

 configuration settings for retention : 

 log.retention.hours: The default retention period is 168 hours (7 days), meaning messages in Kafka topics will be 
 retained for 7 days by default. After that, older messages are automatically deleted.

log.retention.bytes: This parameter controls the total disk space to be used for a topic's log files. Once this size 
is exceeded, older messages are purged.

log.segment.bytes: This defines the maximum size of a log segment file. Once a segment reaches this size, it’s rolled 
over, and a new file is created for new messages.
```

#### Q-15 How do Partitions work in Kafka ?
```bash
In Kafka, a topic serves as a storage space where all messages from producers are kept. Typically, related 
data is stored in separate topics. For instance, a topic named "transactions" would store details of user 
purchases on an e-commerce site, while a topic called "customers" would hold customer information.

Topics are divided into partitions. By default, a topic has one partition, but you can configure it to have more.
 Messages are distributed across these partitions, with each partition having its own offset and being stored on a 
 different server in the Kafka cluster.

For example, if a topic has three partitions across three brokers, and a producer sends 15 messages, the messages 
are distributed in sequence:

Record 1 goes to Partition 0
Record 2 goes to Partition 1
Record 3 goes to Partition 2
Then the cycle repeats, with Record 4 going back to Partition 0, and so on.
```

#### Q-16 What is the primary purpose of log compaction in Kafka? How does log compaction impact the performance of Kafka consumers ?
```bash
The main goal of log compaction in Kafka is to retain the most recent value for each unique key in a topic's log, 
ensuring that the latest state of the data is preserved and reducing storage usage. This allows consumers to access 
the current value more efficiently without having to process older duplicates. 
```

#### Q-17 What is the difference between Partitions and Replicas in a Kafka cluster ?
```bash
Partitions increase throughput by allowing a topic to be split into multiple parts, enabling consumers to read 
from different partitions in parallel, which improves Kafka's scalability and efficiency. 

Replicas, on the other hand, provide redundancy by creating copies of partitions across multiple brokers. 
This ensures fault tolerance because, in the event of a leader broker failure (the broker managing read and 
write operations for a partition), one of the follower replicas can be promoted to take over as the new leader. 

Partition is a single piece of Kafka topic. More partitions allow excellent parallelism when reading from the topics. 
The number of partitions is configured based on per topic.
```

#### Q-18 What is a schema in Kafka, and why is it important for distributed systems ? 
```bash
In Kafka, a schema defines the structure and format of data, such as fields like CustomerID (integer), 
CustomerName (string), and Designation (string).

The Schema Registry stores schemas (commonly in formats like Avro, Protobuf, or JSON Schema) and supports 
schema evolution, allowing data formats to change without breaking existing consumers. This ensures smooth 
data exchange and system reliability as schemas evolve.
```

#### Q-19 What are the differences between leader replica and follower replica in Kafka ?
```bash
Leader replica
The leader replica handles all client read and write requests. It manages the partition’s state and is the 
primary point of interaction for producers and consumers. If the leader fails, its role is transferred to 
one of the follower replicas to maintain availability.

Follower replica
The follower replica replicates data from the leader but does not directly handle client requests. Its 
role is to ensure fault tolerance by keeping an up-to-date copy of the partition’s data.
```

#### Q-20 Why do we use clusters in Kafka, and what are their benefits ?
```bash
A Kafka cluster is made up of multiple brokers that distribute data across several instances, allowing for 
scalability without downtime. These clusters are designed to minimize delays, and in case the primary cluster 
fails, other Kafka clusters can take over to maintain service continuity.

The architecture of a Kafka cluster includes Topics, Brokers, Producers, and Consumers. It efficiently manages 
data streams, making it ideal for big data applications and the development of data-driven applications.
```
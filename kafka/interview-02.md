#### Q-1 What Do ISR and AR represent in Kafka? What does ISR expansion mean ?
```bash
ISR (in-sync replicas)
ISR refers to the replicas that are fully synchronized with the leader replica. These replicas have the latest 
data and are considered reliable for both read and write operations.

AR (assigned replicas)
AR includes all replicas assigned to a partition, both in-sync and out-of-sync replicas. It represents the complete 
set of replicas for a partition.

ISR expansion
ISR expansion occurs when new replicas catch up with the leader and are added to the ISR list. This increases the 
number of up-to-date replicas, improving fault tolerance and reliability.
```

#### Q-2 What is a partitioning key ?
```bash
The partitioning key indicates the destination partition of the message within the producer. A hashing based 
partitioner determines the partition ID when the key is given.
```

#### Q-3 When does QueueFullException occur in the producer ?
```bash
QueueFullException occurs when the producer attempts to send messages at a pace not handleable by the broker.
```

#### Q-4 What maximum message size can the Kafka server receive ?
```bash
The maximum message size that Kafka server can receive is 10 lakh bytes.
```

#### Q-5 Who is the producer in Kafka ?
```bash
The producer is a client who publishes and sends the record. The producer sends data to the broker service. 
The producer applications write data to topics that are ready by consumer applications.

Key characteristics of a Kafka producer:
Message Serialization
Partitioning
Batched Sends
Asynchronous Delivery
Acknowledgments
```

#### Q-6 What is the consumer lag ?
```bash
Reads in Kafka lag behind Writes as there is always some delay between writing and consuming the message. This 
delta between the consuming offset and the latest offset is called consumer lag.
```

#### Q-7 What is Kafka producer Acknowledgement ?
```bash
An acknowledgement or ack is sent to the producer by a broker to acknowledge receipt of the message. Ack level 
defines the number of acknowledgements that the producer requires before considering a request complete.

Kafka producer acknowledgments decide how many nodes must confirm receipt. acks=0 means fire-and-forget. 
acks=1 waits for the leader. 
acks=all waits for all in-sync replicas.
```

#### Q-8 What is load balancing and perform load balancing with Kafka consumers?
```bash
The load balancer distributes loads across multiple systems in caseload gets increased by replicating messages 
on different systems.

Kafka uses consumer groups for balancing. Each partition is assigned to only one consumer in a group. When new 
consumers join or leave, Kafka triggers a rebalance. It redistributes partitions among the active consumers 
automatically.
```

#### Q-9 What is meant by partition offset ?
```bash
The offset uniquely identifies a record within a partition. Topics can have multiple partition logs that allow consumers 
to read in parallel. Consumers can read messages from a specific as well as an offset print of their choice.
```

#### Q-10 How do Kafka producers discover the right broker for a topic ?
```bash
Producers use the Kafka cluster metadata to find the leader broker for each partition. When a producer connects, 
it fetches metadata for the target topic. This metadata tells it which broker is the leader for each partition. 
It then sends data directly to that broker.
```

#### Q-11 What would happen if a consumer is slower than the producer in Kafka ?
```bash
If a consumer is too slow, messages start piling up in the topic. Kafka keeps messages for a configured retention 
period. A slow consumer can catch up within that time. But if the lag grows and retention time expires, the consumer 
might miss data permanently.
```

#### Q-12 How would you design a real-time analytics system using Kafka ?
```bash
I use Kafka as the central event bus. Producers send raw events to a topic. Then I use Kafka Streams or Spark 
Streaming for real-time processing. Aggregated results go to a dashboard or database. Everything is decoupled 
for easier scaling.
```

#### Q-13 What’s the impact of increasing the number of partitions in Kafka ?
```bash
More partitions improve parallelism and throughput. Each partition can be read by one consumer. But too many 
partitions increase metadata overhead. It also slows down broker recovery and leader elections. I balance 
partition count based on expected load and hardware.
```

#### Q-14 What is a Kafka message (record) ?
```bash
A Kafka message (or record) is the basic unit of data within Kafka. 
A message consists of the following components:

Key: An optional identifier for the message, used for partitioning purposes. Kafka uses the key to determine 
which partition the message should be written to. Messages with the same key will always go to the same 
partition, which ensures order for related messages.

Value: The actual content of the message, which can be a string, JSON, Avro, or any other type of data format.

Timestamp: The time at which the message was produced or when Kafka recorded it.

Offset: A unique identifier for the position of the message within a partition. This allows consumers to keep 
track of which messages they have processed.
```

#### Q-15 How do you configure Kafka to produce data with a specific key ?
```bash
Steps to configure:

Producer Configuration: In your producer configuration, set the key.serializer and value.serializer to appropriate 
serializers (e.g., StringSerializer or ByteArraySerializer).

Producer API: When producing messages using the producer API, you can specify the key along with the message value. 
The producer will then use this key for partitioning.

ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key1", "value1");
producer.send(record);
```

#### Q-16 What are Kafka’s default retention policies and how can they be customized ?
```bash
afka retains messages for 7 days and deletes messages based on the size of the logs or the retention time. 
The default configurations are:

Retention Time:
The default retention time is log.retention.ms=16800000 (7 days).

Retention Size:
The default retention size is log.retention.bytes=-1, meaning there is no disk size limit for retention.

How to Customize:

kafka-topics.sh --alter --topic my-topic --config retention.ms=3600000 --bootstrap-server localhost:9092
kafka-topics.sh --alter --topic my-topic --config retention.bytes=500000000 --bootstrap-server localhost:9092
```

#### Q-17 How do you tune Kafka for fault tolerance in a distributed environment ? 
```bash
eplication Factor: Set an appropriate replication factor (e.g., replication.factor=3) for each topic. This ensures that 
data is replicated across multiple brokers, reducing the risk of data loss in case of broker failure.

In-Sync Replicas (ISR): Ensure that the number of in-sync replicas (ISR) is sufficient. Kafka guarantees that only in-sync replicas can serve as leaders, ensuring that data is available even if some brokers fail.

Leader Election: Enable automatic leader election for partitions to ensure that a new leader is chosen when a broker fails. 
You can tune min.insync.replicas to define the minimum number of replicas that must acknowledge a write for the write to be considered successful.

Producer Retries: Enable producer retries (retries=3) to ensure that temporary network issues do not result in data loss. 
Setting acks=all ensures that retries are consistent across replicas.

Disk I/O Optimization: Ensure that brokers have adequate disk I/O performance. Use high-performance SSDs for faster 
read/write access.

Monitoring and Alerts: Set up monitoring and alerts for potential issues such as under-replicated partitions, broker failures, 
or disk usage thresholds to take proactive action before failures impact availability.
```

#### Q-18 What is the need for message compression in Apache Kafka ?
```bash
Due to reduced size, it reduces the latency in which messages are sent to Kafka.
Reduced bandwidth allows the producers to send more net messages to the broker.
When the data is stored in Kafka via cloud platforms, it can reduce the cost in cases where the cloud 
services are paid.
Message compression leads to reduced disk load, which will lead to faster read and write requests.
```

#### Q-19 Can a consumer read more than one partition from a topic ?
```bash
Yes, if the number of partitions is greater than the number of consumers in a consumer group, then a consumer 
will have to read more than one partition from a topic.
```

#### Q-20 What are the responsibilities of a Controller Broker in Kafka ?
```bash
creating and deleting topics
Adding partitions and assigning leaders to the partitions
Managing the brokers in a cluster - adding new brokers, active broker shutdown, and broker failures
Leader Election
Reallocation of partitions.
```
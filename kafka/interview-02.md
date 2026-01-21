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
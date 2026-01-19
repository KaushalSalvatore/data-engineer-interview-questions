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

#### Q-2
```bash
```

#### Q-3
```bash
```

#### Q-4
```bash
```

#### Q-5
```bash
```

#### Q-6
```bash
```

#### Q-7
```bash
```

#### Q-8
```bash
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
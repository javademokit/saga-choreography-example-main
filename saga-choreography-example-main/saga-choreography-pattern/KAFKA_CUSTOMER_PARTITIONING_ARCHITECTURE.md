# Kafka Customer-Based Partitioning and Ordered Event Processing Architecture

## 1) Enterprise Context (Cloud-Native Domains -> Kafka)

```mermaid
flowchart LR
  %% Domains / Producers
  subgraph BP[Business Platforms on Cloud-Native Runtime]
    B1[Banking Domain\nProducer: Customer Registration]
    B2[Telecom Domain\nProducer: Subscriber Update]
    B3[E-commerce Domain\nProducer: Order Creation]
    B4[Billing System\nProducer: Bill Generation]
    B5[Order Processing System\nProducer: Payment Processing]
  end

  subgraph CP[Container Platform]
    K8s[☸ Kubernetes / OpenShift Cluster]
  end

  subgraph KC[Kafka Cluster]
    BK1[(Broker-1)]
    BK2[(Broker-2)]
    BK3[(Broker-3)]
  end

  B1 --> K8s
  B2 --> K8s
  B3 --> K8s
  B4 --> K8s
  B5 --> K8s

  K8s -->|Produce to topic: customer-events\nKey = CustomerId| BK1
  K8s --> BK2
  K8s --> BK3

  classDef domain fill:#e8f0fe,stroke:#2f5fb3,stroke-width:1px,color:#0b1f44;
  classDef plat fill:#eafaf1,stroke:#1e824c,stroke-width:1px,color:#0d3b24;
  classDef kafka fill:#fff4e6,stroke:#b35c00,stroke-width:1px,color:#5a2c00;

  class B1,B2,B3,B4,B5 domain;
  class K8s plat;
  class BK1,BK2,BK3 kafka;
```

## 2) Partitioning Strategy (Keyed by Customer ID)

**Kafka routing rule:**

```text
Partition = hash(CustomerId) % NumberOfPartitions
```

**Topic:** `customer-events`  
**Partitions:** `Partition-0` to `Partition-5` (6 total)

```mermaid
flowchart LR
  P1[Customer Registration\nCustomerId=1001]
  P2[Order Creation\nCustomerId=1001]
  P3[Payment Processing\nCustomerId=1001]
  P4[Bill Generation\nCustomerId=2001]

  H1[Kafka Producer Partitioner\nPartition = hash(CustomerId) % 6]

  subgraph T[Topic: customer-events]
    Q0[Partition-0]
    Q1[Partition-1]
    Q2[Partition-2]
    Q3[Partition-3]
    Q4[Partition-4]
    Q5[Partition-5]
  end

  P1 --> H1
  P2 --> H1
  P3 --> H1
  P4 --> H1

  H1 -->|CustomerId=1001 -> Partition-2| Q2
  H1 -->|CustomerId=1001 -> Partition-2| Q2
  H1 -->|CustomerId=1001 -> Partition-2| Q2
  H1 -->|CustomerId=2001 -> Partition-4| Q4
```

## 3) Ordering Guarantee Inside a Partition

```mermaid
sequenceDiagram
  autonumber
  participant PR as Producers (Multiple Domains)
  participant PT as customer-events:Partition-2
  participant C as Assigned Consumer

  PR->>PT: CustomerId=1001, Event=Customer Registration
  PR->>PT: CustomerId=1001, Event=Order Creation
  PR->>PT: CustomerId=1001, Event=Payment Processing
  PT->>C: Offset 120 -> Registration
  PT->>C: Offset 121 -> Order Creation
  PT->>C: Offset 122 -> Payment Processing

  Note over PT,C: Ordering preserved for same key within one partition
```

## 4) Consumer Group Architecture and Partition Assignment

```mermaid
flowchart LR
  subgraph TOP[Topic: customer-events]
    P0[Partition-0]
    P1[Partition-1]
    P2[Partition-2]
    P3[Partition-3]
    P4[Partition-4]
    P5[Partition-5]
  end

  subgraph CGA[Consumer Group A]
    C1[Consumer-1]
    C2[Consumer-2]
    C3[Consumer-3]
  end

  P0 --> C1
  P3 --> C1

  P1 --> C2
  P4 --> C2

  P2 --> C3
  P5 --> C3

  Note1[Each partition is consumed by exactly one consumer\nin the same consumer group at a time]
  P5 -.-> Note1
```

## 5) Event Types Produced by Domains

| Business Domain | Producer Event |
|---|---|
| Banking | Customer Registration |
| Telecom (Subscriber Management) | Subscriber Update |
| E-commerce | Order Creation |
| Billing System | Bill Generation |
| Order Processing System | Payment Processing |

## 6) Design Benefits

- **Message Ordering Guaranteed**: events for the same `CustomerId` are strictly ordered per partition.
- **Horizontal Scalability**: add partitions and consumers to increase parallel processing.
- **High Throughput**: workload is distributed across multiple brokers and partitions.
- **Fault Tolerance**: Kafka replication and consumer group rebalancing support resilience.
- **Customer Data Consistency**: key-based routing avoids cross-partition reordering for a customer timeline.

## 7) Architecture Notes for Review Boards

- Use `CustomerId` as mandatory message key in every producer API contract.
- Keep partition count fixed for stable key distribution; change only with migration planning.
- Use schema registry and versioned event contracts for enterprise governance.
- Monitor partition skew and consumer lag; rebalance producer keys if hotspots appear.
- Deploy producers/consumers as stateless pods on Kubernetes/OpenShift for elastic scaling.


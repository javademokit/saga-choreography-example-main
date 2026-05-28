# 🏗️ Architecture Design Document
## Saga Choreography Pattern — Spring Boot + Kafka + Spring Cloud Stream

---

## 📌 Project Overview

| Property             | Value                            |
|----------------------|----------------------------------|
| Pattern              | Saga Choreography                |
| Architecture         | Event-Driven Microservices       |
| Language             | Java 1.8                         |
| Framework            | Spring Boot 2.5.4                |
| Messaging            | Apache Kafka                     |
| Stream Abstraction   | Spring Cloud Stream 2020.0.3     |
| Reactive Programming | Project Reactor (Flux, Mono)     |
| Build Tool           | Maven (Multi-Module)             |

---

## 🧩 Module Structure

```
saga-choreography-pattern/          ← Parent Maven Project
├── common-dtos/                    ← Shared DTOs & Events (JAR library)
├── order-service/                  ← Microservice 1 (port: 8081)
└── payment-service/                ← Microservice 2 (port: 8082)
```

---

## 🗂️ Module Responsibilities

### 1. `common-dtos` — Shared Library
Shared between both microservices. Contains all events and DTOs.

| Class               | Purpose                                              |
|---------------------|------------------------------------------------------|
| `OrderRequestDto`   | Payload for placing an order                         |
| `OrderResponseDto`  | Response after order processing                      |
| `PaymentRequestDto` | Payload for payment processing                       |
| `OrderEvent`        | Kafka event published by order-service               |
| `PaymentEvent`      | Kafka event published by payment-service             |
| `OrderStatus`       | Enum: ORDER_CREATED, ORDER_COMPLETED, ORDER_CANCELLED|
| `PaymentStatus`     | Enum: PAYMENT_COMPLETED, PAYMENT_FAILED              |
| `Event`             | Common interface (eventId, date)                     |

---

### 2. `order-service` — Port 8081

**Responsibilities:**
- Accepts order creation REST requests
- Publishes `OrderEvent` to Kafka topic `order-event`
- Listens to `PaymentEvent` from Kafka topic `payment-event`
- Updates order status based on payment result
- If payment fails → publishes `ORDER_CANCELLED` event (compensating transaction)

**Key Classes:**

| Class                      | Role                                                    |
|----------------------------|---------------------------------------------------------|
| `OrderController`          | REST API: `POST /order/create`, `GET /order`            |
| `OrderService`             | Business logic, creates PurchaseOrder in DB             |
| `OrderStatusPublisher`     | Pushes OrderEvent into reactive Sinks                   |
| `OrderPublisherConfig`     | Exposes `Supplier<Flux<OrderEvent>>` → Kafka            |
| `EventConsumerConfig`      | `Consumer<PaymentEvent>` ← Kafka `payment-event`        |
| `OrderStatusUpdateHandler` | Updates DB order status, triggers compensating tx       |
| `PurchaseOrder`            | JPA Entity (id, userId, productId, price, statuses)     |
| `OrderRepository`          | Spring Data JPA repository                              |

---

### 3. `payment-service` — Port 8082

**Responsibilities:**
- Listens to `OrderEvent` from Kafka topic `order-event`
- Validates user balance
- Deducts balance if sufficient → `PAYMENT_COMPLETED`
- Rejects if insufficient → `PAYMENT_FAILED`
- Publishes `PaymentEvent` back to Kafka topic `payment-event`
- Handles compensation: restores balance on `ORDER_CANCELLED`

**Key Classes:**

| Class                        | Role                                                 |
|------------------------------|------------------------------------------------------|
| `PaymentConsumerConfig`      | `Function<Flux<OrderEvent>, Flux<PaymentEvent>>`     |
| `PaymentService`             | Core business logic for payment processing           |
| `UserBalance`                | JPA Entity (userId, price/balance)                   |
| `UserTransaction`            | JPA Entity (orderId, userId, amount)                 |
| `UserBalanceRepository`      | Spring Data JPA repository                           |
| `UserTransactionRepository`  | Spring Data JPA repository                           |

---

## 🔄 Event Flow Diagram

```
Client
  │
  │  POST /order/create
  ▼
┌──────────────────────────────────────────┐
│           ORDER-SERVICE (:8081)          │
│                                          │
│  OrderController                         │
│       │                                  │
│       ▼                                  │
│  OrderService ──► saves PurchaseOrder    │
│       │           (status: ORDER_CREATED) │
│       ▼                                  │
│  OrderStatusPublisher                    │
│       │  pushes to Sinks<OrderEvent>     │
│       ▼                                  │
│  OrderPublisherConfig                    │
│  Supplier<Flux<OrderEvent>>              │
└─────────────────┬────────────────────────┘
                  │
                  │  Kafka Topic: "order-event"
                  │  { orderId, userId, amount, ORDER_CREATED }
                  ▼
┌──────────────────────────────────────────┐
│         PAYMENT-SERVICE (:8082)          │
│                                          │
│  PaymentConsumerConfig                   │
│  Function<Flux<OrderEvent>,              │
│           Flux<PaymentEvent>>            │
│       │                                  │
│       ▼                                  │
│  PaymentService.newOrderEvent()          │
│       │                                  │
│       ├─ Balance OK?                     │
│       │   YES → deduct balance           │
│       │          save UserTransaction    │
│       │          → PAYMENT_COMPLETED     │
│       │                                  │
│       └─ Balance NOT OK?                 │
│            → PAYMENT_FAILED             │
└─────────────────┬────────────────────────┘
                  │
                  │  Kafka Topic: "payment-event"
                  │  { orderId, userId, PAYMENT_COMPLETED / PAYMENT_FAILED }
                  ▼
┌──────────────────────────────────────────┐
│           ORDER-SERVICE (:8081)          │
│                                          │
│  EventConsumerConfig                     │
│  Consumer<PaymentEvent>                  │
│       │                                  │
│       ▼                                  │
│  OrderStatusUpdateHandler.updateOrder()  │
│       │                                  │
│       ├─ PAYMENT_COMPLETED               │
│       │   → ORDER_COMPLETED ✅           │
│       │                                  │
│       └─ PAYMENT_FAILED                  │
│           → ORDER_CANCELLED ❌           │
│           → publish ORDER_CANCELLED evt  │
└─────────────────┬────────────────────────┘
                  │
                  │  Kafka Topic: "order-event"
                  │  { orderId, ..., ORDER_CANCELLED }
                  ▼
┌──────────────────────────────────────────┐
│         PAYMENT-SERVICE (:8082)          │
│                                          │
│  PaymentService.cancelOrderEvent()       │
│       │                                  │
│       └─ delete UserTransaction          │
│          restore UserBalance ✅           │
└──────────────────────────────────────────┘
```

---

## 📨 Kafka Topic Mapping

| Topic Name      | Producer        | Consumer         | Event Type     |
|-----------------|-----------------|------------------|----------------|
| `order-event`   | order-service   | payment-service  | `OrderEvent`   |
| `payment-event` | payment-service | order-service    | `PaymentEvent` |

---

## 🔗 Spring Cloud Stream Binding

### order-service (`application.yaml`)
```yaml
spring:
  cloud:
    stream:
      function:
        definition: orderSupplier;paymentEventConsumer
      bindings:
        orderSupplier-out-0:
          destination: order-event
        paymentEventConsumer-in-0:
          destination: payment-event
```

### payment-service (`application.yaml`)
```yaml
spring:
  cloud:
    stream:
      function:
        definition: paymentProcessor
      bindings:
        paymentProcessor-in-0:
          destination: order-event
        paymentProcessor-out-0:
          destination: payment-event
```

---

## 🔁 Saga Compensating Transactions

| Scenario          | Step 1          | Step 2               | Step 3                              |
|-------------------|-----------------|----------------------|-------------------------------------|
| ✅ Happy Path      | ORDER_CREATED   | PAYMENT_COMPLETED    | ORDER_COMPLETED                     |
| ❌ Payment Fails   | ORDER_CREATED   | PAYMENT_FAILED       | ORDER_CANCELLED + balance restored  |

---

## 🗄️ Database Design

### order-service DB
**Table: `purchase_order`**

| Column          | Type    | Description                                   |
|-----------------|---------|-----------------------------------------------|
| id              | INT     | Order ID (PK)                                 |
| user_id         | INT     | User reference                                |
| product_id      | INT     | Product reference                             |
| price           | DOUBLE  | Order amount                                  |
| order_status    | VARCHAR | ORDER_CREATED / ORDER_COMPLETED / ORDER_CANCELLED |
| payment_status  | VARCHAR | PAYMENT_COMPLETED / PAYMENT_FAILED            |

### payment-service DB
**Table: `user_balance`**

| Column   | Type   | Description       |
|----------|--------|-------------------|
| user_id  | INT    | User ID (PK)      |
| price    | DOUBLE | Available balance |

**Table: `user_transaction`**

| Column   | Type   | Description     |
|----------|--------|-----------------|
| order_id | INT    | Order ID (PK)   |
| user_id  | INT    | User reference  |
| amount   | DOUBLE | Deducted amount |

**Pre-loaded User Balances (via `@PostConstruct`):**

| userId | Balance |
|--------|---------|
| 101    | 5000    |
| 102    | 3000    |
| 103    | 4200    |
| 104    | 20000   |
| 105    | 999     |

---

## 🛠️ Tech Stack Summary

| Component            | Technology                         |
|----------------------|------------------------------------|
| Language             | Java 8                             |
| Framework            | Spring Boot 2.5.4                  |
| Messaging Broker     | Apache Kafka                       |
| Stream Abstraction   | Spring Cloud Stream 2020.0.3       |
| Reactive Programming | Project Reactor (Flux, Mono, Sinks)|
| ORM                  | Spring Data JPA / Hibernate        |
| Database             | H2 (in-memory) / configurable      |
| Build                | Maven Multi-Module                 |
| Code Generation      | Lombok                             |

---

## 🚀 How to Run

### 1. Start Kafka & Zookeeper
```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

### 2. Build the project
```bash
mvn clean install
```

### 3. Start order-service (port 8081)
```bash
cd order-service && mvn spring-boot:run
```

### 4. Start payment-service (port 8082)
```bash
cd payment-service && mvn spring-boot:run
```

### 5. Create an Order
```http
POST http://localhost:8081/order/create
Content-Type: application/json

{
  "userId": 101,
  "productId": 1,
  "amount": 1000
}
```

### 6. Get All Orders
```http
GET http://localhost:8081/order
```

---

## 📁 Project File Structure

```
saga-choreography-pattern/
├── ARCHITECTURE.md                          ← This file
├── pom.xml                                  ← Parent POM
├── common-dtos/
│   ├── pom.xml
│   └── src/main/java/com/javatechie/saga/commons/
│       ├── dto/
│       │   ├── OrderRequestDto.java
│       │   ├── OrderResponseDto.java
│       │   └── PaymentRequestDto.java
│       └── event/
│           ├── Event.java
│           ├── OrderEvent.java
│           ├── OrderStatus.java
│           ├── PaymentEvent.java
│           └── PaymentStatus.java
├── order-service/
│   ├── pom.xml
│   └── src/main/java/com/javatechie/saga/order/
│       ├── OrderServiceApplication.java
│       ├── config/
│       │   ├── EventConsumerConfig.java      ← Consumes PaymentEvent from Kafka
│       │   ├── OrderPublisherConfig.java     ← Publishes OrderEvent to Kafka
│       │   └── OrderStatusUpdateHandler.java ← Updates order + triggers compensation
│       ├── controller/
│       │   └── OrderController.java          ← REST API
│       ├── entity/
│       │   └── PurchaseOrder.java
│       ├── repository/
│       │   └── OrderRepository.java
│       └── service/
│           ├── OrderService.java
│           └── OrderStatusPublisher.java
└── payment-service/
    ├── pom.xml
    └── src/main/java/com/javatechie/saga/payment/
        ├── PaymentServiceApplication.java
        ├── config/
        │   └── PaymentConsumerConfig.java    ← Consumes OrderEvent, publishes PaymentEvent
        ├── entity/
        │   ├── UserBalance.java
        │   └── UserTransaction.java
        ├── repository/
        │   ├── UserBalanceRepository.java
        │   └── UserTransactionRepository.java
        └── service/
            └── PaymentService.java           ← Core payment logic
```


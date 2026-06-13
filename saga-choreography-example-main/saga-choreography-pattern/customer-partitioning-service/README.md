# customer-partitioning-service

Standalone Spring Boot application that publishes customer events to Kafka using `CustomerId` as the message key.

## What this app demonstrates

- Keyed event publishing to Kafka topic `customer-events`
- Kafka partition strategy: `partition = hash(customerId) % numberOfPartitions`
- Deterministic routing: same `CustomerId` maps to the same partition
- REST endpoint to preview partition selection before publishing

## Endpoints

- `POST /api/customer-events`
- `GET /api/customer-events/partition/{customerId}?partitions=6`

## Sample request

```json
{
  "customerId": 1001,
  "eventType": "Order Creation",
  "businessDomain": "E-commerce",
  "payload": {
    "orderId": "ORD-1001"
  }
}
```

## Run

```powershell
mvn -pl customer-partitioning-service spring-boot:run
```

## Quick test commands

```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8083/api/customer-events/partition/1001?partitions=6"
```

```powershell
$body = @{
  customerId = 1001
  eventType = "Order Creation"
  businessDomain = "E-commerce"
  payload = @{ orderId = "ORD-1001" }
} | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post -Uri "http://localhost:8083/api/customer-events" -ContentType "application/json" -Body $body
```

## Notes

- Default port: `8083`
- Default topic: `customer-events`
- Update `spring.kafka.bootstrap-servers` in `src/main/resources/application.yaml` as needed.
- Optional startup sample publisher: set `app.sample-runner.enabled=true`.


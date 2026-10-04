# StreamRune Outbox

Umbrella module for transactional outbox pattern implementations. The event store writes an outbox entry in the same transaction as the events it describes; the runtime's `OutboxPoller` claims pending entries and hands them to an adapter's `OutboxPublisher`, which publishes them to an external message broker — at-least-once delivery without two-phase commits. `streamrune-runtime` also ships `HttpOutboxPublisher` for webhook delivery.

## Modules

| Module | Description |
|---|---|
| [streamrune-kafka-outbox](streamrune-kafka-outbox/) | Outbox publisher for Apache Kafka (kafka-clients 3.9) |
| [streamrune-rabbitmq-outbox](streamrune-rabbitmq-outbox/) | Outbox publisher for RabbitMQ (amqp-client 5.25) |

## Requirements

- Java 25+
- `streamrune-core` (the adapters' only StreamRune dependency); `streamrune-runtime` for `OutboxPoller` and `streamrune-postgres` for `PostgresOutboxStore`
- A running Kafka or RabbitMQ broker (Testcontainers used in integration tests)

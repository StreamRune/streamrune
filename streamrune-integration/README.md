# StreamRune Integration

Umbrella module for StreamRune framework integrations.

## Modules

| Module | Framework | Description |
|---|---|---|
| [streamrune-integration-api](streamrune-integration-api/README.md) | Any | Shared: Micrometer metrics, SSE authorization SPI, startup validators |
| [streamrune-spring](streamrune-spring/README.md) | Spring Boot 4 | Auto-configuration, SSE |
| [streamrune-spring-boot-starter](streamrune-spring-boot-starter/README.md) | Spring Boot 4 | Starter: `streamrune-spring`, `streamrune-postgres` and `streamrune-runtime` in one dependency |
| [streamrune-quarkus](streamrune-quarkus/README.md) | Quarkus | ArC/CDI producers, SSE |
| [streamrune-micronaut](streamrune-micronaut/README.md) | Micronaut | DI integration, context propagation |
| [streamrune-postgres](../streamrune-eventstore/streamrune-postgres/README.md) | PostgreSQL | JDBC event store implementation |

Each integration binds the `streamrune.*` configuration keys in its framework's native configuration format, through its own properties type: `StreamRuneProperties` (Spring), `StreamRuneQuarkusProperties` (Quarkus) and `StreamRuneMicronautProperties` (Micronaut).

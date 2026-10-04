# StreamRune

Event sourcing framework for Java.

## Status

StreamRune is not released yet. The only published build is `1.0.0-alpha-SNAPSHOT`, an unreleased
preview of `1.0.0` that lives in the Maven Central snapshot repository, not in Maven Central itself:
a plain `mavenCentral()` repository does not resolve it (see [Preview builds](#preview-builds)).
Every module shares the `org.streamrune` group and that version. See the
[CHANGELOG](CHANGELOG.md) for what the preview contains.

## Preview builds

`1.0.0-alpha-SNAPSHOT` is an unreleased preview of `1.0.0`, rebuilt from `main` each time the Build
workflow passes on it. It is published only to the Maven Central snapshot repository, not to Maven
Central itself, its artifacts are not signed, and it can change or break at any time. It is licensed
under the Business Source License 1.1, like every StreamRune version (see [License](#license)).

To try it, add the snapshot repository and depend on `org.streamrune:*:1.0.0-alpha-SNAPSHOT`.

Gradle (Kotlin DSL):

```kotlin
repositories {
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots/") {
        mavenContent { snapshotsOnly() }
        content { includeGroup("org.streamrune") }
    }
}

dependencies {
    implementation("org.streamrune:streamrune-core:1.0.0-alpha-SNAPSHOT")
    implementation("org.streamrune:streamrune-runtime:1.0.0-alpha-SNAPSHOT")
    implementation("org.streamrune:streamrune-postgres:1.0.0-alpha-SNAPSHOT")
}
```

Maven:

```xml
<repositories>
  <repository>
    <id>central-snapshots</id>
    <url>https://central.sonatype.com/repository/maven-snapshots/</url>
    <releases><enabled>false</enabled></releases>
    <snapshots><enabled>true</enabled></snapshots>
  </repository>
</repositories>

<dependencies>
  <dependency>
    <groupId>org.streamrune</groupId>
    <artifactId>streamrune-core</artifactId>
    <version>1.0.0-alpha-SNAPSHOT</version>
  </dependency>
</dependencies>
```

The other modules in the [Modules](#modules) table use the same group and version.

## Requirements

- **Java 25**
- **PostgreSQL 17 or newer**, when you use `streamrune-postgres` (the in-memory `streamrune-test`
  store needs no database). Every PostgreSQL-backed suite of `postgresTest` runs on 17 and on 18 in
  CI (the broker end-to-end suite on 18 only), nothing older is tested, and the runtime refuses an
  older server at startup — see
  [Production Deployment](docs/guide/production.md#postgresql-setup).

## Quick Start

[Step-by-step guide →](docs/QUICKSTART.md)

Looking for a full working example? The
[StreamRune e-commerce demo](https://github.com/StreamRune/streamrune-ecommerce-demo) — a separate
repository, licensed under the Apache License 2.0 — wires all three frameworks (Spring Boot,
Quarkus, Micronaut) end to end.

## Modules

| Module | Description |
|---|---|
| [streamrune-core](streamrune-core/README.md) | Core API: EventStore contracts, aggregate types, upcasting |
| [streamrune-runtime](streamrune-runtime/README.md) | Runtime: command bus, projections, locker |
| [streamrune-crypto](streamrune-crypto/README.md) | Encryption-at-rest for event payloads |
| [streamrune-test](streamrune-test/README.md) | Test support: in-memory store, fixtures |
| [streamrune-postgres](streamrune-eventstore/streamrune-postgres/README.md) | PostgreSQL-backed EventStore |
| [streamrune-spring](streamrune-integration/streamrune-spring/README.md) | Spring Boot auto-configuration |
| [streamrune-quarkus](streamrune-integration/streamrune-quarkus/README.md) | Quarkus integration |
| [streamrune-micronaut](streamrune-integration/streamrune-micronaut/README.md) | Micronaut integration |
| [streamrune-spring-boot-starter](streamrune-integration/streamrune-spring-boot-starter/README.md) | Single-dependency setup for Spring Boot: auto-configuration, PostgreSQL store and runtime |
| [streamrune-integration-api](streamrune-integration/streamrune-integration-api/README.md) | Pieces shared by the three integrations: metrics, request identity, startup validators |
| [streamrune-outbox](streamrune-outbox/README.md) | Transactional outbox adapters: Kafka (`streamrune-kafka-outbox`) and RabbitMQ (`streamrune-rabbitmq-outbox`) |

## License

StreamRune is dual-licensed:

- **Business Source License 1.1** — free for organizations with total
  annual gross revenue ≤ USD 5,000,000. Converts to Apache 2.0 four
  years after each version's publication date. See `LICENSE`.
- **Commercial License** — required for organizations above the
  revenue threshold. See `LICENSE-COMMERCIAL.md` or contact
  licensing@streamrune.com.

The e-commerce demo is a separate repository under its own license, the Apache License 2.0. Its
code and the tutorial listings may be copied under that license; it does not change the terms of
StreamRune itself above.

## Contributing

Contributions are welcome under BSL 1.1 via DCO sign-off. See
`CONTRIBUTING.md`.

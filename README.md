# StreamRune

Event sourcing framework for Java.

## Status

`1.0.0` will be the first release of StreamRune. `1.0.0` is not on Maven Central yet: the
coordinates under [Installation](#installation) resolve once it is released, and until then the
preview build is the way to try StreamRune (see [Preview builds](#preview-builds)). Releases are
published to Maven Central, signed, with every module under the `org.streamrune` group at the
release's version; the [releases page](https://github.com/StreamRune/streamrune/releases) and the
[CHANGELOG](CHANGELOG.md) say what each release contains. The unsigned preview of the next release
is published from `main`.

## Installation

A release resolves from a plain `mavenCentral()` repository.

Gradle (Kotlin DSL):

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("org.streamrune:streamrune-core:1.0.0")
    implementation("org.streamrune:streamrune-runtime:1.0.0")
    implementation("org.streamrune:streamrune-postgres:1.0.0")
}
```

Maven:

```xml
<dependencies>
  <dependency>
    <groupId>org.streamrune</groupId>
    <artifactId>streamrune-core</artifactId>
    <version>1.0.0</version>
  </dependency>
</dependencies>
```

The other modules in the [Modules](#modules) table use the same group and version.

## Preview builds

`1.0.0-alpha-SNAPSHOT` is the unsigned preview of the next release, rebuilt from `main` each time
the Build workflow passes on it. It is published only to the Maven Central snapshot repository, not
to Maven Central itself, its artifacts are not signed, and it can change or break at any time. It is
licensed under the Business Source License 1.1, like every StreamRune version (see
[License](#license)).

Each module of the preview is published on its own. A Publish Snapshot run that stops part-way
leaves the modules it did not reach on an earlier commit until a later run completes, so while the
newest [Publish Snapshot run](https://github.com/StreamRune/streamrune/actions/workflows/publish-snapshot.yml)
on `main` is red, the preview may mix modules of different commits.

The preview version moves with every release, in the commit that is released, to one that sorts
above that release, and this section names the current one. Depend on every StreamRune module at the
preview version: Maven and Gradle resolve a module that a build also reaches at a release version
(through a starter, a BOM or a platform) to the higher of the two versions, which is the release
whenever the preview version sorts below it.

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

- **Java 25**. The Graal JIT (GraalVM's default compiler) is not supported on JDK 25: run GraalVM
  with `-XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler` — see
  [Supported JVMs](docs/guide/production.md#supported-jvms).
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

Each row is a published artifact, `org.streamrune:<module>`.

| Module | Description |
|---|---|
| [streamrune-core](streamrune-core/README.md) | Core API: EventStore contracts, aggregate types, upcasting |
| [streamrune-runtime](streamrune-runtime/README.md) | Runtime: command bus, projections, locker |
| [streamrune-postgres](streamrune-eventstore/streamrune-postgres/README.md) | PostgreSQL-backed EventStore |
| [streamrune-test](streamrune-test/README.md) | Test support: in-memory store, fixtures |
| [streamrune-spring](streamrune-integration/streamrune-spring/README.md) | Spring Boot auto-configuration |
| [streamrune-spring-boot-starter](streamrune-integration/streamrune-spring-boot-starter/README.md) | Single-dependency setup for Spring Boot: auto-configuration, PostgreSQL store and runtime |
| [streamrune-quarkus](streamrune-integration/streamrune-quarkus/README.md) | Quarkus integration |
| [streamrune-micronaut](streamrune-integration/streamrune-micronaut/README.md) | Micronaut integration |
| [streamrune-integration-api](streamrune-integration/streamrune-integration-api/README.md) | Pieces shared by the three integrations: metrics, request identity, startup validators |
| [streamrune-crypto-api](streamrune-crypto/streamrune-crypto-api/README.md) | Encryption at rest for event payloads: the crypto-shredding Jackson module and the caching engine |
| [streamrune-filesystem-crypto](streamrune-crypto/streamrune-filesystem-crypto/README.md) | Crypto engine with keys in a directory |
| [streamrune-postgres-crypto](streamrune-crypto/streamrune-postgres-crypto/README.md) | Crypto engine with keys in PostgreSQL |
| [streamrune-vault-crypto](streamrune-crypto/streamrune-vault-crypto/README.md) | Crypto engine on the HashiCorp Vault transit engine |
| [streamrune-aws-kms-crypto](streamrune-crypto/streamrune-aws-kms-crypto/README.md) | Crypto engine on an AWS KMS key |
| [streamrune-kafka-outbox](streamrune-outbox/README.md) | Transactional outbox publisher for Kafka |
| [streamrune-rabbitmq-outbox](streamrune-outbox/README.md) | Transactional outbox publisher for RabbitMQ |

The crypto modules have a shared [overview](streamrune-crypto/README.md).

## API stability

From `1.0.0` on, StreamRune follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html):
a minor or patch release does not break code written against the public API, and a breaking
change waits for the next major version. The public API is:

- the public types and members of the published modules, except those marked `@Internal`;
- the configuration keys and their units, the metric names, the health component names and the
  database schema the migrations create.

A type or member annotated `@Internal` (from `streamrune-core`) is public only because another
StreamRune module uses it: the runtime, the PostgreSQL store or one of the integrations. It is not
covered by this policy and may change or disappear in any release. The generated API documentation
leaves it out. Application code does not call, implement or bind it.

The build records the public types and members of each published module in a checked-in file
(`api/<module>.api`), and `check` fails when the compiled module differs from it or when a public
signature names an `@Internal` type, so every change to them is deliberate and visible in review.
The files also list the integrations' `@Bean`, producer and factory methods and the constructors of
their properties types, which are wiring, not API (see below): a change there is reviewed, and does
not wait for a major version.

The `@Bean`, producer and factory methods of the Spring, Quarkus and Micronaut integrations are
wiring that the container calls, not API: an application replaces a bean with one of its own, and
does not call these methods. Their properties types (Spring's `StreamRuneProperties`,
`StreamRuneMicronautProperties` with its `withDefaults()`, Quarkus' `StreamRuneQuarkusProperties`)
are read-only for applications: the configuration keys are API, the types' constructors are not.

Interfaces that applications implement (an `EventStore`, a `CommandBus` decorator, a
`CommandInterceptor`, a `SagaStore` and the other ports) may gain new `default` methods in a minor
release. A class that implements such an interface keeps compiling; a decorator forwards a new
method once it wants the delegate's answer instead of the default.

## License

StreamRune is dual-licensed:

- **Business Source License 1.1** — free for an organization whose total
  annual gross revenue, combined with that of its affiliates, does not
  exceed USD 5,000,000. Converts to Apache 2.0 four years after each
  version's publication date. See `LICENSE`.
- **Commercial License** — required for organizations above the
  revenue threshold. See `LICENSE-COMMERCIAL.md` or contact
  licensing@streamrune.com.

The e-commerce demo is a separate repository under its own license, the Apache License 2.0. Its
code and the tutorial listings may be copied under that license; it does not change the terms of
StreamRune itself above.

## Contributing

Bug reports, reproducers, questions and feature ideas are welcome: please open an issue. Code
contributions (pull requests with code) are not accepted yet. StreamRune is dual-licensed, and
they open once a Contributor License Agreement is in place. See `CONTRIBUTING.md`.

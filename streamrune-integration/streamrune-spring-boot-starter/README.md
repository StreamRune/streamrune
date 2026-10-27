# StreamRune Spring Boot Starter

Single-dependency setup for Spring Boot applications. Bundles auto-configuration, the PostgreSQL event store, and the runtime so you only need one `implementation` line.

## Bundled Modules

| Module | Purpose |
|---|---|
| `streamrune-spring` | Spring Boot auto-configuration and SSE support |
| `streamrune-postgres` | PostgreSQL `EventStore` implementation |
| `streamrune-runtime` | Command bus, aggregate lifecycle, subscriptions |

## Usage

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-spring-boot-starter:1.0.0")
```

```groovy
// build.gradle
implementation 'org.streamrune:streamrune-spring-boot-starter:1.0.0'
```

> A release resolves from `mavenCentral()`. The unsigned preview of the next release,
> `1.0.0-alpha-SNAPSHOT`, needs the Maven Central snapshot repository: see
> [Preview builds](../../README.md#preview-builds). The [CHANGELOG](../../CHANGELOG.md) says what
> each release contains.

## Requirements

- Java 25+
- Spring Boot 4
- PostgreSQL 17 or newer (tested on 17 and 18; an older server is refused at startup)

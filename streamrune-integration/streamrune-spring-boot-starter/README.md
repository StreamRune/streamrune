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
implementation("org.streamrune:streamrune-spring-boot-starter:1.0.0-alpha-SNAPSHOT")
```

```groovy
// build.gradle
implementation 'org.streamrune:streamrune-spring-boot-starter:1.0.0-alpha-SNAPSHOT'
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../../README.md#preview-builds). See
> the [CHANGELOG](../../CHANGELOG.md) for what the preview contains.

## Requirements

- Java 25+
- Spring Boot 4
- PostgreSQL 17 or newer (tested on 17 and 18; an older server is refused at startup)

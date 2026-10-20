# AGENTS.md

Instructions for coding agents, and a reference for anyone who works on the StreamRune framework.
Licensing, the Developer Certificate of Origin and the contribution policy are in
`CONTRIBUTING.md`.

## 1. The repository

StreamRune is a CQRS and event-sourcing framework for Java 25 with PostgreSQL as its event store
and integrations for Spring Boot, Quarkus and Micronaut. Applications wire it with builders and
plain constructors, and the integrations use the same builders. It is one Gradle build: the root
`build.gradle.kts` configures every module (there are no convention plugins), versions live in
`gradle/libs.versions.toml` and the module list in `settings.gradle.kts`.

| Module | What it holds | Its tests need Docker |
|---|---|---|
| `streamrune-core` | The API: event store contracts, aggregate types, envelopes, upcasting | no |
| `streamrune-runtime` | Command bus, projections and their runners, sagas, the outbox poller, dead-letter retry, subscriptions | no |
| `streamrune-test` | The test kit: in-memory stores and buses, fixtures, contract suites | no |
| `streamrune-eventstore/streamrune-postgres` | The PostgreSQL event store and stores, the schema migrations | yes |
| `streamrune-crypto/streamrune-crypto-api` | Encryption at rest for event payloads: the crypto-shredding Jackson module and the caching engine | no |
| `streamrune-crypto/streamrune-filesystem-crypto` | The crypto engine with keys in a directory | no |
| `streamrune-crypto/streamrune-postgres-crypto`, `streamrune-vault-crypto`, `streamrune-aws-kms-crypto` | Crypto engines with keys in PostgreSQL, HashiCorp Vault and AWS KMS | yes |
| `streamrune-integration/streamrune-integration-api` | What the three integrations share: metrics, request identity, startup validators | no |
| `streamrune-integration/streamrune-spring`, `streamrune-quarkus`, `streamrune-micronaut` | The three integrations | yes |
| `streamrune-integration/streamrune-spring-boot-starter` | The Spring Boot starter: dependencies only, no sources | — |
| `streamrune-integration/streamrune-e2e` | End-to-end crash-recovery and broker suites, not published | yes |
| `streamrune-integration/streamrune-integration-tck` | What the three integrations' tests share: the configuration oracle and the doc reading of their doc-lint tests; not published | no |
| `streamrune-outbox/streamrune-kafka-outbox`, `streamrune-rabbitmq-outbox` | Transactional outbox publishers for Kafka and RabbitMQ | yes |

Where each topic is written down:

- `README.md`: the overview and the module table. `docs/QUICKSTART.md`: a first application.
- `docs/guide/`: `concepts.md`, `getting-started.md`, `testing.md` (the test kit), `production.md`
  (PostgreSQL setup, health checks, the metrics table, duration properties, operations runbooks),
  `configuration.md` (every configuration property and its default) and `advanced/`, one guide per
  feature (outbox, sagas, GDPR erasure, idempotency and more).
- Each module's `README.md`: its API and, for an integration, its configuration properties.
- `docs/adr/`: architecture decisions. `docs/reference/api.md`: the API reference.
  `CHANGELOG.md`: what each version changes.

A change to behaviour updates the guide and the README that describe it in the same pull request.

## 2. Build and test

Gradle provisions the Java 25 toolchain; running Gradle itself needs JDK 17 or newer.

| Command | What it runs | Typical wall time |
|---|---|---|
| `./gradlew quickCheck` | Compiles every source set of every module, runs `spotlessCheck`, the tests of the seven modules whose tests need no Docker and every doc-lint test. No JaCoCo, no Javadoc, no Testcontainers. | about 3 minutes when the runtime tests run, under a minute when they are up to date |
| `./gradlew build` | What the `build` check runs in CI: every test suite, Javadoc, the coverage gates, `spotlessCheck` and the build's own verification tasks. Needs Docker. | about 7 minutes; `streamrune-postgres`'s suite is the longest |
| `./gradlew :streamrune-core:test` | One module's tests | |
| `./gradlew :streamrune-core:test --tests "DeadLetterRetryPolicyTest"` | One test class; `--tests` also takes a pattern such as `"*RetryPolicy*"` | |
| `./gradlew :streamrune-integration:streamrune-spring:test` | A nested module's tests | |
| `./gradlew postgresTest -PpostgresImage=postgres:17-alpine` | The PostgreSQL-backed suites on another server version, as the `postgres-17` check runs them | |
| `./gradlew :streamrune-integration:streamrune-e2e:brokerTest` | The Kafka and RabbitMQ end-to-end suite, which is not part of `check` | |
| `./gradlew docLintTest` | The doc-lint tests of every module (section 5); `check` and `quickCheck` run them too | seconds |
| `./gradlew spotlessApply` | Formats the Java sources | |

- **`quickCheck` is the inner loop, `build` is the gate.** A green `quickCheck` says nothing about
  the Testcontainers suites, the coverage gates or Javadoc: run `./gradlew build` before every pull
  request. Every task `quickCheck` runs is one `build` runs as well, and a `build` after a green
  `quickCheck` reuses its test results.
- **Docker** is needed by the modules marked "yes" above: their tests start PostgreSQL, Kafka,
  RabbitMQ, Vault or LocalStack through Testcontainers. Every PostgreSQL container takes its image
  from `PostgresTestImage.NAME` (`gradle/test-support/`), and `verifyPostgresTestImage`, part of
  `check`, fails the build on a test that names an image of its own.
- **Coverage gate.** `check` fails a module below 0.80 instruction or 0.80 branch coverage
  (`streamrune-spring-boot-starter` is exempt; `streamrune-e2e` has no main sources). A build that
  fails after all its tests passed has usually failed this gate; the module's report is
  `build/reports/jacoco/test/html/index.html`.
- **Test summary.** Every test task writes `<module>/build/test-summary.txt` (for a test task not
  named `test`: `test-summary-<task>.txt`): the totals, one line per failure (the test, the first
  line of its exception, joined with the lines after it when it ends in a colon as an AssertJ
  message does — each line cut to 120 characters, and a message longer than 600 cut in its middle,
  so the verdict at its end stays — and the first stack frame in `org.streamrune`) and the ten slowest tests. A failure
  outside any test, such as a test JVM that stops, is listed under the run's name and counted on its
  own. Every test task, `check`, `build` and `quickCheck` ends with the line
  `N tests, F failed — see build/reports/test-summary.txt`, where F counts both kinds of failure;
  that file collects the summaries of the run's test tasks. A test task that fails for a reason of
  its own prints the line too: with no test matching `--tests` it reads `0 tests, 0 failed`, and
  Gradle's failure block below it says "No tests found for given includes". Each entry carries the
  time it was written: a build that stops before a test task runs (a compile error, or an earlier failure
  without `--continue`) leaves that task's entry from an earlier run. Read the summary before the
  HTML reports under `build/reports/tests/`.
- **Mutation testing.** `./gradlew :streamrune-core:pitest` and `./gradlew :streamrune-runtime:pitest`
  run PIT, which changes the compiled code one small mutation at a time and lists every mutant no
  test detects; the task prints the path of its report (`build/reports/pitest/index.html`). It is
  not part of `check`. `-PpitestTargets=` narrows the mutated classes and `-PpitestTests=` the tests
  run against them, both comma-separated globs that default to `org.streamrune.*`:
  `./gradlew :streamrune-core:pitest -PpitestTargets=org.streamrune.core.DeadLetterRetryPolicy`.
  PIT first runs every selected test once to see what it covers: about three minutes over all of
  `streamrune-runtime`'s tests, seconds with `-PpitestTests` narrowed to the class's own test. A
  narrowed run can report a mutant as surviving that a test outside the selection kills, so confirm
  a survivor without `-PpitestTests` before writing a test for it.
- **Exit codes.** Never pipe Gradle through `tail`, `grep` or `head` when its result matters: the
  pipeline reports the status of its last command, not Gradle's. Redirect and print the status
  instead, `./gradlew build > build.log 2>&1; echo "EXIT=$?"`, and start a full build in the
  background.
- **Reused results.** The build cache and the configuration cache are on, so an unchanged test task
  reports `UP-TO-DATE` or `FROM-CACHE` without running. `--rerun` runs one task again:
  `./gradlew :streamrune-core:test --rerun`.

## 3. Conventions

- **Explicit code, no hidden behaviour.** Every component exists because a builder or a constructor
  created it, and the three integrations create theirs with the same builders. An annotation is
  configuration that a named component reads: `@RequireRole` and `@RequirePermission`
  for the authorization check, `@Encrypted` for the crypto-shredding module, `@Auditable` and
  `@Cacheable` for the auditing and caching query buses, `@ProjectionConfig` for an integration's
  projection registration. A new annotation comes with the component that reads it.
- **Comments and javadoc state what the code does now, and why,** in the present tense (section 5).
- **No `--enable-preview`.** Published class files must load on any Java 25 runtime, so the
  framework uses finalized language features only.
- **Commits** follow Conventional Commits with a scope, for example `fix(outbox): …`; the types and
  a template are in `.gitmessage` and `CONTRIBUTING.md`. Sign off every commit with
  `git commit --signoff`: the `dco` check verifies that the trailer names the commit's author.
- **Pull requests** go to `main` and are squash-merged. The required checks are `build`,
  `postgres-17` and `dco`.
- **Formatting** is google-java-format through Spotless. `check` runs `spotlessCheck`, so `build`
  and the `build` check in CI fail on a formatting violation; the pre-commit hook
  (`gradle/hooks/pre-commit`, installed by `./gradlew installGitHooks` and by every `build`) runs
  the same check before each commit, and `./gradlew spotlessApply` fixes what it reports.
- **Build scripts and the configuration cache.** Code that runs at execution time (`doLast`,
  `onlyIf`, a test listener) captures plain values computed at configuration time, never `project`
  or a top-level declaration of the script; the root `build.gradle.kts` keeps such helpers in nested
  `object`s.

## 4. Testing conventions

- **A failing test first.** Every behaviour change starts with a test that fails without it.
- **One mutant per fix.** Prove that the test guards the change: remove the change, run the covering
  tests, see them fail, restore it. `pitest` does the same for every mutation of a class.
- **The kit instead of hand-written doubles.** `streamrune-test` (package `org.streamrune.test`)
  provides the following; `docs/guide/testing.md` shows the most used ones in examples:
  - in-memory stores and buses: `InMemoryEventStore`, `InMemoryEventStoreFactory`,
    `InMemoryCommandBus`, `InMemoryQueryBus`, `InMemoryEventSubscription`, `InMemoryOffsetStore`,
    `InMemoryOutboxStore`, `InMemoryDeadLetterQueue`, `InMemoryProjectionDeadLetterStore`,
    `InMemoryProjectionRepository`, `InMemorySagaStore`, `InMemorySagaDeadLetterStore`,
    `InMemoryCommandInbox`, `InMemoryCryptoEngine`;
  - fixtures: `DeciderFixture`, `EventStoreFixture`, `ProjectionFixture`;
  - `MutableClock`: tests advance time instead of sleeping;
  - `EventPayloadAssertions` for payload round trips and `SagaEnvelopes` for the envelopes of a
    saga's replies;
  - `ForwardingOutboxStore`: override one method to make that outbox-store call fail.
- **Contract suites.** A new implementation of a store interface extends the matching abstract
  suite in its test sources (`OutboxStoreContract`, `SagaStoreContract`,
  `SagaDeadLetterStoreContract`, `SagaForwardFenceContract`, `AtomicBatchProcessorContract`,
  `CommandBusProvenanceContract`) and so passes the tests every implementation passes.
- **Durable multi-step flows.** A flow that writes in more than one step (an append and an offset, a
  claim and a publish, a saga step and its command) lists its crash points, one after each write,
  and has a test per crash point that stops the flow there, runs it again and asserts that it
  converges.
- **PostgreSQL containers** start as `new PostgreSQLContainer<>(PostgresTestImage.NAME)`, and a
  module whose tests start one is listed in `postgresBackedModules` in the root build script.

## 5. Public text

Everything in this repository is published: code, comments, javadoc, test names, log messages,
build files, guides, commit messages and pull request descriptions. All of it follows three rules:

- Present tense. Say what the code does and why, not how it came to be.
- No labels from issue trackers, reviews or private notes, and no reference to material a reader
  cannot open.
- No space before a period.

`InternalTrackerIdRatchetTest` in `streamrune-core` reads every `.java`, `.md` and build file in the
repository and fails on tracker and review labels (review-finding labels included, in test sources
too), on references to private review material and on history wording: a reviewer named by role, a
numbered roadmap stage, work deferred to the roadmap, and outside test sources and code samples the
state before or after a fix and the words that narrate an earlier behaviour (the test's javadoc
lists them). It also fails on a space before a period in prose (javadoc, and markdown outside fenced
blocks), and it reads a phrase on across a line break, so a reflowed comment does not hide one.
Write a state as it is: "cannot", "is not", "has lost it", and "the clock that computes" for a
purpose. Its failure message lists every offending `file:line` with the rule it breaks. The other
repository-wide ratchets:

- `LogSinkPolicyRatchetTest` (`streamrune-core`): an identifier reaches a log record or a thread name
  only through the sanitizer.
- `LoggingFacadeRatchetTest` (`streamrune-core`): logging goes through SLF4J, never `System.Logger`
  or `java.util.logging`.
- `SagaProvenanceWriterRatchetTest` (`streamrune-core`): only the listed files bind a dispatch cause
  or create a saga origin.
- `SagaStateWriteRatchetTest` (`streamrune-postgres`): every statement that writes `saga_state` is
  classified, and a write that can end a saga's forward phase takes the saga's fence lock.

Prose that mirrors code changes in the same commit as the code; the doc-lint tests (`*DocLintTest`,
run by `./gradlew docLintTest`, `check` and `quickCheck`) fail when it does not:

- a meter constant in `MetricNames` and the `### Metrics` table of `docs/guide/production.md`
  (`MetricNamesDocLintTest`, which also checks the integration-api README's meter table and every
  meter name in the guides);
- a relay component name in `RelayComponentNames`, or an integration's health check name, and the
  two health tables of `docs/guide/production.md` (`RelayComponentNamesDocLintTest`);
- a public type of the test kit and `docs/guide/testing.md` and `streamrune-test/README.md`
  (`TestKitDocLintTest`);
- a configuration property or its default and `docs/guide/configuration.md`, the configuration
  metadata of Spring and Micronaut (`additional-spring-configuration-metadata.json`,
  `configuration-metadata.json`) and the Quarkus README's listing, and every guide table with a
  `Default` column (`ConfigurationDocLintTest` in each integration). Its oracle is what the
  integration binds and reads: the properties types as the framework binds them, every bean
  condition and injected property, the keys looked up in code. A default is written the way the
  configuration reference describes (`7d`, `1m`, `—` for none);
- a change a user notices and `CHANGELOG.md`.

## 6. Pitfalls

- **`git stash` is shared** by every worktree of a clone, so another session can pop an entry. In a
  worktree, set work aside with a commit on your branch.
- **Quarkus client proxies.** A normal-scoped or intercepted `BaseProjection` subclass needs
  `quarkus.arc.transform-unproxyable-classes=true`, the Quarkus default, because
  `BaseProjection.process(List, ProjectionRepository)` is final.
- **Micronaut collection injection.** A singleton registered at runtime
  (`ApplicationContext.registerSingleton`) is not reliably collected into an injected `List`; a
  test that needs a bean in such a list declares it as a compile-time bean in a `@Factory`.
- **`LogSinkPolicyRatchetTest` fails outside Gradle by design.** One of its tests reads the
  `-Dstreamrune.logsink.declared-sources` property that the `test` task supplies. Run it with
  `./gradlew :streamrune-core:test`, not from an IDE.
- **Broker suites.** Kafka and RabbitMQ containers boot slowly under parallel container start, so the
  `@Tag("broker")` tests run in `brokerTest` (nightly and on release tags), not in `check`.
- **Testcontainers without Docker.** No test skips itself when Docker is missing: without a running
  Docker daemon the container suites fail, most of them in class setup, which the summary lists as
  `initializationError`.
- **The e-commerce demo** (a separate repository) builds against a sibling `../streamrune` checkout
  when one exists. A change to the public API is checked against the demo by building the demo with
  this checkout as that sibling.

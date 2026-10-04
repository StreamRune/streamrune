description =
    "StreamRune end-to-end crash-recovery / HA tests: real Postgres (Testcontainers) proving " +
        "runner- and relay-level exactly-once invariants across simulated crashes."

// Test-only module: no main sources. It exercises the already-published runtime + postgres
// event-store modules end to end. Everything is declared as testImplementation/testRuntimeOnly.
dependencies {
    testImplementation(project(":streamrune-runtime"))
    testImplementation(project(":streamrune-eventstore:streamrune-postgres"))
    testImplementation(project(":streamrune-test"))

    // Broker outbox adapters + their broker clients, exercised by the @Tag("broker") OutboxBrokerE2EIT
    // through the real OutboxPoller relay. Only used by the broker-tagged tests (excluded from the
    // default `test` task), but declared unconditionally so the module always compiles.
    testImplementation(project(":streamrune-outbox:streamrune-kafka-outbox"))
    testImplementation(project(":streamrune-outbox:streamrune-rabbitmq-outbox"))
    testImplementation(libs.kafka.clients)
    testImplementation(libs.rabbitmq.client)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.rabbitmq)

    testImplementation(libs.postgresql)
    testImplementation(libs.bundles.testcontainers.pg)
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.jackson.datatype.jsr310)

    // junit-bom + junit-jupiter (testImplementation) and junit-platform-launcher
    // (testRuntimeOnly) are contributed to every subproject by the root build's
    // `subprojects` block, so they are not repeated here.
}

// This module has NO main sources, so JaCoCo measures zero instructions/branches. JaCoCo skips a
// COVEREDRATIO rule whose denominator is zero (it would pass vacuously), but the root build wires
// `jacocoTestCoverageVerification` into `check` for every non-exempt module. Disable verification
// here explicitly so a build of a sources-free, test-only module can never fail on the coverage
// gate — there is no production code in this module to cover.
tasks.withType<JacocoCoverageVerification>().configureEach {
    enabled = false
}

// Crash-recovery scenarios use awaitility with multi-second bounds and start a Postgres container;
// give them more headroom than the default per-test timeout would allow under a cold container.
tasks.withType<Test>().configureEach {
    // Run the IT classes (named *IT) which JUnit Platform discovers like any other test class.
    // No special include filter is needed — there are no unit tests in this module.
    systemProperty("junit.jupiter.execution.timeout.default", "5m")
}

// ---------------------------------------------------------------------------------------------
// Broker-test isolation.
//
// OutboxBrokerE2EIT boots Kafka and RabbitMQ containers, which are materially slower and flakier
// than the Postgres container the rest of the suite shares — under parallel container startup they
// intermittently time out. They carry JUnit @Tag("broker"). The default `test` task EXCLUDES that
// tag, so `:streamrune-integration:streamrune-e2e:test` stays Postgres-only and reliable (and it is
// the task wired into `check`). The broker tests run via the `brokerTest` task below, which runs
// ONLY @Tag("broker") and is deliberately NOT wired into `check`.
//
// "not in `check`" must not mean "never runs". `brokerTest` holds the ONLY end-to-end
// strict per-aggregate ordering assertion in the repo (real OutboxPoller + real publisher + real
// broker); every OutboxPollerTest uses a fake publisher and every outbox-module broker integration
// test bypasses the poller. It is therefore driven by .github/workflows/broker-e2e-test.yml on a
// nightly cron, on workflow_dispatch, and on every `v*` tag push. The tag push
// only starts that workflow — the gate that actually makes a release cut unable to ship without
// it having run (and passed) is verifyReleaseChecks in the root build.gradle.kts, which queries
// the GitHub Checks API for this workflow's check-run (job name "broker-e2e") on the exact tagged
// commit before the Central upload task is allowed to run.
// ---------------------------------------------------------------------------------------------
tasks.named<Test>("test") {
    useJUnitPlatform {
        excludeTags("broker")
    }
}

val brokerTest by tasks.registering(Test::class) {
    description = "Runs the @Tag(\"broker\") outbox→broker E2E tests (Kafka + RabbitMQ). " +
        "Not wired into `check` — these brokers are flaky to boot under parallel container " +
        "startup — but IS run by .github/workflows/broker-e2e-test.yml (nightly, dispatch, v* tag)."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform {
        includeTags("broker")
    }
    systemProperty("junit.jupiter.execution.timeout.default", "5m")
    // Always re-run on request: there is nothing to cache about a once-off broker smoke test.
    outputs.upToDateWhen { false }
    // The root build finalizes every Test task with `jacocoTestReport`, which `dependsOn("test")` —
    // that would drag the default Postgres-only `test` run along whenever `brokerTest` is invoked.
    // This module has no main sources to cover, so a coverage report is meaningless here; clear the
    // finalizer so `brokerTest` runs ONLY the broker-tagged tests, standalone.
    setFinalizedBy(emptyList<Any>())
}

plugins {
    alias(libs.plugins.jmh)
}

description = "StreamRune Postgres event store: JDBC-backed event store, projection repository, LISTEN/NOTIFY subscription."

dependencies {
    api(project(":streamrune-core"))
    // PostgresNotificationSubscription implements ReadPoisonAware and HybridEventSubscription
    // implements ReadPoisonAware/PushHealthAware — both public org.streamrune.runtime SPI types —
    // so this must be api(...), not compileOnly(...): a `compileOnly` project dependency is absent
    // from the published POM ENTIRELY (worse than `implementation`, which at least publishes at
    // Maven `runtime` scope), so a consumer referencing either public type directly (both have
    // public constructors / a builder()) fails to COMPILE without streamrune-runtime resolvable on
    // ITS OWN compile classpath. No dependency cycle: streamrune-runtime depends only on
    // streamrune-core, streamrune-crypto-api and external libraries — never back on this module.
    // Enforced by the verifyModularApiScopes check in the root build (extended to also see
    // compileOnly project deps, so a leak through one is caught too).
    api(project(":streamrune-runtime"))
    // A PUBLIC method takes a crypto-api type directly —
    // PostgresEventStoreFactory.cryptoEngine(CryptoEngine) — so this must be api(...), not
    // implementation(...): an `implementation` dependency publishes at Maven `runtime` scope, and
    // a consumer calling that builder method needs CryptoEngine resolvable on ITS OWN compile
    // classpath to even write the call. Enforced by the verifyModularApiScopes check in the root
    // build.
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.postgresql)
    // api, not implementation, by decision: HikariCP is the pool this module builds (the LISTEN
    // pool, the HikariCP convenience of PgAdvisoryLocker.withDedicatedPool), and one public method
    // takes a HikariCP type — withDedicatedPool(..., MetricsTrackerFactory), kept so an application
    // gets the lock pool's metrics in one call — so a consumer calling it needs HikariCP resolvable
    // on ITS OWN compile classpath, which an `implementation` dependency (Maven `runtime` scope)
    // does not give it. No other public signature names a HikariCP type: withDedicatedPool(DataSource)
    // takes a pool of any library and createListenDataSource returns ListenDataSource.
    api(libs.hikaricp)
    // GraalVM native-image compatibility: HikariConfig.setMetricRegistry / setHealthCheckRegistry
    // do `instanceof` checks against optional Dropwizard-metrics (codahale) and Micrometer types.
    // GraalVM's build-time analysis requires those classes to be resolvable even though HikariCP
    // never instantiates them here (the framework sets no metric registry; a MetricsTrackerFactory
    // is only the one an application hands to withDedicatedPool), so they are dead-code-
    // eliminated from the native binary. Without them on the classpath, Quarkus/Micronaut native
    // builds fail with ClassNotFound (Spring native works only because Spring Boot ships its own
    // HikariCP native hints). runtimeOnly keeps them off consumers' compile classpath + inert on JVM.
    runtimeOnly(libs.dropwizard.metrics.core)
    runtimeOnly(libs.dropwizard.metrics.healthchecks)
    runtimeOnly(libs.dropwizard.metrics5.core)
    runtimeOnly(libs.dropwizard.metrics5.healthchecks)
    runtimeOnly(libs.micrometer.core)
    implementation(libs.slf4j.api)
    api(libs.flyway.core)
    runtimeOnly(libs.flyway.postgresql)
    // Streamrune-runtime is now api(...) above, which already reaches the test classpath
    // (testImplementation extends implementation extends api) — the separate testImplementation(...)
    // declaration this module carried is redundant now and was removed.
    testImplementation(project(":streamrune-test"))
    // Prove initializeSchema() auto-provisions the crypto schema (db/crypto-migration)
    // when the configured CryptoEngine needs it, so durable GDPR erasure works out of the box. Uses
    // a real PostgresCryptoEngine + its crypto-migration resources.
    testImplementation(project(":streamrune-crypto:streamrune-postgres-crypto"))
    testImplementation(libs.bundles.testcontainers.pg)
    testImplementation(libs.assertj.core)
    testImplementation(libs.mockito.core)
    // PostgresBackendTest checks that the foreign-store refusal names a SQLite store's backend
    // ('sqlite'), with a real SqliteEventAuditStore. Test scope only: streamrune-sqlite's own tests
    // depend on this module the same way, and neither main source set sees the other.
    testImplementation(project(":streamrune-sqlite"))
    // Crash tests drive a ContinuousProjectionRunner on a virtual thread and wait for its checkpoint.
    testImplementation(libs.awaitility)
    // Log-assertion tests pin operator-facing log lines, whose only observable effect IS the line,
    // so they need a real slf4j binding whose events a test can capture (logback ListAppender).
    // Test scope only — no main artifact gains a logging backend. Quiet defaults in
    // src/test/resources/logback-test.xml.
    testImplementation(libs.logback.classic)

    // src/jmh: PostgresEventStoreBenchmark and its smoke test (src/jmh/README.md). The JMH source
    // set sees the test classpath (the plugin's includeTests), so it starts its PostgreSQL through
    // the same Testcontainers dependency and PostgresTestImage as the tests.
}

// ── The saga_state write ratchet, in a test task of its own ─────────────────
//
// SagaStateWriteRatchetTest reads every module's main Java sources and main SQL resources at
// runtime, but Gradle knows only what a task declares. A saga_state write planted in
// streamrune-sqlite (which this module does not depend on) changed none of :streamrune-postgres:test's
// declared inputs, so with `org.gradle.caching=true` the task came FROM-CACHE and the ratchet never
// ran. The scanned tree is therefore declared as the input of the task that runs the ratchet, and a
// manifest of the declared files is handed to the test, which fails when what it scans and what
// Gradle declared differ in either direction (the same fix as streamrune-core's log-sink ratchet).
//
// The ratchet runs in `sagaStateWriteRatchetTest`, not in `test`: declared on `test`, the tree made a
// main-source edit anywhere in the repository re-run this module's whole Testcontainers suite. The
// task needs no Docker; `check` (and so `build` and CI) and the root `quickCheck` run it.
val sagaStateWriteSources =
    fileTree(rootDir) {
        // Mirrors SagaStateWriteRatchetTest#mainJavaSources and #sqlResources: every .java under a
        // src/main/java and every .sql under a src/main, at any depth, outside the directories its
        // walk prunes.
        include("**/src/main/java/**/*.java", "**/src/main/**/*.sql")
        exclude("**/build/**", "**/out/**", "**/.git/**", "**/.gradle/**", "**/.idea/**")
        exclude("**/node_modules/**")
    }

val sagaStateWriteSourceManifest =
    tasks.register("sagaStateWriteSourceManifest") {
        group = "verification"
        description =
            "Writes the source list declared as an input of the saga_state write ratchet, so the " +
                "test can prove the declaration covers what it scans."
        val sources = sagaStateWriteSources
        val repoRoot = rootDir
        val manifest = layout.buildDirectory.file("saga-state-writes/declared-sources.txt")
        inputs
            .files(sources)
            .withPropertyName("sagaStateWriteSources")
            .withPathSensitivity(PathSensitivity.RELATIVE)
        outputs.file(manifest)
        doLast {
            val file = manifest.get().asFile
            file.parentFile.mkdirs()
            file.writeText(
                sources.files
                    .map { it.relativeTo(repoRoot).invariantSeparatorsPath }
                    .sorted()
                    .joinToString("\n"),
            )
        }
    }

val sagaStateWriteRatchetClass = "org.streamrune.postgres.SagaStateWriteRatchetTest"
val ratchetTestSourceSet = the<SourceSetContainer>()["test"]

val sagaStateWriteRatchetTest =
    tasks.register<Test>("sagaStateWriteRatchetTest") {
        group = "verification"
        description =
            "Runs SagaStateWriteRatchetTest: every saga_state write of every module's main sources " +
                "is classified."
        testClassesDirs = ratchetTestSourceSet.output.classesDirs
        classpath = ratchetTestSourceSet.runtimeClasspath
        // Jupiter only: the ArchUnit engine does not apply the class filter and would run the
        // module's architecture rules here a second time.
        useJUnitPlatform { includeEngines("junit-jupiter") }
        filter { includeTestsMatching(sagaStateWriteRatchetClass) }
        inputs
            .files(sagaStateWriteSources)
            .withPropertyName("sagaStateWriteSources")
            .withPathSensitivity(PathSensitivity.RELATIVE)
        dependsOn(sagaStateWriteSourceManifest)
        val manifest = sagaStateWriteSourceManifest.map { it.outputs.files.singleFile }
        // An argument provider, not `systemProperty`, so the absolute path stays out of the cache key.
        jvmArgumentProviders.add(
            org.gradle.process.CommandLineArgumentProvider {
                listOf("-Dstreamrune.sagastate.declared-sources=${manifest.get().absolutePath}")
            },
        )
        // The module's JaCoCo report depends on `test`; as a finalizer it would drag the whole
        // Testcontainers suite into a standalone ratchet run. The ratchet covers no main code, so
        // only the summary finalizer stays (the docLintTest choice).
        setFinalizedBy(listOf(":testSummary"))
    }

tasks.named<Test>("test") { filter { excludeTestsMatching(sagaStateWriteRatchetClass) } }

// PostgresBackendTest opens a SQLite database: the driver loads its native library and the
// instance guard calls flock(2) through the Foreign Function and Memory API. Enabling native
// access keeps the JVM's restricted-method warnings out of the test output, as streamrune-sqlite
// does for its own tests.
tasks.withType<Test>().configureEach { jvmArgs("--enable-native-access=ALL-UNNAMED") }
tasks.named("check") { dependsOn(sagaStateWriteRatchetTest) }

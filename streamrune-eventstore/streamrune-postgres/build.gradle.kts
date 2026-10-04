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
    // compileOnly project deps, which is what let this leak stand uncaught until now).
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
    implementation(libs.hikaricp)
    // GraalVM native-image compatibility: HikariConfig.setMetricRegistry / setHealthCheckRegistry
    // do `instanceof` checks against optional Dropwizard-metrics (codahale) and Micrometer types.
    // GraalVM's build-time analysis requires those classes to be resolvable even though HikariCP
    // never instantiates them here (no MetricsTrackerFactory is set), so they are dead-code-
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
    // Toxiproxy in front of PostgreSQL to prove a black-holed leadership connection
    // steps the leader down within the socket-timeout bound and frees connectionLock (behaviour,
    // not just configuration).
    testImplementation(libs.testcontainers.toxiproxy)
    testImplementation(libs.assertj.core)
    testImplementation(libs.mockito.core)
    // Crash tests drive a ContinuousProjectionRunner on a virtual thread and wait for its checkpoint.
    testImplementation(libs.awaitility)
    // Log-assertion tests pin operator-facing log lines, whose only observable effect IS the line,
    // so they need a real slf4j binding whose events a test can capture (logback ListAppender).
    // Test scope only — no main artifact gains a logging backend. Quiet defaults in
    // src/test/resources/logback-test.xml.
    testImplementation(libs.logback.classic)
}

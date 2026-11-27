description = "StreamRune SQLite backend: the SqliteDatabase handle with its instance guard, the schema series, and every store on one SQLite file (event store, checkpoints, sagas, outbox, inbox, dead letters, audit, read models), for small single-instance services."

dependencies {
    api(project(":streamrune-core"))
    // api, not implementation: SqliteDatabase is opened on a DataSource whose connections the
    // driver creates, and an application configures the driver's connection properties
    // (journal_mode, synchronous, transaction_mode, ...) through the driver's own types.
    api(libs.sqlite.jdbc)
    // Flyway applies the schema series. Its SQLite support is part of flyway-core, so no
    // flyway-database-* artifact is needed (Postgres needs flyway-database-postgresql). No public
    // signature of this module names a Flyway type, so implementation, not api.
    implementation(libs.flyway.core)
    // Flyway is told to log through SLF4J (see SqliteSchemaMigration), and SqliteDatabase.validate
    // logs warnings through it.
    implementation(libs.slf4j.api)
    // The event store (de)serializes events and snapshots with Jackson, encrypts @Encrypted fields
    // through the crypto module's CryptoShreddingModule and refuses an @Encrypted type without a
    // crypto engine through its EncryptedTypeGuard, and guards its tracing with the runtime's
    // GuardedTracing, as PostgresEventStore does. No public signature of this module names a type
    // of these modules (CryptoEngine and the tracing SPI live in core), so implementation, not api.
    // The saga store's public signatures name Jackson's ObjectMapper, which streamrune-core already
    // exports as api, as for PostgresSagaStore.
    implementation(project(":streamrune-runtime"))
    implementation(project(":streamrune-crypto:streamrune-crypto-api"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)
    testImplementation(project(":streamrune-test"))
    testImplementation(libs.assertj.core)
    // A real pool, so the concurrency tests run appends on connections that are really concurrent.
    testImplementation(libs.hikaricp)
    // SqliteBackendTest checks that the foreign-store refusal names a Postgres store's backend
    // ('postgres'), with a real PostgresEventAuditStore. Test scope only: streamrune-postgres's own
    // tests depend on this module the same way, and neither main source set sees the other.
    testImplementation(project(":streamrune-postgres"))
    // Log-assertion tests pin operator-facing log lines, whose only observable effect IS the line,
    // so they need a real slf4j binding whose events a test can capture (logback ListAppender).
    // Test scope only — no main artifact gains a logging backend. Quiet defaults in
    // src/test/resources/logback-test.xml.
    testImplementation(libs.logback.classic)
}

// The driver loads its native library and the instance guard calls flock(2) through the Foreign
// Function and Memory API; enabling native access keeps the JVM's restricted-method warnings out
// of the test output. The forked JVMs of the guard tests pass the same option.
tasks.withType<Test>().configureEach {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

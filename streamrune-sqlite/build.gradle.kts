description = "StreamRune SQLite backend: the SqliteDatabase handle, its instance guard and schema series, for small single-instance services."

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
    testImplementation(libs.assertj.core)
}

// The driver loads its native library and the instance guard calls flock(2) through the Foreign
// Function and Memory API; enabling native access keeps the JVM's restricted-method warnings out
// of the test output. The forked JVMs of the guard tests pass the same option.
tasks.withType<Test>().configureEach {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

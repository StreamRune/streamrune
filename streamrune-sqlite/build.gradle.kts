description = "StreamRune SQLite backend: the SqliteDatabase handle and its instance guard, for small single-instance services."

dependencies {
    api(project(":streamrune-core"))
    // api, not implementation: SqliteDatabase is opened on a DataSource whose connections the
    // driver creates, and an application configures the driver's connection properties
    // (journal_mode, synchronous, transaction_mode, ...) through the driver's own types.
    api(libs.sqlite.jdbc)
    testImplementation(libs.assertj.core)
}

// The driver loads its native library and the instance guard calls flock(2) through the Foreign
// Function and Memory API; enabling native access keeps the JVM's restricted-method warnings out
// of the test output. The forked JVMs of the guard tests pass the same option.
tasks.withType<Test>().configureEach {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

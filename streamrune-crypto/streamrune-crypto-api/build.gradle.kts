description = "StreamRune crypto: encryption-at-rest primitives for event payloads."

dependencies {
    api(project(":streamrune-core"))
    testImplementation(project(":streamrune-test"))
    // Every non-`java.*` name in this module's module-info.java `requires` list must
    // be `api` (POM `compile` scope), because a consumer's `requires org.streamrune.crypto` makes
    // javac resolve this module's WHOLE requires graph on the COMPILE module path — a plain
    // (non-transitive) `requires` is just as compile-visible to the resolver as a transitive one.
    // At `implementation` (POM `runtime` scope) a modular consumer aborts with "module not found".
    // jackson-core was not declared at all and arrived only transitively through jackson-databind;
    // module-info requires it by name, so it must be an explicit, versioned declaration.
    // Enforced by the `verifyModularApiScopes` check in the root build.
    api(libs.jackson.databind)
    api(libs.jackson.core)
    api(libs.caffeine)
    api(libs.slf4j.api)
    testImplementation(libs.mockito.core)
    // Log-assertion tests capture WARN events with a logback ListAppender; test scope only.
    testImplementation(libs.logback.classic)
    // JdbcSubjectKeyStoreTest and JdbcForgottenSubjectsTest run the shared JDBC helpers on a
    // SQLite file, so this module's own coverage needs no Docker; test scope only.
    testImplementation(libs.sqlite.jdbc)
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

// Byte Buddy (used by Mockito) does not yet officially support Java 25.
// Enable experimental support until an updated Byte Buddy version is released.
tasks.withType<Test> {
    jvmArgs("-Dnet.bytebuddy.experimental=true")
}

// The JDBC helper tests create their SQLite database from the DDL that streamrune-sqlite-crypto
// ships, read from disk. Declared as a test input so an edit to it re-runs them instead of being
// served from the build cache; the path reaches the tests through an argument provider, so the
// absolute path stays out of the cache key.
tasks.named<Test>("test") {
    val sqliteCryptoDdl =
        layout.projectDirectory.file(
            "../streamrune-sqlite-crypto/src/main/resources/db/sqlite-crypto-migration/V001__crypto.sql",
        )
    inputs
        .file(sqliteCryptoDdl)
        .withPropertyName("sqliteCryptoDdl")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    val ddlPath = sqliteCryptoDdl.asFile.absolutePath
    jvmArgumentProviders.add(
        org.gradle.process.CommandLineArgumentProvider {
            listOf("-Dstreamrune.crypto.sqlite-ddl=$ddlPath")
        },
    )
}

// The SQLite driver loads its native library; enabling native access keeps the JVM's
// restricted-method warnings out of the test output, as streamrune-sqlite does for its own tests.
tasks.withType<Test>().configureEach { jvmArgs("--enable-native-access=ALL-UNNAMED") }

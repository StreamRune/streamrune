description = "StreamRune crypto: SQLite-backed per-subject key store, forgotten subjects and in-process forget signal, for small single-instance services."

dependencies {
    api(project(":streamrune-core"))
    // SqliteForgottenSubjectStore implements ForgottenSubjectStore, InProcessCryptoForgetSignal
    // implements CryptoForgetSignal and SqliteCryptoBackend implements CryptoBackend, all
    // crypto-api types, and every public constructor and builder of this module takes a
    // SqliteDatabase of streamrune-sqlite: a consumer that names them must have both on its own
    // compile class path, so api, not implementation (verifyModularApiScopes).
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    api(project(":streamrune-sqlite"))
    implementation(libs.slf4j.api)
    testImplementation(project(":streamrune-test"))
    testImplementation(libs.assertj.core)
    // A real pool: the pool-of-one cases and the contracts on a pool.
    testImplementation(libs.hikaricp)
    testImplementation(libs.jackson.databind)
    testImplementation(libs.jackson.datatype.jsr310)
    // The characterization test pins the engine's WARN lines and their logger category (a
    // logback ListAppender); test scope only. Quiet defaults in src/test/resources/logback-test.xml.
    testImplementation(libs.logback.classic)
}

// streamrune-sqlite's driver loads its native library and its instance guard calls flock(2)
// through the Foreign Function and Memory API; enabling native access keeps the JVM's
// restricted-method warnings out of the test output.
tasks.withType<Test>().configureEach {
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

description = "StreamRune crypto: PostgreSQL-backed per-subject key store for encryption at rest."

dependencies {
    api(project(":streamrune-core"))
    // JdbcForgottenSubjectStore implements ForgottenSubjectStore and
    // PostgresCryptoForgetSignal implements CryptoForgetSignal — both crypto-api SPI types — so
    // this must be api(...), not implementation(...): an `implementation` dependency publishes at
    // Maven `runtime` scope, and a consumer referencing either public type directly (both are
    // meant to be constructed directly, per their class javadoc) would fail to COMPILE without
    // crypto-api on ITS OWN compile classpath. Mirrors the streamrune-vault-crypto sibling.
    // Enforced by the verifyModularApiScopes check in the root build.
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    implementation(libs.hikaricp)
    implementation(libs.postgresql)
    implementation(libs.slf4j.api)
    testImplementation(project(":streamrune-crypto:streamrune-crypto-api"))
    testImplementation(libs.bundles.testcontainers.pg)
    testImplementation(libs.postgresql)
}

// PostgresCryptoEngineTest pins the README's copy-the-DDL block to the shipped baseline by reading
// README.md from disk. Declare it as a test input so a README-only edit re-runs that pin instead of
// being served from the build cache.
tasks.named<Test>("test") {
    inputs.file("README.md").withPropertyName("readme").withPathSensitivity(PathSensitivity.RELATIVE)
}

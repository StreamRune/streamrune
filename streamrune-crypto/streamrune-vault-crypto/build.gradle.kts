description = "StreamRune crypto: HashiCorp Vault-backed per-subject key store for encryption at rest."

dependencies {
    api(project(":streamrune-core"))
    // The ForgottenSubjectStore SPI + InMemoryForgottenSubjectStore
    // live in the shared crypto-api module (consumed by both KMS and Vault). Exposed via the public
    // builder, so api(...).
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    implementation(libs.jackson.databind)
    // The encrypt post-check's best-effort cleanup of a resurrected
    // transit key logs an operator-actionable ERROR when the cleanup itself fails.
    implementation(libs.slf4j.api)
    // A GDPR forget through ForgetSubjectService (in streamrune-runtime), end to end against a real
    // Vault transit key that encrypt created on first use.
    testImplementation(project(":streamrune-runtime"))
    testImplementation(libs.mockito.core)
    testImplementation(libs.testcontainers.vault)
    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.wiremock.standalone)
    // Durability test uses the shared JDBC tombstone store.
    testImplementation(project(":streamrune-crypto:streamrune-postgres-crypto"))
    testImplementation(libs.bundles.testcontainers.pg)
    testImplementation(libs.postgresql)
}

// Byte Buddy (used by Mockito) does not yet officially support Java 25.
tasks.withType<Test> {
    jvmArgs("-Dnet.bytebuddy.experimental=true")
}

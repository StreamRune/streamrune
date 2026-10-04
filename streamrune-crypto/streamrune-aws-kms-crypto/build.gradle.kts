description = "StreamRune crypto: AWS KMS-backed envelope key management for encryption at rest."

dependencies {
    api(project(":streamrune-core"))
    // The ForgottenSubjectStore SPI + InMemoryForgottenSubjectStore live in the shared
    // crypto-api module (consumed by both KMS and Vault). Exposed via the public builder, so api(...).
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    // `api`, not `implementation`. module-info.java declares
    // `requires transitive software.amazon.awssdk.services.kms` and the public builder takes a
    // KmsClient (AwsKmsCryptoEngine.Builder#kmsClient), so the AWS SDK is on this module's
    // compile-time surface twice over. `implementation` publishes it at POM `runtime` scope, which
    // fails a plain consumer at `cannot find symbol: class KmsClient` on the documented wiring —
    // and fails a MODULAR consumer outright, since javac resolves the full `requires` graph on the
    // COMPILE module path ("module not found: software.amazon.awssdk.services.kms").
    api(libs.aws.kms)
    testImplementation(project(":streamrune-test"))
    // Prove the auto-wired ForgetSubjectService (in streamrune-runtime) completes a
    // GDPR forget end-to-end on the KMS backend — crypto-shred tombstone + read-model purger.
    testImplementation(project(":streamrune-runtime"))
    testImplementation(libs.mockito.core)
    testImplementation(libs.testcontainers.localstack)
    testImplementation(libs.testcontainers.junit)
}

// Byte Buddy (used by Mockito) does not yet officially support Java 25.
tasks.withType<Test> {
    jvmArgs("-Dnet.bytebuddy.experimental=true", "--add-opens", "java.base/java.lang=ALL-UNNAMED")
}

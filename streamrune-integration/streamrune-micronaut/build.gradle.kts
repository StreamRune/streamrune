description = "StreamRune Micronaut integration: factory beans, configuration properties, HTTP filter."

dependencies {
    annotationProcessor(libs.micronaut.inject.java)
    testAnnotationProcessor(libs.micronaut.inject.java)

    api(project(":streamrune-core"))
    api(project(":streamrune-runtime"))
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    api(project(":streamrune-crypto:streamrune-filesystem-crypto"))
    api(project(":streamrune-crypto:streamrune-postgres-crypto"))
    api(project(":streamrune-crypto:streamrune-vault-crypto"))
    api(project(":streamrune-crypto:streamrune-aws-kms-crypto"))
    // The event store MUST be on the published jar's runtime classpath. StreamRuneMicronautModule
    // hard-references PostgresEventStoreFactory/PostgresAuditStore/etc. from a @Requires(beans =
    // DataSource.class) @Factory method, so a standalone Micronaut consumer (no transitive postgres)
    // crashed with NoClassDefFoundError at context startup. `implementation` (matching Quarkus; Spring
    // gets it via the spring-boot-starter's `api`) puts it on the runtime closure. There is no
    // streamrune-micronaut-starter, so this module must carry it directly.
    implementation(project(":streamrune-eventstore:streamrune-postgres"))
    api(project(":streamrune-integration:streamrune-integration-api"))

    // Micronaut core
    implementation(libs.micronaut.runtime)
    implementation(libs.micronaut.http.server.netty)
    implementation(libs.micronaut.inject)
    implementation(libs.micronaut.validation)
    implementation(libs.micronaut.serde.jackson)
    implementation(libs.jakarta.annotation.api)
    implementation(libs.snakeyaml)

    // AWS SDK
    implementation(libs.aws.kms)

    // Health
    compileOnly(libs.micronaut.management)
    compileOnly(libs.micrometer.core)
    compileOnly(libs.opentelemetry.api)
    compileOnly(libs.micronaut.security)
    testImplementation(libs.micronaut.security)

    // Reactive
    implementation(libs.reactor.core)

    // Testing
    testImplementation(libs.assertj.core)
    testImplementation(project(":streamrune-test"))
    // The configuration oracle the doc-lint test holds the guides and the metadata against.
    testImplementation(project(":streamrune-integration:streamrune-integration-tck"))
    // The boot test that runs a TRANSACTIONAL_LOCAL and an AT_LEAST_ONCE_IDEMPOTENT projection on one
    // runner awaits their writes; no other dependency puts Awaitility on this test classpath.
    testImplementation(libs.awaitility)
    testImplementation(
        "io.micronaut:micronaut-http-client:${libs.versions.micronaut.asProvider().get()}")
    testImplementation(project(":streamrune-eventstore:streamrune-postgres"))
    testImplementation(libs.micronaut.management)
    testImplementation(libs.micronaut.test.junit5)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.mockito.core)
    // micrometer-core is compileOnly on main and integration-api keeps it
    // `implementation`, so it never reached this module's test compile classpath.
    // MicronautOutboxMetricsWiringTest builds the factory's MicrometerStreamRuneMetrics over a
    // SimpleMeterRegistry to pin the four blockage/skip series, like Spring's and Quarkus's.
    testImplementation(libs.micrometer.core)
    testImplementation(libs.reactor.test)
    testImplementation(libs.opentelemetry.sdk.testing)
    // Mirroring the streamrune-runtime and streamrune-postgres precedent: the
    // SseController failure-path WARNs are only observable as the log line itself, so pinning them
    // needs a real slf4j binding whose events a test can capture (logback ListAppender). Without
    // it this module's tests bound slf4j to NOP. Test scope only — no main artifact gains a logging
    // backend. Quiet defaults in src/test/resources/logback-test.xml.
    testImplementation(libs.logback.classic)
    // The context test that boots the default wiring over a real PostgreSQL database (the event
    // store checks the server version and migrates the schema when it is built).
    testImplementation(libs.postgresql)
    testImplementation(libs.bundles.testcontainers.pg)
}

// ConfigurationDocLintTest reads the guides, this module's README, the pages listed in
// gradle/doc-lint-pages.txt (declared by the root build) and the main sources of this module and of
// streamrune-integration-api (the keys read in code) (docLintTest, see the root build): declared, so
// an edit to any of them re-runs it.
tasks.named<Test>("docLintTest") {
    inputs
        .files(
            fileTree(rootDir.resolve("docs/guide")) { include("**/*.md") },
            file("README.md"),
            fileTree("src/main/java") { include("**/*.java") },
            fileTree(rootDir.resolve("streamrune-integration/streamrune-integration-api/src/main/java")) {
                include("**/*.java")
            },
        )
        .withPropertyName("configurationDocs")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

// IntegrationWiringContract, which this module's test task runs, reads the production guide (the
// health check names, the saga warning it quotes) and the configuration reference: declared, so an
// edit to either re-runs the contract.
tasks.named<Test>("test") {
    inputs
        .files(
            rootDir.resolve("docs/guide/production.md"),
            rootDir.resolve("docs/guide/configuration.md"),
        )
        .withPropertyName("wiringContractGuides")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

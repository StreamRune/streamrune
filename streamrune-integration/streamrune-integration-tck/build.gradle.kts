description =
    "StreamRune integration TCK: what the Spring, Quarkus and Micronaut integrations' tests share " +
        "to hold the three to one contract. Not published."

// Test infrastructure for the integrations' own test source sets (each declares it as
// testImplementation); nothing here ships. The configuration oracle reads the meter names and the
// span attribute names of streamrune-core.
dependencies {
    api(project(":streamrune-core"))
    api(project(":streamrune-runtime"))
    api(project(":streamrune-integration:streamrune-integration-api"))
    api(project(":streamrune-test"))
    api(platform(libs.junit.bom))
    api("org.junit.jupiter:junit-jupiter-api")
    // The tracing rules run a command against an OpenTelemetry SDK whose spans an in-memory
    // exporter keeps; each integration's tests already have the SDK testing module on their
    // classpath. A test JVM without OpenTelemetry leaves it out (see the integrations' builds).
    api(libs.opentelemetry.sdk.testing)
    // The reference application keeps the warning the dead-letter retry runner logs when it is
    // built, as an integration's start logs it: a binding whose events it can capture.
    testImplementation(libs.logback.classic)
}

// The contract's own tests read the production guide and the configuration reference, as the
// integrations' test tasks do when they run the contract, and the list of the pages the doc-lints
// read besides the guides: declared, so an edit to any of them re-runs them.
tasks.named<Test>("test") {
    inputs
        .files(
            rootDir.resolve("docs/guide/production.md"),
            rootDir.resolve("docs/guide/configuration.md"),
            rootDir.resolve("gradle/doc-lint-pages.txt"),
        )
        .withPropertyName("wiringContractGuides")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

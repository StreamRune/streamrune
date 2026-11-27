plugins {
    alias(libs.plugins.quarkus)
}

description =
    "StreamRune Quarkus bootstrap tests: a real Quarkus application, built and started by the " +
        "Quarkus Gradle plugin on streamrune-quarkus, Quarkus REST and Agroal, against PostgreSQL."

// Test-only module: no main sources. The application under test is the test source set itself —
// the Quarkus plugin augments it and @QuarkusTest starts it the way `quarkusBuild` and
// `quarkus-run.jar` would, static initialization included. streamrune-quarkus's own tests boot Arc
// and Quarkus REST piece by piece and cannot see what only a full Quarkus start does: SmallRye's
// refusal of an unmapped key under the mapping's prefix, and the configuration a bean created during
// static initialization is given.
dependencies {
    testImplementation(enforcedPlatform(libs.quarkus.bom))
    testImplementation(project(":streamrune-integration:streamrune-quarkus"))
    testImplementation(libs.quarkus.rest)
    testImplementation(libs.quarkus.agroal)
    testImplementation(libs.quarkus.jdbc.postgresql)
    testImplementation(libs.quarkus.junit5)
    testImplementation(libs.bundles.testcontainers.pg)
    testImplementation(libs.assertj.core)
}

// The application is the test source set, which @QuarkusTest builds and starts itself: the
// plugin's packaging of the (empty) main source set would only add an empty application to every
// `assemble`.
tasks
    .matching { it.name in setOf("quarkusBuild", "quarkusAppPartsBuild", "quarkusDependenciesBuild") }
    .configureEach { enabled = false }

// No main sources, so nothing to cover (see streamrune-e2e).
tasks.withType<JacocoCoverageVerification>().configureEach {
    enabled = false
}

tasks.withType<Test>().configureEach {
    // A key set as a system property of the JVM the application starts in, beside the file keys
    // of src/test/resources/application.properties.
    systemProperty("streamrune.projections.legacy-feed.enabled", "false")
    // The application under test starts in the test JVM; JBoss LogManager is the Quarkus logging.
    systemProperty("java.util.logging.manager", "org.jboss.logmanager.LogManager")
    // A start that hangs fails the test instead of the CI job.
    systemProperty("junit.jupiter.execution.timeout.default", "5m")
}

plugins {
    alias(libs.plugins.jmh)
}

description = "StreamRune runtime: virtual-thread command bus, projection runners, aggregate locker."

dependencies {
    api(project(":streamrune-core"))
    compileOnly(libs.opentelemetry.api)
    // PUBLIC methods take crypto-api types directly — e.g.
    // DeadLetterRetryRunner.createObjectMapper(CryptoEngine) — so this must be api(...), not
    // implementation(...): an `implementation` dependency publishes at Maven `runtime` scope, and
    // a consumer calling either method needs CryptoEngine resolvable on ITS OWN compile classpath
    // to even write the call. Enforced by the verifyModularApiScopes check in the root build.
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.slf4j.api)
    implementation(libs.caffeine)
    implementation(libs.cron.utils)
    testImplementation(project(":streamrune-test"))
    testImplementation(libs.opentelemetry.api)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.mockito.core)
    testImplementation(libs.mockito.junit.jupiter)
    testImplementation(libs.assertj.core)
    testImplementation(libs.awaitility)
    // @jakarta.annotation.Priority — used by CommandInterceptorOrderingTest to assert user
    // interceptors declare a cross-framework order that sorted() honors
    testImplementation(libs.jakarta.annotation.api)
    // Mirroring the streamrune-postgres precedent: the
    // OutboxPoller aggregated transport WARN's diagnosability — naming a representative entry and
    // not over-claiming "broker-wide" — is only observable as the log line itself, so pinning it
    // needs a real slf4j binding whose events a test can capture (logback ListAppender). Test scope
    // only — no main artifact gains a logging backend. Quiet defaults in src/test/resources/
    // logback-test.xml.
    testImplementation(libs.logback.classic)

    // JSR-380 Bean Validation API
    implementation(libs.jakarta.validation.api)
    // Hibernate Validator reference implementation (for tests)
    testImplementation(libs.hibernate.validator)
    testImplementation(libs.jakarta.el) // EL implementation for @Size
}

// The JMH setup — JSON results, the -PbenchCi settings of the nightly benchmark workflow, the
// JUnit dependencies and the `jmhSmokeTest` task of src/jmh, `jmhJar` declared incompatible with
// the configuration cache — is shared by every module that applies the plugin (root build).

// Runs GraalJitGuardLiveTest, and only that class, on a test JVM with the flags
// -PgraalJitGuardTestFlags names, so that the guard meets the JVM as an application meets it:
// `none` (the default, which the guard refuses on the Graal JIT) or `c2`
// (-XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler, which it accepts). Every
// other test JVM of a GraalVM build runs with the JVMCI compiler off (the root build). Not part of
// `check`: the GraalVM workflow (graalvm-jit.yml) runs it on GraalVM with each, and
// -PexpectGraalJitGuard says what the guard must answer.
val graalJitGuardTestFlags =
    when (val flags = (findProperty("graalJitGuardTestFlags") as String?) ?: "none") {
        "none" -> emptyList()
        "c2" -> listOf("-XX:+UnlockExperimentalVMOptions", "-XX:-UseJVMCICompiler")
        else -> throw GradleException("-PgraalJitGuardTestFlags must be none or c2, not '$flags'")
    }
tasks.register<Test>("graalJitGuardTest") {
    description =
        "Runs GraalJitGuardLiveTest on a test JVM with the flags -PgraalJitGuardTestFlags names."
    group = "verification"
    testClassesDirs = sourceSets["test"].output.classesDirs
    classpath = sourceSets["test"].runtimeClasspath
    useJUnitPlatform()
    filter { includeTestsMatching("org.streamrune.runtime.GraalJitGuardLiveTest") }
    jvmArgs(graalJitGuardTestFlags)
}

// RelayComponentNamesDocLintTest reads the production guide and the three integrations' health
// sources as text (docLintTest, see the root build): declared, so an edit to any of them re-runs it.
tasks.named<Test>("docLintTest") {
    inputs
        .files(
            rootDir.resolve("docs/guide/production.md"),
            rootDir.resolve(
                "streamrune-integration/streamrune-spring/src/main/java/org/streamrune/spring/" +
                    "StreamRuneAutoConfiguration.java"),
            rootDir.resolve(
                "streamrune-integration/streamrune-quarkus/src/main/java/org/streamrune/quarkus/" +
                    "StreamRuneHealthCheck.java"),
            rootDir.resolve(
                "streamrune-integration/streamrune-micronaut/src/main/java/org/streamrune/micronaut/" +
                    "StreamRuneHealthIndicator.java"),
        )
        .withPropertyName("healthDocs")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

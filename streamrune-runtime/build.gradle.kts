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

    // For JMH benchmarks
    jmhImplementation(libs.jmh.core)
    jmhAnnotationProcessor(libs.jmh.generator.annprocess)

    // StreamRuneBenchmarkSmokeTest is a plain JUnit test colocated in
    // src/jmh/java (so it can call the @State/@Benchmark classes' public setup/benchmark methods
    // directly) that invokes every benchmark method twice outside the JMH harness. Compilation
    // alone does not catch a benchmark that compiles fine
    // but throws -- or silently measures nothing -- on a real invocation; this closes that gap.
    // See the jmhSmokeTest task below, which wires it into `check`.
    jmhImplementation(platform(libs.junit.bom))
    jmhImplementation("org.junit.jupiter:junit-jupiter")
    jmhImplementation(libs.assertj.core)
    jmhRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// The JMH plugin's `jmhJar` captures the `Project` instance in a task action, so storing the
// configuration cache entry for any graph containing it fails with "cannot serialize object of
// type 'org.gradle.api.internal.project.DefaultProject'". Before this, `./gradlew :streamrune-runtime:jmh`
// could not complete at all: the benchmark jar was built and then the build failed on the cache
// store. Declaring the incompatibility makes Gradle DEGRADE instead of fail — it disables the
// configuration cache for that invocation and reports no problem.
//
// The cost is bounded to benchmark runs. `jmhJar` is only ever in the graph when a developer asks
// for `jmh`/`jmhJar` explicitly; `check` gates the benchmarks via `compileJmhJava`, which is config-cache clean, so the normal
// `build`/`check` path keeps the configuration cache intact.
tasks.named("jmhJar") {
    notCompatibleWithConfigurationCache(
        "me.champeau.jmh's jmhJar holds a Project reference; benchmarks are an on-demand task")
}

// Runs StreamRuneBenchmarkSmokeTest -- and only that class, since
// nothing else under src/jmh carries JUnit annotations for JUnit Platform to discover; the actual
// @State/@Benchmark classes in the same source set are simply skipped during discovery, not an
// error. This task is wired into `check`: a benchmark that throws (or measures an
// unpopulated/degenerate state) on a real invocation must fail the build, not sit invisible until
// someone runs `:streamrune-runtime:jmh` by hand.
val jmhSmokeTestTask = tasks.register<Test>("jmhSmokeTest") {
    description =
        "Runs each JMH benchmark method twice outside the JMH harness (StreamRuneBenchmarkSmokeTest) " +
            "to catch a benchmark that compiles but breaks on a real invocation."
    group = "verification"
    testClassesDirs = sourceSets["jmh"].output.classesDirs
    classpath = sourceSets["jmh"].runtimeClasspath
    useJUnitPlatform()
}

tasks.named("check") {
    dependsOn(jmhSmokeTestTask)
}

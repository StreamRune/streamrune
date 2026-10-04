plugins {
    alias(libs.plugins.jmh)
    // Fray (CMU-PASTA) controlled-scheduler concurrency tester. Drives a deterministic
    // thread scheduler under a JVMTI agent to explore interleavings of the runtime's
    // concurrency primitives (aggregate locker, polling subscription). See src/fray.
    id("org.pastalab.fray.gradle") version "0.8.5"
}

description = "StreamRune runtime: virtual-thread command bus, projection runners, aggregate locker."

// Dedicated source set for Fray controlled-scheduling probes. Kept OUT of the JaCoCo
// 0.80 gate (they are scheduling explorations, not coverage) and out of the normal
// `test` task. Run them via :streamrune-runtime:frayTest.
//
// The source set is named `frayCheck` so its generated configurations
// (frayCheckImplementation / frayCheckCompileOnly) match what the Fray plugin derives
// from FrayExtension.testTask below — that is how the plugin knows where to add
// fray-core / fray-junit / fray-runtime and the platform-native JVMTI agent.
val frayCheck: SourceSet by sourceSets.creating {
    // The probes live in src/fray/java (not the src/frayCheck/java convention) so the
    // directory name reads as the intent ("fray probes"). Override the srcDir so the
    // frayCheck source set actually compiles them — without this, compileFrayCheckJava is
    // NO-SOURCE and the probes never build or run.
    java.setSrcDirs(listOf("src/fray/java"))
    resources.setSrcDirs(listOf("src/fray/resources"))
    compileClasspath += sourceSets.main.get().output + sourceSets.test.get().output
    runtimeClasspath += sourceSets.main.get().output + sourceSets.test.get().output
}

// The fray source set reuses the in-memory test doubles, so inherit the test deps.
configurations.named("frayCheckImplementation") {
    extendsFrom(configurations.testImplementation.get())
}
configurations.named("frayCheckRuntimeOnly") {
    extendsFrom(configurations.testRuntimeOnly.get())
}

// Point the Fray plugin at the dedicated `frayCheck` task/source set instead of `test`,
// so the fray-junit engine + JVMTI agent never leak into the normal `test`/`check` run
// and the probes never count toward JaCoCo coverage. The plugin then creates the
// `frayTest` task that runs `frayCheck`'s @FrayTest classes under the controlled
// scheduler (jlinked JDK + JVMTI agent).
configure<org.pastalab.fray.gradle.FrayExtension> {
    testTask = "frayCheck"
}

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

    // JUnit platform for the fray source set. fray-core / fray-junit / fray-runtime and
    // the platform-native JVMTI agent are added automatically by the Fray plugin onto the
    // frayCheck* configurations (derived from FrayExtension.testTask = "frayCheck").
    "frayCheckImplementation"(platform(libs.junit.bom))
    "frayCheckImplementation"("org.junit.jupiter:junit-jupiter")
    "frayCheckRuntimeOnly"("org.junit.platform:junit-platform-launcher")
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

// `frayCheck` runs the fray source set's @FrayTest classes. The Fray plugin's `frayTest`
// task mirrors this task's classpath/test classes and re-runs them under the JVMTI agent
// + controlled scheduler. Run Fray probes via `:streamrune-runtime:frayTest`.
val frayCheckTask = tasks.register<Test>("frayCheck") {
    description = "Compiles and lists the Fray concurrency probes (driven for real by frayTest)."
    group = "verification"
    testClassesDirs = frayCheck.output.classesDirs
    classpath = frayCheck.runtimeClasspath
    useJUnitPlatform()
    // Not wired into `check`: scheduling probes, run on demand.
}

// Runs StreamRuneBenchmarkSmokeTest -- and only that class, since
// nothing else under src/jmh carries JUnit annotations for JUnit Platform to discover; the actual
// @State/@Benchmark classes in the same source set are simply skipped during discovery, not an
// error. Unlike frayCheckTask above, THIS task is wired into `check`: a benchmark that throws (or
// measures an unpopulated/degenerate state) on a real invocation must fail the build, not sit
// invisible until someone runs `:streamrune-runtime:jmh` by hand.
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

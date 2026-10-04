description = "StreamRune Spring Boot integration: auto-configuration, configuration properties, scoped-value filter."

dependencies {
    api(project(":streamrune-core"))
    api(project(":streamrune-runtime"))
    // Implementation (not compileOnly) so the Postgres event store is on the PUBLISHED POM's
    // runtime classpath — the auto-configuration instantiates PostgresEventStoreFactory /
    // PostgresCommandInbox / PostgresOffsetStore unconditionally when a DataSource is present, and
    // the streamrune-spring README tells users to add streamrune-spring alone. compileOnly is
    // dropped from the POM, so that documented install crashed at startup with NoClassDefFoundError:
    // org/streamrune/postgres/PostgresEventStoreFactory. Quarkus and Micronaut already carry it as
    // implementation — this restores parity.
    implementation(project(":streamrune-eventstore:streamrune-postgres"))
    api(project(":streamrune-integration:streamrune-integration-api"))
    api(project(":streamrune-crypto:streamrune-filesystem-crypto"))
    api(project(":streamrune-crypto:streamrune-postgres-crypto"))
    api(project(":streamrune-crypto:streamrune-vault-crypto"))
    api(project(":streamrune-crypto:streamrune-aws-kms-crypto"))
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    implementation(libs.aws.kms)
    // Jackson 2 JavaTimeModule for the dead-letter fallback ObjectMapper (Boot 4 ships Jackson 3)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.spring.boot.autoconfigure)
    compileOnly(libs.spring.boot.starter.web)
    compileOnly(libs.spring.boot.starter.actuator)
    compileOnly(libs.jakarta.servlet.api)
    compileOnly(libs.opentelemetry.api)
    compileOnly(libs.spring.security.core)
    compileOnly(libs.jakarta.validation.api)

    // Testing
    testImplementation(project(":streamrune-test"))
    // ProjectionAutoConfigTest boots a runner over real in-memory stores and awaits its delivery.
    testImplementation(libs.awaitility)
    testImplementation(libs.spring.security.core)
    // streamrune-postgres is now an implementation dependency, so it is already on the test
    // classpath — no separate testImplementation needed.
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.spring.boot.starter.actuator)
    testImplementation(libs.spring.boot.starter.web)
    testImplementation(libs.jakarta.servlet.api)
    testImplementation(libs.opentelemetry.sdk.testing)
    testImplementation(libs.jakarta.validation.api)
    testImplementation(libs.postgresql)
    testImplementation(libs.bundles.testcontainers.pg)
    testRuntimeOnly(libs.hibernate.validator)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// A second test JVM whose classpath carries the Bean Validation API but NO provider —
// the common transitive-classpath state (any library shipping jakarta.validation-api, no
// hibernate-validator) in which StreamRune must degrade to validation-disabled instead of failing
// context refresh with NoProviderFoundException. This cannot be expressed as a classloader trick
// inside the main test JVM: jakarta.validation's bootstrap falls back to resolving providers
// through its OWN classloader when the thread-context classloader yields none, so a
// FilteredClassLoader can never hide a provider that is on the JVM classpath. hibernate-validator
// therefore stays on the main `test` classpath (keeping the provider-present wiring covered) and
// the degradation test runs here instead.
val noValidationProviderTestClass = "org.streamrune.spring.BeanValidationNoProviderTest"

val testSourceSet = project.the<SourceSetContainer>()["test"]

val noValidationProviderTest =
    tasks.register<Test>("noValidationProviderTest") {
        group = "verification"
        description =
            "Runs the Bean Validation API-without-provider degradation test on a " +
                "classpath with hibernate-validator removed."
        testClassesDirs = testSourceSet.output.classesDirs
        classpath =
            testSourceSet.runtimeClasspath.filter { !it.name.startsWith("hibernate-validator") }
        filter { includeTestsMatching(noValidationProviderTestClass) }
    }

// A test JVM whose classpath GENUINELY lacks the optional dependencies this
// module declares compileOnly — the servlet API, Spring Boot's health (actuator) module, Bean
// Validation, and Spring MVC. That is a WebFlux app, a Jersey app, and a headless HA
// worker. FilteredClassLoader cannot express it: it owns no URLs and delegates every non-filtered
// name to the parent, so a framework class such as ScopedValueFilter is still DEFINED by the
// parent — whose classpath still has servlet-api — and its jakarta.servlet.Filter superinterface
// links fine. Only a real classpath makes Class#getDeclaredMethods fail on an optional-typed @Bean
// signature, which is what breaks the whole configuration class before any condition is consulted.
val optionalDependencyAbsentTestClass = "org.streamrune.spring.OptionalDependencyAbsentTest"

val optionalDependencyAbsentTest =
    tasks.register<Test>("optionalDependencyAbsentTest") {
        group = "verification"
        description =
            "Boots the auto-configuration on a classpath with the servlet API, Spring Boot " +
                "health, Bean Validation and Spring MVC removed."
        testClassesDirs = testSourceSet.output.classesDirs
        // Jar name prefixes to strip. Declared as a local (not a script property) so the filter
        // lambda captures only a plain list and stays configuration-cache serializable.
        // OptionalDependencyAbsentTest asserts the resulting absence itself, so a dependency
        // upgrade that renames or re-homes one of these fails loudly instead of silently
        // re-admitting the type and leaving every assertion vacuous.
        val strippedJarPrefixes =
            listOf(
                "jakarta.servlet-api",
                "tomcat-embed-core",
                "spring-webmvc",
                "spring-boot-health",
                "jakarta.validation-api",
                "hibernate-validator",
            )
        classpath =
            testSourceSet.runtimeClasspath.filter { jar ->
                strippedJarPrefixes.none { prefix -> jar.name.startsWith(prefix) }
            }
        filter { includeTestsMatching(optionalDependencyAbsentTestClass) }
    }

tasks.named<Test>("test") {
    filter {
        excludeTestsMatching(noValidationProviderTestClass)
        excludeTestsMatching(optionalDependencyAbsentTestClass)
    }

    // StreamRuneFailureAnalyzerTest reads this module's own
    // src/main/java as TEXT — deliberately, because a knob named only in a FailureAnalyzer's
    // javadoc or comment must still be a real property. Source text is not one of the test task's
    // inputs, though: the task sees the COMPILED classpath, and a comment-only edit that shifts no
    // line numbers produces byte-identical class files. Measured, not assumed — rewriting one
    // comment line in StreamRuneFailureAnalyzer.java to name `streamrune.no-such-knob` left
    // `> Task :streamrune-integration:streamrune-spring:test UP-TO-DATE` and a green build, with
    // the guard test that exists to catch exactly that never executing.
    //
    // Narrower than the repo-wide log-sink ratchet (this scan never leaves its own module), but the
    // same defect: a test whose subject is not among its declared inputs runs only by luck.
    // Declaring the tree costs nothing here — any change under it already recompiles this module.
    inputs
        .files(fileTree("src/main/java") { include("**/*.java") })
        .withPropertyName("failureAnalyzerSourceScan")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

tasks.named("check") { dependsOn(noValidationProviderTest, optionalDependencyAbsentTest) }

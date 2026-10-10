// Jandex (io.smallrye:jandex) is used below to generate META-INF/jandex.idx for the
// published jar so Quarkus applications index this library without configuration.
buildscript {
    repositories {
        mavenCentral()
    }
    dependencies {
        classpath("io.smallrye:jandex:3.5.3")
    }
}

description = "StreamRune Quarkus integration: CDI producers, config mapping, request filter, lifecycle."

dependencies {
    api(project(":streamrune-core"))
    api(project(":streamrune-runtime"))
    implementation(project(":streamrune-eventstore:streamrune-postgres"))
    api(project(":streamrune-integration:streamrune-integration-api"))

    // Crypto engine modules
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    api(project(":streamrune-crypto:streamrune-filesystem-crypto"))
    api(project(":streamrune-crypto:streamrune-postgres-crypto"))
    api(project(":streamrune-crypto:streamrune-vault-crypto"))
    api(project(":streamrune-crypto:streamrune-aws-kms-crypto"))

    implementation(libs.aws.kms)
    implementation(libs.quarkus.core)
    implementation(libs.quarkus.arc)
    implementation(libs.quarkus.vertx)
    compileOnly(libs.quarkus.rest)
    compileOnly(libs.quarkus.smallrye.health)
    implementation(libs.opentelemetry.api)
    compileOnly(libs.quarkus.security)
    // The crypto-forget LISTEN pool is opened from the application's Agroal datasource settings
    // when the injected DataSource is Agroal (every quarkus-agroal application); compile-only so an
    // application without Agroal does not inherit it.
    compileOnly(libs.agroal.api)
    // Annotations of the native-image substitution in org.streamrune.quarkus.graal; the image
    // builder provides them, so nothing is added to the application.
    compileOnly(libs.graalvm.nativeimage)
    testImplementation(libs.graalvm.nativeimage)
    implementation(libs.jakarta.validation.api)
    implementation(libs.micrometer.core)
    testImplementation(libs.micrometer.core)
    testImplementation(libs.jakarta.validation.api)
    testImplementation(libs.quarkus.security)
    testImplementation(libs.quarkus.rest)
    testImplementation(libs.quarkus.smallrye.health)

    // Testing
    // Same Jandex version the jandexIndex task below uses to WRITE META-INF/jandex.idx, so
    // JandexConfigMappingIndexTest reads the shipped index with a format-compatible IndexReader.
    testImplementation("io.smallrye:jandex:3.5.3")
    testImplementation(libs.assertj.core)
    testImplementation(project(":streamrune-test"))
    // The boot test of an at-least-once projection beside a TRANSACTIONAL_LOCAL one awaits its write.
    testImplementation(libs.awaitility)
    testImplementation(project(":streamrune-eventstore:streamrune-postgres"))
    testImplementation(libs.quarkus.junit5)
    // Real-Arc container boot test (QuarkusBeanWiringTest): QuarkusUnitTest builds a synthetic
    // deployment in-process and boots a genuine Arc container, so @DefaultBean resolution,
    // @ConfigMapping binding, and app-bean-wins-over-default are actually exercised — the defect
    // class that shipped the Micronaut DI bug. quarkus-arc-deployment supplies the Arc augmentation
    // step QuarkusUnitTest runs; both are test-only and never leak into the published jar.
    testImplementation(libs.quarkus.junit.internal)
    testImplementation(libs.quarkus.arc.deployment)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.mockito.core)
    testImplementation(libs.opentelemetry.sdk.testing)
    // A real Agroal pool against a PostgreSQL container, the DataSource shape quarkus-agroal injects.
    testImplementation(libs.agroal.api)
    testImplementation(libs.agroal.pool)
    testImplementation(libs.postgresql)
    testImplementation(libs.bundles.testcontainers.pg)
}

// Generates a Jandex index (META-INF/jandex.idx) from the compiled classes and packages
// it into the jar. Quarkus indexes dependency jars that ship either a Jandex index or a
// META-INF/beans.xml (this module ships both); without one of them, CDI producers and
// JAX-RS providers in this library are invisible to applications.
val jandexOutputDir = layout.buildDirectory.dir("generated/jandex")

val jandexIndex = tasks.register("jandexIndex") {
    dependsOn(tasks.named("classes"))
    val classesDirs = sourceSets["main"].output.classesDirs
    val indexFile = jandexOutputDir.get().file("META-INF/jandex.idx").asFile
    inputs.files(classesDirs)
    outputs.file(indexFile)
    doLast {
        val indexer = org.jboss.jandex.Indexer()
        classesDirs.files
            .flatMap { dir -> dir.walkTopDown().filter { it.isFile && it.extension == "class" }.toList() }
            .sortedBy { it.path }
            .forEach { classFile -> classFile.inputStream().use { indexer.index(it) } }
        indexFile.parentFile.mkdirs()
        indexFile.outputStream().use { org.jboss.jandex.IndexWriter(it).write(indexer.complete()) }
    }
}

tasks.named<Jar>("jar") {
    dependsOn(jandexIndex)
    from(jandexOutputDir)
}

// JandexConfigMappingIndexTest reads the assembled jar's META-INF/jandex.idx to prove the
// @ConfigMapping interface stays indexed — the only thing that makes StreamRuneQuarkusProperties
// resolvable in a published-jar consumer (this is a plain CDI-producers library, no -deployment
// module). Wire the test to the jar artifact and hand its path over via a system property.
tasks.named<Test>("test") {
    val jarArtifact = tasks.named<Jar>("jar").flatMap { it.archiveFile }
    dependsOn(tasks.named("jar"))
    jvmArgumentProviders.add(
        org.gradle.process.CommandLineArgumentProvider {
            listOf("-Dstreamrune.quarkus.jar=${jarArtifact.get().asFile.absolutePath}")
        },
    )
}

// The Micrometer version this build declares must not exceed the one the Quarkus BOM manages.
// Gradle resolves the highest version requested, so a higher declared version would replace the
// Quarkus-managed one in a Gradle-built Quarkus application; the Quarkus Micrometer extension's
// native-image configuration is written for the managed version, and the image fails to build.
// The configuration below resolves micrometer-core with only the Quarkus BOM deciding the version.
val quarkusManagedMicrometer: Configuration by configurations.creating {
    isCanBeConsumed = false
    isCanBeResolved = true
}

dependencies {
    quarkusManagedMicrometer(platform(libs.quarkus.bom))
    quarkusManagedMicrometer("io.micrometer:micrometer-core")
}

val verifyMicrometerVersion = tasks.register("verifyMicrometerVersion") {
    description = "Fails when the declared Micrometer version exceeds the one the Quarkus BOM manages."
    group = "verification"
    val declared = libs.versions.micrometer.get()
    val managed = quarkusManagedMicrometer.incoming.resolutionResult.rootComponent.map { root ->
        root.dependencies
            .filterIsInstance<org.gradle.api.artifacts.result.ResolvedDependencyResult>()
            .map { it.selected.moduleVersion!! }
            .single { it.group == "io.micrometer" && it.name == "micrometer-core" }
            .version
    }
    inputs.property("declared", declared)
    inputs.property("managed", managed)
    doLast {
        val managedVersion = managed.get()
        fun parts(version: String) = version.split('.', '-').mapNotNull { it.toIntOrNull() }
        val exceeds = parts(declared).zip(parts(managedVersion))
            .firstOrNull { (d, m) -> d != m }
            ?.let { (d, m) -> d > m } ?: false
        if (exceeds) {
            throw GradleException(
                "gradle/libs.versions.toml declares Micrometer $declared, but the Quarkus BOM manages " +
                    "$managedVersion. Declare at most $managedVersion: a higher version replaces the " +
                    "Quarkus-managed one in Gradle-built Quarkus applications and breaks their native image.",
            )
        }
    }
}

tasks.named("check") {
    dependsOn(verifyMicrometerVersion)
}

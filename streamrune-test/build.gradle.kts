description = "StreamRune test support: in-memory event store, decider fixture, test harness."

dependencies {
    api(project(":streamrune-core"))
    // SagaFixture builds a SagaRunner and a SagaTimeoutRunner, whose builders its API exposes.
    api(project(":streamrune-runtime"))
    // All `api`, because module-info.java requires each of them by name.
    //
    // The previous reasoning here — "InMemoryEventStore.serializing() registers
    // CryptoShreddingModule internally but no public signature exposes org.streamrune.crypto
    // types, so the dependency must not leak into consumers' compile classpaths (implementation,
    // not api)" — is sound Java-library hygiene and simultaneously impossible under JPMS. A
    // consumer's `requires org.streamrune.test` makes javac resolve THIS module's whole `requires`
    // graph on the COMPILE module path; a name available only at runtime (POM `runtime` scope,
    // which is what `implementation` publishes) makes that resolution fail with "module not
    // found". Encapsulation of these types is expressed by module-info NOT re-exporting them
    // (`requires`, not `requires transitive`), not by the POM scope.
    // Enforced by the `verifyModularApiScopes` check in the root build.
    api(project(":streamrune-crypto:streamrune-crypto-api"))
    // The JUnit floor the kit runs on (see `junit-floor` in the version catalog), not a JUnit BOM:
    // an exported BOM would move every JUnit module of an application to its version. The kit's
    // main sources compile against the floor, so they cannot use API it lacks; its tests run on
    // the framework's JUnit (the root build's testImplementation platform). The compiler does not
    // see API that JUnit 6 removed (MethodOrderer.Alphanumeric, for one): the kit's own tests, on
    // JUnit 6, are what catch a use of it.
    api(libs.junit.jupiter.api.floor)
    api(libs.jackson.databind)
    api(libs.jackson.core)
    api(libs.jackson.datatype.jsr310)
    // InMemoryEventStore logs through slf4j; `api` for the same JPMS reason as
    // the rest of this block (module-info requires org.slf4j).
    api(libs.slf4j.api)
}

// TestKitDocLintTest reads the testing guide, this module's README and its own main sources (the
// list of public types) (docLintTest, see the root build): declared, so an edit re-runs it.
tasks.named<Test>("docLintTest") {
    inputs
        .files(
            rootDir.resolve("docs/guide/testing.md"),
            file("README.md"),
            fileTree("src/main/java") { include("**/*.java") },
        )
        .withPropertyName("kitDocs")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}

// The kit's published metadata must leave an application's JUnit version alone: no platform or BOM
// among its dependencies (Gradle module metadata and POM), and junit-jupiter-api required at the
// floor, with no strict version. Runs on the files the publication would upload.
val verifyJUnitFloor =
    tasks.register("verifyJUnitFloor") {
        // Locals, not script-level vals: doLast must not capture the script object (configuration
        // cache).
        val junitFloor = libs.versions.junit.floor.get()
        val publishedMetadata = files(tasks.withType<GenerateModuleMetadata>())
        val publishedPoms = files(tasks.withType<GenerateMavenPom>())
        group = "verification"
        description =
            "Fails when the published metadata exports a JUnit BOM or requires more than the " +
                "JUnit floor."
        inputs.files(publishedMetadata).withPropertyName("metadata")
        inputs.files(publishedPoms).withPropertyName("poms")
        inputs.property("floor", junitFloor)
        val report = layout.buildDirectory.file("reports/junit-floor.txt")
        outputs.file(report).withPropertyName("report")
        doLast {
            val problems = mutableListOf<String>()
            val metadata = publishedMetadata.files.filter { it.isFile }
            val poms = publishedPoms.files.filter { it.isFile }
            if (metadata.isEmpty() || poms.isEmpty()) {
                problems += "no generated module metadata or POM to check"
            }
            metadata.forEach { file ->
                @Suppress("UNCHECKED_CAST")
                val json = groovy.json.JsonSlurper().parse(file) as Map<String, Any?>
                @Suppress("UNCHECKED_CAST")
                val variants = json["variants"] as List<Map<String, Any?>>
                for (variant in variants) {
                    // Constraints count too: one on a JUnit module raises it like a dependency.
                    @Suppress("UNCHECKED_CAST")
                    val dependencies =
                        (variant["dependencies"] as List<Map<String, Any?>>?).orEmpty() +
                            (variant["dependencyConstraints"] as List<Map<String, Any?>>?).orEmpty()
                    for (dependency in dependencies) {
                        val coordinates = "${dependency["group"]}:${dependency["module"]}"
                        @Suppress("UNCHECKED_CAST")
                        val attributes = (dependency["attributes"] as Map<String, Any?>?).orEmpty()
                        val category = attributes["org.gradle.category"]
                        if (category == "platform" || category == "enforced-platform") {
                            problems += "${file.name} (${variant["name"]}): $coordinates is a $category"
                        }
                        if (coordinates.startsWith("org.junit")) {
                            @Suppress("UNCHECKED_CAST")
                            val version = (dependency["version"] as Map<String, Any?>?).orEmpty()
                            if (coordinates != "org.junit.jupiter:junit-jupiter-api" ||
                                version["requires"] != junitFloor ||
                                version.containsKey("strictly")) {
                                problems += "${file.name} (${variant["name"]}): $coordinates $version " +
                                    "(expected only org.junit.jupiter:junit-jupiter-api requiring $junitFloor)"
                            }
                        }
                    }
                }
            }
            poms.forEach { file ->
                val text = file.readText()
                if (text.contains("<dependencyManagement>")) {
                    problems += "${file.name} imports a BOM in <dependencyManagement>"
                }
                val junit =
                    Regex("""<artifactId>(junit-[^<]+)</artifactId>\s*<version>([^<]*)</version>""")
                        .findAll(text).map { it.groupValues[1] to it.groupValues[2] }.toList()
                if (junit != listOf("junit-jupiter-api" to junitFloor)) {
                    problems += "${file.name}: JUnit dependencies $junit " +
                        "(expected only junit-jupiter-api $junitFloor)"
                }
            }
            report.get().asFile.parentFile.mkdirs()
            report.get().asFile.writeText(
                "floor: $junitFloor\nproblems: ${problems.size}\n" + problems.joinToString("\n"))
            if (problems.isNotEmpty()) {
                throw GradleException(
                    "streamrune-test would publish a JUnit dependency that moves an application's " +
                        "JUnit version:\n  " + problems.joinToString("\n  "))
            }
        }
    }
tasks.named("check") { dependsOn(verifyJUnitFloor) }

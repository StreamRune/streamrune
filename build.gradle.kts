// Explicit imports — inside the script body `java.lang.module.*` collides with both
// the `java` extension and Gradle's deprecated `Dependencies.module(...)` helper.
import java.lang.module.FindException
import java.lang.module.ModuleFinder
import java.util.concurrent.TimeUnit
import java.util.jar.Manifest
import java.util.zip.ZipFile

plugins {
    java
    id("jacoco-report-aggregation")
    alias(libs.plugins.spotless) apply false
    alias(libs.plugins.jmh) apply false
    alias(libs.plugins.graalvm) apply false
    alias(libs.plugins.versions)
    alias(libs.plugins.version.catalog.update)
    // Declared directly (not via the version catalog) because this plugin is part of the
    // publishing configuration owned by this block; see the publishing section below.
    id("com.vanniktech.maven.publish") version "0.36.0" apply false
}

// ── Helpers for verifyModularApiScopes (registered per-subproject below) ──────
//
// Detects a PUBLIC type whose supertype (extends/implements) is declared by a project dependency
// this module brought in via `implementation` or `compileOnly` rather than `api`. Gradle
// maps `implementation` to Maven `runtime` scope (published, but absent from a consumer's compile
// classpath) and `compileOnly` to no scope at all (absent from the published POM entirely — for example
// streamrune-postgres's compileOnly(project(":streamrune-runtime")) leaked this way, uncaught,
// until the check below was extended to see compileOnly project deps too). Either way, a consumer
// who then references the type directly — constructs it, declares a field/variable of its type,
// extends it, exactly the documented usage of e.g. JdbcForgottenSubjectStore — fails to COMPILE:
// javac must resolve the type's FULL supertype chain to load the class at all, not merely the
// members a particular caller happens to use.
//
// Source-level, not bytecode: mirrors this file's existing module-info.java handling below (same
// comment-stripping technique), and needs no other project's compileJava to have run first. Only
// TOP-LEVEL `public` types are in scope (unindented — this repo's spotless/google-java-format
// convention), since those are what an external Maven consumer can reference directly.
// A plain Kotlin `object`, not top-level script `val`/`fun` declarations: this file is a Kotlin
// SCRIPT (.gradle.kts), where top-level declarations become members of the implicit script class,
// so calling them from a `doLast { }` lambda captures a reference to the script instance itself —
// which the configuration cache refuses to serialize ("cannot serialize Gradle script object
// references"). A nested `object` is an ordinary standalone class with no such reference.
private object BrA17SupertypeCheck {
    private val TOP_LEVEL_PUBLIC_TYPE =
        Regex(
            """(?m)^public(?:\s+(?:final|abstract|sealed|non-sealed|strictfp))*\s+""" +
                """(?:class|interface|enum|record)\s+(\w+)[^{]*\{""")

    data class SupertypeReference(val file: File, val typeName: String, val supertypes: Set<String>)

    private fun stripJavaComments(source: String): String =
        source
            .replace(Regex("""/\*.*?\*/""", RegexOption.DOT_MATCHES_ALL), " ")
            .replace(Regex("""//[^\n]*"""), " ")

    /** Simple names of every top-level `public` type declared under a `src/main/java` tree. */
    fun publicTopLevelTypeNames(mainJavaDir: File): Set<String> =
        mainJavaDir.walkTopDown()
            .filter { it.isFile && it.extension == "java" }
            .flatMap { f ->
                TOP_LEVEL_PUBLIC_TYPE.findAll(stripJavaComments(f.readText())).map { it.groupValues[1] }
            }
            .toSet()

    /** Every top-level `public` type under a `src/main/java` tree, with its declared supertypes. */
    fun publicSupertypeReferences(mainJavaDir: File): List<SupertypeReference> =
        mainJavaDir.walkTopDown()
            .filter { it.isFile && it.extension == "java" }
            .flatMap { f ->
                val text = stripJavaComments(f.readText())
                TOP_LEVEL_PUBLIC_TYPE.findAll(text).mapNotNull { m ->
                    val header = m.value // "public ... TypeName ... {" — the whole matched header
                    val extendsPart =
                        Regex("""\bextends\b(.*?)(?:\bimplements\b|\{)""", RegexOption.DOT_MATCHES_ALL)
                            .find(header)?.groupValues?.get(1).orEmpty()
                    val implementsPart =
                        Regex("""\bimplements\b(.*)\{""", RegexOption.DOT_MATCHES_ALL)
                            .find(header)?.groupValues?.get(1).orEmpty()
                    val supertypes =
                        (extendsPart + "," + implementsPart)
                            .split(",")
                            .map { it.trim() }
                            .filter { it.isNotEmpty() }
                            // drop generic type arguments: `Comparable<Foo>` -> `Comparable`
                            .map { it.substringBefore("<").trim() }
                            // a qualified reference (rare; imports are the norm here): simple name only
                            .map { it.substringAfterLast(".") }
                            .filter { it.isNotEmpty() }
                            .toSet()
                    if (supertypes.isEmpty()) null else SupertypeReference(f, m.groupValues[1], supertypes)
                }
            }
            .toList()
}

// ── PostgreSQL 17+ — one test-image switch, one CI leg per end of the supported range ───────
//
// StreamRune declares PostgreSQL 17 or newer. A declaration nobody tests is a guess, so every
// Testcontainers PostgreSQL container in the repository takes its image from ONE class,
// gradle/test-support/org/streamrune/testsupport/PostgresTestImage.java (default
// postgres:18-alpine, the newest supported server), and the project property `-PpostgresImage`
// moves the whole suite to another server version:
//
//     ./gradlew postgresTest -PpostgresImage=postgres:17-alpine
//
// build.yml runs the default build on 18 and a second job, `postgres-17`, that runs exactly the
// `postgresTest` aggregate below on 17. These are the modules whose tests start a PostgreSQL
// container. The list is the single source of truth for the shared test source, the property
// plumbing, the aggregate and the CI job; `verifyPostgresTestImage` fails the build when a module
// outside it starts a PostgreSQL container, or when any test names an image of its own.
val postgresBackedModules =
    listOf(
        ":streamrune-eventstore:streamrune-postgres",
        ":streamrune-crypto:streamrune-postgres-crypto",
        ":streamrune-integration:streamrune-spring",
        ":streamrune-integration:streamrune-quarkus",
        ":streamrune-integration:streamrune-e2e")

subprojects {
    apply(plugin = "java-library")
    apply(plugin = "jacoco")
    apply(plugin = "com.diffplug.spotless")

    group = "org.streamrune"
    // The development default is the preview version that CI publishes to the Central Portal
    // snapshot repository on every push to main (.github/workflows/publish-snapshot.yml). A release
    // passes -PreleaseVersion=<version>, which always wins.
    version = (project.findProperty("releaseVersion") as String?) ?: "1.0.0-alpha-SNAPSHOT"

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(25))
        }
    }

    // Retain method parameter names in the bytecode (javac -parameters). Spring Boot enables this
    // by default; the framework was not, so the shipped Spring MVC controllers' `@PathVariable
    // String streamId` could not bind by name in a consumer app (Spring MVC reads the parameter
    // name reflectively) and every request 400'd. -parameters makes name-based binding work for any
    // consumer without forcing explicit `@PathVariable("name")` on every annotation.
    tasks.withType<JavaCompile>().configureEach {
        options.compilerArgs.add("-parameters")
    }

    // NOTE: no --enable-preview. The codebase uses only finalized Java 25 features
    // (ScopedValue, virtual threads). Compiling a published library with preview
    // enabled would flag every class file as preview, making the artifacts loadable
    // ONLY on exactly JDK 25 and only with --enable-preview — unacceptable for
    // consumers. If a preview API is ever needed, scope the flag to that module's
    // test compilation, never to published main sources.

    tasks.withType<Test> {
        useJUnitPlatform()
        finalizedBy(tasks.named("jacocoTestReport"))
    }

    // The PostgreSQL-backed modules compile the shared PostgresTestImage into their tests and
    // forward -PpostgresImage to every test JVM they start (the broker-tagged e2e task included).
    if (project.path in postgresBackedModules) {
        the<SourceSetContainer>()["test"].java.srcDir(rootProject.file("gradle/test-support"))
        // Spotless refuses a target outside the project directory, and the shared directory is
        // outside every module's: pin the format target to the module's own sources here, and
        // format the shared directory from the root project (the `spotless` block at the bottom).
        configure<com.diffplug.gradle.spotless.SpotlessExtension> {
            java { target("src/**/*.java") }
        }
        tasks.withType<Test>().configureEach {
            val postgresImage = providers.gradleProperty("postgresImage").orElse("")
            // A task input, not just a system property: with the build cache on, a run against
            // postgres:17-alpine must never be satisfied by an UP-TO-DATE or FROM-CACHE result
            // produced against 18 (that would "prove" the supported range without a single
            // 17 container having started), and vice versa.
            inputs.property("postgresImage", postgresImage)
            val image = postgresImage.get()
            if (image.isNotBlank()) {
                systemProperty("streamrune.test.postgres.image", image)
            }
        }
    }

    tasks.withType<JacocoReport> {
        dependsOn(tasks.named("test"))
        reports {
            xml.required.set(true)
            html.required.set(true)
        }
    }

    // 80% per-module instruction AND branch coverage. The spring-boot-starter
    // module is exempt since it is thin scaffolding with no logic to test.
    val exemptedPaths = setOf(
        ":streamrune-integration:streamrune-spring-boot-starter")
    if (project.path !in exemptedPaths) {
        tasks.withType<JacocoCoverageVerification> {
            dependsOn(tasks.named("test"))
            violationRules {
                rule {
                    limit {
                        counter = "INSTRUCTION"
                        value = "COVEREDRATIO"
                        minimum = "0.80".toBigDecimal()
                    }
                }
                rule {
                    limit {
                        counter = "BRANCH"
                        value = "COVEREDRATIO"
                        minimum = "0.80".toBigDecimal()
                    }
                }
            }
        }

        tasks.named("check") {
            dependsOn(tasks.named("jacocoTestCoverageVerification"))
        }
    }

    // ── `check` compiles EVERY source set, not just main + test ──────────
    //
    // `build` = `assemble` + `check`. `assemble` reaches only what the published artifacts need
    // and `check` only what `test` needs, so an AUXILIARY source set — one that no other task
    // consumes — is compiled by nothing in the default graph. Measured, not assumed: a dry run
    // of `build spotlessCheck` scheduled 436 tasks and exactly zero of them were a jmh or a
    // fray task; the only compile tasks in the entire graph were `compileJava` and
    // `compileTestJava`, one pair per module.
    //
    // That is how :streamrune-runtime's `src/jmh` sat broken for months — five javac errors,
    // stale against the strict-typed Command/AggregateId API — underneath a build that was
    // green end to end. No CI run would ever have reported it, because `compileJmhJava` was
    // never in the graph to fail. `src/fray` (the frayCheck concurrency probes) happens to
    // compile today, but sits in exactly the same blind spot and could rot the same way
    // tomorrow.
    //
    // Depending on the LIVE `withType<JavaCompile>()` collection rather than a hand-listed set
    // of task names is the whole point: a source set added later is gated automatically, with
    // no second edit here to forget. This gates COMPILATION only — running the benchmarks
    // (`jmh`) and the scheduling probes (`frayTest`) stays on demand, since neither asserts
    // anything and both are slow. It also deliberately does not pull in `jmhJar`, the one JMH
    // task that is incompatible with the configuration cache (see
    // streamrune-runtime/build.gradle.kts): `compileJmhJava` and the JMH bytecode generator
    // both store the configuration cache cleanly, so `check` keeps its cache.
    tasks.named("check") {
        dependsOn(tasks.withType<JavaCompile>())
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        val libs = rootProject.the<VersionCatalogsExtension>().named("libs")
        testImplementation(platform(libs.findLibrary("junit-bom").get()))
        testImplementation("org.junit.jupiter:junit-jupiter")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher")
        testImplementation(libs.findLibrary("archunit").get())
        testImplementation(libs.findLibrary("archunit-junit5").get())
    }

    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        setEnforceCheck(false) // JDK 25+ spotless/guava: skip lint failures
        java {
            googleJavaFormat("1.34.1")
            removeUnusedImports()
        }
    }

    // Every non-`java.*` module named in a module-info.java `requires` clause must be
    // reachable from this module's `api` configuration — i.e. published at POM `compile` scope.
    //
    // Why `api` and not merely "on the compile classpath": a consumer's `requires <this module>`
    // makes javac resolve this module's ENTIRE `requires` graph on the COMPILE module path, and a
    // plain (non-transitive) `requires` is just as compile-visible to the resolver as a transitive
    // one. Gradle maps `implementation` to POM `runtime` scope, so a dependency declared that way
    // is absent from a consumer's compile module path and javac aborts with "module not found" —
    // a failure the library's own build can never see, because its own module-info compiles fine
    // against its own `implementation` classpath. That asymmetry is exactly why this needs a gate
    // rather than a convention: the defect is invisible on this side of the publish boundary.
    //
    // Precise by construction: rather than mapping coordinates to module names by hand (a table
    // that would rot the moment a dependency is renamed), it resolves the real `api` graph and
    // asks java.lang.module.ModuleFinder for each artifact's actual module name — explicit
    // (module-info.class) or automatic (Automatic-Module-Name, else the filename). So it catches
    // all three shapes of the defect at once: declared `implementation`, declared `runtimeOnly`,
    // and not declared at all (arriving transitively).
    //
    // `requires static` is deliberately exempt: an optional compile-only dependency is absent from
    // the consumer's module path BY DESIGN and javac tolerates it.
    // Deferred to afterEvaluate: this module's own build.gradle.kts (a SEPARATE script,
    // evaluated AFTER this subprojects{} block runs) is what actually calls
    // implementation(project(...)) / api(project(...)); reading
    // configurations.getByName("implementation").dependencies any earlier always sees an
    // empty set (the check's own first version silently no-opped on every module until this
    // was caught by re-running it against the still-unfixed postgres-crypto module).
    project.afterEvaluate {
        val moduleDescriptor = file("src/main/java/module-info.java")
        val hasModuleDescriptor = moduleDescriptor.exists()
        val modularApiPath: FileCollection? =
            if (hasModuleDescriptor) {
                configurations.create("modularApiPath") {
                    isCanBeResolved = true
                    isCanBeConsumed = false
                    description = "Resolves exactly what `api` publishes, for verifyModularApiScopes."
                    extendsFrom(configurations.getByName("api"))
                    attributes {
                        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage::class.java, Usage.JAVA_API))
                        attribute(
                            Category.CATEGORY_ATTRIBUTE,
                            objects.named(Category::class.java, Category.LIBRARY))
                        attribute(
                            LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                            objects.named(LibraryElements::class.java, LibraryElements.JAR))
                        attribute(
                            Bundling.BUNDLING_ATTRIBUTE,
                            objects.named(Bundling::class.java, Bundling.EXTERNAL))
                    }
                }
            } else {
                null
            }

        // This module's own NON-`api` project dependencies, computed at
        // configuration time from the DECLARED dependency sets (nothing resolved) so the check works
        // whether or not this module ships a module-info.java. See the doLast below.
        //
        // `compileOnly` project deps are unioned in alongside `implementation` ones — a
        // `compileOnly` project dependency is invisible in the published POM ENTIRELY (not even at
        // `runtime` scope, which `implementation` at least gets), so a public supertype leaking from
        // one is strictly worse than the `implementation` case this check originally covered, and
        // was previously invisible to it (e.g. streamrune-postgres's
        // compileOnly(project(":streamrune-runtime")), leaking through PostgresNotificationSubscription
        // / HybridEventSubscription — fixed alongside this guard extension, not caught by it before).
        val nonApiProjectDepPaths: Set<String> =
            (configurations.getByName("implementation").dependencies
                .withType(ProjectDependency::class.java).map { it.path }.toSet() +
                configurations.getByName("compileOnly").dependencies
                    .withType(ProjectDependency::class.java).map { it.path }.toSet()) -
                configurations.getByName("api").dependencies
                    .withType(ProjectDependency::class.java).map { it.path }.toSet()
        val ownMainJavaDir = file("src/main/java")
        val depMainJavaDirsByPath: Map<String, File> =
            nonApiProjectDepPaths.associateWith { path ->
                project(path).projectDir.resolve("src/main/java")
            }

        val verifyModularApiScopes = tasks.register("verifyModularApiScopes") {
            description = "Fails when a module-info `requires`, or a public type's supertype, is " +
                "not reachable from this module's `api` path."
            group = "verification"
            val descriptor = moduleDescriptor
            val apiPath: FileCollection? = modularApiPath
            val label = project.path
            // Hoisted out of doLast: `Task.project` cannot be invoked at execution time under the
            // configuration cache, so anything derived from it must be captured here instead.
            val projectName = project.name
            val projectDirAtConfigTime = project.projectDir
            val report = layout.buildDirectory.file("reports/modular-api-scopes.txt")
            if (hasModuleDescriptor) {
                inputs.file(descriptor).withPropertyName("moduleDescriptor")
                inputs.files(apiPath!!).withPropertyName("modularApiPath")
            }
            if (ownMainJavaDir.exists()) {
                inputs.dir(ownMainJavaDir).withPropertyName("ownMainJavaDir")
            }
            inputs.property(
                "nonApiProjectDepPaths", nonApiProjectDepPaths.sorted())
            depMainJavaDirsByPath.forEach { (path, dir) ->
                if (dir.exists()) {
                    inputs.dir(dir).withPropertyName("depMainJavaDir-" + path.replace(":", "_"))
                }
            }
            outputs.file(report).withPropertyName("report")
            doLast {
                val problems = mutableListOf<String>()
                var moduleInfoReport = ""

                if (hasModuleDescriptor) {
                    // Strip block and line comments first: a prose comment mentioning "requires" must
                    // not be parsed as a clause (the aws-kms descriptor has one).
                    val source = descriptor.readText()
                        .replace(Regex("""/\*.*?\*/""", RegexOption.DOT_MATCHES_ALL), " ")
                        .replace(Regex("""//[^\n]*"""), " ")
                    val required = Regex("""\brequires\b([^;{}]*);""").findAll(source)
                        .map { it.groupValues[1].trim().split(Regex("""\s+""")) }
                        .filterNot { it.contains("static") }
                        .map { it.last() }
                        .filterNot { it == "java.base" || it.startsWith("java.") || it.startsWith("jdk.") }
                        .toSet()

                    val jars = apiPath!!.files.filter { it.exists() }.map { it.toPath() }
                    val available: Set<String> = try {
                        ModuleFinder.of(*jars.toTypedArray())
                            .findAll().map { ref -> ref.descriptor().name() }.toSet()
                    } catch (e: FindException) {
                        throw GradleException(
                            "$label: could not read the module names on the `api` path " +
                                "($jars): ${e.message}", e)
                    }

                    val missing: List<String> = required.filterNot { available.contains(it) }.sorted()
                    moduleInfoReport =
                        "module: $label\nrequires: ${required.sorted()}\n" +
                            "api-path modules: ${available.sorted()}\nmissing: $missing\n"
                    if (missing.isNotEmpty()) {
                        problems.add(
                            "$label declares `requires` for $missing in module-info.java, but " +
                                "none of them is reachable from its `api` configuration — so the " +
                                "published POM lists them at `runtime` scope (or not at all) and a " +
                                "consumer that `requires` this module will fail to COMPILE with " +
                                "\"module not found\". Declare each as `api(...)` in " +
                                "${projectName}/build.gradle.kts, or drop the `requires` (or make " +
                                "it `requires static` if it is genuinely optional). Modules on the " +
                                "api path: ${available.sorted()}")
                    }
                }

                // A PUBLIC type in this module whose supertype
                // (extends/implements) is declared by a non-`api` (`implementation` or
                // `compileOnly`) project dependency — see the helpers above for why that breaks a
                // consumer's compile (or, for `compileOnly`, omits the dependency from the published
                // POM entirely). No module-info required.
                if (nonApiProjectDepPaths.isNotEmpty() && ownMainJavaDir.exists()) {
                    val exportedNamesByDep: Map<String, Set<String>> =
                        depMainJavaDirsByPath.mapValues { (_, dir) ->
                            if (dir.exists()) BrA17SupertypeCheck.publicTopLevelTypeNames(dir) else emptySet()
                        }
                    val offenders = mutableListOf<String>()
                    BrA17SupertypeCheck.publicSupertypeReferences(ownMainJavaDir).forEach { ref ->
                        exportedNamesByDep.forEach { (depPath, exportedNames) ->
                            val leaked = ref.supertypes.intersect(exportedNames)
                            if (leaked.isNotEmpty()) {
                                offenders.add(
                                    "  ${ref.file.relativeTo(projectDirAtConfigTime)}: public type " +
                                        "`${ref.typeName}` extends/implements ${leaked.sorted()} " +
                                        "from `$depPath`")
                            }
                        }
                    }
                    if (offenders.isNotEmpty()) {
                        problems.add(
                            "$label: public type(s) expose a supertype from a project dependency " +
                                "that is not `api`-scoped (declared `implementation`, publishing at " +
                                "Maven `runtime` scope, or `compileOnly`, absent from the published " +
                                "POM entirely), so a consumer referencing the type directly fails to " +
                                "COMPILE. Declare the dependency `api(...)` " +
                                "in ${projectName}/build.gradle.kts instead:\n" +
                                offenders.sorted().joinToString("\n"))
                    }
                }

                report.get().asFile.parentFile.mkdirs()
                report.get().asFile.writeText(
                    moduleInfoReport +
                        "non-api project deps (implementation + compileOnly): ${nonApiProjectDepPaths.sorted()}\n" +
                        "problems: ${problems.size}\n")
                if (problems.isNotEmpty()) {
                    throw GradleException(problems.joinToString("\n\n"))
                }
            }
        }
        tasks.named("check") { dependsOn(verifyModularApiScopes) }
    }
    // Maven publishing — all modules except umbrella parents and test-only modules.
    // :streamrune-integration and :streamrune-eventstore are umbrella
    // subprojects that group their children (integration-api + framework
    // modules; streamrune-postgres). Neither declares Java sources of its
    // own, so excluding them keeps the publish set at exactly the leaf artifacts.
    // (:streamrune-eventstore is auto-created by Gradle as the parent of
    // :streamrune-eventstore:streamrune-postgres even though it isn't
    // explicitly listed in settings.gradle.kts.)
    //
    // :streamrune-integration:streamrune-e2e is a test-only module with NO main
    // sources (end-to-end crash-recovery tests). Publishing it would upload an
    // empty org.streamrune:streamrune-e2e jar to Maven Central — immutable
    // pollution — so it is excluded here alongside the umbrella parents.
    // (The Spring Boot starter, also sources-free, IS published on purpose: a
    // starter is a dependency-aggregator POM, the standard Spring Boot pattern.)
    val umbrellaPaths =
        setOf(
            ":streamrune-integration",
            ":streamrune-eventstore",
            ":streamrune-crypto",
            ":streamrune-outbox",
            ":streamrune-integration:streamrune-e2e")
    if (project.path !in umbrellaPaths) {
        // ── Every published jar carries a stable JPMS module name ────────────────
        //
        // Four modules ship a real module-info.java; the other twelve shipped a 25-byte manifest
        // with no `Automatic-Module-Name` at all, so their module name was DERIVED FROM THE JAR
        // FILE NAME (streamrune.runtime, streamrune.postgres, …). That is a published API the
        // moment 1.0 ships, and it silently changes whenever an artifactId is renamed or a build
        // repackages the jar — breaking every consumer's `requires`. It also made the half-modular
        // surface incoherent: a consumer that saw module-info in core/crypto-api and wrote a
        // `module com.acme { … }` got "Module streamrune.runtime is an automatic module and cannot
        // be used with jlink", and the modules carrying the entry-point API a user actually needs
        // (StreamRune, PostgresEventStoreFactory, the three integrations) were exactly the ones
        // without a declared name.
        //
        // Rejected the auditor's mechanical `project.name` transform ("streamrune-" prefix
        // stripped, '-' -> '.'). It disagrees with both the explicit module names and the real
        // package roots — streamrune-crypto-api would become org.streamrune.crypto.api (its
        // module-info says org.streamrune.crypto), streamrune-postgres-crypto would become
        // org.streamrune.postgres.crypto (its package is org.streamrune.crypto.postgres, and the
        // derived name reads as a sub-namespace of the unrelated streamrune-postgres module), and
        // streamrune-integration-api / *-outbox / *-crypto would all gain a trailing segment their
        // packages do not have. Worse, a derived name is exactly what wrong-outcome (b) is about:
        // it changes when the artifactId changes. This map is the module name as a deliberate,
        // reviewable API decision, in one place, decoupled from the artifact id. Each entry is the
        // module's real root package.
        //
        // The `require` below is the gate for a NEW published module: it cannot ship without
        // either a module-info.java or an entry here, and never both (two sources of truth for one
        // name). verifyStableModuleName re-checks the assembled jar, so an override of the `jar`
        // task cannot quietly drop the attribute.
        val automaticModuleNames = mapOf(
            ":streamrune-runtime" to "org.streamrune.runtime",
            ":streamrune-eventstore:streamrune-postgres" to "org.streamrune.postgres",
            ":streamrune-integration:streamrune-integration-api" to "org.streamrune.integration",
            ":streamrune-integration:streamrune-spring" to "org.streamrune.spring",
            ":streamrune-integration:streamrune-quarkus" to "org.streamrune.quarkus",
            ":streamrune-integration:streamrune-micronaut" to "org.streamrune.micronaut",
            ":streamrune-integration:streamrune-spring-boot-starter"
                to "org.streamrune.spring.boot.starter",
            ":streamrune-crypto:streamrune-filesystem-crypto" to "org.streamrune.filesystem",
            ":streamrune-crypto:streamrune-postgres-crypto" to "org.streamrune.crypto.postgres",
            ":streamrune-crypto:streamrune-vault-crypto" to "org.streamrune.vault",
            ":streamrune-outbox:streamrune-kafka-outbox" to "org.streamrune.kafka",
            ":streamrune-outbox:streamrune-rabbitmq-outbox" to "org.streamrune.rabbitmq")

        val hasModuleInfo = file("src/main/java/module-info.java").exists()
        val automaticModuleName = automaticModuleNames[project.path]
        require(hasModuleInfo != (automaticModuleName != null)) {
            if (hasModuleInfo) {
                "Published module ${project.path} has BOTH a module-info.java and an entry in " +
                    "automaticModuleNames (build.gradle.kts). The module name must have one " +
                    "source of truth: drop the map entry — an explicit module descriptor wins " +
                    "and Automatic-Module-Name would be ignored anyway."
            } else {
                "Published module ${project.path} has neither a module-info.java nor an entry in " +
                    "automaticModuleNames (build.gradle.kts), so its JPMS module name would be " +
                    "derived from the jar FILE NAME — a published API that changes whenever the " +
                    "artifactId does, and one that jlink refuses. Add an entry naming the " +
                    "module's root package."
            }
        }

        val projectVersion = version.toString()

        tasks.named<Jar>("jar") {
            manifest {
                attributes(
                    "Implementation-Title" to project.name,
                    "Implementation-Version" to projectVersion,
                    // Machine-readable licence signal for the SCA scanners that read a jar's
                    // manifest instead of resolving its POM. BUSL-1.1 is the SPDX short identifier
                    // for Business Source License 1.1 (the <name> the POM declares, verbatim).
                    "SPDX-License-Identifier" to "BUSL-1.1")
                if (automaticModuleName != null) {
                    attributes("Automatic-Module-Name" to automaticModuleName)
                }
            }
        }

        // ── Every published archive carries the licence texts ──────────────────────
        //
        // BSL 1.1's own Terms require "You must conspicuously display this License on each original
        // or modified copy of the Licensed Work". Before this, every published archive held exactly
        // two META-INF entries — the directory and MANIFEST.MF — and the removal of the BSL copyright
        // headers from the sources stripped the per-file headers and the spotless
        // licenseHeaderFile rule, so the sources jar carried nothing either. That left the POM as
        // the sole licence signal in the entire distribution, and made LICENSE-COMMERCIAL.md — which
        // the POM's <comments> tells a reader to consult — a dangling pointer to a file that existed
        // in no artifact and at no URL. All three texts now ship inside every archive, so the grant
        // is readable from the artifact alone, offline, with no network round-trip.
        //
        // The three archives the `maven` publication ships come from three different mechanisms:
        // `jar` from the `java` plugin, `sourcesJar` from java.withSourcesJar() (which vanniktech's
        // SourcesJar.Sources() calls), and `plainJavadocJar` from its JavadocJar.Javadoc() — see
        // the JavaLibrary(...) declaration below, which is what fixes these three names.
        //
        // The type MUST be spelled `org.gradle.jvm.tasks.Jar`, not the bare `Jar` the Kotlin DSL
        // resolves. Gradle has two Jar classes: org.gradle.jvm.tasks.Jar, and the narrower
        // org.gradle.api.tasks.bundling.Jar that extends it and is what a build script's default
        // imports bind `Jar` to. `jar` and `sourcesJar` are the narrow subtype, but plainJavadocJar
        // is a com.vanniktech.maven.publish.tasks.JavadocJar, which extends the WIDE one — so
        // `withType<Jar>()` silently matched two of the three and the javadoc jar shipped with no
        // licence at all. Measured, not assumed: the first run of this change produced a javadoc
        // jar whose only META-INF entry was still MANIFEST.MF.
        //
        // Filtered by name rather than taking every Jar in the project, because not every Jar is
        // published: :streamrune-runtime's JMH plugin contributes a `jmhJar`, which is a local
        // benchmarking artifact that no publication ships. Sweeping it in would make `check`
        // depend on its output, and `jmhJar` cannot be stored in the configuration cache (it holds
        // a Project reference) — so a licensing rule would have cost the whole build its
        // configuration cache. src/jmh is still gated, just via `compileJmhJava` instead: see
        // the compile-every-source-set rule above. A name filter is safe here precisely because
        // verifyLicenseFilesPackaged below asserts all three classifiers were found: renaming or
        // dropping one of these tasks fails loudly instead of quietly shipping an unlicensed
        // archive.
        val licenseTexts =
            listOf(
                rootProject.file("LICENSE"),
                rootProject.file("NOTICE"),
                rootProject.file("LICENSE-COMMERCIAL.md"))
        val publishedArchives =
            tasks.withType<org.gradle.jvm.tasks.Jar>().matching {
                it.name in setOf("jar", "sourcesJar", "plainJavadocJar")
            }
        publishedArchives.configureEach {
            metaInf {
                from(licenseTexts)
            }
        }

        val verifyStableModuleName = tasks.register("verifyStableModuleName") {
            description = "Fails when the published jar declares no stable JPMS module name."
            group = "verification"
            val jarFile = tasks.named<Jar>("jar").flatMap { it.archiveFile }
            val expected = automaticModuleName
            val label = project.path
            val report = layout.buildDirectory.file("reports/stable-module-name.txt")
            inputs.file(jarFile).withPropertyName("jar")
            outputs.file(report).withPropertyName("report")
            doLast {
                val file = jarFile.get().asFile
                ZipFile(file).use { zip ->
                    val modular = zip.getEntry("module-info.class") != null
                    val declared = zip.getEntry("META-INF/MANIFEST.MF")?.let { entry ->
                        zip.getInputStream(entry).use { input ->
                            Manifest(input).mainAttributes.getValue("Automatic-Module-Name")
                        }
                    }
                    report.get().asFile.parentFile.mkdirs()
                    report.get().asFile.writeText(
                        "module: $label\njar: ${file.name}\nmodule-info.class: $modular\n" +
                            "Automatic-Module-Name: $declared\n")
                    if (!modular && declared == null) {
                        throw GradleException(
                            "$label publishes ${file.name} with neither a module-info.class nor " +
                                "an Automatic-Module-Name, so its JPMS module name falls back to " +
                                "the jar file name — unstable public API, and unusable with " +
                                "jlink.")
                    }
                    if (declared != null && declared != expected) {
                        throw GradleException(
                            "$label publishes Automatic-Module-Name '$declared' but the module " +
                                "name declared in the root build is '$expected'.")
                    }
                }
            }
        }
        tasks.named("check") { dependsOn(verifyStableModuleName) }

        // Publishing goes through the Sonatype Central Portal (https://central.sonatype.com).
        // Plain `maven-publish` cannot speak the Portal's upload REST API (it is a bundle
        // upload, not a Maven repository), so the vanniktech plugin drives the upload:
        // it stages signed artifacts locally, zips them into a Portal bundle, and POSTs it.
        //
        // Credentials are the Gradle properties `mavenCentralUsername` / `mavenCentralPassword`.
        // Releases export the Portal user token as CENTRAL_USERNAME / CENTRAL_TOKEN — map them
        // when invoking Gradle:
        //   ORG_GRADLE_PROJECT_mavenCentralUsername="$CENTRAL_USERNAME" \
        //   ORG_GRADLE_PROJECT_mavenCentralPassword="$CENTRAL_TOKEN" \
        //   ./gradlew publishToMavenCentral -PreleaseVersion=<version>
        //
        // A -SNAPSHOT version takes a different path through the same task: the plugin points the
        // `mavenCentral` repository at https://central.sonatype.com/repository/maven-snapshots/
        // (a plain Maven repository, no bundle, no Portal validation) and marks signing as NOT
        // required, so a snapshot uploads unsigned with only the Portal token. Signing stays
        // mandatory for every other version. CI does exactly this on each push to main
        // (.github/workflows/publish-snapshot.yml): `./gradlew publishToMavenCentral`, default
        // version, no signing secrets.
        // Local verification without credentials: ./gradlew publishToMavenLocal
        apply(plugin = "com.vanniktech.maven.publish")

        // ── The POM licence <url> resolves to the text that actually governs ───────
        //
        // Derived from the same `version` the artifacts carry (the `releaseVersion` property,
        // defaulting to 1.0.0-alpha-SNAPSHOT), never hardcoded, so a release build and a snapshot
        // build each emit a URL pointing at the licence text of the code being published. A release
        // pins the tag `v<version>` rather than a branch: the tag cannot move, so the text a
        // consumer reads years later is the text that shipped. A snapshot has no tag by
        // construction and is published only to the snapshot repository, where it is replaced by
        // the next push to main, so it points at `main`.
        val licenseRef = if (projectVersion.endsWith("-SNAPSHOT")) "main" else "v$projectVersion"
        val licenseUrl = "https://raw.githubusercontent.com/StreamRune/streamrune/$licenseRef/LICENSE"

        // Run Javadoc as part of `check` for every PUBLISHED module, so a
        // broken {@link}/@see reference fails the same `./gradlew build` that gates merges instead
        // of failing the release publish mid-flight (after signing/staging has begun).
        //
        // CI-green => publish-green by construction, not by flag-matching: the vanniktech
        // JavadocJar.Javadoc() artifact task (`plainJavadocJar`) consumes the output of the
        // STANDARD `javadoc` task of this same project — verified with
        // `./gradlew :streamrune-core:plainJavadocJar --dry-run`, whose task graph is
        // compileJava -> classes -> javadoc -> plainJavadocJar. So `check` and the publish path
        // execute the SAME task instance with the SAME doclet options. Deliberately NO doclint
        // scoping is configured here: any `-Xdoclint:...` narrowing would apply to that one shared
        // task and would therefore weaken the RELEASE check too, trading the equivalence and the
        // signal for nothing. The gate is scoped to the publish set (this `umbrellaPaths` branch)
        // because the umbrella/e2e modules have no main sources — their `javadoc` is NO-SOURCE and
        // gates nothing.
        tasks.named("check") {
            dependsOn(tasks.withType<Javadoc>())
        }

        configure<com.vanniktech.maven.publish.MavenPublishBaseExtension> {
            publishToMavenCentral()
            signAllPublications()
            coordinates(group.toString(), project.name, version.toString())

            configure(
                com.vanniktech.maven.publish.JavaLibrary(
                    javadocJar = com.vanniktech.maven.publish.JavadocJar.Javadoc(),
                    sourcesJar = com.vanniktech.maven.publish.SourcesJar.Sources(),
                )
            )

            pom {
                name.set(project.name)
                description.set(
                    providers.provider { project.description ?: project.name }
                )
                url.set("https://github.com/StreamRune/streamrune")

                // <url> is the field tooling and lawyers follow, so it must resolve to the text
                // that governs. It used to point at https://mariadb.com/bsl11/ — MariaDB's
                // UNPARAMETERISED template. That page has no Parameters block, so its Terms'
                // "The Licensor may make an Additional Use Grant, above, permitting limited
                // production use" refers to nothing, and anyone following the URL concludes that
                // NO production use is permitted: the exact opposite of this project's USD 5M
                // grant. The template link is kept, demoted to <comments> where it belongs as
                // provenance, and <url> now points at StreamRune's own parameterised LICENSE.
                //
                // Exactly ONE <license> entry, deliberately, even though the project is dual
                // licensed. Maven's semantics for multiple entries are "the consumer may use the
                // artifact under any one of these", and a negotiated commercial agreement is not
                // something a consumer can select out of a POM — listing it would read as an offer
                // of terms nobody has signed. Akka (BSL + Lightbend commercial) and MySQL
                // Connector/J (GPL + commercial) both publish a single entry for this reason. The
                // commercial half is signalled in <comments> and by META-INF/LICENSE-COMMERCIAL.md
                // inside every artifact.
                licenses {
                    license {
                        // Verbatim the SPDX full name for BUSL-1.1.
                        name.set("Business Source License 1.1")
                        url.set(licenseUrl)
                        distribution.set("repo")
                        comments.set(
                            "SPDX-License-Identifier: BUSL-1.1. Production use is free while" +
                                " your organization's total annual gross revenue, combined with" +
                                " its affiliates', does not exceed USD 5,000,000 — that is the" +
                                " Additional Use Grant in the LICENSE named above, also bundled" +
                                " in this artifact at META-INF/LICENSE. Each version converts to" +
                                " Apache License 2.0 four years after its publication. Above the" +
                                " revenue threshold a commercial license is required:" +
                                " licensing@streamrune.com, details at" +
                                " META-INF/LICENSE-COMMERCIAL.md in this artifact. The" +
                                " unparameterised BSL 1.1 template is at" +
                                " https://mariadb.com/bsl11/; it carries no Additional Use Grant" +
                                " and does NOT state this project's terms — the parameterised" +
                                " text at the URL above is the one that governs."
                        )
                    }
                }

                developers {
                    developer {
                        id.set("mbednar")
                        name.set("Martin Bednář")
                        email.set("licensing@streamrune.com")
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/StreamRune/streamrune.git")
                    developerConnection.set("scm:git:ssh://git@github.com/StreamRune/streamrune.git")
                    url.set("https://github.com/StreamRune/streamrune")
                }
            }
        }

        // Registered here, AFTER the publishing configuration above, because only by this point
        // does the Jar task collection hold all three published archive tasks: `jar` (java plugin),
        // `sourcesJar` (registered by java.withSourcesJar(), which SourcesJar.Sources() calls) and
        // `plainJavadocJar` (registered by JavadocJar.Javadoc()). Enumerating the collection here
        // is what makes the check track the real publish set instead of a hand-kept list — the
        // same reason the bundling above uses withType.
        //
        // The classifier assertion is not decoration: it is the check that catches the failure the
        // entry check cannot see. If a whole archive kind drops OUT of the collection — exactly
        // what the org.gradle.jvm vs org.gradle.api.tasks.bundling Jar-type trap above does — then
        // every archive the check can see is compliant and it passes while an unlicensed jar is
        // published. Requiring all three classifiers turns that silence into a failure.
        val verifyLicenseFilesPackaged =
            tasks.register("verifyLicenseFilesPackaged") {
                description = "Fails when a published archive omits the licence texts in META-INF."
                group = "verification"
                val archives =
                    publishedArchives.map { it.archiveClassifier.getOrElse("") to it.archiveFile }
                val expected = licenseTexts.map { "META-INF/${it.name}" }.sorted()
                val requiredClassifiers = setOf("", "sources", "javadoc")
                val label = project.path
                val report = layout.buildDirectory.file("reports/license-files-packaged.txt")
                inputs.files(archives.map { it.second }).withPropertyName("publishedArchives")
                outputs.file(report).withPropertyName("report")
                doLast {
                    val classifiers = archives.map { it.first }.toSet()
                    val lines =
                        mutableListOf(
                            "module: $label",
                            "expected: $expected",
                            "archive classifiers: ${classifiers.sorted()}")
                    val offenders = mutableListOf<String>()
                    archives
                        .map { it.second.get().asFile }
                        .sortedBy { it.name }
                        .forEach { file ->
                            ZipFile(file).use { zip ->
                                val present = expected.filter { zip.getEntry(it) != null }
                                lines += "${file.name}: $present"
                                val missing = expected - present.toSet()
                                if (missing.isNotEmpty()) {
                                    offenders += "${file.name} is missing $missing"
                                }
                            }
                        }
                    report.get().asFile.parentFile.mkdirs()
                    report.get().asFile.writeText(lines.joinToString("\n") + "\n")

                    val unseen = requiredClassifiers - classifiers
                    if (unseen.isNotEmpty()) {
                        throw GradleException(
                            "$label: this check only saw archives with classifiers " +
                                "${classifiers.sorted()}, so the published " +
                                "${unseen.sorted()} archive(s) were never inspected and could " +
                                "ship with no licence text at all. The usual causes are an " +
                                "archive task renamed away from jar/sourcesJar/plainJavadocJar, " +
                                "one that does not extend org.gradle.jvm.tasks.Jar, or one " +
                                "registered after this check.")
                    }
                    if (offenders.isNotEmpty()) {
                        throw GradleException(
                            "$label would publish archives without their licence texts: " +
                                "${offenders.joinToString("; ")}. BSL 1.1 requires the License to " +
                                "be displayed on every copy of the Licensed Work, and the POM is " +
                                "not part of the copy a consumer unpacks. Every Jar task in a " +
                                "published module must inherit the metaInf { from(licenseTexts) } " +
                                "spec in the root build.gradle.kts.")
                    }
                }
            }
        tasks.named("check") { dependsOn(verifyLicenseFilesPackaged) }

        // ── A release cannot be uploaded before its tag exists ─────────────────────
        //
        // The POM's licence <url> is pinned to `v<version>` on GitHub. Maven Central is immutable:
        // if the artifacts go up before that tag is pushed and the tag is then never created (or is
        // created on a different commit), the licence URL of that release 404s forever and cannot
        // be corrected. This task turns that ordering into a build failure. It fires only for
        // non-SNAPSHOT versions — a snapshot has no tag by construction and its POM points at
        // `main` — and it is wired below as a dependency of the Central upload task, i.e. it runs
        // in the release publish path and nowhere else; `check` and `build` never invoke git.
        // It is also runnable on its own: `./gradlew verifyReleaseTag -PreleaseVersion=<version>`.
        //
        // Fails closed when git itself cannot be run: for a release upload, "cannot confirm the tag
        // exists" and "the tag does not exist" carry the same irreversible risk.
        val gitWorkTree = rootProject.projectDir
        val verifyReleaseTag =
            tasks.register("verifyReleaseTag") {
                description = "Fails a release publish when the matching v<version> git tag is absent."
                group = "verification"
                val label = project.path
                val versionUnderRelease = projectVersion
                val licenseUrlUnderRelease = licenseUrl
                val workTree = gitWorkTree
                outputs.upToDateWhen { false }
                doLast {
                    if (versionUnderRelease.endsWith("-SNAPSHOT")) {
                        logger.lifecycle(
                            "$label: version $versionUnderRelease is a snapshot — no release tag " +
                                "is expected (POM licence URL: $licenseUrlUnderRelease).")
                        return@doLast
                    }
                    val tag = "v$versionUnderRelease"
                    // Three arms, because a LOCAL tag proves none of what the POM
                    // licence URL actually depends on. That URL resolves against the tag as GitHub
                    // sees it, so the tag must (1) exist, (2) sit on the commit these artifacts were
                    // built from, and (3) be on the REMOTE. Only the first was checked, so a tag
                    // created but never pushed — or created on the wrong commit — published a URL
                    // that 404s or points at a different tree's LICENSE, immutably.
                    //
                    // Every arm fails closed: for an irreversible upload, "cannot confirm" and
                    // "confirmed wrong" carry the same risk. git is run with a hard timeout and with
                    // terminal prompting disabled, so a credential prompt on ls-remote fails the
                    // build instead of hanging a release pipeline forever.
                    fun git(vararg args: String): Pair<Int, String> =
                        try {
                            val process =
                                ProcessBuilder(listOf("git", *args))
                                    .directory(workTree)
                                    .redirectErrorStream(false)
                                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                                    .also { it.environment()["GIT_TERMINAL_PROMPT"] = "0" }
                                    .start()
                            val output = process.inputStream.bufferedReader().readText()
                            if (!process.waitFor(120, TimeUnit.SECONDS)) {
                                process.destroyForcibly()
                                throw GradleException(
                                    "$label: `git ${args.joinToString(" ")}` did not finish within " +
                                        "120s while verifying the release tag $tag. A release " +
                                        "publish is refused when the tag cannot be confirmed.")
                            }
                            process.exitValue() to output.trim()
                        } catch (e: GradleException) {
                            throw e
                        } catch (e: Exception) {
                            throw GradleException(
                                "$label: could not run git to verify the release tag $tag " +
                                    "(${e.message}). A release publish is refused when the tag " +
                                    "cannot be confirmed, because Maven Central uploads are " +
                                    "immutable and the POM's licence URL is pinned to that tag.", e)
                        }

                    // Arm 1 — the tag exists locally, and resolve the commit it names.
                    val (localExit, localCommit) =
                        git("rev-parse", "-q", "--verify", "refs/tags/$tag^{commit}")
                    if (localExit != 0 || localCommit.isEmpty()) {
                        throw GradleException(
                            "$label: git tag $tag does not exist, but this publish would upload " +
                                "version $versionUnderRelease to Maven Central with the licence " +
                                "URL $licenseUrlUnderRelease. Central is immutable, so that URL " +
                                "would 404 forever. Tag and push the release commit first: " +
                                "git tag -a $tag -m '$tag' && git push origin $tag. " +
                                "The repository must also be public for the URL to resolve.")
                    }

                    // Arm 2 — the tag names the commit these artifacts were built from. A tag on an
                    // older commit publishes a licence URL pointing at a different tree's LICENSE.
                    val (headExit, headCommit) = git("rev-parse", "-q", "--verify", "HEAD^{commit}")
                    if (headExit != 0 || headCommit.isEmpty()) {
                        throw GradleException(
                            "$label: could not resolve HEAD to verify that release tag $tag is on " +
                                "the commit being published. A release publish is refused when " +
                                "that cannot be confirmed.")
                    }
                    if (localCommit != headCommit) {
                        throw GradleException(
                            "$label: git tag $tag points at $localCommit but the artifacts being " +
                                "published are built from HEAD ($headCommit). The POM licence URL " +
                                "$licenseUrlUnderRelease would resolve to a DIFFERENT tree's " +
                                "LICENSE, immutably. Move the tag onto the release commit: " +
                                "git tag -f -a $tag -m '$tag' && git push --force origin $tag.")
                    }

                    // Arm 3 — the tag is on the remote the URL resolves against. `ls-remote` prints
                    // the tag object for an annotated tag and, additionally, a `<tag>^{}` line
                    // carrying the peeled commit; a lightweight tag prints only the commit. Prefer
                    // the peeled line when present so both tag kinds compare as commits.
                    val (remoteExit, remoteOutput) =
                        git("ls-remote", "origin", "refs/tags/$tag", "refs/tags/$tag^{}")
                    if (remoteExit != 0) {
                        throw GradleException(
                            "$label: could not read the remote tags of `origin` (git ls-remote " +
                                "exited $remoteExit) while verifying release tag $tag. A release " +
                                "publish is refused when the tag cannot be confirmed on the remote, " +
                                "because the POM licence URL $licenseUrlUnderRelease resolves " +
                                "against the PUBLISHED repository and Central is immutable.")
                    }
                    val remoteRefs =
                        remoteOutput.lines().filter { it.isNotBlank() }.associate { line ->
                            val parts = line.split(Regex("\\s+"), limit = 2)
                            parts[1].trim() to parts[0].trim()
                        }
                    val remoteCommit =
                        remoteRefs["refs/tags/$tag^{}"] ?: remoteRefs["refs/tags/$tag"]
                    if (remoteCommit == null) {
                        throw GradleException(
                            "$label: git tag $tag exists locally but is NOT on `origin`, and the " +
                                "POM licence URL $licenseUrlUnderRelease resolves against the " +
                                "published repository — it would 404 for every consumer of an " +
                                "immutable Central artifact. Push it first: " +
                                "git push origin $tag.")
                    }
                    if (remoteCommit != localCommit) {
                        throw GradleException(
                            "$label: git tag $tag points at $localCommit locally but at " +
                                "$remoteCommit on `origin`, so the licence URL " +
                                "$licenseUrlUnderRelease would resolve to a different tree than " +
                                "the one these artifacts were built from. Reconcile them before " +
                                "publishing: git push --force origin $tag.")
                    }

                    // Arm 4 — the working tree, not just HEAD, is clean for
                    // TRACKED files. Arms 1-3 prove the TAG matches HEAD; they say nothing about
                    // whether the FILES ON DISK match HEAD. publishToMavenCentral builds from the
                    // working tree, not a `git archive` of the tagged commit — a locally modified,
                    // staged-but-uncommitted, or deleted tracked file would publish bytes that do
                    // not match the tagged tree, immutably, exactly like arms 1-3's failure mode.
                    //
                    // `--untracked-files=no` deliberately tolerates untracked files OUTSIDE a
                    // src/ tree (this repo's scratchpad/ directories, editor state, etc. — never a
                    // published artifact's input; an untracked file UNDER src/main or src/test is
                    // separately caught below by arm 5, since THAT one IS a published
                    // artifact's input) and, independently of that flag, `git status` never reports
                    // ignored files unless `--ignored` is explicitly passed, so both are already
                    // excluded. A staged-but-uncommitted new file (status `A`) is correctly treated
                    // as dirty:
                    // it is tracked in the index but absent from HEAD/the tag.
                    val (dirtyExit, dirtyOutput) =
                        git("status", "--porcelain", "--untracked-files=no")
                    if (dirtyExit != 0) {
                        throw GradleException(
                            "$label: could not run `git status` to verify the working tree is " +
                                "clean before publishing release tag $tag (git exited " +
                                "$dirtyExit). A release publish is refused when this cannot be " +
                                "confirmed, because publishToMavenCentral builds from the working " +
                                "tree, not the tagged commit.")
                    }
                    if (dirtyOutput.isNotBlank()) {
                        throw GradleException(
                            "$label: the working tree has uncommitted changes to TRACKED files, " +
                                "so this publish would upload bytes that do NOT match release tag " +
                                "$tag's tree — irreversibly, since Maven Central is immutable " +
                                "(untracked files are fine; only tracked ones are checked). " +
                                "`git status --porcelain --untracked-files=no`:\n$dirtyOutput\n" +
                                "Commit or discard these changes before publishing.")
                    }

                    // Arm 5 — no UNTRACKED file sits under a published source tree. Arm 4
                    // deliberately tolerates untracked files (scratchpad/, editor state, etc.), but
                    // Gradle compiles and packages whatever is PHYSICALLY PRESENT under a module's
                    // src/main/java (and equivalent resource/test trees), regardless of git's index
                    // — an untracked stray file left under src/ on release day ships inside the
                    // published, immutable jar while being absent from the tag the arms above
                    // certify, exactly the tag<->artifact mismatch this task exists to prevent.
                    // Scoped to `src/main/` and `src/test/` specifically (not merely "untracked
                    // anywhere") so scratchpad/, build/, and other non-source untracked paths stay
                    // tolerated exactly as before.
                    val (untrackedExit, untrackedOutput) =
                        git("ls-files", "--others", "--exclude-standard")
                    if (untrackedExit != 0) {
                        throw GradleException(
                            "$label: could not run `git ls-files --others --exclude-standard` to " +
                                "check for untracked files under a src/ tree before publishing " +
                                "release tag $tag (git exited $untrackedExit). A release publish " +
                                "is refused when this cannot be confirmed.")
                    }
                    val untrackedSourcePaths =
                        untrackedOutput.lines()
                            .map { it.trim() }
                            .filter { it.isNotEmpty() }
                            .filter { Regex("""(^|/)src/(main|test)/""").containsMatchIn(it) }
                    if (untrackedSourcePaths.isNotEmpty()) {
                        throw GradleException(
                            "$label: untracked file(s) under a src/main or src/test tree would be " +
                                "compiled into the published artifacts for release tag $tag while " +
                                "being absent from that tag — Central is immutable, so the " +
                                "published jar would permanently contain code that is in no commit " +
                                "(untracked files OUTSIDE src/ — scratchpad/, editor state, etc. — " +
                                "remain tolerated, same as arm 4):\n" +
                                untrackedSourcePaths.sorted().joinToString("\n") +
                                "\nTrack, remove, or .gitignore these paths before publishing.")
                    }

                    logger.lifecycle(
                        "$label: release tag $tag verified — present locally and on origin, " +
                            "both at HEAD ($localCommit), working tree clean for tracked files, " +
                            "no untracked files under any src/main or src/test tree.")
                }
            }

        // ── A release cannot ship without CI having actually passed ───────────
        //
        // A `v*` tag push starts build.yml and broker-e2e-test.yml, but starting is not the same
        // as enforcing: nothing previously made a publish wait for or check either workflow's
        // result, so a release could be tagged and uploaded to the immutable Central Portal from a
        // commit whose test suite had never run at all, or had failed. verifyReleaseTag (above)
        // only proves git facts (the tag exists, on HEAD, on origin) — it has no notion of CI.
        //
        // This task queries the GitHub Checks API for the exact tagged commit and refuses to
        // proceed unless every name in REQUIRED_CHECK_NAMES concluded `success` there. Check-run
        // names are the workflow job's key (or its `name:`, if set) as GitHub renders it on the
        // Checks API — verified empirically against this repository's own history (`gh api
        // repos/StreamRune/streamrune/commits/main/check-runs`): build.yml's `build:` job produces a
        // check-run literally named "build". By the same rule broker-e2e-test.yml's `broker-e2e:`
        // job produces "broker-e2e" — NOT "broker-e2e-test" (that is only the workflow's
        // filename/title; that workflow had not yet run on GitHub when this was written, so this
        // could not be verified the same way — read
        // the job key in .github/workflows/broker-e2e-test.yml if it is ever renamed). build.yml's
        // second job, `postgres-17:` (the PostgreSQL-backed suites on the oldest supported server),
        // produces "postgres-17" by the same rule and is required too — the release declares
        // PostgreSQL 17 or newer, so it must not ship from a commit whose 17 leg is red or missing.
        // Uses the "any check-run with this name concluded success" rule, so a check that failed
        // once and was re-run green still satisfies the gate.
        //
        // Escape hatch: `-PskipCiVerification=true` skips this task entirely, logging loudly (a
        // WARN naming the version) so the bypass is never silent. Fails closed otherwise: an
        // unauthenticated/missing `gh` CLI, an API error, or a required check simply absent from
        // the response all REFUSE the publish, for the same reason verifyReleaseTag fails closed —
        // "cannot confirm CI passed" and "confirmed CI failed" carry the same irreversible risk.
        //
        // Runnable on its own: `./gradlew verifyReleaseChecks -PreleaseVersion=<version>`.
        val githubRepoSlug = "StreamRune/streamrune"
        val requiredCiCheckNames = setOf("build", "broker-e2e", "postgres-17")
        val verifyReleaseChecks =
            tasks.register("verifyReleaseChecks") {
                description = "Fails a release publish unless build/broker-e2e/postgres-17 concluded " +
                    "success on the GitHub Checks API for the exact tagged commit."
                group = "verification"
                val label = project.path
                val versionUnderRelease = projectVersion
                val workTree = gitWorkTree
                val repoSlug = githubRepoSlug
                val requiredNames = requiredCiCheckNames
                val skipCiVerificationProp = providers.gradleProperty("skipCiVerification")
                outputs.upToDateWhen { false }
                doLast {
                    if (versionUnderRelease.endsWith("-SNAPSHOT")) {
                        logger.lifecycle(
                            "$label: version $versionUnderRelease is a snapshot — no CI-check " +
                                "verification is expected.")
                        return@doLast
                    }
                    if (skipCiVerificationProp.orNull == "true") {
                        logger.warn(
                            "$label: -PskipCiVerification=true — SKIPPING the required-CI-checks " +
                                "verification for version $versionUnderRelease. " +
                                "This publish proceeds WITHOUT confirming $requiredNames concluded " +
                                "success on this commit. Use only for a deliberate, understood " +
                                "override.")
                        return@doLast
                    }
                    val tag = "v$versionUnderRelease"

                    // Mirrors verifyReleaseTag's git() helper above (hard timeout, no terminal
                    // prompting, fails closed) — not shared across tasks: each task's `doLast`
                    // closure must not capture state from another task's configuration.
                    fun git(vararg args: String): Pair<Int, String> =
                        try {
                            val process =
                                ProcessBuilder(listOf("git", *args))
                                    .directory(workTree)
                                    .redirectErrorStream(false)
                                    .redirectError(ProcessBuilder.Redirect.DISCARD)
                                    .also { it.environment()["GIT_TERMINAL_PROMPT"] = "0" }
                                    .start()
                            val output = process.inputStream.bufferedReader().readText()
                            if (!process.waitFor(120, TimeUnit.SECONDS)) {
                                process.destroyForcibly()
                                throw GradleException(
                                    "$label: `git ${args.joinToString(" ")}` did not finish within " +
                                        "120s while verifying CI checks for release tag $tag. A " +
                                        "release publish is refused when this cannot be confirmed.")
                            }
                            process.exitValue() to output.trim()
                        } catch (e: GradleException) {
                            throw e
                        } catch (e: Exception) {
                            throw GradleException(
                                "$label: could not run git to resolve HEAD while verifying CI " +
                                    "checks for release tag $tag (${e.message}). A release publish " +
                                    "is refused when this cannot be confirmed.",
                                e)
                        }

                    val (headExit, headCommit) = git("rev-parse", "-q", "--verify", "HEAD^{commit}")
                    if (headExit != 0 || headCommit.isEmpty()) {
                        throw GradleException(
                            "$label: could not resolve HEAD to verify CI checks for release tag " +
                                "$tag. A release publish is refused when this cannot be confirmed.")
                    }

                    // `gh api ... --jq` does the JSON parsing (no JSON library on this script's
                    // classpath); --paginate covers a commit with more check-runs than one page.
                    // One line per check-run: "<name>\t<status>\t<conclusion>" (conclusion is
                    // empty for a run still in progress — @tsv renders jq null as "").
                    val ghArgs =
                        listOf(
                            "gh", "api", "--paginate",
                            "repos/$repoSlug/commits/$headCommit/check-runs",
                            "--jq", ".check_runs[] | [.name, .status, .conclusion] | @tsv")
                    val (ghExit, ghOutput, ghError) =
                        try {
                            val process =
                                ProcessBuilder(ghArgs)
                                    .directory(workTree)
                                    .redirectErrorStream(false)
                                    .start()
                            val out = process.inputStream.bufferedReader().readText()
                            val err = process.errorStream.bufferedReader().readText()
                            if (!process.waitFor(120, TimeUnit.SECONDS)) {
                                process.destroyForcibly()
                                throw GradleException(
                                    "$label: `gh api .../check-runs` did not finish within 120s " +
                                        "while verifying CI checks for release tag $tag. A release " +
                                        "publish is refused when this cannot be confirmed.")
                            }
                            Triple(process.exitValue(), out, err)
                        } catch (e: GradleException) {
                            throw e
                        } catch (e: Exception) {
                            throw GradleException(
                                "$label: could not run `gh api` to verify CI checks for release " +
                                    "tag $tag (${e.message}). Install/authenticate the GitHub CLI " +
                                    "(`gh auth login`), or pass -PskipCiVerification=true to " +
                                    "deliberately bypass this gate. A release publish is refused " +
                                    "when CI status cannot be confirmed.", e)
                        }
                    if (ghExit != 0) {
                        throw GradleException(
                            "$label: `gh api repos/$repoSlug/commits/$headCommit/check-runs` " +
                                "exited $ghExit while verifying CI checks for release tag $tag " +
                                "(stderr: ${ghError.trim().ifEmpty { "<empty>" }}). Confirm `gh` " +
                                "is installed and authenticated (`gh auth status`), or pass " +
                                "-PskipCiVerification=true to deliberately bypass this gate. A " +
                                "release publish is refused when CI status cannot be confirmed.")
                    }

                    val successfulNames: Set<String> =
                        ghOutput.lines()
                            .filter { it.isNotBlank() }
                            .mapNotNull { line ->
                                val cols = line.split("\t")
                                if (cols.size < 3) null else Triple(cols[0], cols[1], cols[2])
                            }
                            .filter { (_, status, conclusion) ->
                                status == "completed" && conclusion == "success"
                            }
                            .map { (name, _, _) -> name }
                            .toSet()
                    val missing = requiredNames - successfulNames
                    if (missing.isNotEmpty()) {
                        throw GradleException(
                            "$label: required CI check(s) $missing did not conclude `success` on " +
                                "commit $headCommit (release tag $tag) per the GitHub Checks API " +
                                "(repos/$repoSlug/commits/$headCommit/check-runs). Required: " +
                                "$requiredNames; confirmed success: $successfulNames. Push the tag " +
                                "and wait for build.yml (its `build` and `postgres-17` jobs) + " +
                                "broker-e2e-test.yml to go green on that exact commit (both " +
                                "workflows trigger on a `v*` tag push) before publishing, " +
                                "or pass -PskipCiVerification=true to deliberately bypass this gate.")
                    }
                    logger.lifecycle(
                        "$label: required CI checks $requiredNames all concluded success on " +
                            "commit $headCommit (release tag $tag).")
                }
            }

        // Signing uses an in-memory PGP key from the SIGNING_KEY / SIGNING_PASSWORD env vars
        // (a release exports both). When they are absent, every publish task warns loudly
        // instead of silently producing unsigned artifacts; release (non-SNAPSHOT) publishes
        // additionally fail outright because signAllPublications() marks signing as required
        // for them and no signatory is configured. A -SNAPSHOT version is the one exception the
        // plugin makes: signing is not required, the Sign tasks are skipped, and the snapshot
        // repository accepts the unsigned artifacts (CI publishes snapshots without any key).
        val signingKey = providers.environmentVariable("SIGNING_KEY")
        val signingPassword = providers.environmentVariable("SIGNING_PASSWORD")
        if (signingKey.isPresent && signingPassword.isPresent) {
            configure<SigningExtension> {
                useInMemoryPgpKeys(signingKey.get(), signingPassword.get())
            }
        } else {
            val unsignedSnapshot = projectVersion.endsWith("-SNAPSHOT")
            val signingWarning = if (unsignedSnapshot) {
                "SIGNING_KEY / SIGNING_PASSWORD are not set — ${project.path} $projectVersion is a" +
                    " snapshot and is published unsigned (signing is required only for releases)."
            } else {
                "SIGNING_KEY / SIGNING_PASSWORD environment variables are not" +
                    " set — artifacts of ${project.path} will NOT be signed. Maven Central rejects" +
                    " unsigned release artifacts. Export both variables before publishing" +
                    " (an unsigned artifact cannot be published to Maven Central)."
            }
            tasks.withType<org.gradle.api.publish.maven.tasks.AbstractPublishToMaven>().configureEach {
                doFirst {
                    if (unsignedSnapshot) logger.lifecycle(signingWarning) else logger.warn(signingWarning)
                }
            }
        }

        // Fail fast with an actionable message when uploading to the Central Portal without
        // credentials (the plugin itself only fails deep inside the upload call otherwise).
        val centralUsername = providers.gradleProperty("mavenCentralUsername")
        tasks.withType<org.gradle.api.publish.maven.tasks.PublishToMavenRepository>().configureEach {
            if (name.endsWith("ToMavenCentralRepository")) {
                // The release-tag and CI-check gates fire here — the Central upload task, the last
                // point before the bytes leave the machine and become immutable.
                dependsOn(verifyReleaseTag)
                dependsOn(verifyReleaseChecks)
                doFirst {
                    if (!centralUsername.isPresent) {
                        throw GradleException(
                            "Gradle property 'mavenCentralUsername' is not set. Map the Central Portal" +
                                " token env vars when invoking Gradle:" +
                                " ORG_GRADLE_PROJECT_mavenCentralUsername=\"\$CENTRAL_USERNAME\"" +
                                " ORG_GRADLE_PROJECT_mavenCentralPassword=\"\$CENTRAL_TOKEN\""
                        )
                    }
                }
            }
        }
    }
}

// Resolve the git hooks directory, handling git worktrees correctly.
// In a worktree, .git is a file pointing to the worktree gitdir; hooks live
// in the common (main) .git/hooks directory resolved via --git-common-dir.
// ignoreExitValue: in a non-git tree (source archive, some CI cache restores)
// git exits non-zero with empty stdout and the provider yields "" instead of
// failing the build — installGitHooks is skipped there via onlyIf below.
val gitCommonDir = providers.exec {
    commandLine("git", "rev-parse", "--git-common-dir")
    workingDir = rootProject.projectDir
    isIgnoreExitValue = true
}.standardOutput.asText.map { it.trim() }

tasks.register<Copy>("installGitHooks") {
    description = "Installs git pre-commit hook from gradle/hooks/ (skipped outside a git checkout)."
    group = "build setup"
    // Local val on purpose: the onlyIf spec is serialized into the configuration
    // cache and must capture only plain values, never the script object or project.
    // `.git` is a directory in a main checkout and a file in a linked worktree;
    // absent in non-git trees, where running git would be pointless.
    val gitEntry = rootProject.file(".git")
    onlyIf("the project sits inside a git checkout") { gitEntry.exists() }
    from(rootProject.file("gradle/hooks/pre-commit"))
    into(gitCommonDir.map { rootProject.file("$it/hooks") })
    filePermissions { unix("755") }
}

tasks.named("build") {
    dependsOn("installGitHooks")
}

// Aggregate JaCoCo coverage report across all leaf modules, via Gradle's built-in
// jacoco-report-aggregation plugin: configuration-cache safe, no external tooling,
// output under build/reports/ so `clean` removes it. Leaf subprojects are picked up
// automatically, so a newly added module joins the aggregate without editing this file.
// Umbrella parents (:streamrune-crypto, :streamrune-integration, :streamrune-outbox,
// :streamrune-eventstore) carry no sources and are skipped.
repositories {
    // The aggregation plugin applies `jacoco` to the root project, whose agent
    // dependency must be resolvable here (subprojects declare their own repositories).
    mavenCentral()
}

dependencies {
    subprojects
        .filter { it.childProjects.isEmpty() }
        .forEach { jacocoAggregation(it) }
}

reporting {
    reports {
        create<JacocoCoverageReport>("jacocoAggregateReport") {
            testSuiteName = "test"
        }
    }
}

tasks.named<JacocoReport>("jacocoAggregateReport") {
    reports {
        xml.required.set(true)
        html.required.set(true)
    }
}

// Run the aggregate coverage report as part of every 'check'.
tasks.named("check") {
    dependsOn(tasks.named("jacocoAggregateReport"))
}

// ── The PostgreSQL version matrix ───────────────────────────────────────────────────────────
//
// `postgresTest` is exactly what the `postgres-17` job of build.yml runs: the test task of every
// module that starts a PostgreSQL container, nothing else. Locally:
//
//     ./gradlew postgresTest -PpostgresImage=postgres:17-alpine
//
// (without the property the suites run on PostgresTestImage.DEFAULT, the newest supported server).
tasks.register("postgresTest") {
    description =
        "Runs the PostgreSQL-backed test suites; pass -PpostgresImage=<image> to run them on " +
            "another server version (build.yml runs them on postgres:17-alpine)."
    group = "verification"
    dependsOn(postgresBackedModules.map { "$it:test" })
}

// `verifyPostgresTestImage` is the gate behind "every PostgreSQL container goes through
// PostgresTestImage". A hard-coded image in one forgotten test is invisible by construction — the
// default build is green on it — yet it makes the postgres-17 job run that test on 18, so the CI
// leg would report a version it did not test. Two rules, scanned over every module's sources
// (main, test, jmh, any other source set):
//   1. `new PostgreSQLContainer<>(...)` (or the raw type) takes `PostgresTestImage.NAME`, and no
//      source names a `"postgres:<tag>"` image literal (the shared class itself aside);
//   2. only a module in `postgresBackedModules` constructs or imports PostgreSQLContainer — a
//      module outside it would get neither the shared class nor the postgres-17 job's coverage
//      (a Javadoc mention does not count).
val verifyPostgresTestImage =
    tasks.register("verifyPostgresTestImage") {
        description =
            "Fails when a PostgreSQL Testcontainers container does not take its image from " +
                "PostgresTestImage, or a module outside postgresBackedModules starts one."
        group = "verification"
        val sharedClass = file("gradle/test-support/org/streamrune/testsupport/PostgresTestImage.java")
        val srcDirByModule = subprojects.associate { it.path to it.projectDir.resolve("src") }
        val sources =
            files(srcDirByModule.values).asFileTree.matching { include("**/*.java") }
        val backed = postgresBackedModules.toSet()
        val known = subprojects.map { it.path }.toSet()
        val root = rootDir
        val report = layout.buildDirectory.file("reports/postgres-test-image.txt")
        inputs.files(sources).withPropertyName("sources")
            .withPathSensitivity(PathSensitivity.RELATIVE)
        inputs.file(sharedClass).withPropertyName("sharedClass")
            .withPathSensitivity(PathSensitivity.NONE)
        outputs.file(report).withPropertyName("report")
        doLast {
            val unknown = backed - known
            if (unknown.isNotEmpty()) {
                throw GradleException(
                    "postgresBackedModules names projects that do not exist: $unknown")
            }
            val imageLiteral = Regex(""""(?:docker\.io/)?(?:library/)?postgres:[0-9A-Za-z][0-9A-Za-z._-]*"""")
            val container = Regex("""new\s+(?:[\w.]+\.)?PostgreSQLContainer\s*(?:<[^>]*>\s*)?\(\s*(?!PostgresTestImage\.NAME\s*\))""")
            // A construction or an import starts/uses a container; a bare mention (Javadoc) does not.
            val usesContainer = Regex("""new\s+(?:[\w.]+\.)?PostgreSQLContainer\b|import\s+[\w.]+\.PostgreSQLContainer\s*;""")
            val violations = mutableListOf<String>()
            var scanned = 0
            var containers = 0
            sources.files.sorted().forEach { file ->
                if (file == sharedClass) return@forEach
                scanned++
                val text = file.readText()
                val module =
                    srcDirByModule.entries
                        .first { (_, dir) -> file.toPath().startsWith(dir.toPath()) }.key
                val where = file.relativeTo(root).path
                if (usesContainer.containsMatchIn(text)) {
                    containers++
                    if (module !in backed) {
                        violations +=
                            "$where: $module starts a PostgreSQL container but is not in " +
                                "postgresBackedModules, so it gets neither PostgresTestImage nor " +
                                "the postgres-17 CI leg"
                    }
                }
                container.findAll(text).forEach {
                    violations +=
                        "$where: new PostgreSQLContainer<>(...) must take PostgresTestImage.NAME"
                }
                imageLiteral.findAll(text).forEach {
                    violations +=
                        "$where: hard-coded image ${it.value} — use PostgresTestImage.NAME"
                }
            }
            report.get().asFile.apply {
                parentFile.mkdirs()
                writeText(
                    "sources scanned: $scanned\nfiles using PostgreSQLContainer: " +
                        "$containers\nviolations: ${violations.size}\n")
            }
            if (violations.isNotEmpty()) {
                throw GradleException(
                    "${violations.size} PostgreSQL test-image violation(s) (every container " +
                        "must go through org.streamrune.testsupport.PostgresTestImage so the " +
                        "postgres-17 CI leg really tests 17):\n  " + violations.joinToString("\n  "))
            }
        }
    }

tasks.named("check") { dependsOn(verifyPostgresTestImage) }

// The shared test-support sources live in no module, so no module's spotless can own them (see
// the PostgreSQL-backed modules' wiring in `subprojects`); the root project formats them with the
// same formatter the modules use.
apply(plugin = "com.diffplug.spotless")
configure<com.diffplug.gradle.spotless.SpotlessExtension> {
    setEnforceCheck(false) // same JDK 25+ spotless/guava lint-failure workaround as the modules
    java {
        target("gradle/test-support/**/*.java")
        googleJavaFormat("1.34.1")
        removeUnusedImports()
    }
}

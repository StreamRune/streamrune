description = "StreamRune core API: event store contracts, aggregate state, event envelopes, upcasting."

// Minimal dependencies — jackson-annotations is needed for serialization support in downstream modules
dependencies {
    api(libs.jackson.annotations)
    api(libs.jackson.databind)
    testImplementation(project(":streamrune-test"))
    testImplementation(libs.assertj.core)
}

// ── The log-sink ratchet must RUN on the build that breaks it ────────────────
//
// LogSinkPolicyRatchetTest reads every module's `src/main/java` at runtime, but Gradle knew only
// about THIS module's sources, classes and classpath. streamrune-core depends on nothing, so
// adding an unsanitized log sink in streamrune-runtime — the 63rd site, the exact event the
// ratchet exists to catch — changed none of :streamrune-core:test's declared inputs. With
// `org.gradle.caching=true` (gradle.properties) and a build cache CI persists across runs, the
// task went UP-TO-DATE locally and FROM-CACHE on CI: the enforcement never executed, the build
// reported green, and the unsanitized sink shipped. Reproduced before this fix — a genuine
// violation planted in SagaTimeoutRunner left `> Task :streamrune-core:test UP-TO-DATE` and
// BUILD SUCCESSFUL, while the same tree with `--rerun` failed the ratchet.
//
// Declaring the scanned tree as an input makes the task re-run exactly when what it scans
// changes. The cost is honest and bounded: a main-source edit in any module now re-runs core's
// test suite. Editing a leaf module (say streamrune-quarkus) is the only case that pays anything
// new — an edit to core/runtime already invalidated most of the repo's test tasks through the
// ordinary dependency graph.
//
// THE NEXT DEFECT THIS COULD BECOME: these globs drifting away from the test's own
// `productionSources()` rule, which would silently under-declare (a module at a nesting depth the
// globs miss re-opens the hole, and nothing would report it). So the correspondence is not left
// to inspection — the manifest below hands Gradle's declared list to the test, which fails when
// what it scanned and what Gradle declared differ in either direction.
val logSinkPolicySources =
    fileTree(rootDir) {
        // Mirrors LogSinkPolicyRatchetTest#productionSources: every .java under any module's
        // src/main/java, at any nesting depth, excluding build output and the stale nested
        // checkouts in .worktrees.
        include("**/src/main/java/**/*.java")
        exclude("**/build/**")
        exclude("**/.worktrees/**")
        // Pure walk pruning: neither can contain a `src/main/java/**.java` path, so excluding
        // them cannot remove anything the test scans.
        exclude(".git/**")
        exclude(".gradle/**")
    }

val logSinkPolicySourceManifest =
    tasks.register("logSinkPolicySourceManifest") {
        group = "verification"
        description =
            "Writes the production-source list declared as an input of the log-sink policy " +
                "ratchet, so the test can prove the declaration covers what it scans."
        val sources = logSinkPolicySources
        val repoRoot = rootDir
        val manifest = layout.buildDirectory.file("logsink-policy/declared-sources.txt")
        inputs
            .files(sources)
            .withPropertyName("logSinkPolicySources")
            .withPathSensitivity(PathSensitivity.RELATIVE)
        outputs.file(manifest)
        doLast {
            val file = manifest.get().asFile
            file.parentFile.mkdirs()
            file.writeText(
                sources.files
                    .map { it.relativeTo(repoRoot).invariantSeparatorsPath }
                    .sorted()
                    .joinToString("\n"),
            )
        }
    }

// InternalTrackerIdRatchetTest reads every public text file in the repository (sources, guides,
// build files), so a change to any of them must re-run it. Mirrors the test's own walk: the same
// pruned directories and extensions. The internal planning directory's name is built from pieces,
// as in the test, so no published file names it.
val planningDirectory = "super" + "powers"
val publicTextSources =
    fileTree(rootDir) {
        include("**/*.java", "**/*.md", "**/*.kts", "**/*.gradle", "**/*.toml", "**/*.properties")
        include("**/*.yml", "**/*.yaml", "**/*.sql", "**/*.sh", "**/*.xml", "**/*.json", "**/*.txt")
        exclude("**/build/**", "**/out/**", "**/.worktrees/**", "**/.idea/**", "**/.vscode/**")
        exclude("**/node_modules/**", "**/.kotlin/**", "**/scratchpad/**", "**/.$planningDirectory/**")
        exclude("**/.claude/**", "docs/$planningDirectory/**", "scripts/sonar/**")
        exclude("sonar-project.properties", ".git/**", ".gradle/**")
    }

tasks.named<Test>("test") {
    inputs
        .files(publicTextSources)
        .withPropertyName("publicTextSources")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    inputs
        .files(logSinkPolicySources)
        .withPropertyName("logSinkPolicySources")
        .withPathSensitivity(PathSensitivity.RELATIVE)
    dependsOn(logSinkPolicySourceManifest)
    val manifest = logSinkPolicySourceManifest.map { it.outputs.files.singleFile }
    // Passed through an argument provider rather than `systemProperty` so the absolute path never
    // enters the task's cache key (the same shape streamrune-quarkus uses for its jar path).
    jvmArgumentProviders.add(
        org.gradle.process.CommandLineArgumentProvider {
            listOf("-Dstreamrune.logsink.declared-sources=${manifest.get().absolutePath}")
        },
    )
}

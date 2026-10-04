description = "StreamRune integration API: framework-agnostic integration contracts."

dependencies {
    api(project(":streamrune-core"))
    implementation(libs.micrometer.core)
    implementation(libs.slf4j.api)

    // The meter-shape guard needs a REAL PrometheusMeterRegistry, not SimpleMeterRegistry.
    // Simple tolerates two meters that share a name but differ in tag keys (it dedups by the full
    // id); Prometheus rejects the second registration outright, because the Prometheus data model
    // requires every series of a metric family to carry the same label keys. Every test in this
    // module used SimpleMeterRegistry, so a same-name/different-tag-keys collision was invisible to
    // the suite while flatlining the series on the registry most adopters actually run. Test-only —
    // it never reaches the published POM. Version pinned to the catalog's micrometer version so the
    // registry can never drift from micrometer-core.
    testImplementation("io.micrometer:micrometer-registry-prometheus:${libs.versions.micrometer.get()}")
    // Fluent exception-message assertions (ProjectionProcessorResolutionTest). Test-only.
    testImplementation(libs.assertj.core)
}

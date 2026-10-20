description =
    "StreamRune integration TCK: what the Spring, Quarkus and Micronaut integrations' tests share " +
        "to hold the three to one contract. Not published."

// Test infrastructure for the integrations' own test source sets (each declares it as
// testImplementation); nothing here ships. The configuration oracle reads the meter names of
// streamrune-core and the span attribute names of streamrune-runtime, and the OpenTelemetry API
// those names are declared with comes from the integration that runs it.
dependencies {
    api(project(":streamrune-core"))
    api(project(":streamrune-runtime"))
    api(project(":streamrune-integration:streamrune-integration-api"))
    api(project(":streamrune-test"))
    api(platform(libs.junit.bom))
    api("org.junit.jupiter:junit-jupiter-api")
    compileOnly(libs.opentelemetry.api)
    testImplementation(libs.opentelemetry.api)
}

description = "StreamRune Outbox adapter for Apache Kafka."

dependencies {
    api(project(":streamrune-core"))
    implementation(libs.kafka.clients)
    // Direct dependency for the builder WARN — kafka-clients
    // already carries slf4j-api transitively, but the publisher's own logging must not lean on a
    // transitive.
    implementation(libs.slf4j.api)
    testImplementation(libs.mockito.core)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.junit)
}

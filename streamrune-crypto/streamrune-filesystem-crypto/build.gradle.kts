plugins {
    alias(libs.plugins.jmh)
}

description = "StreamRune crypto: filesystem-backed per-subject key store for encryption at rest."

dependencies {
    api(project(":streamrune-core"))
    implementation(libs.slf4j.api)

    // src/jmh: CryptoShreddingBenchmark (src/jmh/README.md) serializes events through the
    // ObjectMapper PostgresEventStore builds, so the benchmark measures the event store's own
    // serialization path; no database is involved. It is declared in org.streamrune.postgres to
    // reach the package-private PostgresEventStore.createObjectMapper, which works because the JMH
    // source set runs on the class path (no module-info here or in streamrune-postgres).
    jmhImplementation(project(":streamrune-eventstore:streamrune-postgres"))
    jmhImplementation(libs.jackson.databind)
}

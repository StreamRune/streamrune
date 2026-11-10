plugins {
    alias(libs.plugins.jmh)
}

description = "StreamRune crypto: filesystem-backed per-subject key store for encryption at rest."

dependencies {
    api(project(":streamrune-core"))
    implementation(libs.slf4j.api)

    // src/jmh: CryptoShreddingBenchmark (src/jmh/README.md) serializes events through the
    // ObjectMapper PostgresEventStore builds, so the benchmark measures the event store's own
    // serialization path; no database is involved.
    jmhImplementation(project(":streamrune-eventstore:streamrune-postgres"))
    jmhImplementation(libs.jackson.databind)
}

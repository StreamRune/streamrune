plugins {
    alias(libs.plugins.jmh)
}

description = "StreamRune crypto: filesystem-backed per-subject key store for encryption at rest."

dependencies {
    api(project(":streamrune-core"))
    // The shared cipher seam and v1 wire format live there; they appear in no public signature of this module.
    implementation(project(":streamrune-crypto:streamrune-crypto-api"))
    implementation(libs.slf4j.api)
    testImplementation(project(":streamrune-test"))
    // The characterization test pins the engine's WARN lines and their logger category (a
    // logback ListAppender); test scope only. Quiet defaults in src/test/resources/logback-test.xml.
    testImplementation(libs.logback.classic)

    // src/jmh: CryptoShreddingBenchmark (src/jmh/README.md) serializes events through the
    // ObjectMapper PostgresEventStore builds (StoreObjectMappers.eventsAndSagas, reached through
    // streamrune-postgres's api dependency on streamrune-runtime), so the benchmark measures the
    // event store's own serialization path; no database is involved. It is declared in
    // org.streamrune.postgres, which works because the JMH source set runs on the class path (no
    // module-info here or in streamrune-postgres).
    jmhImplementation(project(":streamrune-postgres"))
    jmhImplementation(libs.jackson.databind)
}

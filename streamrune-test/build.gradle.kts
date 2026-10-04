description = "StreamRune test support: in-memory event store, decider fixture, test harness."

dependencies {
    api(project(":streamrune-core"))
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
    api(platform(libs.junit.bom))
    api("org.junit.jupiter:junit-jupiter-api")
    api(libs.jackson.databind)
    api(libs.jackson.core)
    api(libs.jackson.datatype.jsr310)
    // InMemoryEventStore logs through slf4j; `api` for the same JPMS reason as
    // the rest of this block (module-info requires org.slf4j).
    api(libs.slf4j.api)
}

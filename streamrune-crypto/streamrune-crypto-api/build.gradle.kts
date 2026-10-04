description = "StreamRune crypto: encryption-at-rest primitives for event payloads."

dependencies {
    api(project(":streamrune-core"))
    testImplementation(project(":streamrune-test"))
    // Every non-`java.*` name in this module's module-info.java `requires` list must
    // be `api` (POM `compile` scope), because a consumer's `requires org.streamrune.crypto` makes
    // javac resolve this module's WHOLE requires graph on the COMPILE module path — a plain
    // (non-transitive) `requires` is just as compile-visible to the resolver as a transitive one.
    // At `implementation` (POM `runtime` scope) a modular consumer aborts with "module not found".
    // jackson-core was not declared at all and arrived only transitively through jackson-databind;
    // module-info requires it by name, so it must be an explicit, versioned declaration.
    // Enforced by the `verifyModularApiScopes` check in the root build.
    api(libs.jackson.databind)
    api(libs.jackson.core)
    api(libs.caffeine)
    api(libs.slf4j.api)
    testImplementation(libs.mockito.core)
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

// Byte Buddy (used by Mockito) does not yet officially support Java 25.
// Enable experimental support until an updated Byte Buddy version is released.
tasks.withType<Test> {
    jvmArgs("-Dnet.bytebuddy.experimental=true")
}

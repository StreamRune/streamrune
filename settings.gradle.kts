plugins {
    // Auto-provisions the Java 25 toolchain on machines/CI without a local JDK 25.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "streamrune"

include(
    "streamrune-core",
    "streamrune-eventstore:streamrune-postgres",
    "streamrune-runtime",
    "streamrune-crypto",
    "streamrune-crypto:streamrune-crypto-api",
    "streamrune-crypto:streamrune-filesystem-crypto",
    "streamrune-crypto:streamrune-postgres-crypto",
    "streamrune-crypto:streamrune-vault-crypto",
    "streamrune-crypto:streamrune-aws-kms-crypto",
    "streamrune-test",
    "streamrune-integration",
    "streamrune-integration:streamrune-integration-api",
    "streamrune-integration:streamrune-spring",
    "streamrune-integration:streamrune-quarkus",
    "streamrune-integration:streamrune-micronaut",
    "streamrune-integration:streamrune-spring-boot-starter",
    "streamrune-integration:streamrune-e2e",
    "streamrune-outbox:streamrune-kafka-outbox",
    "streamrune-outbox:streamrune-rabbitmq-outbox"
)

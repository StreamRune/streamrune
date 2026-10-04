description = "StreamRune Outbox adapter for RabbitMQ."

dependencies {
    api(project(":streamrune-core"))
    implementation(libs.rabbitmq.client)
    testImplementation(libs.mockito.core)
    testImplementation(libs.testcontainers.rabbitmq)
    testImplementation(libs.testcontainers.junit)
}

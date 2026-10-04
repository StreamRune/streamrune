description = "StreamRune Spring Boot Starter — one-dependency setup for Spring Boot applications."

dependencies {
    api(project(":streamrune-integration:streamrune-spring"))
    api(project(":streamrune-eventstore:streamrune-postgres"))
    api(project(":streamrune-runtime"))
}

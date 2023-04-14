plugins {
    kotlin("jvm") version "1.8.0"
    id("com.github.johnrengelman.shadow") version "7.1.0"
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib:1.8.0")
    implementation("org.jetbrains.kotlinx.spark:kotlin-spark-api_3.3.0_2.12:1.2.3")
    implementation("org.jetbrains.kotlinx:kotlinx-cli:0.3.5")
    compileOnly("org.apache.spark:spark-sql_2.12:3.3.0")
}

tasks.test {
    useJUnitPlatform()
}

kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}

tasks.jar {
    isZip64 = true
    manifest.attributes["Main-Class"] = "WordCount"
    val dependencies = configurations
        .runtimeClasspath
        .get()
        .map(::zipTree)
    from(dependencies)
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    exclude("META-INF/*")
    exclude("org/apache/spark/*")
    exclude("org/scala-lang/*")
}
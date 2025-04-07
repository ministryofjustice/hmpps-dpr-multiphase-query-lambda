plugins {
  kotlin("jvm") version "2.0.21"
  id("jacoco")
  id("com.github.johnrengelman.shadow") version "8.1.1"
  id("org.barfuin.gradle.jacocolog") version "3.1.0"
  id("org.owasp.dependencycheck")  version "8.2.1"
}

configurations {
  testImplementation { exclude(group = "org.junit.vintage") }
}

dependencies {
  implementation("com.amazonaws:aws-lambda-java-core:1.2.3")
  implementation("software.amazon.awssdk:redshiftdata:2.29.20")
  implementation("software.amazon.awssdk:athena:2.31.3")

  //test
  testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:5.10.1")
}
java {
  toolchain.languageVersion.set(JavaLanguageVersion.of(21))
}

kotlin {
  jvmToolchain(21)
}

tasks {
  withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions.jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21
  }
}
repositories {
  mavenLocal()
  mavenCentral()
}

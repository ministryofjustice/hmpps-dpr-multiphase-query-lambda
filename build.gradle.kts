import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

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
  testImplementation("org.junit.jupiter:junit-jupiter-api:5.10.1")
  testImplementation("org.junit.jupiter:junit-jupiter-params:5.10.1")
  testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
  testImplementation("org.mockito:mockito-core:5.18.0")
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

tasks.test {
  finalizedBy(tasks.jacocoTestReport)
}

tasks.jacocoTestReport {
  dependsOn(tasks.test)
}

java.sourceCompatibility = JavaVersion.VERSION_21

tasks.jar {
  enabled = true
}

tasks.assemble {
  dependsOn(tasks.shadowJar)
}

java {
  withSourcesJar()
  withJavadocJar()
}

tasks {
  withType<Test> {
    useJUnitPlatform()
  }
  withType<ShadowJar> {
    // <WORKAROUND for="https://github.com/johnrengelman/shadow/issues/448">
    configurations = listOf(
      project.configurations.implementation.get(),
      project.configurations.runtimeOnly.get()
    ).onEach { it.isCanBeResolved = true }
  }
}
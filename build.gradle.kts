import net.ltgt.gradle.errorprone.errorprone

plugins {
    java
    jacoco
    id("com.diffplug.spotless") version "8.9.0"
    id("net.ltgt.errorprone") version "5.1.1"
}

group = "io.github.laiwf94"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencyLocking {
    lockAllConfigurations()
}

dependencies {
    errorprone("com.google.errorprone:error_prone_core:2.50.0")
    constraints {
        // Error Prone pulls in Guava 33.5.0-jre, which has GHSA-xxph-c9ww-hj94 (#76).
        errorprone("com.google.guava:guava:33.7.2-jre") {
            because("GHSA-xxph-c9ww-hj94 is fixed in 33.7.2")
        }
    }

    testImplementation(platform("org.junit:junit-bom:5.14.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.jar {
    manifest {
        attributes("Main-Class" to "io.github.laiwf94.keel.Main")
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-parameters", "-Xlint:all,-processing", "-Werror"))
    options.errorprone {
        disableWarningsInGeneratedCode = true
        allErrorsAsWarnings = false
    }
}

tasks.test {
    useJUnitPlatform()
    maxHeapSize = "1g"
    // Simulation seeds: -Dkeel.sim.seeds=N runs N seeds per simulation test, -Dkeel.sim.seed=S
    // replays one, -Dkeel.sim.faults and -Dkeel.sim.steps replay it with only those faults and
    // client steps, and -Dkeel.sim.shrink=true shrinks it (at most -Dkeel.sim.shrink.budget
    // replays).
    systemProperty("keel.sim.seeds", providers.systemProperty("keel.sim.seeds").getOrElse("1000"))
    listOf("keel.sim.seed", "keel.sim.faults", "keel.sim.steps", "keel.sim.shrink", "keel.sim.shrink.budget")
        .forEach { name -> providers.systemProperty(name).orNull?.let { systemProperty(name, it) } }
    testLogging {
        events("failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
    finalizedBy(tasks.jacocoTestReport)
}

// Times the linearizability checker against the search it replaced; see LinearizabilityBenchmark.
tasks.register<JavaExec>("linearizabilityBenchmark") {
    description = "Times the linearizability checker on long histories."
    classpath = sourceSets.test.get().runtimeClasspath
    mainClass = "io.github.laiwf94.keel.sim.LinearizabilityBenchmark"
    // Each checker runs in a JVM of its own with these options; a heap of its full size from the
    // start keeps G1 from resizing it during the timed runs.
    minHeapSize = "1g"
    maxHeapSize = "1g"
}

jacoco {
    toolVersion = "0.8.15"
}

tasks.jacocoTestReport {
    dependsOn(tasks.test)
    reports {
        xml.required = true
        html.required = true
    }
}

tasks.jacocoTestCoverageVerification {
    dependsOn(tasks.test)
    violationRules {
        rule {
            limit {
                counter = "LINE"
                minimum = "0.90".toBigDecimal()
            }
        }
    }
}

tasks.check {
    dependsOn(tasks.jacocoTestCoverageVerification)
}

spotless {
    java {
        googleJavaFormat("1.36.1")
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
    kotlinGradle {
        target("*.gradle.kts")
        trimTrailingWhitespace()
        endWithNewline()
    }
}
